"""Durable, quota-aware fixtures/results polling and immutable publication.

Only the explicit worker command calls providers. Status describes source checks,
not guaranteed immediate scores, complete coverage, or in-game telemetry.
"""

import argparse
import hashlib
import json
import os
import re
import sqlite3
import time
from contextlib import closing, contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit

import httpx
from dotenv import load_dotenv
from pydantic import ValidationError

from .connectors import ProviderError, collect, commit_state
from .engine import Pipeline
from .schema import digest
from .snapshot import export_snapshot

STATUS_VERSION = "arena-live-status-1.0"
PROVIDERS = {
    "football-data": dict(token="FOOTBALL_DATA_TOKEN", host="api.football-data.org", limit=10, window=60),
    "pandascore": dict(token="PANDASCORE_TOKEN", host="api.pandascore.co", limit=1000, window=3600),
}
ERROR_CODES = frozenset({
    "awaiting_credentials", "quota_exhausted", "authentication_denied", "provider_rate_limited",
    "provider_unavailable", "provider_http_error", "network_error", "network_timeout",
    "invalid_provider_payload", "ingest_failed", "publication_failed", "cursor_commit_failed",
    "worker_interrupted", "clock_regression", "invalid_endpoint", "worker_error",
})


def _utc(value):
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError("The live clock must return an aware datetime")
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _now():
    return datetime.now(timezone.utc)


def _atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    with temporary.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(value, indent=2, allow_nan=False, ensure_ascii=False) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(path)


class LiveFailure(RuntimeError):
    def __init__(self, code):
        if code not in ERROR_CODES:
            raise ValueError("Unknown live error code")
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class LiveConfig:
    provider: str
    competition: str = "PL"
    pages: int = 1
    interval: int = 60
    request_limit: int | None = None
    competitions: tuple[str, ...] | None = None

    def __post_init__(self):
        if self.provider not in PROVIDERS:
            raise ValueError("Unsupported live provider")
        if (type(self.pages) is not int or not 1 <= self.pages <= 10
                or type(self.interval) is not int or not 30 <= self.interval <= 86400):
            raise ValueError("Use 1..10 pages and an interval between 30 and 86400 seconds")
        if self.provider == "football-data" and self.pages != 1:
            raise ValueError("Football competition requests do not use pagination; pages must be one")
        if (not isinstance(self.competition, str) or not self.competition.isascii()
                or not self.competition.isalnum() or not 1 <= len(self.competition) <= 12):
            raise ValueError("Competition must be an ASCII alphanumeric code of 1..12 characters")
        if self.request_limit is not None and (type(self.request_limit) is not int
                or not 1 <= self.request_limit <= PROVIDERS[self.provider]["limit"]):
            raise ValueError("A custom request limit may only lower the provider's free-plan budget")
        if self.competitions is not None:
            if (self.provider != "football-data" or not isinstance(self.competitions, tuple)
                    or not 1 <= len(self.competitions) <= 10 or len(set(self.competitions)) != len(self.competitions)
                    or any(not isinstance(code, str) or not code.isascii() or not code.isalnum()
                           or not 1 <= len(code) <= 12 for code in self.competitions)):
                raise ValueError("competitions must be a unique tuple of 1..10 football competition codes")
            per_poll = len(self.competitions)
            # Reject an intrinsically unsafe steady-state cadence before any
            # writes. Retries still use the durable quota and may defer a poll.
            if per_poll * ((60 + self.interval - 1) // self.interval) > PROVIDERS[self.provider]["limit"]:
                raise ValueError("Selected competition cadence exceeds the conservative free request budget")

    @property
    def source_scope(self):
        return dict(kind="competition_list" if self.provider == "football-data" else "match_phases",
                    competition_codes=list(self.competitions or (self.competition,)) if self.provider == "football-data" else [],
                    phases=[] if self.provider == "football-data" else ["past", "upcoming", "running"], pages=self.pages)


@contextmanager
def _writer_lock(path):
    """Process-held local OS lock; automatically released on process death."""
    with Path(path).open("a+b") as handle:
        if Path(path).stat().st_size == 0:
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            raise LiveFailure("worker_error") from None
        try:
            yield
        finally:
            if os.name == "nt":
                import msvcrt
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


class LiveLedger:
    """Request reservations survive failed polls, retries and process restarts."""
    def __init__(self, path, config):
        self.path, self.config = Path(path), config
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self.connect()) as db, db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS configuration(key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS polls(id INTEGER PRIMARY KEY, started_at TEXT NOT NULL,
                  finished_at TEXT, status TEXT NOT NULL, error_code TEXT,
                  source_rows INTEGER NOT NULL DEFAULT 0, changed_rows INTEGER NOT NULL DEFAULT 0,
                  unsupported_rows INTEGER NOT NULL DEFAULT 0, snapshot TEXT, snapshot_changed INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS requests(id INTEGER PRIMARY KEY, poll_id INTEGER NOT NULL,
                  attempted_at TEXT NOT NULL, status_code INTEGER, error_code TEXT,
                  FOREIGN KEY(poll_id) REFERENCES polls(id));
                CREATE INDEX IF NOT EXISTS request_window ON requests(attempted_at);
            """)
            expected = json.dumps(dict(provider=config.provider, source_scope=config.source_scope),
                                  sort_keys=True)
            previous = db.execute("SELECT value FROM configuration WHERE key='source_scope'").fetchone()
            if previous and previous[0] != expected:
                raise ValueError("Live workspace source scope changed; use a separate base workspace")
            db.execute("INSERT OR IGNORE INTO configuration VALUES ('source_scope',?)", (expected,))

    def connect(self):
        db = sqlite3.connect(self.path, timeout=30)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        return db

    def begin(self, now):
        with closing(self.connect()) as db, db:
            db.execute("UPDATE polls SET status='interrupted',finished_at=?,error_code='worker_interrupted' "
                       "WHERE status='polling'", (now,))
            return db.execute("INSERT INTO polls(started_at,status) VALUES (?,'polling')", (now,)).lastrowid

    def interrupt(self, now):
        with closing(self.connect()) as db, db:
            db.execute("UPDATE polls SET status='interrupted',finished_at=?,error_code='worker_interrupted' "
                       "WHERE status='polling'", (now,))

    def finish(self, poll_id, now, status, error=None, *, source_rows=0, changed_rows=0, unsupported_rows=0,
               snapshot=None, snapshot_changed=False):
        with closing(self.connect()) as db, db:
            db.execute("UPDATE polls SET finished_at=?,status=?,error_code=?,source_rows=?,changed_rows=?,"
                       "unsupported_rows=?,snapshot=?,snapshot_changed=? WHERE id=?",
                       (now, status, error, source_rows, changed_rows, unsupported_rows, snapshot,
                        int(snapshot_changed), poll_id))

    def budget(self, now):
        config = PROVIDERS[self.config.provider]
        limit = self.config.request_limit or config["limit"]
        cutoff = _utc(datetime.fromisoformat(now.replace("Z", "+00:00")) - timedelta(seconds=config["window"]))
        with closing(self.connect()) as db:
            used = db.execute("SELECT COUNT(*) FROM requests WHERE attempted_at > ?", (cutoff,)).fetchone()[0]
        return dict(limit=limit, window_seconds=config["window"], used=used, remaining=max(0, limit - used))

    def reserve(self, poll_id, now):
        config = PROVIDERS[self.config.provider]
        limit = self.config.request_limit or config["limit"]
        cutoff = _utc(datetime.fromisoformat(now.replace("Z", "+00:00")) - timedelta(seconds=config["window"]))
        with closing(self.connect()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            last = db.execute("SELECT MAX(attempted_at) FROM requests").fetchone()[0]
            if last and now < last:
                raise LiveFailure("clock_regression")
            used = db.execute("SELECT COUNT(*) FROM requests WHERE attempted_at > ?", (cutoff,)).fetchone()[0]
            if used >= limit:
                raise LiveFailure("quota_exhausted")
            return db.execute("INSERT INTO requests(poll_id,attempted_at) VALUES (?,?)", (poll_id, now)).lastrowid

    def request_result(self, request_id, status_code=None, error=None):
        with closing(self.connect()) as db, db:
            db.execute("UPDATE requests SET status_code=?,error_code=? WHERE id=?", (status_code, error, request_id))

    def summary(self):
        with closing(self.connect()) as db:
            counts = dict(db.execute("SELECT status,COUNT(*) FROM polls GROUP BY status"))
            last = db.execute("SELECT * FROM polls ORDER BY id DESC LIMIT 1").fetchone()
            success = db.execute("SELECT * FROM polls WHERE status='succeeded' ORDER BY id DESC LIMIT 1").fetchone()
            change = db.execute("SELECT finished_at FROM polls WHERE status='succeeded' AND snapshot_changed=1 "
                                "ORDER BY id DESC LIMIT 1").fetchone()
            requests = db.execute("SELECT COUNT(*) FROM requests WHERE poll_id=?", (last["id"],)).fetchone()[0] if last else 0
        return dict(poll_counts=dict(attempted=sum(counts.values()), succeeded=counts.get("succeeded", 0),
                failed=counts.get("failed", 0), awaiting_credentials=counts.get("awaiting_credentials", 0),
                quota_limited=counts.get("quota_limited", 0), interrupted=counts.get("interrupted", 0)),
            last=dict(last) if last else None, success=dict(success) if success else None,
            last_change_at=change[0] if change else None, requests=requests)


class BudgetClient:
    """Wrap every GET so connector retries consume the same durable quota."""
    def __init__(self, client, ledger, poll_id, clock):
        self.client, self.ledger, self.poll_id, self.clock = client, ledger, poll_id, clock
        self.last_error_code = None

    def get(self, url, **kwargs):
        parts = urlsplit(url)
        if (parts.scheme != "https" or parts.hostname != PROVIDERS[self.ledger.config.provider]["host"]
                or parts.username or parts.password or parts.query or parts.fragment):
            raise LiveFailure("invalid_endpoint")
        request_id = self.ledger.reserve(self.poll_id, _utc(self.clock()))
        self.last_error_code = None
        try:
            response = self.client.get(url, follow_redirects=False, **kwargs)
        except httpx.TransportError as error:
            self.last_error_code = "network_timeout" if isinstance(error, httpx.TimeoutException) else "network_error"
            self.ledger.request_result(request_id, error=self.last_error_code)
            # RemoteProtocolError is transient but is outside the existing
            # connector's NetworkError hierarchy. Convert without exception text.
            if isinstance(error, httpx.RemoteProtocolError):
                raise httpx.NetworkError("Provider transport failure") from None
            raise
        code = response.status_code
        if code in (401, 403):
            self.last_error_code = "authentication_denied"
        elif code == 429:
            self.last_error_code = "provider_rate_limited"
        elif code >= 500:
            self.last_error_code = "provider_unavailable"
        elif code >= 400:
            self.last_error_code = "provider_http_error"
        self.ledger.request_result(request_id, status_code=code, error=self.last_error_code)
        return response


class LiveWorker:
    def __init__(self, workspace: Path, config: LiveConfig, *, client=None, clock=_now):
        self.config, self.clock, self.client = config, clock, client
        self.root = Path(workspace) / config.provider
        self.root.mkdir(parents=True, exist_ok=True)
        self.snapshots = self.root / "snapshots"
        self.snapshots.mkdir(exist_ok=True)
        self.cursors = {code: self.root / f"{code}-cursor.json"
                        for code in config.source_scope["competition_codes"]}
        self.cursor = (next(iter(self.cursors.values())) if config.provider == "football-data"
                       else self.root / "cursor.json")
        self.ledger = LiveLedger(self.root / "live.db", config)
        self.pipeline = Pipeline(self.root / "pipeline")

    def _status(self, now):
        summary = self.ledger.summary()
        last, success = summary["last"], summary["success"]
        poll_status = last["status"] if last else "awaiting_credentials"
        status = {"succeeded": "ready" if success and success["snapshot"] else "empty",
                  "failed": "degraded", "interrupted": "degraded"}.get(poll_status, poll_status)
        return dict(schema_version=STATUS_VERSION, provider=self.config.provider, data_kind="api", status=status,
            source_scope=self.config.source_scope,
            last_attempt_at=last["started_at"] if last else None,
            last_success_at=success["finished_at"] if success else None,
            last_change_at=summary["last_change_at"], poll_interval_seconds=self.config.interval,
            last_error_code=last["error_code"] if last else "awaiting_credentials",
            latest_snapshot=success["snapshot"] if success else None, poll_counts=summary["poll_counts"],
            last_poll=dict(source_rows=last["source_rows"] if last else 0,
                           changed_rows=last["changed_rows"] if last else 0,
                           unsupported_rows=last["unsupported_rows"] if last else 0, requests=summary["requests"]),
            capabilities=dict(tier="free", update_mode="polled_rest", fixtures_and_results=True,
                              scores_delayed=True if self.config.provider == "football-data" else None,
                              in_game_round_feed=False), request_budget=self.ledger.budget(now))

    def _publish_status(self, now):
        status = self._status(now)
        _atomic_json(self.snapshots / "LIVE_STATUS.json", status)
        return status

    def _candidate_snapshot(self):
        events = self.pipeline.events()
        if not events:
            return None
        if {e["provider"] for e in events} != {self.config.provider}:
            raise LiveFailure("publication_failed")
        version = digest({"events": events})[:16]
        latest = self.snapshots / "LATEST"
        if latest.exists():
            snapshot_name = latest.read_text(encoding="utf-8").strip()
            if not re.fullmatch(r"snapshot-[0-9a-f]{16}-[0-9a-f]{8}", snapshot_name):
                raise LiveFailure("publication_failed")
            candidate = self.snapshots / snapshot_name
            if candidate.is_symlink():
                raise LiveFailure("publication_failed")
            manifest = json.loads((candidate / "manifest.json").read_text(encoding="utf-8"))
            if (manifest.get("schema_version") != "2.0" or manifest.get("data_kind") != "api"
                    or manifest.get("providers") != [self.config.provider]):
                raise LiveFailure("publication_failed")
            if manifest["dataset_version"] == version:
                files = manifest.get("files")
                if not isinstance(files, dict) or not files:
                    raise LiveFailure("publication_failed")
                actual = set()
                for path in candidate.rglob("*"):
                    if path.is_symlink():
                        raise LiveFailure("publication_failed")
                    if path.is_file() and path != candidate / "manifest.json":
                        actual.add(path.relative_to(candidate).as_posix())
                if actual != set(files):
                    raise LiveFailure("publication_failed")
                for name, checksum in files.items():
                    pure = PurePosixPath(name)
                    if (pure.is_absolute() or ".." in pure.parts or "\\" in name or ":" in name
                            or str(pure) != name or not isinstance(checksum, str)
                            or not re.fullmatch(r"[0-9a-f]{64}", checksum)):
                        raise LiveFailure("publication_failed")
                    calculated = hashlib.sha256()
                    with (candidate / name).open("rb") as handle:
                        for block in iter(lambda: handle.read(1024 * 1024), b""):
                            calculated.update(block)
                    if calculated.hexdigest() != checksum:
                        raise LiveFailure("publication_failed")
                return snapshot_name
        return export_snapshot(self.pipeline, self.snapshots, with_predictions=False).name

    def poll_once(self):
        """One bounded poll; health failures are typed and never contain exception text."""
        with _writer_lock(self.root / ".writer.lock"):
            started = _utc(self.clock())
            poll_id = self.ledger.begin(started)
            self._publish_status(started)
            if not os.environ.get(PROVIDERS[self.config.provider]["token"]):
                self.ledger.finish(poll_id, _utc(self.clock()), "awaiting_credentials", "awaiting_credentials")
                return self._publish_status(_utc(self.clock()))
            owned, client = self.client is None, self.client or httpx.Client(follow_redirects=False)
            budget_client = BudgetClient(client, self.ledger, poll_id, self.clock)
            phase, meta, changed = "collect", {}, 0
            try:
                rows, cursor_updates = [], []
                meta = dict(returned=0, unsupported=0)
                selected = list(self.cursors.items()) if self.cursors else [(self.config.competition, self.cursor)]
                for competition, cursor in selected:
                    scope_rows, scope_meta = collect(self.config.provider, cursor, competition,
                                                   self.config.pages, client=budget_client, clock=self.clock)
                    rows.extend(scope_rows)
                    meta["returned"] += scope_meta["returned"]
                    meta["unsupported"] += scope_meta["unsupported"]
                    cursor_updates.append((cursor, scope_meta["state"]))
                phase = "ingest"
                if rows:
                    result = self.pipeline.ingest(rows)
                    if result["quarantined"]:
                        raise LiveFailure("ingest_failed")
                    changed = result["accepted"]
                phase = "publication"
                previous = self.ledger.summary()["success"]
                candidate = self._candidate_snapshot()
                snapshot_changed = candidate is not None and (not previous or candidate != previous["snapshot"])
                phase = "cursor"
                for cursor, state in cursor_updates:
                    commit_state(cursor, state)
                self.ledger.finish(poll_id, _utc(self.clock()), "succeeded", source_rows=meta["returned"],
                    changed_rows=changed, unsupported_rows=meta["unsupported"], snapshot=candidate,
                    snapshot_changed=snapshot_changed)
            except Exception as error:
                if isinstance(error, LiveFailure):
                    code = error.code
                elif phase != "collect":
                    code = {"ingest": "ingest_failed", "publication": "publication_failed",
                            "cursor": "cursor_commit_failed"}[phase]
                elif isinstance(error, ProviderError):
                    code = budget_client.last_error_code or "provider_unavailable"
                elif isinstance(error, httpx.TransportError):
                    code = "network_timeout" if isinstance(error, httpx.TimeoutException) else "network_error"
                elif isinstance(error, httpx.HTTPStatusError):
                    code = budget_client.last_error_code or "provider_http_error"
                elif isinstance(error, (ValidationError, ValueError, KeyError, TypeError)):
                    code = "invalid_provider_payload"
                else:
                    code = "worker_error"
                self.ledger.finish(poll_id, _utc(self.clock()),
                    "quota_limited" if code == "quota_exhausted" else "failed", code,
                    source_rows=meta.get("returned", 0), changed_rows=changed,
                    unsupported_rows=meta.get("unsupported", 0))
            finally:
                if owned:
                    client.close()
            return self._publish_status(_utc(self.clock()))

    def run(self, cycles=1, *, sleep=time.sleep, forever=False):
        if type(forever) is not bool or (not forever and (type(cycles) is not int or not 1 <= cycles <= 100)):
            raise ValueError("cycles must be between 1 and 100")
        cycle = 0
        while forever or cycle < cycles:
            status = self.poll_once()
            yield status
            cycle += 1
            if forever or cycle < cycles:
                sleep(self.config.interval)

    def stop(self):
        with _writer_lock(self.root / ".writer.lock"):
            now = _utc(self.clock())
            self.ledger.interrupt(now)
            return self._publish_status(now)


def main():
    parser = argparse.ArgumentParser(description="Durable free fixtures/results polling; no in-game round feed")
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--provider", choices=tuple(PROVIDERS), required=True)
    scope = parser.add_mutually_exclusive_group()
    scope.add_argument("--competition", default="PL")
    scope.add_argument("--competitions", help="Comma-separated football scope, e.g. PL,PD,BL1,SA,FL1,BSA")
    parser.add_argument("--pages", type=int, default=1)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--cycles", type=int, default=1)
    mode.add_argument("--forever", action="store_true", help="Foreground service; interrupt with Ctrl+C")
    parser.add_argument("--interval", type=int, default=60)
    parser.add_argument("--env-file", type=Path)
    args = parser.parse_args()
    if args.env_file:
        if not args.env_file.is_file():
            parser.error("The selected local credential file does not exist")
        load_dotenv(args.env_file, override=True)
    else:
        load_dotenv(Path.cwd() / ".env", override=False)
    if not 1 <= args.cycles <= 100:
        parser.error("cycles must be between 1 and 100")
    competitions = tuple(args.competitions.split(",")) if args.competitions else None
    config = LiveConfig(args.provider, args.competition, args.pages, args.interval, competitions=competitions)
    worker = LiveWorker(args.workspace, config)
    try:
        for status in worker.run(args.cycles, forever=args.forever):
            print(json.dumps(status, indent=2, allow_nan=False), flush=True)
    except KeyboardInterrupt:
        # No exception details or credentials are printed; publication remains
        # selected while an interrupted in-flight poll is recorded explicitly.
        worker.stop()
        return


if __name__ == "__main__":
    main()
