"""Durable, quota-aware fixtures/results polling and immutable publication.

Only the explicit worker command calls providers. Status describes source checks,
not guaranteed immediate scores, complete coverage, or in-game telemetry.
"""

import argparse
import json
import os
import re
import sqlite3
import time
from contextlib import closing, contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlsplit

import httpx
from dotenv import load_dotenv
from pydantic import ValidationError

from .connectors import ProviderError, collect, commit_state
from .calendar import publish_calendar
from .provider_policy import ERROR_CODES, PROVIDERS
from .engine import Pipeline
from .schema import digest
from .snapshot import export_snapshot
from .provider_publication import (
    MAX_INTENT_BYTES, PublicationError, archive_references,
    canonical, checked, commit_selection, current_publication, describe_calendar,
    describe_canonical, parse_json, read_bytes, safe, sha256, validate_intent, Intent,
)

STATUS_VERSION = "arena-live-status-1.0"


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
        if not isinstance(self.competition, str) or not re.fullmatch(r"[A-Z0-9]{1,12}", self.competition):
            raise ValueError("Competition must be an uppercase ASCII alphanumeric code of 1..12 characters")
        if self.request_limit is not None and (type(self.request_limit) is not int
                or not 1 <= self.request_limit <= PROVIDERS[self.provider]["limit"]):
            raise ValueError("A custom request limit may only lower the provider's free-plan budget")
        if self.competitions is not None:
            if (self.provider != "football-data" or not isinstance(self.competitions, tuple)
                    or not 1 <= len(self.competitions) <= 10 or len(set(self.competitions)) != len(self.competitions)
                    or any(not isinstance(code, str) or not re.fullmatch(r"[A-Z0-9]{1,12}", code)
                           for code in self.competitions)):
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
                CREATE TABLE IF NOT EXISTS publication_intents(poll_id INTEGER PRIMARY KEY,
                  payload TEXT NOT NULL, payload_sha256 TEXT NOT NULL, phase TEXT NOT NULL,
                  FOREIGN KEY(poll_id) REFERENCES polls(id));
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
            if db.execute("SELECT 1 FROM publication_intents WHERE phase='prepared'").fetchone():
                raise LiveFailure("publication_failed")
            db.execute("UPDATE polls SET status='interrupted',finished_at=?,error_code='worker_interrupted' "
                       "WHERE status='polling'", (now,))
            return db.execute("INSERT INTO polls(started_at,status) VALUES (?,'polling')", (now,)).lastrowid

    def prepare(self, poll_id, value):
        body = canonical(checked(Intent, value))
        if len(body) > MAX_INTENT_BYTES:
            raise PublicationError("Recovery intent byte bound exceeded")
        with closing(self.connect()) as db, db:
            db.execute("INSERT INTO publication_intents VALUES (?,?,?,'prepared')",
                       (poll_id, body.decode(), sha256(body)))

    def update_selection(self, poll_id, value):
        body = canonical(checked(Intent, value))
        if len(body) > MAX_INTENT_BYTES:
            raise PublicationError("Recovery intent byte bound exceeded")
        with closing(self.connect()) as db, db:
            changed = db.execute("UPDATE publication_intents SET payload=?,payload_sha256=? "
                                 "WHERE poll_id=? AND phase='prepared'",
                                 (body.decode(), sha256(body), poll_id)).rowcount
            if changed != 1:
                raise PublicationError("Missing prepared recovery intent")

    def prepared(self):
        with closing(self.connect()) as db:
            rows = db.execute("SELECT i.*,p.started_at FROM publication_intents i JOIN polls p "
                              "ON p.id=i.poll_id WHERE i.phase='prepared' ORDER BY i.poll_id LIMIT 2").fetchall()
        if len(rows) > 1:
            raise PublicationError("Multiple outstanding publication intents")
        if not rows:
            return None
        row = dict(rows[0])
        if len(row["payload"].encode()) > MAX_INTENT_BYTES or sha256(row["payload"].encode()) != row["payload_sha256"]:
            raise PublicationError("Recovery intent checksum mismatch")
        row["intent"] = parse_json(row.pop("payload").encode())
        return row

    def complete_publication(self, poll_id, now, intent):
        metrics, candidate = intent["metrics"], intent["candidate"]
        snapshot = candidate["canonical"]["basename"] if candidate["canonical"] else None
        with closing(self.connect()) as db, db:
            previous = db.execute("SELECT status,finished_at FROM polls WHERE id=?", (poll_id,)).fetchone()
            if previous and previous["status"] == "succeeded":
                now = previous["finished_at"]
            db.execute("UPDATE polls SET finished_at=?,status='succeeded',error_code=NULL,source_rows=?,"
                       "changed_rows=?,unsupported_rows=?,snapshot=?,snapshot_changed=? WHERE id=?",
                       (now, metrics["source_rows"], metrics["changed_rows"], metrics["unsupported_rows"],
                        snapshot, int(metrics["snapshot_changed"]), poll_id))

    def acknowledge_publication(self, poll_id):
        with closing(self.connect()) as db, db:
            db.execute("UPDATE publication_intents SET phase='completed' WHERE poll_id=?", (poll_id,))

    def recovery_failure(self, now, code, *, preserve_success=False):
        with closing(self.connect()) as db, db:
            rows = db.execute("SELECT poll_id FROM publication_intents WHERE phase='prepared'").fetchall()
            for row in rows:
                condition = " AND status!='succeeded'" if preserve_success else ""
                db.execute("UPDATE polls SET status='failed',finished_at=?,error_code=? WHERE id=?" + condition,
                           (now, code, row["poll_id"]))
            return bool(rows)

    def interrupt(self, now):
        with closing(self.connect()) as db, db:
            db.execute("UPDATE polls SET status='interrupted',finished_at=?,error_code='worker_interrupted' "
                       "WHERE status='polling'", (now,))

    def finish(self, poll_id, now, status, error=None, *, source_rows=0, changed_rows=0, unsupported_rows=0,
               snapshot=None, snapshot_changed=False):
        with closing(self.connect()) as db, db:
            db.execute("UPDATE polls SET finished_at=?,status=?,error_code=?,source_rows=?,changed_rows=?,"
                       "unsupported_rows=?,snapshot=?,snapshot_changed=? WHERE id=? AND status!='succeeded'",
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
        safe(self.root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.snapshots = self.root / "snapshots"
        safe(self.snapshots)
        self.snapshots.mkdir(exist_ok=True)
        self.cursors = {code: self.root / f"{code}-cursor.json"
                        for code in config.source_scope["competition_codes"]}
        self.cursor = (next(iter(self.cursors.values())) if config.provider == "football-data"
                       else self.root / "cursor.json")
        safe(self.root / "live.db")
        safe(self.root / "pipeline")
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
        selected = current_publication(self.root, now=self.clock())
        latest = self.snapshots / "LATEST"
        safe(latest)
        snapshot_name = (selected["canonical"]["basename"] if selected and selected["canonical"] else
                         read_bytes(latest, 100).decode().strip() if not selected and latest.exists() else None)
        if snapshot_name is not None:
            if not re.fullmatch(r"snapshot-[0-9a-f]{16}-[0-9a-f]{8}", snapshot_name):
                raise LiveFailure("publication_failed")
            candidate = self.snapshots / snapshot_name
            descriptor = describe_canonical(candidate, self.config.provider, now=self.clock())
            if descriptor["dataset_version"] == version:
                return snapshot_name
        return export_snapshot(self.pipeline, self.snapshots, with_predictions=False,
                               select_latest=False, generated_at=self.clock()).name

    def _legacy_pointers(self, selection):
        # These compatibility views converge after the authoritative selector.
        # New consumers never combine them to select a pair.
        for role, directory, pointer in (("canonical", self.snapshots, "LATEST"),
                                         ("calendar", self.root / "calendars", "CALENDAR_LATEST")):
            if selection[role] is not None:
                target = directory / pointer
                safe(target)
                temporary = target.with_suffix(".tmp")
                with temporary.open("w", encoding="ascii", newline="\n") as handle:
                    handle.write(selection[role]["basename"] + "\n")
                    handle.flush()
                    os.fsync(handle.fileno())
                temporary.replace(target)

    def _finish_prepared(self, row):
        intent = validate_intent(self.root, row["intent"], now=self.clock(), started_at=row["started_at"])
        candidate, poll_id = intent["candidate"], row["poll_id"]
        if candidate["poll_id"] != poll_id or candidate["source_scope"] != self.config.source_scope:
            raise PublicationError("Recovery poll or source scope mismatch")
        selected = current_publication(self.root, now=self.clock())
        already_selected = selected is not None and selected["poll_id"] == poll_id
        if already_selected:
            if selected != intent["selection"]:
                raise PublicationError("Selected publication differs from durable intent")
        elif (selected["fingerprint"] if selected else None) != intent["previous_fingerprint"]:
            raise PublicationError("Publication authority changed during recovery")
        # Cursor commits are idempotent and do not create source observations.
        for cursor in intent["cursors"]:
            path = self.root / cursor["basename"]
            safe(path)
            try:
                if not path.exists() or parse_json(read_bytes(path, MAX_INTENT_BYTES)) != cursor["state"]:
                    commit_state(path, cursor["state"])
                # The standalone connector preserves its public API. Ensure
                # cursor bytes are durable before selecting the prepared pair.
                with path.open("r+b") as handle:
                    os.fsync(handle.fileno())
                if os.name != "nt":
                    directory = os.open(path.parent, os.O_RDONLY)
                    try:
                        os.fsync(directory)
                    finally:
                        os.close(directory)
            except Exception:
                raise LiveFailure("cursor_commit_failed") from None
        if not already_selected:
            def persist(selection):
                intent["selection"] = selection
                self.ledger.update_selection(poll_id, intent)
            try:
                selected = commit_selection(self.root, candidate, clock=self.clock, persist=persist)
            except Exception:
                raise LiveFailure("publication_failed") from None
        try:
            self.ledger.complete_publication(poll_id, _utc(self.clock()), intent)
            self._legacy_pointers(selected)
            status = self._publish_status(_utc(self.clock()))
            self.ledger.acknowledge_publication(poll_id)
            # Optional display facts have their own checksum and exact source
            # publication pin. A failure cannot replace or invalidate scores.
            try:
                from .match_insights import publish
                publish(self.root, selected, [self.root / ref["path"] for ref in intent["archives"]],
                        started_at=row["started_at"], generated_at=self.clock())
            except (PublicationError, OSError, sqlite3.Error, ValueError, KeyError, TypeError):
                pass
            return status
        except Exception:
            raise LiveFailure("publication_failed") from None

    def poll_once(self):
        """One bounded poll; health failures are typed and never contain exception text."""
        with _writer_lock(self.root / ".writer.lock"):
            prepared = None
            try:
                prepared = self.ledger.prepared()
                if prepared is not None:
                    return self._finish_prepared(prepared)
                # Fail closed if a previously selected pair has been corrupted.
                current_publication(self.root, now=self.clock())
            except Exception as error:
                code = error.code if isinstance(error, LiveFailure) else "publication_failed"
                if not self.ledger.recovery_failure(_utc(self.clock()), code,
                                                    preserve_success=isinstance(error, LiveFailure)):
                    poll_id = self.ledger.begin(_utc(self.clock()))
                    self.ledger.finish(poll_id, _utc(self.clock()), "failed", code)
                return self._publish_status(_utc(self.clock()))
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
                rows, cursor_updates, raw_archives = [], [], []
                meta = dict(returned=0, unsupported=0)
                selected = list(self.cursors.items()) if self.cursors else [(self.config.competition, self.cursor)]
                for competition, cursor in selected:
                    safe(cursor)
                    scope_rows, scope_meta = collect(self.config.provider, cursor, competition,
                                                   self.config.pages, client=budget_client, clock=self.clock)
                    rows.extend(scope_rows)
                    meta["returned"] += scope_meta["returned"]
                    meta["unsupported"] += scope_meta["unsupported"]
                    cursor_updates.append((cursor, scope_meta["state"]))
                    raw_archives.extend(scope_meta["raw_archives"])
                collected_at = _utc(self.clock())
                archives, sources = archive_references(self.root, raw_archives, self.config.source_scope,
                    collected_at=collected_at, started_at=started)
                phase = "ingest"
                if rows:
                    result = self.pipeline.ingest(rows)
                    if result["quarantined"]:
                        raise LiveFailure("ingest_failed")
                    changed = result["accepted"]
                phase = "publication"
                previous = current_publication(self.root, now=self.clock())
                snapshot_name = self._candidate_snapshot()
                descriptor = (describe_canonical(self.snapshots / snapshot_name, self.config.provider, now=self.clock())
                              if snapshot_name else None)
                calendar_descriptor = None
                if self.config.provider == "pandascore":
                    announced_ids = {f"pandascore:{raw.get('id')}" for source in sources for raw in source["payload"]
                                     if isinstance(raw, dict)}
                    canonical_ids = {event["match_id"] for event in self.pipeline.events()} & announced_ids
                    calendar = publish_calendar(raw_archives, self.root / "calendars", archive_root=self.root / "provider_raw",
                        pages=self.config.pages, canonical_match_ids=canonical_ids,
                        generated_at=self.clock(), select_latest=False)
                    calendar_descriptor = describe_calendar(self.root / "calendars" / f"calendar-{calendar['dataset_version']}",
                                                           now=self.clock())
                candidate = dict(provider=self.config.provider, data_kind="api", poll_id=poll_id,
                    source_scope=self.config.source_scope, collected_at=collected_at,
                    canonical=descriptor, calendar=calendar_descriptor)
                old_snapshot = previous["canonical"]["basename"] if previous and previous["canonical"] else None
                if previous is None:
                    last_success = self.ledger.summary()["success"]
                    old_snapshot = last_success["snapshot"] if last_success else None
                intent = dict(schema_version="arena-provider-publication-intent-1.0", candidate=candidate,
                    previous_fingerprint=previous["fingerprint"] if previous else None, archives=archives,
                    cursors=[dict(basename=path.name, state=state) for path, state in cursor_updates],
                    metrics=dict(source_rows=meta["returned"], changed_rows=changed, unsupported_rows=meta["unsupported"],
                                 snapshot_changed=snapshot_name is not None and snapshot_name != old_snapshot), selection=None)
                validate_intent(self.root, intent, now=self.clock(), started_at=started)
                self.ledger.prepare(poll_id, intent)
                phase = "cursor"
                return self._finish_prepared(self.ledger.prepared())
            except Exception as error:
                if isinstance(error, LiveFailure):
                    code = error.code
                elif isinstance(error, PublicationError):
                    code = "publication_failed"
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
