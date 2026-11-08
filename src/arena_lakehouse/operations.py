"""Local stage orchestration. Delta commits are separate; publication is gated.

An OS-held writer lock disappears on process death. SQLite records immutable job
inputs and every stage attempt, allowing a failed or interrupted stage to retry.
No cross-table atomicity is claimed: only a succeeded, verified run is readable.
"""

import json
import os
import sqlite3
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from .schema import MatchEvent, canonical

STAGES = ("bronze", "silver", "gold", "verify")


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def validated_new_events(existing, records):
    """Reject ambiguous revisions before any Delta stage is committed.

    The reference engine supports per-record quarantine. This strict publication
    engine rejects the entire candidate batch; the raw candidate is preserved in
    quarantine. It never silently picks a conflicting version as the winner.
    """
    by_version, by_id, grouped = {}, {}, {}
    for raw in existing:
        event = MatchEvent.model_validate(raw).row()
        by_version[(event["match_id"], event["revision"])] = event
        by_id[event["event_id"]] = event
        grouped.setdefault(event["match_id"], []).append(event)
    accepted = []
    for raw in records:
        event = MatchEvent.model_validate(raw).row()
        key = event["match_id"], event["revision"]
        old = by_id.get(event["event_id"]) or by_version.get(key)
        if old:
            ignored = {"observed_at", "completed_at"} if event["provider"] != "synthetic" else set()
            if {k: v for k, v in old.items() if k not in ignored} != {
                k: v for k, v in event.items() if k not in ignored
            }:
                raise ValueError("Conflicting event ID or match revision")
            continue
        by_id[event["event_id"]] = by_version[key] = event
        grouped.setdefault(event["match_id"], []).append(event)
        accepted.append(event)
    for versions in grouped.values():
        versions.sort(key=lambda e: e["revision"])
        identity = ("provider", "sport", "team_a_id", "team_b_id")
        if any(any(e[k] != versions[0][k] for k in identity) for e in versions):
            raise ValueError("A match revision changed its immutable identity")
        if any(a["observed_at"] >= b["observed_at"] for a, b in zip(versions, versions[1:])):
            raise ValueError("Revision observation times must increase strictly")
    return sorted(accepted, key=lambda e: (e["match_id"], e["revision"]))


def bounded_window(records, start, end, max_events=100_000):
    """Select observed events in [from,to); UTC bounds and a 31-day ceiling."""
    from datetime import timedelta
    begin, finish = (datetime.fromisoformat(x.replace("Z", "+00:00")) for x in (start, end))
    if begin.tzinfo is None or finish.tzinfo is None:
        raise ValueError("Backfill bounds must include a timezone")
    begin, finish = begin.astimezone(timezone.utc), finish.astimezone(timezone.utc)
    if not timedelta(0) < finish - begin <= timedelta(days=31):
        raise ValueError("Backfill must cover more than zero and at most 31 days")
    if not 1 <= max_events <= 100_000:
        raise ValueError("max_events must be 1..100000")
    selected = [r for r in records if begin <= MatchEvent.model_validate(r).observed_at < finish]
    if len(selected) > max_events:
        raise ValueError("Backfill exceeds its event budget; split the window")
    return selected


class StageLedger:
    def __init__(self, root: Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.executescript("""
              CREATE TABLE IF NOT EXISTS jobs(run_id TEXT PRIMARY KEY, created_at TEXT,
                status TEXT, payload TEXT, error TEXT);
              CREATE TABLE IF NOT EXISTS attempts(run_id TEXT, stage TEXT, attempt INTEGER,
                started_at TEXT, finished_at TEXT, status TEXT, duration_seconds REAL,
                error TEXT, PRIMARY KEY(run_id,stage,attempt));
            """)

    def connect(self):
        db = sqlite3.connect(self.root / "stages.db", timeout=30)
        db.row_factory = sqlite3.Row
        return db

    @contextmanager
    def writer(self):
        path = self.root / ".writer.lock"
        with path.open("a+b") as handle:
            try:
                if path.stat().st_size == 0:
                    handle.write(b"0")
                    handle.flush()
                handle.seek(0)
                if os.name == "nt":
                    import msvcrt
                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as error:
                raise RuntimeError("Another stage runner holds the writer lock") from error
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

    def pending(self):
        with self.connect() as db:
            row = db.execute("SELECT run_id FROM jobs WHERE status != 'succeeded' ORDER BY rowid LIMIT 1").fetchone()
        return row[0] if row else None

    def latest_success(self):
        with self.connect() as db:
            row = db.execute("SELECT run_id FROM jobs WHERE status='succeeded' ORDER BY rowid DESC LIMIT 1").fetchone()
        return self.job(row[0]) if row else None

    def create(self, payload):
        if self.pending():
            raise ValueError("Resume the unfinished run before submitting another batch")
        run_id = uuid.uuid4().hex
        with self.connect() as db:
            db.execute("INSERT INTO jobs VALUES (?,?,?,?,?)", (run_id, utc_now(), "pending", canonical(payload), None))
        return run_id

    def job(self, run_id):
        with self.connect() as db:
            row = db.execute("SELECT * FROM jobs WHERE run_id=?", (run_id,)).fetchone()
        if row is None:
            raise ValueError("Unknown run ID")
        result = dict(row)
        result["payload"] = json.loads(result["payload"])
        return result

    def attempts(self, run_id):
        with self.connect() as db:
            return [dict(r) for r in db.execute("SELECT * FROM attempts WHERE run_id=? ORDER BY rowid", (run_id,))]

    def execute(self, run_id, callbacks, fail_after=None, max_attempts=2):
        if fail_after is not None and fail_after not in STAGES:
            raise ValueError("Unknown injected failure stage")
        if not 1 <= max_attempts <= 3:
            raise ValueError("Stage retries must be 1..3")
        job = self.job(run_id)
        if job["status"] == "succeeded":
            return job
        with self.connect() as db:
            # Process death leaves a running attempt. Its Delta commit may have
            # succeeded, so mark interrupted and run that idempotent stage again.
            db.execute("UPDATE attempts SET status='interrupted',finished_at=? WHERE run_id=? AND status='running'",
                       (utc_now(), run_id))
            db.execute("UPDATE jobs SET status='running',error=NULL WHERE run_id=?", (run_id,))
        try:
            for stage in STAGES:
                previous = [a for a in self.attempts(run_id) if a["stage"] == stage]
                if previous and previous[-1]["status"] == "succeeded":
                    continue
                for retry in range(max_attempts):
                    attempt = len(previous) + retry + 1
                    start = time.perf_counter()
                    with self.connect() as db:
                        db.execute("INSERT INTO attempts VALUES (?,?,?,?,?,?,?,?)",
                                   (run_id, stage, attempt, utc_now(), None, "running", None, None,))
                    try:
                        callbacks[stage](job["payload"])
                    except Exception as error:
                        with self.connect() as db:
                            db.execute("UPDATE attempts SET status='failed',finished_at=?,duration_seconds=?,error=? "
                                       "WHERE run_id=? AND stage=? AND attempt=?", (utc_now(), time.perf_counter() - start,
                                       type(error).__name__, run_id, stage, attempt))
                        if isinstance(error, (TimeoutError, ConnectionError)) and retry + 1 < max_attempts:
                            time.sleep(0.25 * (retry + 1))
                            continue
                        raise
                    with self.connect() as db:
                        db.execute("UPDATE attempts SET status='succeeded',finished_at=?,duration_seconds=? "
                                   "WHERE run_id=? AND stage=? AND attempt=?", (utc_now(), time.perf_counter() - start,
                                   run_id, stage, attempt))
                    break
                if fail_after == stage:
                    raise RuntimeError(f"Injected failure after {stage} commit")
            with self.connect() as db:
                db.execute("UPDATE jobs SET status='succeeded',error=NULL WHERE run_id=?", (run_id,))
        except Exception as error:
            with self.connect() as db:
                db.execute("UPDATE jobs SET status='failed',error=? WHERE run_id=?", (type(error).__name__, run_id))
            raise
        return self.job(run_id)
