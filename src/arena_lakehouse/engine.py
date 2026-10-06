"""Transactional reference pipeline and independent export oracle for Spark.

SQLite is a small local control/validation plane. Spark/Delta is an optional
execution backend; the reference backend does not pretend to be a lakehouse.
"""

import json
import sqlite3
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from pydantic import ValidationError

from .schema import MatchEvent, canonical, digest, outcome


def latest(events: list[dict], as_of: str | None = None) -> list[dict]:
    chosen = {}
    for event in events:
        if as_of and event["observed_at"] > as_of:
            continue
        old = chosen.get(event["match_id"])
        if old is None or event["revision"] > old["revision"]:
            chosen[event["match_id"]] = event
    return sorted(chosen.values(), key=lambda x: x["match_id"])


def team_stats(matches: list[dict]) -> list[dict]:
    # A provider can rename a team. Identity is the stable scoped ID, not its
    # display name. Select the most recently observed name even if that match
    # is upcoming or cancelled, then aggregate completed results by ID.
    names = {}
    for match in sorted(matches, key=lambda x: (x["observed_at"], x["match_id"])):
        for side in ("a", "b"):
            names[(match["sport"], match[f"team_{side}_id"])] = match[f"team_{side}"]
    stats = {}
    for match in sorted(matches, key=lambda x: (x["start_time"], x["match_id"])):
        if match["status"] != "finished":
            continue
        result = outcome(match)
        for side, other in (("a", "b"), ("b", "a")):
            key = (match["sport"], match[f"team_{side}_id"])
            row = stats.setdefault(key, dict(sport=key[0], team_id=key[1],
                team_name=names[key], matches=0, wins=0, draws=0, losses=0,
                score_for=0, score_against=0, last_match_at=match["completed_at"]))
            row["matches"] += 1
            row["wins"] += int(result == side)
            row["draws"] += int(result == "draw")
            row["losses"] += int(result == other)
            row["score_for"] += match[f"score_{side}"]
            row["score_against"] += match[f"score_{other}"]
            row["last_match_at"] = max(row["last_match_at"], match["completed_at"])
    for row in stats.values():
        row["win_rate"] = row["wins"] / row["matches"]
    return sorted(stats.values(), key=lambda x: (x["sport"], x["team_id"]))


def history(events: list[dict]) -> list[dict]:
    groups = defaultdict(list)
    for event in events:
        groups[event["match_id"]].append(event)
    rows = []
    for versions in groups.values():
        versions.sort(key=lambda x: x["revision"])
        for i, event in enumerate(versions):
            rows.append(dict(event, valid_from=event["observed_at"],
                             valid_to=versions[i + 1]["observed_at"] if i + 1 < len(versions) else None))
    return sorted(rows, key=lambda x: (x["match_id"], x["revision"]))


class Pipeline:
    def __init__(self, root: Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.executescript("""
              CREATE TABLE IF NOT EXISTS events(event_id TEXT PRIMARY KEY, match_id TEXT,
                revision INTEGER, payload_hash TEXT, payload TEXT, UNIQUE(match_id, revision));
              CREATE TABLE IF NOT EXISTS quarantine(id TEXT PRIMARY KEY, reason TEXT, payload TEXT);
              CREATE TABLE IF NOT EXISTS runs(run_id TEXT PRIMARY KEY, started_at TEXT,
                status TEXT, duration_seconds REAL, error TEXT);
              CREATE TABLE IF NOT EXISTS blocked(event_id TEXT PRIMARY KEY);
              CREATE TABLE IF NOT EXISTS blocked_versions(match_id TEXT, revision INTEGER,
                PRIMARY KEY(match_id,revision));
              CREATE TABLE IF NOT EXISTS blocked_matches(match_id TEXT PRIMARY KEY);
            """)

    def connect(self):
        return sqlite3.connect(self.root / "control.db", timeout=30)

    def ingest(self, records: list[dict], fail_after: int | None = None) -> dict:
        run_id, start = uuid.uuid4().hex, time.perf_counter()
        now = datetime.now(timezone.utc).isoformat()
        accepted = duplicates = quarantined = 0
        # Bronze raw payloads stay available even when validation rejects them.
        landing = self.root / "landing"
        landing.mkdir(exist_ok=True)
        (landing / f"{run_id}.jsonl").write_text(
            "\n".join(canonical(r) for r in records) + "\n", encoding="utf-8")
        with self.connect() as db:
            db.execute("INSERT INTO runs VALUES (?,?,?,?,?)", (run_id, now, "running", None, None))
        try:
            with self.connect() as db:
                db.execute("BEGIN IMMEDIATE")
                for index, raw in enumerate(records):
                    if fail_after is not None and index == fail_after:
                        raise RuntimeError("Injected failure before transaction commit")
                    try:
                        event = MatchEvent.model_validate(raw).row()
                    except ValidationError:
                        self._quarantine(db, raw, "schema_or_business_rule")
                        quarantined += 1
                        continue
                    if db.execute("SELECT 1 FROM blocked WHERE event_id=?", (event["event_id"],)).fetchone():
                        quarantined += 1
                        continue
                    if db.execute("SELECT 1 FROM blocked_matches WHERE match_id=?", (event["match_id"],)).fetchone():
                        self._quarantine(db, event, "revision_time_or_identity")
                        quarantined += 1
                        continue
                    if db.execute("SELECT 1 FROM blocked_versions WHERE match_id=? AND revision=?",
                                  (event["match_id"], event["revision"])).fetchone():
                        self._quarantine(db, event, "conflicting_revision")
                        quarantined += 1
                        continue
                    old = db.execute("SELECT event_id,payload_hash,payload FROM events WHERE event_id=? "
                                     "OR (match_id=? AND revision=?)",
                                     (event["event_id"], event["match_id"], event["revision"])).fetchone()
                    if old:
                        previous = json.loads(old[2])
                        if previous["match_id"] != event["match_id"]:
                            # A globally reused event ID corrupts both match
                            # chains. Blocking both is independent of arrival
                            # order and prevents later alternate IDs restoring
                            # a payload that was previously rejected.
                            for ident in (previous["match_id"], event["match_id"]):
                                known = db.execute("SELECT payload FROM events WHERE match_id=?", (ident,)).fetchall()
                                for item in known:
                                    self._quarantine(db, json.loads(item[0]), "revision_time_or_identity")
                                db.execute("DELETE FROM events WHERE match_id=?", (ident,))
                                db.execute("INSERT OR IGNORE INTO blocked_matches VALUES (?)", (ident,))
                            self._quarantine(db, event, "revision_time_or_identity")
                            quarantined += 1
                            continue
                        # A source event retried after a cursor crash can be observed later.
                        # Keep the first known availability; do not misclassify it as a conflicting score.
                        ignored = {"observed_at", "completed_at"} if event["provider"] != "synthetic" else set()
                        if ({k: v for k, v in previous.items() if k not in ignored}
                            == {k: v for k, v in event.items() if k not in ignored}):
                            duplicates += 1
                        else:
                            # Never choose a winner for conflicting source versions.
                            self._quarantine(db, json.loads(old[2]), "conflicting_revision")
                            self._quarantine(db, event, "conflicting_revision")
                            db.executemany("INSERT OR IGNORE INTO blocked VALUES (?)",
                                           [(old[0],), (event["event_id"],)])
                            db.execute("DELETE FROM events WHERE event_id=?", (old[0],))
                            db.execute("INSERT OR IGNORE INTO blocked_versions VALUES (?,?)",
                                       (event["match_id"], event["revision"]))
                            quarantined += 1
                        continue
                    neighbours = db.execute("SELECT payload FROM events WHERE match_id=?",
                                            (event["match_id"],)).fetchall()
                    incompatible = False
                    for item in neighbours:
                        other = json.loads(item[0])
                        identity = ("provider", "sport", "team_a_id", "team_b_id")
                        if any(event[k] != other[k] for k in identity):
                            incompatible = True
                        if ((other["revision"] < event["revision"] and other["observed_at"] >= event["observed_at"])
                            or (other["revision"] > event["revision"] and other["observed_at"] <= event["observed_at"])):
                            incompatible = True
                    if incompatible:
                        # The source has supplied an inconsistent chain. Reject
                        # the whole match symmetrically, regardless of delivery
                        # order, rather than preserving whichever arrived first.
                        for item in neighbours:
                            self._quarantine(db, json.loads(item[0]), "revision_time_or_identity")
                        self._quarantine(db, event, "revision_time_or_identity")
                        db.execute("DELETE FROM events WHERE match_id=?", (event["match_id"],))
                        db.execute("INSERT OR IGNORE INTO blocked_matches VALUES (?)", (event["match_id"],))
                        quarantined += 1
                        continue
                    db.execute("INSERT INTO events VALUES (?,?,?,?,?)", (event["event_id"],
                               event["match_id"], event["revision"], digest(event), canonical(event)))
                    accepted += 1
                db.execute("UPDATE runs SET status='succeeded',duration_seconds=? WHERE run_id=?",
                           (time.perf_counter() - start, run_id))
        except Exception as error:
            with self.connect() as db:
                db.execute("UPDATE runs SET status='failed',duration_seconds=?,error=? WHERE run_id=?",
                           (time.perf_counter() - start, type(error).__name__, run_id))
            raise
        return dict(run_id=run_id, accepted=accepted, duplicates=duplicates, quarantined=quarantined)

    @staticmethod
    def _quarantine(db, raw, reason):
        key = digest({"raw": raw, "reason": reason})
        db.execute("INSERT OR IGNORE INTO quarantine VALUES (?,?,?)", (key, reason, canonical(raw)))

    def events(self):
        with self.connect() as db:
            return [json.loads(x[0]) for x in db.execute("SELECT payload FROM events ORDER BY match_id,revision")]

    def quality(self):
        with self.connect() as db:
            counts = dict(db.execute("SELECT reason,COUNT(*) FROM quarantine GROUP BY reason"))
            total = db.execute("SELECT COUNT(*) FROM events").fetchone()[0]
            last_run = db.execute("SELECT run_id,status FROM runs ORDER BY rowid DESC LIMIT 1").fetchone()
        checks = [dict(rule=k, table="bronze_events", passed=total, failed=v, severity="warning")
                  for k, v in counts.items()]
        return dict(run_id=last_run[0] if last_run else "empty", checks=checks,
                    summary=dict(valid_events=total, quarantined=sum(counts.values()),
                                 last_run_status=last_run[1] if last_run else "empty"))
