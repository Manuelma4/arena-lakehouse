"""Disk-backed, independently reconstructed expectations for scale verification.

Only the event contract is shared with the pipeline. SQLite reconstructs state
and metrics from caller-supplied accepted input, never from Spark's Bronze.
Rows and Parquet row groups are bounded by ``batch_rows``; SQLite sorts and
indexes use disk. This is a correctness oracle, not a performance benchmark.
"""

import hashlib
import json
import sqlite3
import uuid
from collections.abc import Iterable, Iterator, Mapping
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .schema import MatchEvent

EVENT_COLUMNS = (
    "event_id", "match_id", "provider", "sport", "competition", "revision",
    "team_a_id", "team_a", "team_b_id", "team_b", "start_time", "observed_at",
    "completed_at", "status", "score_a", "score_b", "best_of",
)
METRIC_COLUMNS = (
    "sport", "team_id", "team_name", "matches", "wins", "draws", "losses",
    "score_for", "score_against", "last_match_at", "win_rate",
)
_INTEGERS = {"revision", "score_a", "score_b", "best_of"}
_EVENT_SCHEMA = pa.schema([
    (name, pa.int32() if name in _INTEGERS else pa.string()) for name in EVENT_COLUMNS
])
_HISTORY_SCHEMA = pa.schema([
    *_EVENT_SCHEMA, ("valid_from", pa.string()), ("valid_to", pa.string()), ("is_current", pa.bool_()),
])
_METRIC_SCHEMA = pa.schema([
    (name, pa.float64() if name == "win_rate" else pa.int64()
     if name in {"matches", "wins", "draws", "losses", "score_for", "score_against"}
     else pa.string()) for name in METRIC_COLUMNS
])
TABLE_SCHEMAS = {
    "bronze_observations": _EVENT_SCHEMA,
    "silver_matches": _EVENT_SCHEMA,
    "silver_history": _HISTORY_SCHEMA,
    "gold_team_stats": _METRIC_SCHEMA,
}


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _file_hash(path):
    result = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(64 * 1024), b""):
            result.update(block)
    return result.hexdigest()


class OracleConflict(ValueError):
    """The candidate cannot be an unambiguous accepted observation set."""


class OracleMismatch(AssertionError):
    def __init__(self, report):
        self.report = report
        failed = [name for name, data in report["tables"].items() if not data["equal"]]
        super().__init__("Full row multiset mismatch: " + ", ".join(failed))


class ScaleOracle:
    """One writer, persisted source observations, transactional batch rejection.

    ``ingest_batch``/``add`` consume one-pass iterables; replay keeps the first
    accepted provider observation's availability metadata. Conflicts reject
    the whole candidate transaction, rather than selecting an arbitrary row.
    SQLite's page cache is 16 MiB, mmap is disabled, and temporary sorts use
    files. The bound concerns application row batches, not total SQLite/Spark
    process memory. Reference exports are retained for inspection.
    """

    def __init__(self, database_path: Path, *, batch_rows: int = 4096):
        if type(batch_rows) is not int or not 1 <= batch_rows <= 65_536:
            raise ValueError("batch_rows must be an integer between 1 and 65536")
        self.database_path = Path(database_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self.batch_rows = batch_rows
        self.last_report = None
        self._generation = None
        self._reference_cache = None
        self.db = sqlite3.connect(self.database_path, timeout=30)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA temp_store=FILE")
        self.db.execute("PRAGMA cache_size=-16384")
        self.db.execute("PRAGMA mmap_size=0")
        columns = ",".join(
            f'"{name}" {"INTEGER" if name in _INTEGERS else "TEXT"}' for name in EVENT_COLUMNS
        )
        self.db.executescript(f"""
          CREATE TABLE IF NOT EXISTS observations ({columns}, payload TEXT NOT NULL,
            PRIMARY KEY(match_id,revision), UNIQUE(event_id));
          CREATE TABLE IF NOT EXISTS ingest_totals (
            singleton INTEGER PRIMARY KEY CHECK(singleton=1),
            received INTEGER NOT NULL, accepted INTEGER NOT NULL, replayed INTEGER NOT NULL);
          INSERT OR IGNORE INTO ingest_totals VALUES(1,0,0,0);
        """)

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        self.close()

    def close(self):
        self.db.close()

    def ingest_batch(self, records: Iterable[MatchEvent | dict]) -> dict:
        """Accept a complete transaction or roll it back, including counters.

        Contract normalization is shared; conflict and interval validation are
        independent indexed lookups. Only adjacent revisions need examination
        because every previously accepted chain is already strictly monotonic.
        """
        received = accepted = replayed = cancellations = 0
        sql = (f"INSERT INTO observations ({','.join(EVENT_COLUMNS)},payload) VALUES "
               f"({','.join('?' for _ in range(len(EVENT_COLUMNS) + 1))})")
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            for candidate in records:
                received += 1
                event = (candidate if isinstance(candidate, MatchEvent)
                         else MatchEvent.model_validate(candidate)).row()
                existing = self.db.execute(
                    "SELECT payload FROM observations WHERE event_id=? OR (match_id=? AND revision=?)",
                    (event["event_id"], event["match_id"], event["revision"]),
                ).fetchall()
                if existing:
                    ignored = {"observed_at", "completed_at"} if event["provider"] != "synthetic" else set()
                    business = {k: v for k, v in event.items() if k not in ignored}
                    if len(existing) != 1 or business != {
                        k: v for k, v in json.loads(existing[0]["payload"]).items() if k not in ignored
                    }:
                        raise OracleConflict("Conflicting event ID or match revision")
                    replayed += 1
                    continue
                previous = self.db.execute(
                    "SELECT provider,sport,team_a_id,team_b_id,observed_at FROM observations "
                    "WHERE match_id=? AND revision<? ORDER BY revision DESC LIMIT 1",
                    (event["match_id"], event["revision"]),
                ).fetchone()
                following = self.db.execute(
                    "SELECT provider,sport,team_a_id,team_b_id,observed_at FROM observations "
                    "WHERE match_id=? AND revision>? ORDER BY revision LIMIT 1",
                    (event["match_id"], event["revision"]),
                ).fetchone()
                identity = ("provider", "sport", "team_a_id", "team_b_id")
                if any(any(other[key] != event[key] for key in identity)
                       for other in (previous, following) if other is not None):
                    raise OracleConflict("Match chain identity changed")
                if ((previous is not None and previous["observed_at"] >= event["observed_at"])
                        or (following is not None and event["observed_at"] >= following["observed_at"])):
                    raise OracleConflict("Revision observations must be strictly increasing")
                self.db.execute(sql, (*[event[key] for key in EVENT_COLUMNS], _json(event)))
                accepted += 1
                cancellations += int(event["status"] == "cancelled")
            self.db.execute(
                "UPDATE ingest_totals SET received=received+?,accepted=accepted+?,replayed=replayed+? "
                "WHERE singleton=1", (received, accepted, replayed),
            )
        if accepted:
            self._generation = None
        return dict(received=received, accepted=accepted, replayed=replayed,
                    accepted_cancellation_observations=cancellations)

    add = ingest_batch

    def _materialize(self):
        generation = self.db.execute("SELECT accepted FROM ingest_totals WHERE singleton=1").fetchone()[0]
        if self._generation == generation:
            return
        # Physical disk tables and indexes avoid retaining all rows, matches,
        # or team histories in Python. Names include nonfinished current rows.
        statements = (
            "DROP TABLE IF EXISTS _current",
            "CREATE TABLE _current AS SELECT e.* FROM observations e WHERE NOT EXISTS "
            "(SELECT 1 FROM observations n WHERE n.match_id=e.match_id AND n.revision>e.revision)",
            "CREATE UNIQUE INDEX _current_id ON _current(match_id)",
            "DROP TABLE IF EXISTS _sides",
            "CREATE TABLE _sides AS SELECT sport,match_id,observed_at,status,team_a_id AS team_id,"
            "team_a AS team_name,score_a AS score_for,score_b AS score_against,completed_at FROM _current "
            "UNION ALL SELECT sport,match_id,observed_at,status,team_b_id,team_b,score_b,score_a,"
            "completed_at FROM _current",
            "CREATE INDEX _side_names ON _sides(sport,team_id,observed_at DESC,match_id DESC)",
            "DROP TABLE IF EXISTS _gold",
            "CREATE TABLE _gold AS SELECT g.sport,g.team_id,(SELECT s.team_name FROM _sides s "
            "WHERE s.sport=g.sport AND s.team_id=g.team_id ORDER BY s.observed_at DESC,s.match_id DESC "
            "LIMIT 1) AS team_name,g.matches,g.wins,g.draws,g.losses,g.score_for,g.score_against,"
            "g.last_match_at,CAST(g.wins AS REAL)/g.matches AS win_rate FROM "
            "(SELECT sport,team_id,COUNT(*) AS matches,SUM(score_for>score_against) AS wins,"
            "SUM(score_for=score_against) AS draws,SUM(score_for<score_against) AS losses,"
            "SUM(score_for) AS score_for,SUM(score_against) AS score_against,"
            "MAX(completed_at) AS last_match_at FROM _sides WHERE status='finished' "
            "GROUP BY sport,team_id) g",
        )
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            for statement in statements:
                self.db.execute(statement)
        self._generation = generation

    def iter_rows(self, table: str) -> Iterator[dict]:
        """Stream canonical rows; callers choose whether to collect tiny tests."""
        if table not in TABLE_SCHEMAS:
            raise ValueError("Unknown oracle table")
        self._materialize()
        columns = ",".join(EVENT_COLUMNS)
        queries = {
            "bronze_observations": f"SELECT {columns} FROM observations ORDER BY match_id,revision",
            "silver_matches": f"SELECT {columns} FROM _current ORDER BY match_id",
            "silver_history": f"SELECT {','.join('e.' + c for c in EVENT_COLUMNS)},"
            "e.observed_at AS valid_from,(SELECT n.observed_at FROM observations n "
            "WHERE n.match_id=e.match_id AND n.revision>e.revision ORDER BY n.revision LIMIT 1) AS valid_to,"
            "NOT EXISTS(SELECT 1 FROM observations n WHERE n.match_id=e.match_id "
            "AND n.revision>e.revision) AS is_current FROM observations e ORDER BY e.match_id,e.revision",
            "gold_team_stats": f"SELECT {','.join(METRIC_COLUMNS)} FROM _gold ORDER BY sport,team_id",
        }
        cursor = self.db.execute(queries[table])
        try:
            while rows := cursor.fetchmany(self.batch_rows):
                for row in rows:
                    result = dict(row)
                    if table == "silver_history":
                        result["is_current"] = bool(result["is_current"])
                    yield result
        finally:
            cursor.close()

    def counts(self) -> dict:
        self._materialize()
        totals = dict(self.db.execute("SELECT received,accepted,replayed FROM ingest_totals").fetchone())
        totals.update(
            observations=self.db.execute("SELECT COUNT(*) FROM observations").fetchone()[0],
            matches=self.db.execute("SELECT COUNT(*) FROM _current").fetchone()[0],
            gold_rows=self.db.execute("SELECT COUNT(*) FROM _gold").fetchone()[0],
            cancelled_current_matches=self.db.execute("SELECT COUNT(*) FROM _current WHERE status='cancelled'")
            .fetchone()[0],
            cancellation_observations=self.db.execute("SELECT COUNT(*) FROM observations WHERE status='cancelled'")
            .fetchone()[0],
        )
        # Immediate prior revision, including late inserted intermediate rows.
        transitions = self.db.execute(
            "SELECT COUNT(*) AS revised_observations,"
            "COALESCE(SUM(e.status='finished' AND p.status='finished' AND "
            "(e.score_a!=p.score_a OR e.score_b!=p.score_b)),0) AS score_corrections,"
            "COALESCE(SUM(e.team_a!=p.team_a OR e.team_b!=p.team_b),0) AS rename_observations "
            "FROM observations e JOIN observations p ON p.match_id=e.match_id AND p.revision="
            "(SELECT MAX(n.revision) FROM observations n WHERE n.match_id=e.match_id AND n.revision<e.revision)"
        ).fetchone()
        totals.update(dict(transitions))
        return totals

    def export_reference(self, output_dir: Path) -> dict:
        """Write fresh explicit-schema Parquet tables in bounded Arrow batches."""
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=False)
        manifest = dict(oracle_version="sqlite-scale-oracle-1.0", tables={}, counts=self.counts(),
                        batch_rows=self.batch_rows, complete=False)
        observed_hash = hashlib.sha256()
        for name, schema in TABLE_SCHEMAS.items():
            target = output_dir / f"{name}.parquet"
            count, batch = 0, []
            with pq.ParquetWriter(target, schema, compression="snappy") as writer:
                for row in self.iter_rows(name):
                    if name == "bronze_observations":
                        observed_hash.update((_json(row) + "\n").encode("utf-8"))
                    batch.append(row)
                    if len(batch) == self.batch_rows:
                        writer.write_table(pa.Table.from_pylist(batch, schema=schema), row_group_size=self.batch_rows)
                        count += len(batch)
                        batch.clear()
                if batch:
                    writer.write_table(pa.Table.from_pylist(batch, schema=schema), row_group_size=self.batch_rows)
                    count += len(batch)
            manifest["tables"][name] = dict(file=target.name, rows=count, bytes=target.stat().st_size,
                                             sha256=_file_hash(target))
        manifest.update(complete=True, accepted_observation_sha256=observed_hash.hexdigest())
        (output_dir / "manifest.json").write_text(_json(manifest) + "\n", encoding="utf-8")
        return manifest

    def verify(self, spark, paths: Mapping[str, str]) -> dict:
        """Root's opt-in signature; reuse unchanged state, retain all exports.

        Exact replay still compares complete Spark multisets. Each changed
        accepted state gets a new reference; no measurement or failure evidence
        is deleted automatically. Plan disk capacity before large runs.
        """
        generation = self.db.execute("SELECT accepted FROM ingest_totals WHERE singleton=1").fetchone()[0]
        reused = self._reference_cache is not None and self._reference_cache[0] == generation
        if reused:
            reference_dir = self._reference_cache[1]
        else:
            reference_dir = self.database_path.parent / f"reference-{uuid.uuid4().hex}"
            self.export_reference(reference_dir)
            self._reference_cache = (generation, reference_dir)
        frames = {name: spark.read.format("delta").load(paths[name]) for name in TABLE_SCHEMAS}
        report = self.verify_spark(frames, reference_dir)
        report["reference_reused"] = reused
        report["reference_retention"] = "All changed-state reference exports retained; no automatic pruning"
        return report

    def verify_spark(self, frames: Mapping, reference_dir: Path, *, sample_limit: int = 5) -> dict:
        """Full duplicate-preserving differences; no complete dataset collect.

        ExceptAll resolves columns positionally: explicitly select identical
        canonical columns on both sides. All differences are counted in Spark;
        only at most ``sample_limit`` rows per direction enter the driver.
        Runtime audit columns are excluded. Active Gold excludes tombstones.
        """
        if type(sample_limit) is not int or not 0 <= sample_limit <= 20:
            raise ValueError("sample_limit must be an integer between 0 and 20")
        reference_dir = Path(reference_dir)
        manifest = json.loads((reference_dir / "manifest.json").read_text(encoding="utf-8"))
        if manifest.get("complete") is not True or set(manifest["tables"]) != set(TABLE_SCHEMAS):
            raise ValueError("Incomplete oracle reference export")
        report = dict(status="verified", method="full_row_multiset_exceptAll_both_directions",
                      reference_dir=str(reference_dir), reference_manifest=manifest, tables={},
                      input_basis="independently supplied accepted observations",
                      current_ingest_totals=dict(self.db.execute(
                          "SELECT received,accepted,replayed FROM ingest_totals WHERE singleton=1").fetchone()),
                      checked_at=datetime.now(timezone.utc).isoformat(), driver_sample_limit=sample_limit)
        for name, schema in TABLE_SCHEMAS.items():
            entry = manifest["tables"][name]
            expected_path = reference_dir / f"{name}.parquet"
            if _file_hash(expected_path) != entry["sha256"]:
                raise ValueError("Oracle reference checksum mismatch")
            frame = frames[name]
            if name == "gold_team_stats" and "deleted_at" in frame.columns:
                frame = frame.where("deleted_at IS NULL")
            columns = schema.names
            if name == "silver_history" and "is_current" not in frame.columns:
                columns = [c for c in columns if c != "is_current"]
            actual = frame.select(*columns)
            expected = frame.sparkSession.read.parquet(str(expected_path)).select(*columns)
            actual_types = dict(actual.dtypes)
            expected_types = dict(expected.dtypes)
            schema_equal = actual_types == expected_types
            missing = expected.exceptAll(actual)
            unexpected = actual.exceptAll(expected)
            missing_count, unexpected_count = missing.count(), unexpected.count()
            actual_count = entry["rows"] - missing_count + unexpected_count
            data = dict(expected_rows=entry["rows"], actual_rows=actual_count,
                        missing_rows=missing_count, unexpected_rows=unexpected_count,
                        schema_equal=schema_equal, expected_types=expected_types, actual_types=actual_types,
                        equal=schema_equal and missing_count == unexpected_count == 0,
                        missing_sample=[], unexpected_sample=[])
            if name == "silver_history":
                data["is_current_checked"] = "is_current" in columns
                data["nonoverlap_checked"] = True
            if sample_limit and missing_count:
                data["missing_sample"] = [row.asDict() for row in missing.limit(sample_limit).collect()]
            if sample_limit and unexpected_count:
                data["unexpected_sample"] = [row.asDict() for row in unexpected.limit(sample_limit).collect()]
            report["tables"][name] = data
        report["status"] = "verified" if all(r["equal"] for r in report["tables"].values()) else "mismatch"
        self.last_report = report
        if report["status"] != "verified":
            raise OracleMismatch(report)
        return report
