"""Small independent, explicitly derived expectations; no Spark runtime needed."""

import hashlib
from collections import Counter

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from arena_lakehouse.scale_oracle import OracleConflict, OracleMismatch, ScaleOracle, TABLE_SCHEMAS


def event(match="m", revision=1, *, day=2, status="finished", **changes):
    raw = dict(event_id=f"{match}:{revision}", match_id=match, provider="synthetic",
               sport="football", competition="Independent tiny league", revision=revision,
               team_a_id="a", team_a="Alpha", team_b_id="b", team_b="Beta",
               start_time="2025-01-01T00:00:00Z", observed_at=f"2025-01-{day:02}T00:00:00Z",
               completed_at="2025-01-01T02:00:00Z" if status == "finished" else None,
               status=status, score_a=2 if status == "finished" else None,
               score_b=0 if status == "finished" else None, best_of=1)
    raw.update(changes)
    return raw


def rows(oracle, table):
    # These fixtures have at most a handful of rows. Production APIs stream.
    return list(oracle.iter_rows(table))


def test_intermediate_revision_repairs_intervals_without_changing_current(tmp_path):
    old = event(revision=1, day=2, score_a=4, score_b=1)
    middle = event(revision=2, day=3, score_a=1, score_b=1)
    cancelled = event(revision=3, day=4, status="cancelled")
    with ScaleOracle(tmp_path / "oracle.db", batch_rows=1) as oracle:
        oracle.ingest_batch(iter([cancelled, old]))
        first = rows(oracle, "silver_history")
        assert [(r["revision"], r["valid_to"]) for r in first] == [
            (1, "2025-01-04T00:00:00.000000Z"), (3, None),
        ]
        oracle.add(iter([middle]))
        history = rows(oracle, "silver_history")
        assert [(r["revision"], r["valid_from"], r["valid_to"], r["is_current"]) for r in history] == [
            (1, "2025-01-02T00:00:00.000000Z", "2025-01-03T00:00:00.000000Z", False),
            (2, "2025-01-03T00:00:00.000000Z", "2025-01-04T00:00:00.000000Z", False),
            (3, "2025-01-04T00:00:00.000000Z", None, True),
        ]
        assert [(r["revision"], r["status"]) for r in rows(oracle, "silver_matches")] == [(3, "cancelled")]
        assert rows(oracle, "gold_team_stats") == []
        assert oracle.counts()["score_corrections"] == 1
        assert oracle.counts()["revised_observations"] == 2
        assert oracle.counts()["cancellation_observations"] == 1


def test_gold_names_completion_maximum_maps_and_disconnected_cancellation(tmp_path):
    records = [
        event("f1", day=8, completed_at="2025-01-07T00:00:00Z"),
        event("f2", day=9, team_b_id="c", team_b="Cedar", score_a=0, score_b=0,
              start_time="2025-01-03T00:00:00Z", completed_at="2025-01-04T00:00:00Z"),
        event("f3", day=10, team_a_id="b", team_a="Beta", team_b_id="a", team_b="Alpha",
              score_a=3, score_b=1, completed_at="2025-01-06T00:00:00Z"),
        event("a-name", day=11, status="scheduled", team_a="Alpha Scheduled",
              start_time="2025-02-01T00:00:00Z"),
        event("z-name", day=11, status="cancelled", team_a="Alpha Renamed"),
        event("lone", day=2, team_a_id="d", team_a="Delta", team_b_id="e", team_b="Echo"),
        event("lone", revision=2, day=12, status="cancelled", team_a_id="d", team_a="Delta",
              team_b_id="e", team_b="Echo"),
        event("v", day=6, sport="valorant", competition="Independent tiny series", best_of=3,
              team_a="Valorant Alpha", team_b_id="v", team_b="Vector", score_a=2, score_b=1,
              completed_at="2025-01-05T00:00:00Z"),
    ]
    with ScaleOracle(tmp_path / "oracle.db", batch_rows=2) as oracle:
        oracle.ingest_batch(iter(reversed(records)))
        expected = [
            dict(sport="football", team_id="a", team_name="Alpha Renamed", matches=3,
                 wins=1, draws=1, losses=1, score_for=3, score_against=3,
                 last_match_at="2025-01-07T00:00:00.000000Z", win_rate=1 / 3),
            dict(sport="football", team_id="b", team_name="Beta", matches=2,
                 wins=1, draws=0, losses=1, score_for=3, score_against=3,
                 last_match_at="2025-01-07T00:00:00.000000Z", win_rate=0.5),
            dict(sport="football", team_id="c", team_name="Cedar", matches=1,
                 wins=0, draws=1, losses=0, score_for=0, score_against=0,
                 last_match_at="2025-01-04T00:00:00.000000Z", win_rate=0.0),
            dict(sport="valorant", team_id="a", team_name="Valorant Alpha", matches=1,
                 wins=1, draws=0, losses=0, score_for=2, score_against=1,
                 last_match_at="2025-01-05T00:00:00.000000Z", win_rate=1.0),
            dict(sport="valorant", team_id="v", team_name="Vector", matches=1,
                 wins=0, draws=0, losses=1, score_for=1, score_against=2,
                 last_match_at="2025-01-05T00:00:00.000000Z", win_rate=0.0),
        ]
        assert rows(oracle, "gold_team_stats") == expected
        assert oracle.counts()["cancelled_current_matches"] == 2
        before = oracle.export_reference(tmp_path / "before")
        assert oracle.ingest_batch(iter(records))["replayed"] == 8
        after = oracle.export_reference(tmp_path / "after")
        assert before["accepted_observation_sha256"] == after["accepted_observation_sha256"]
        assert {k: v["sha256"] for k, v in before["tables"].items()} == {
            k: v["sha256"] for k, v in after["tables"].items()
        }
        assert oracle.counts()["received"] == 16
        assert oracle.counts()["accepted"] == 8


@pytest.mark.parametrize("change", [
    dict(event_id="conflict", score_a=3),
    dict(match_id="other", event_id="m:1"),
    dict(revision=2, event_id="m:2", observed_at="2025-01-02T00:00:00Z"),
    dict(revision=2, event_id="m:2", observed_at="2025-01-03T00:00:00Z", team_b_id="changed"),
])
def test_conflicts_roll_back_candidate_rows_and_counters(tmp_path, change):
    with ScaleOracle(tmp_path / "oracle.db") as oracle:
        oracle.add([event()])
        before = oracle.counts()
        with pytest.raises(OracleConflict):
            oracle.ingest_batch(iter([event("unrelated"), dict(event(), **change)]))
        assert oracle.counts() == before
        assert [r["match_id"] for r in rows(oracle, "bronze_observations")] == ["m"]


def test_generator_exception_rolls_back_and_database_reopens(tmp_path):
    def broken():
        yield event()
        raise RuntimeError("input stream failed")
    database = tmp_path / "a # 20%" / "oracle.db"
    with ScaleOracle(database) as oracle:
        with pytest.raises(RuntimeError, match="input stream failed"):
            oracle.add(broken())
        assert oracle.counts()["observations"] == 0
        oracle.add([event()])
    with ScaleOracle(database) as reopened:
        assert reopened.counts()["observations"] == 1
        assert reopened.add([event()])["replayed"] == 1
    database.unlink()  # All handles are closed on Windows too.


def test_real_provider_replay_retains_first_accepted_availability(tmp_path):
    first = event(provider="football-data")
    replay = dict(first, observed_at="2025-01-10T00:00:00+02:00",
                  completed_at="2025-01-01T03:00:00Z")
    with ScaleOracle(tmp_path / "oracle.db") as oracle:
        oracle.add([first, replay])
        accepted = rows(oracle, "bronze_observations")
        assert len(accepted) == 1
        assert accepted[0]["observed_at"] == "2025-01-02T00:00:00.000000Z"
        assert accepted[0]["completed_at"] == "2025-01-01T02:00:00.000000Z"


def test_typed_empty_exports_and_bounded_row_groups(tmp_path):
    with ScaleOracle(tmp_path / "empty.db", batch_rows=2) as oracle:
        empty = oracle.export_reference(tmp_path / "empty")
        assert empty["complete"]
        for name, schema in TABLE_SCHEMAS.items():
            file = pq.ParquetFile(tmp_path / "empty" / empty["tables"][name]["file"])
            assert file.schema_arrow == schema
            assert file.metadata.num_rows == 0
        def stream():
            for number in range(7):
                yield event(f"match-{number}")
        oracle.ingest_batch(stream())
        export = oracle.export_reference(tmp_path / "populated")
        for name, entry in export["tables"].items():
            path = tmp_path / "populated" / entry["file"]
            file = pq.ParquetFile(path)
            assert file.schema_arrow == TABLE_SCHEMAS[name]
            assert hashlib.sha256(path.read_bytes()).hexdigest() == entry["sha256"]
            assert max(file.metadata.row_group(i).num_rows for i in range(file.metadata.num_row_groups)) <= 2
        assert pq.read_table(tmp_path / "populated/bronze_observations.parquet").schema.field("revision").type == pa.int32()
        with pytest.raises(FileExistsError):
            oracle.export_reference(tmp_path / "populated")


def test_engine_parity_is_an_additional_crosscheck_not_the_expected_fixture(tmp_path):
    from arena_lakehouse.engine import history, latest, team_stats
    from arena_lakehouse.schema import MatchEvent
    accepted = [MatchEvent.model_validate(e).row() for e in [
        event(revision=3, day=4, team_a="New Alpha"), event(revision=1),
        event(revision=2, day=3, score_a=0, score_b=0), event("disconnected", status="scheduled"),
    ]]
    with ScaleOracle(tmp_path / "oracle.db") as oracle:
        oracle.add(iter(accepted))
        assert rows(oracle, "silver_matches") == latest(accepted)
        actual = [{k: v for k, v in r.items() if k != "is_current"} for r in rows(oracle, "silver_history")]
        assert actual == history(accepted)
        assert rows(oracle, "gold_team_stats") == team_stats(latest(accepted))


class FakeRow(dict):
    def asDict(self):
        return dict(self)


class TinyFrame:
    """Test-only multiset implementation; collecting requires an explicit limit."""
    def __init__(self, rows, dtypes, session, *, bounded=False):
        self.rows, self.dtypes, self.sparkSession = rows, dtypes, session
        self.columns = [name for name, _ in dtypes]
        self.bounded = bounded

    def select(self, *columns):
        return TinyFrame([{k: r[k] for k in columns} for r in self.rows],
                         [(k, dict(self.dtypes)[k]) for k in columns], self.sparkSession)

    def exceptAll(self, other):
        left = Counter(tuple(r[k] for k in self.columns) for r in self.rows)
        right = Counter(tuple(r[k] for k in self.columns) for r in other.rows)
        return TinyFrame([dict(zip(self.columns, value)) for value, n in (left-right).items()
                          for _ in range(n)], self.dtypes, self.sparkSession)

    def count(self):
        return len(self.rows)

    def limit(self, n):
        return TinyFrame(self.rows[:n], self.dtypes, self.sparkSession, bounded=True)

    def collect(self):
        assert self.bounded, "Verifier attempted an unbounded driver collect"
        return [FakeRow(r) for r in self.rows]


class TinyReader:
    def __init__(self):
        self.read = self
        self.frames = {}

    def format(self, name):
        assert name == "delta"
        return self

    def load(self, path):
        return self.frames[path]

    def parquet(self, path):
        table = pq.read_table(path)
        types = {pa.string(): "string", pa.int32(): "int", pa.int64(): "bigint",
                 pa.bool_(): "boolean", pa.float64(): "double"}
        return TinyFrame(table.to_pylist(), [(f.name, types[f.type]) for f in table.schema], self)


def test_full_multiset_check_detects_same_count_duplicate_and_bounds_diagnostics(tmp_path):
    with ScaleOracle(tmp_path / "oracle.db") as oracle:
        oracle.add([event("one"), event("two")])
        directory = tmp_path / "reference"
        oracle.export_reference(directory)
        reader = TinyReader()
        frames = {name: reader.parquet(str(directory / f"{name}.parquet")) for name in TABLE_SCHEMAS}
        # Equal row counts and identical team totals are insufficient: one
        # canonical match is silently replaced by a duplicate of another.
        silver = frames["silver_matches"]
        silver.rows = [silver.rows[0], silver.rows[0]]
        with pytest.raises(OracleMismatch) as failure:
            oracle.verify_spark(frames, directory, sample_limit=1)
        report = failure.value.report
        comparison = report["tables"]["silver_matches"]
        assert comparison["actual_rows"] == comparison["expected_rows"] == 2
        assert comparison["missing_rows"] == comparison["unexpected_rows"] == 1
        assert len(comparison["missing_sample"]) == len(comparison["unexpected_sample"]) == 1
        assert report["tables"]["silver_history"]["is_current_checked"]
        assert oracle.last_report is report


def test_unchanged_replay_reuses_reference_but_still_compares_all_tables(tmp_path):
    with ScaleOracle(tmp_path / "oracle.db") as oracle:
        oracle.add([event("one")])
        seed = tmp_path / "seed"
        oracle.export_reference(seed)
        reader = TinyReader()
        reader.frames = {name: reader.parquet(str(seed / f"{name}.parquet")) for name in TABLE_SCHEMAS}
        paths = {name: name for name in TABLE_SCHEMAS}
        first = oracle.verify(reader, paths)
        oracle.add([event("one")])
        second = oracle.verify(reader, paths)
        assert first["status"] == second["status"] == "verified"
        assert first["reference_reused"] is False
        assert second["reference_reused"] is True
        assert first["reference_dir"] == second["reference_dir"]
        assert second["current_ingest_totals"] == dict(received=2, accepted=1, replayed=1)
        assert set(second["tables"]) == set(TABLE_SCHEMAS)
        assert all(t["equal"] for t in second["tables"].values())
        assert len(list(tmp_path.glob("reference-*"))) == 1
        # Corruption on replay must not be hidden by reference reuse.
        reader.frames["silver_matches"].rows.clear()
        with pytest.raises(OracleMismatch):
            oracle.verify(reader, paths)
        assert len(list(tmp_path.glob("reference-*"))) == 1


def test_no_overlap_uses_normalized_instants_and_not_timestamp_spelling(tmp_path):
    one = event(observed_at="2025-01-02T01:30:00+02:00")
    two = event(revision=2, day=3, observed_at="2025-01-02T00:00:00Z")
    with ScaleOracle(tmp_path / "oracle.db") as oracle:
        oracle.add([two, one])
        history = rows(oracle, "silver_history")
        assert history[0]["valid_from"] == "2025-01-01T23:30:00.000000Z"
        assert history[0]["valid_to"] == history[1]["valid_from"]
        assert sum(r["is_current"] for r in history) == 1
        with pytest.raises(OracleConflict):
            oracle.add([event(revision=3, observed_at="2025-01-02T01:00:00+01:00")])


@pytest.mark.parametrize("batch_rows", [0, -1, True, 1.0, 65537])
def test_invalid_resource_batch_bound_rejected_before_database_creation(tmp_path, batch_rows):
    with pytest.raises(ValueError, match="batch_rows"):
        ScaleOracle(tmp_path / "must-not-exist.db", batch_rows=batch_rows)
    assert not (tmp_path / "must-not-exist.db").exists()
