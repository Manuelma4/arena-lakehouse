"""Preflight regressions over original observations, without a Spark runtime.

The relational double supplies retained rows using inner/semi join semantics.
Tests exercise the actual replay/validation/ledger/quarantine path and keep Delta
execution separate. They do not certify Spark/Delta execution or throughput.
"""

import copy
import json

import pytest

from arena_lakehouse.incremental import IncrementalPipeline
from arena_lakehouse.synthetic import generate


class Row:
    def __init__(self, value):
        self.value = value

    def asDict(self):
        return dict(self.value)


class Relation:
    def __init__(self, rows):
        self.rows = [dict(row) for row in rows]

    def join(self, other, key, how="inner"):
        assert how in ("inner", "left_semi")
        keys = {row[key] for row in other.rows}
        return Relation(row for row in self.rows if row[key] in keys)

    def unionByName(self, other):
        return Relation(self.rows + other.rows)

    def dropDuplicates(self, keys):
        seen, rows = set(), []
        for row in self.rows:
            identity = tuple(row[key] for key in keys)
            if identity not in seen:
                seen.add(identity)
                rows.append(row)
        return Relation(rows)

    def select(self, *columns):
        return Relation({key: row[key] for key in columns} for row in self.rows)

    def collect(self):
        return [Row(row) for row in self.rows]


class Session:
    def createDataFrame(self, rows, schema):
        columns = [field.strip().split()[0] for field in schema.split(",")]
        return Relation(dict(zip(columns, values, strict=True)) for values in rows)


class PreflightPipeline(IncrementalPipeline):
    def __init__(self, root, retained):
        super().__init__(Session(), root)
        self.retained = copy.deepcopy(retained)
        self.executions = []

    def _exists(self, name):
        assert name == "bronze_observations"
        return bool(self.retained)

    def _read(self, name):
        assert name == "bronze_observations"
        return Relation(self.retained)

    def _execute(self, run_id, fail_after=None):
        # Successful preflight inputs reach this boundary. No Delta/model work.
        job = self.ledger.job(run_id)
        self.executions.append(job["payload"])
        return job


def completed():
    return [row for row in generate(40, faults=False)
            if row["sport"] == "football" and row["status"] == "finished"][:3]


def assert_rejected_before_stages(pipeline, candidates):
    before = copy.deepcopy(pipeline.retained)
    with pytest.raises(ValueError, match="Conflicting event ID"):
        pipeline.replay(candidates)
    assert pipeline.retained == before
    assert pipeline.executions == []
    assert pipeline.ledger.pending() is None
    with pipeline.ledger.connect() as db:
        assert db.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM attempts").fetchone()[0] == 0
    reports = list((pipeline.root / "quarantine").glob("*.json"))
    assert len(reports) == 1
    report = json.loads(reports[0].read_text(encoding="utf-8"))
    assert report["status"] == "entire_candidate_rejected"
    assert report["error"] == "ValueError"
    raw = pipeline.root / report["raw_file"]
    assert [json.loads(line) for line in raw.read_text(encoding="utf-8").splitlines()] == candidates


def test_retained_event_id_cannot_be_reassigned_to_an_unrelated_match(tmp_path):
    old, other, _ = completed()
    candidate = dict(other, event_id=old["event_id"])
    assert candidate["match_id"] != old["match_id"]
    pipeline = PreflightPipeline(tmp_path, [old])
    assert_rejected_before_stages(pipeline, [candidate])


def test_historical_event_id_cannot_be_reassigned_when_current_revision_has_another_id(tmp_path):
    current, other, _ = completed()
    historical = next(row for row in generate(40, faults=False)
                      if row["match_id"] == current["match_id"] and row["revision"] < current["revision"])
    assert historical["status"] == "scheduled"
    assert historical["event_id"] != current["event_id"]
    pipeline = PreflightPipeline(tmp_path, [historical, current])
    assert_rejected_before_stages(pipeline, [dict(other, event_id=historical["event_id"])])


@pytest.mark.parametrize("reverse", [False, True])
def test_cross_match_collision_rejects_entire_batch_including_valid_new_rows(tmp_path, reverse):
    old, other, valid = completed()
    collision = dict(other, event_id=old["event_id"])
    candidates = [valid, collision] if not reverse else [collision, valid]
    pipeline = PreflightPipeline(tmp_path, [old])
    assert_rejected_before_stages(pipeline, candidates)


def test_identity_lookup_does_not_turn_exact_replay_into_a_conflict(tmp_path):
    old, _, _ = completed()
    pipeline = PreflightPipeline(tmp_path, [old])
    result = pipeline.replay([old, old])
    assert result["payload"]["accepted"] == 0
    assert result["payload"]["duplicates"] == 2
    assert result["payload"]["events"] == []
    assert len(pipeline.executions) == 1
    assert not (pipeline.root / "quarantine").exists()


def test_same_match_chain_still_rejects_a_different_event_at_an_existing_revision(tmp_path):
    old, _, _ = completed()
    candidate = dict(old, event_id=old["event_id"] + ":conflict", score_a=old["score_a"] + 1)
    pipeline = PreflightPipeline(tmp_path, [old])
    assert_rejected_before_stages(pipeline, [candidate])


def test_unrelated_retained_rows_do_not_block_a_valid_candidate(tmp_path):
    old, other, _ = completed()
    pipeline = PreflightPipeline(tmp_path, [old])
    result = pipeline.replay([other])
    assert result["payload"]["accepted"] == 1
    assert result["payload"]["duplicates"] == 0
    assert result["payload"]["events"] == [other]
    assert result["payload"]["matches"] == [other["match_id"]]
    assert old["match_id"] not in result["payload"]["matches"]


def test_api_retry_clock_is_preserved_as_duplicate(tmp_path):
    original = dict(completed()[0], provider="football-data")
    retry = dict(original, observed_at="2026-10-07T15:00:00.000000Z")
    pipeline = PreflightPipeline(tmp_path, [original])
    result = pipeline.replay([retry])
    assert result["payload"]["accepted"] == 0
    assert result["payload"]["duplicates"] == 1
    assert pipeline.retained == [original]
