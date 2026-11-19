"""Exercise persisted identity conflicts and export isolation together."""

import json
from contextlib import closing

import pyarrow.parquet as pq

from arena_lakehouse.engine import Pipeline
from arena_lakehouse.snapshot import export_snapshot
from arena_lakehouse.synthetic import generate


def test_export_isolates_a_concurrent_deletion_from_persisted_conflicts(tmp_path, monkeypatch):
    pipeline = Pipeline(tmp_path / "lake")
    original = next(e for e in generate(30, faults=False) if e["sport"] == "football" and e["status"] == "finished")
    blocked = dict(original, match_id="blocked-b", event_id="shared-id")
    pipeline.ingest([blocked, dict(blocked, event_id="other-blocked-id", score_a=blocked["score_a"] + 1)])
    active = dict(original, match_id="active-a", event_id="alternate-a")
    sentinel = dict(original, match_id="sentinel", event_id="sentinel-event")
    pipeline.ingest([active, sentinel])
    previous_quality = pipeline.quality()
    connect = pipeline.connect
    with closing(connect()) as db:
        db.execute("PRAGMA journal_mode=WAL")
    concurrent_run = None

    def interleave(statement):
        nonlocal concurrent_run
        if statement.startswith("SELECT payload FROM events") and concurrent_run is None:
            concurrent_run = "in-progress"
            concurrent_run = pipeline.ingest([dict(active, event_id=blocked["event_id"])])["run_id"]

    def traced_connect():
        db = connect()
        db.set_trace_callback(interleave)
        return db

    monkeypatch.setattr(pipeline, "connect", traced_connect)
    output = tmp_path / "snapshots"
    first = export_snapshot(pipeline, output, with_predictions=False)
    assert concurrent_run not in (None, "in-progress")
    assert json.loads((first / "quality.json").read_text()) == previous_quality

    def match_ids(snapshot):
        bronze = [json.loads(line)["match_id"] for line in (snapshot / "bronze/events.jsonl").read_text().splitlines()]
        gold = pq.read_table(snapshot / "gold/matches.parquet", columns=["match_id"])["match_id"].to_pylist()
        assert sorted(bronze) == sorted(gold)
        quality = json.loads((snapshot / "quality.json").read_text())
        assert quality["summary"]["valid_events"] == len(bronze)
        return set(gold), quality["run_id"]

    assert match_ids(first) == ({active["match_id"], sentinel["match_id"]}, previous_quality["run_id"])
    second = export_snapshot(pipeline, output, with_predictions=False)
    assert match_ids(second) == ({sentinel["match_id"]}, concurrent_run)
    assert (output / "LATEST").read_text() == second.name

    restarted = Pipeline(pipeline.root)
    restarted.ingest([dict(active, event_id="retry-a"), dict(blocked, event_id="retry-b")])
    assert [e["match_id"] for e in restarted.events()] == [sentinel["match_id"]]
