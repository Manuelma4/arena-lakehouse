import random
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from arena_lakehouse.engine import Pipeline, history, latest, team_stats
from arena_lakehouse.prediction import features, train_and_predict
from arena_lakehouse.schema import MatchEvent
from arena_lakehouse.snapshot import export_snapshot
from arena_lakehouse.synthetic import generate


def test_export_does_not_follow_a_stale_pointer_symlink(tmp_path):
    pipeline = Pipeline(tmp_path / "lake")
    pipeline.ingest(generate(20, faults=False))
    output = tmp_path / "snapshots"
    output.mkdir()
    sentinel = tmp_path / "sentinel"
    sentinel.write_text("keep", encoding="utf-8")
    (output / ".LATEST.tmp").symlink_to(sentinel)

    snapshot = export_snapshot(pipeline, output, with_predictions=False)

    assert sentinel.read_text(encoding="utf-8") == "keep"
    assert not (output / "LATEST").is_symlink()
    assert (output / "LATEST").read_text(encoding="utf-8") == snapshot.name


def test_interleaved_exports_publish_independent_pointers(tmp_path, monkeypatch):
    pipeline = Pipeline(tmp_path / "lake")
    pipeline.ingest(generate(20, faults=False))
    output = tmp_path / "snapshots"
    replace = Path.replace
    other_snapshot = None
    interleaved = False

    def interleave(pointer, target):
        nonlocal other_snapshot, interleaved
        if target.name == "LATEST" and not interleaved:
            interleaved = True
            other_snapshot = export_snapshot(pipeline, output, with_predictions=False)
        return replace(pointer, target)

    monkeypatch.setattr(Path, "replace", interleave)
    snapshot = export_snapshot(pipeline, output, with_predictions=False)

    assert other_snapshot is not None and other_snapshot != snapshot
    assert (other_snapshot / "manifest.json").is_file()
    assert (snapshot / "manifest.json").is_file()
    assert (output / "LATEST").read_text(encoding="utf-8") == snapshot.name
    assert not list(output.glob(".LATEST*.tmp"))


def test_replay_is_idempotent_and_quarantine_is_stable(tmp_path):
    pipeline = Pipeline(tmp_path)
    records = generate(100)
    first = pipeline.ingest(records)
    before = pipeline.events()
    summary = pipeline.quality()["summary"]
    second = pipeline.ingest(records)
    assert first["duplicates"] == 2
    assert first["quarantined"] == 2
    assert second["accepted"] == 0
    assert pipeline.events() == before
    assert pipeline.quality()["summary"]["quarantined"] == summary["quarantined"] == 2


@pytest.mark.parametrize("seed", range(5))
def test_order_independence_and_scd2_intervals(tmp_path, seed):
    records = generate(80, seed=seed, faults=False)
    ordered = Pipeline(tmp_path / "ordered")
    shuffled = Pipeline(tmp_path / "shuffled")
    ordered.ingest(sorted(records, key=lambda e: (e["match_id"], e["revision"])))
    random.Random(seed + 100).shuffle(records)
    shuffled.ingest(records)
    assert shuffled.events() == ordered.events()
    assert team_stats(latest(shuffled.events())) == team_stats(latest(ordered.events()))
    for row in history(shuffled.events()):
        assert row["valid_to"] is None or row["valid_from"] < row["valid_to"]


def test_failure_rolls_back_and_replay_recovers(tmp_path):
    records = generate(60)
    pipeline = Pipeline(tmp_path / "actual")
    with pytest.raises(RuntimeError, match="Injected"):
        pipeline.ingest(records, fail_after=10)
    assert pipeline.events() == []
    assert pipeline.quality()["summary"]["last_run_status"] == "failed"
    with pytest.raises(ValueError, match="successful"):
        export_snapshot(pipeline, tmp_path / "snapshots")
    pipeline.ingest(records)
    expected = Pipeline(tmp_path / "expected")
    expected.ingest(records)
    assert pipeline.events() == expected.events()


def test_ambiguous_revision_cannot_be_reintroduced(tmp_path):
    original = next(e for e in generate(30, faults=False) if e["sport"] == "football" and e["status"] == "finished")
    conflict = dict(original, event_id="conflict", score_a=original["score_a"] + 1)
    pipeline = Pipeline(tmp_path)
    pipeline.ingest([original, conflict])
    assert pipeline.events() == []
    pipeline.ingest([dict(original, event_id="third-attempt")])
    assert pipeline.events() == []


def test_late_correction_converges_with_full_reconstruction(tmp_path):
    records = generate(60, faults=False)
    old = next(e for e in latest(records) if e["sport"] == "football" and e["status"] == "finished")
    later = (datetime.fromisoformat(old["observed_at"].replace("Z", "+00:00")) + timedelta(days=10)).isoformat()
    correction = MatchEvent.model_validate(dict(old, event_id="late", revision=3,
                                               observed_at=later, score_a=old["score_a"] + 1)).row()
    incremental = Pipeline(tmp_path / "incremental")
    incremental.ingest(records)
    incremental.ingest([correction])
    full = Pipeline(tmp_path / "full")
    full.ingest([correction] + records)
    assert incremental.events() == full.events()
    assert team_stats(latest(incremental.events())) == team_stats(latest(full.events()))


def test_future_results_and_target_label_do_not_change_features():
    events = generate(140, faults=False)
    target = sorted([e for e in latest(events) if e["status"] == "finished"], key=lambda e: e["start_time"])[40]
    original = features(target, events, target["start_time"])
    altered = []
    for event in events:
        change = dict(event)
        if event["observed_at"] >= target["start_time"] and event["status"] == "finished":
            change["score_a"], change["score_b"] = event["score_b"], event["score_a"]
        altered.append(change)
    assert features(target, altered, target["start_time"]) == original
    assert original[1]["latest_result_used"] is None or original[1]["latest_result_used"] < target["start_time"]


def test_model_chronology_probability_contract_and_data_kind():
    predictions, report = train_and_predict(generate(240, faults=False))
    assert report["data_kind"] == "synthetic"
    assert predictions
    for prediction in predictions:
        assert sum(prediction["probabilities"].values()) == pytest.approx(1)
        assert prediction["as_of"] < prediction["start_time"]
    for sport in report["sports"].values():
        assert sport["train_last_kickoff"] < sport["validation_first_kickoff"] < sport["test_first_kickoff"]
        assert "frequency_baseline" in sport["test"]


def test_real_results_collected_today_cannot_supply_past_features():
    events = generate(80, faults=False)
    for event in events:
        event["observed_at"] = "2026-10-06T12:00:00.000000Z"
    _, report = train_and_predict(events)
    assert all(r["status"] == "insufficient_point_in_time_history" for r in report["sports"].values())


def test_export_rejects_generation_before_observation_and_preserves_latest(tmp_path):
    pipeline = Pipeline(tmp_path / "pipeline")
    pipeline.ingest(generate(30, faults=False))
    cutoff = datetime.fromisoformat(max(event["observed_at"] for event in pipeline.events()))
    snapshots = tmp_path / "snapshots"
    first = export_snapshot(pipeline, snapshots, with_predictions=False, generated_at=cutoff)
    pointer = snapshots / "LATEST"
    assert pointer.read_text().strip() == first.name
    before = {path.name for path in snapshots.iterdir()}

    with pytest.raises(ValueError, match="before the latest observation"):
        export_snapshot(pipeline, snapshots, with_predictions=False, generated_at=cutoff - timedelta(microseconds=1))

    assert pointer.read_text().strip() == first.name
    assert {path.name for path in snapshots.iterdir()} == before


def test_failed_snapshot_write_cleans_staging_and_preserves_latest(tmp_path, monkeypatch):
    import pyarrow.parquet as pq

    pipeline = Pipeline(tmp_path / "pipeline")
    pipeline.ingest(generate(30, faults=False))
    snapshots = tmp_path / "snapshots"
    first = export_snapshot(pipeline, snapshots, with_predictions=False)
    pointer = snapshots / "LATEST"
    before = {path.name for path in snapshots.iterdir()}
    write_table = pq.write_table

    def fail_after_first_table(table, path, *args, **kwargs):
        if path.name == "team_stats.parquet":
            assert path.with_name("matches.parquet").is_file()
            raise OSError("injected snapshot write failure")
        return write_table(table, path, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(pq, "write_table", fail_after_first_table)
        with pytest.raises(OSError, match="injected snapshot write failure"):
            export_snapshot(pipeline, snapshots, with_predictions=False)

    assert pointer.read_text().strip() == first.name
    assert {path.name for path in snapshots.iterdir()} == before
    retried = export_snapshot(pipeline, snapshots, with_predictions=False)
    assert retried != first
    assert pointer.read_text().strip() == retried.name


def test_failed_pointer_replace_preserves_selection_and_cleans_its_staging_file(tmp_path, monkeypatch):
    pipeline = Pipeline(tmp_path / "lake")
    pipeline.ingest(generate(30, faults=False))
    output = tmp_path / "snapshots"
    first = export_snapshot(pipeline, output, with_predictions=False)
    pointer = output / "LATEST"
    original_selection = pointer.read_bytes()
    original_manifest = (first / "manifest.json").read_bytes()
    replace = Path.replace
    unselected = None

    def fail_pointer(source, target):
        nonlocal unselected
        if target == pointer:
            unselected = output / source.read_text(encoding="utf-8")
            assert (unselected / "manifest.json").is_file()
            assert (unselected / "gold/matches.parquet").is_file()
            raise OSError("injected pointer replacement failure")
        return replace(source, target)

    with monkeypatch.context() as patch:
        patch.setattr(Path, "replace", fail_pointer)
        with pytest.raises(OSError, match="injected pointer replacement failure"):
            export_snapshot(pipeline, output, with_predictions=False)

    assert unselected is not None and unselected != first
    assert (unselected / "manifest.json").is_file()
    assert pointer.read_bytes() == original_selection
    assert (first / "manifest.json").read_bytes() == original_manifest
    assert not list(output.glob(".staging-*"))
    assert not list(output.glob(".LATEST*.tmp"))
    retried = export_snapshot(pipeline, output, with_predictions=False)
    assert retried not in (first, unselected)
    assert pointer.read_text(encoding="utf-8") == retried.name


def test_only_upcoming_matches_keep_empty_gold_schema(tmp_path):
    import pyarrow.parquet as pq
    records = [r for r in generate(30, faults=False) if r["status"] == "scheduled"]
    pipeline = Pipeline(tmp_path / "pipeline")
    pipeline.ingest(records)
    snapshot = export_snapshot(pipeline, tmp_path / "snapshots")
    table = pq.read_table(snapshot / "gold/team_stats.parquet")
    assert table.num_rows == 0
    assert {"team_name", "sport", "wins", "win_rate"} <= set(table.column_names)
