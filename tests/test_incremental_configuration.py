"""Verification and publication bookkeeping work without starting Spark."""

import json
from pathlib import Path

import pytest

from arena_lakehouse.incremental import IncrementalPipeline
from arena_lakehouse.operations import STAGES


def test_independent_verification_receives_actual_paths_and_propagates_failure(tmp_path):
    calls = []
    spark = object()

    def reject(actual_spark, paths):
        calls.append((actual_spark, paths))
        raise AssertionError("Independent current-state mismatch")

    pipeline = IncrementalPipeline(spark, tmp_path, verification=reject, bronze_timeout_seconds=900)
    with pytest.raises(AssertionError, match="current-state mismatch"):
        pipeline._verify({})
    assert calls == [(spark, pipeline.paths)]
    assert pipeline.bronze_timeout_seconds == 900
    assert all(str(tmp_path) in path for path in pipeline.paths.values())


@pytest.mark.parametrize("timeout", [0, 3601, True, 1.5, "180"])
def test_invalid_deadline_is_rejected_before_creating_workspace(tmp_path, timeout):
    root = tmp_path / "unused"
    with pytest.raises(ValueError, match="bronze_timeout_seconds"):
        IncrementalPipeline(None, root, bronze_timeout_seconds=timeout)
    assert not root.exists()


def test_default_demo_verification_and_deadline_are_retained(tmp_path):
    pipeline = IncrementalPipeline(None, tmp_path)
    assert pipeline.verification is None
    assert pipeline.bronze_timeout_seconds == 180


@pytest.mark.parametrize("previous_marker", [None, "{", '{"run_id": "older-run"}'])
def test_resume_recovers_publication_marker_after_verified_run(tmp_path, monkeypatch, previous_marker):
    pipeline = IncrementalPipeline(None, tmp_path)
    calls = []
    for stage in STAGES:
        monkeypatch.setattr(pipeline, f"_{stage}", lambda _, stage=stage: calls.append(stage))
    run = pipeline.ledger.create({"accepted": 2, "matches": ["example"], "teams": [["football", "team"]]})
    pipeline.ledger.execute(run, {stage: getattr(pipeline, f"_{stage}") for stage in STAGES})
    marker = tmp_path / "last-success.json"
    if previous_marker is not None:
        marker.write_text(previous_marker, encoding="utf-8")

    restored = IncrementalPipeline(None, tmp_path)
    for stage in STAGES:
        monkeypatch.setattr(restored, f"_{stage}", lambda _: pytest.fail("A verified stage was rerun"))
    result = restored.resume(run)

    assert result["status"] == "succeeded"
    assert calls == list(STAGES)
    saved = json.loads(marker.read_text(encoding="utf-8"))
    assert saved["run_id"] == run
    assert (saved["accepted"], saved["changed_matches"], saved["affected_teams"]) == (2, 1, 1)
    assert saved["succeeded_at"] == restored.ledger.attempts(run)[-1]["finished_at"]
    before = marker.read_bytes()
    restored.resume(run)
    assert marker.read_bytes() == before


@pytest.mark.parametrize("remove_marker", [False, True])
def test_resuming_older_success_preserves_latest_publication_marker(tmp_path, monkeypatch, remove_marker):
    pipeline = IncrementalPipeline(None, tmp_path)
    for stage in STAGES:
        monkeypatch.setattr(pipeline, f"_{stage}", lambda _: None)
    runs = []
    for accepted in (1, 2):
        run = pipeline.ledger.create({"accepted": accepted, "matches": [], "teams": []})
        pipeline.resume(run)
        runs.append(run)
    marker = tmp_path / "last-success.json"
    before = marker.read_bytes()
    assert json.loads(before)["run_id"] == runs[-1]
    if remove_marker:
        marker.unlink()
    pipeline.resume(runs[0])
    assert marker.read_bytes() == before


def test_failed_marker_replacement_preserves_previous_success_and_can_resume(tmp_path, monkeypatch):
    pipeline = IncrementalPipeline(None, tmp_path)
    for stage in STAGES:
        monkeypatch.setattr(pipeline, f"_{stage}", lambda _: None)
    first = pipeline.ledger.create({"accepted": 1, "matches": [], "teams": []})
    pipeline.resume(first)
    marker = tmp_path / "last-success.json"
    before = marker.read_bytes()
    second = pipeline.ledger.create({"accepted": 2, "matches": [], "teams": []})
    replace = Path.replace

    def fail_marker(source, destination):
        if destination == marker:
            raise OSError("Synthetic marker replacement failure")
        return replace(source, destination)

    with monkeypatch.context() as patch:
        patch.setattr(Path, "replace", fail_marker)
        with pytest.raises(OSError, match="marker replacement"):
            pipeline.resume(second)
    assert pipeline.ledger.job(second)["status"] == "succeeded"
    assert marker.read_bytes() == before
    pipeline.resume(second)
    assert json.loads(marker.read_text(encoding="utf-8"))["run_id"] == second
    assert not marker.with_suffix(".tmp").exists()
