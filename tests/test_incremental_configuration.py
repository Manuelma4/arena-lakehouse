"""A scale verifier must still gate publication and surface failures."""

import pytest

from arena_lakehouse.incremental import IncrementalPipeline


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
