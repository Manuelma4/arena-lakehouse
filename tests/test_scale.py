import hashlib
import json
from dataclasses import replace
from datetime import datetime, timedelta
from itertools import islice

import pytest

from arena_lakehouse.scale import (
    ORIGIN,
    WorkloadConfig,
    _pair,
    iter_batches,
    iter_observations,
    resource_sample,
    run_scenario,
    validate_workload,
    write_workload,
)
from arena_lakehouse.schema import canonical


def config(**changes):
    return WorkloadConfig(**dict(observations=100, namespace="scale-test", shard_observations=40,
                                shuffle_buffer=20, **changes))


@pytest.mark.parametrize("changes", [
    {"observations": 39}, {"observations": 41}, {"observations": 1_000_002}, {"observations": True},
    {"namespace": "../outside"}, {"namespace": "Real Team"}, {"seed": -1},
    {"shard_observations": 100_002}, {"shard_observations": 3},
    {"shuffle_buffer": 42}, {"shuffle_buffer": 3}, {"teams_per_sport": 17}, {"horizon_days": 32},
])
def test_config_rejects_unsafe_or_unbounded_inputs(changes):
    with pytest.raises(ValueError):
        replace(config(), **changes)


def test_exact_ten_thousand_unique_observations_and_bounded_dates():
    spec = WorkloadConfig(10_000, "scale-exact-10k")
    rows = list(iter_observations(spec))
    assert len(rows) == len({r["event_id"] for r in rows}) == 10_000
    assert len({r["match_id"] for r in rows}) == 5_000
    assert {r["provider"] for r in rows} == {"synthetic"}
    assert {r["revision"] for r in rows} == {1, 2}
    assert {r["status"] for r in rows} == {"scheduled", "finished"}
    assert len({r[f"team_{side}_id"] for r in rows for side in ("a", "b")}) == 128
    for row in rows:
        kickoff = datetime.fromisoformat(row["start_time"].replace("Z", "+00:00"))
        assert ORIGIN <= kickoff <= ORIGIN + timedelta(days=30)
        assert "synthetic:scale:scale-exact-10k:" in row["match_id"]
        if row["sport"] == "valorant" and row["status"] == "finished":
            assert max(row["score_a"], row["score_b"]) == 2
            assert min(row["score_a"], row["score_b"]) < 2


def test_million_configuration_is_lazy_and_keeps_the_same_horizon():
    spec = WorkloadConfig(1_000_000, "scale-exact-1m", shard_observations=100_000)
    iterator = iter_observations(spec)
    prefix = list(islice(iterator, 1001))
    assert len(prefix) == 1001
    assert all("synthetic:scale:scale-exact-1m:" in r["event_id"] for r in prefix)
    final = _pair(spec, 499_999)[1]
    assert final["start_time"] == "2025-01-30T20:00:00.000000Z"
    assert final["observed_at"] == "2025-01-30T23:10:00.000000Z"
    # This test checks the lazy API and last-event arithmetic, not a measured
    # million-event Spark run or a full million-record generation.


def test_counter_choices_and_wire_are_reproducible_across_sharding(tmp_path):
    spec = config()
    first = write_workload(spec, tmp_path / "a")
    second = write_workload(spec, tmp_path / "b")
    assert first == second
    assert (tmp_path / "a/manifest.json").read_bytes() == (tmp_path / "b/manifest.json").read_bytes()
    assert [s["observations"] for s in first["shards"]] == [40, 40, 20]
    assert sum(s["observations"] for s in first["shards"]) == 100
    assert first["base_unique_observations"] == first["received_observations"] == 100
    assert first["fault_observations"] == first["correction_observations"] == 0
    for name, digest in first["files"].items():
        body = (tmp_path / "a" / name).read_bytes()
        assert b"\r" not in body
        assert hashlib.sha256(body).hexdigest() == digest
        assert body == (tmp_path / "b" / name).read_bytes()
    different_shards = replace(spec, shard_observations=20)
    assert list(iter_observations(spec)) == list(iter_observations(different_shards))
    assert validate_workload(tmp_path / "a") == first
    assert len(list(iter_batches(tmp_path / "a"))) == 3
    with pytest.raises(ValueError, match="must be new"):
        write_workload(spec, tmp_path / "a")


def test_identity_namespaces_do_not_collide_and_seed_changes_scores():
    a = list(iter_observations(config()))
    b = list(iter_observations(replace(config(), namespace="scale-other")))
    assert not {r["event_id"] for r in a} & {r["event_id"] for r in b}
    assert not {r["team_a_id"] for r in a} & {r["team_a_id"] for r in b}
    assert _pair(config(), 0) != _pair(replace(config(), seed=43), 0)


def reseal(root):
    path = root / "manifest.json"
    manifest = json.loads(path.read_text())
    for shard in manifest["shards"]:
        body = (root / shard["path"]).read_bytes()
        shard["bytes"], shard["sha256"] = len(body), hashlib.sha256(body).hexdigest()
        manifest["files"][shard["path"]] = shard["sha256"]
    del manifest["dataset_version"]
    manifest["dataset_version"] = hashlib.sha256(canonical(manifest).encode()).hexdigest()[:16]
    path.write_text(json.dumps(manifest))


def test_unsealed_tampering_is_rejected_before_batch_yield(tmp_path):
    write_workload(config(), tmp_path / "workload")
    shard = tmp_path / "workload/shards/000000.jsonl"
    shard.write_bytes(shard.read_bytes().replace(b"scheduled", b"cancelled", 1))
    with pytest.raises(ValueError, match="checksum"):
        next(iter_batches(tmp_path / "workload"))


def test_resealed_valid_score_and_duplicate_id_still_fail_deterministic_validation(tmp_path):
    root = tmp_path / "workload"
    write_workload(config(), root)
    shard = root / "shards/000000.jsonl"
    rows = [json.loads(line) for line in shard.read_bytes().splitlines()]
    football = next(r for r in rows if r["sport"] == "football" and r["status"] == "finished")
    football["score_a"] += 1
    shard.write_bytes(b"".join((canonical(r) + "\n").encode() for r in rows))
    reseal(root)
    with pytest.raises(ValueError, match="deterministic"):
        validate_workload(root)
    rows[1] = rows[0]
    shard.write_bytes(b"".join((canonical(r) + "\n").encode() for r in rows))
    reseal(root)
    with pytest.raises(ValueError, match="deterministic"):
        validate_workload(root)


def test_extra_files_and_path_traversal_are_rejected(tmp_path):
    root = tmp_path / "workload"
    write_workload(config(), root)
    (root / "secret.txt").write_text("extra")
    with pytest.raises(ValueError, match="extra"):
        validate_workload(root)
    (root / "secret.txt").unlink()
    path = root / "manifest.json"
    manifest = json.loads(path.read_text())
    manifest["shards"][0]["path"] = "../outside.jsonl"
    del manifest["dataset_version"]
    manifest["dataset_version"] = hashlib.sha256(canonical(manifest).encode()).hexdigest()[:16]
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="metadata"):
        validate_workload(root)


def test_scenario_preserves_failure_report_without_starting_spark(tmp_path, monkeypatch):
    root, output = tmp_path / "workload", tmp_path / "scenario"
    write_workload(config(), root)
    def fail_session():
        raise RuntimeError("deliberate offline session failure")
    monkeypatch.setattr("arena_lakehouse.spark_backend.session", fail_session)
    with pytest.raises(RuntimeError, match="offline session failure"):
        run_scenario(root, output)
    report = json.loads((output / "scale-report.json").read_text())
    assert report["status"] == "failed"
    assert report["expected_counts"]["base_unique_observations"] == 100
    assert "base_unique_observations" not in report
    assert report["received_observations"] == report["submitted_fault_observations"] == 0
    assert report["runtime_source_sha256"]["scale.py"]
    assert report["predictions"] == "not_run"
    assert report["workload_manifest_sha256"] == hashlib.sha256((root / "manifest.json").read_bytes()).hexdigest()
    assert report["error_type"] == "RuntimeError"
    assert report["elapsed_seconds"] >= 0
    assert report["workspace_disk_bytes"] > 0
    assert report["resource_end"].keys() == resource_sample().keys()


def test_large_python_oracle_run_rejected_before_workspace_writes(tmp_path):
    root = tmp_path / "workload"
    write_workload(WorkloadConfig(10_002, "scale-too-large"), root)
    with pytest.raises(ValueError, match="limited to 10000"):
        run_scenario(root, tmp_path / "scenario")
    assert not (tmp_path / "scenario").exists()
