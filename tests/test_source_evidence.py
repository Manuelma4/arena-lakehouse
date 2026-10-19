"""Independent package bytes, binding and failed-publication invariants."""

import hashlib
import json
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from arena_lakehouse import source_evidence as evidence
from arena_lakehouse.engine import Pipeline
from arena_lakehouse.snapshot import export_snapshot
from arena_lakehouse.synthetic import generate

CLOCK = datetime(2026, 10, 18, tzinfo=timezone.utc)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def json_bytes(value):
    return (json.dumps(value, ensure_ascii=False, allow_nan=False, indent=2) + "\n").encode()


@pytest.fixture(scope="module")
def base_snapshot(tmp_path_factory):
    root = tmp_path_factory.mktemp("source-evidence-fixture")
    pipeline = Pipeline(root / "pipeline")
    pipeline.ingest(generate(40))
    return export_snapshot(pipeline, root / "snapshots", with_predictions=False, generated_at=CLOCK)


@pytest.fixture
def snapshot(base_snapshot, tmp_path):
    return Path(shutil.copytree(base_snapshot, tmp_path / "snapshot"))


def revise(root, filename, value=None, *, raw=None):
    (root / filename).write_bytes(json_bytes(value) if raw is None else raw)
    manifest = json.loads((root / "manifest.json").read_bytes())
    manifest["files"][filename] = sha((root / filename).read_bytes())
    (root / "manifest.json").write_bytes(json_bytes(manifest))


def contents(root):
    return {p.relative_to(root).as_posix(): p.read_bytes() for p in root.rglob("*") if p.is_file()}


def export(root, target):
    return evidence.export_source_evidence(root, target, generated_at=CLOCK)


def test_original_bytes_manifest_binding_and_deterministic_identity(snapshot, tmp_path):
    original = contents(snapshot)
    target = export(snapshot, tmp_path / "packet")
    assert set(contents(target)) == evidence.PACKAGE_FILES
    for filename in evidence.METADATA:
        assert (target / filename).read_bytes() == original[filename]
    assert (target / "source-manifest.json").read_bytes() == original["manifest.json"]
    manifest = json.loads((target / "manifest.json").read_bytes())
    native = json.loads(original["manifest.json"])
    snapshot_identity = {key: native[key] for key in ("schema_version", "producer_version", "files")}
    assert manifest["source"]["snapshot_fingerprint"] == sha(json.dumps(
        snapshot_identity, sort_keys=True, separators=(",", ":")).encode())
    assert manifest["source"]["manifest_sha256"] == sha(original["manifest.json"])
    identity = {key: manifest[key] for key in ("schema_version", "source", "files")}
    digest = sha(json.dumps(identity, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode())
    assert manifest["package_fingerprint"] == digest
    assert manifest["package_version"] == digest[:16]
    assert manifest["files"] == {name: sha((target / name).read_bytes()) for name in evidence.SOURCE_FILES}
    later = evidence.export_source_evidence(snapshot, tmp_path / "later", generated_at=CLOCK + timedelta(seconds=1))
    later_manifest = json.loads((later / "manifest.json").read_bytes())
    assert later_manifest["package_fingerprint"] == digest
    assert contents(snapshot) == original
    assert not list(tmp_path.glob(".source-evidence-*"))


def test_exact_existing_packet_reused_and_changed_never_replaced(snapshot, tmp_path):
    target = export(snapshot, tmp_path / "packet")
    before = contents(target)
    assert export(snapshot, target) == target
    with pytest.raises(evidence.SourceEvidenceError, match="Existing destination"):
        evidence.export_source_evidence(snapshot, target, generated_at=CLOCK + timedelta(seconds=1))
    assert contents(target) == before
    (target / "empty").mkdir()
    with pytest.raises(evidence.SourceEvidenceError):
        export(snapshot, target)


@pytest.mark.parametrize("location", ["same", "child", "ancestor"])
def test_source_destination_overlap_rejected(snapshot, location):
    target = snapshot if location == "same" else snapshot / "packet" if location == "child" else snapshot.parent
    before = contents(snapshot)
    with pytest.raises(evidence.SourceEvidenceError, match="overlap"):
        export(snapshot, target)
    assert contents(snapshot) == before


@pytest.mark.parametrize("change", ["extra", "missing", "checksum", "directory", "bad_parquet"])
def test_full_source_is_validated_before_staging(snapshot, tmp_path, change):
    if change == "extra":
        (snapshot / "extra.json").write_text("{}")
    elif change == "missing":
        (snapshot / "model_report.json").unlink()
    elif change == "checksum":
        (snapshot / "gold/matches.parquet").write_bytes(b"changed")
    elif change == "directory":
        (snapshot / "extra").mkdir()
    else:
        revise(snapshot, "gold/matches.parquet", raw=b"not parquet")
    with pytest.raises(evidence.SourceEvidenceError):
        export(snapshot, tmp_path / "packet")
    assert not (tmp_path / "packet").exists()
    assert not list(tmp_path.glob(".source-evidence-*"))


@pytest.mark.parametrize("filename,raw", [
    ("quality.json", b'{"run_id":"one","run_id":"two"}'),
    ("model_report.json", b'{"value":NaN}'),
    ("model_report.json", b'{"value":1e999}'),
    ("model_report.json", b'{"value":1,"value":2}'),
    ("quality.json", b" " * (evidence.MIB + 1)),
], ids=["duplicate-quality", "nan", "infinite-exponent", "duplicate-model-report", "oversized-metadata"])
def test_duplicate_nonfinite_and_oversized_json_rejected(snapshot, tmp_path, filename, raw):
    revise(snapshot, filename, raw=raw)
    with pytest.raises(evidence.SourceEvidenceError):
        export(snapshot, tmp_path / "packet")


@pytest.mark.parametrize("change", ["schema", "provider", "dataset", "future_cutoff", "extra", "path", "case"])
def test_bad_source_manifest_rejected(snapshot, tmp_path, change):
    path = snapshot / "manifest.json"
    value = json.loads(path.read_bytes())
    if change == "schema":
        value["schema_version"] = "3.0"
    elif change == "provider":
        value["data_kind"] = "api"
    elif change == "dataset":
        value["dataset_version"] = "invalid"
    elif change == "future_cutoff":
        value["as_of"] = "2099-01-01T00:00:00Z"
    elif change == "extra":
        value["credentials"] = "unsupported"
    elif change == "path":
        value["files"]["../outside"] = "a" * 64
    else:
        value["files"]["Quality.json"] = value["files"]["quality.json"]
    path.write_bytes(json_bytes(value))
    with pytest.raises(evidence.SourceEvidenceError):
        export(snapshot, tmp_path / "packet")


@pytest.mark.parametrize("change", ["boolean_count", "sum", "passed", "empty_group", "extra", "duplicate", "status"])
def test_quality_aggregates_are_strict(snapshot, tmp_path, change):
    quality = json.loads((snapshot / "quality.json").read_bytes())
    assert quality["checks"]
    if change == "boolean_count":
        quality["summary"]["valid_events"] = True
    elif change == "sum":
        quality["summary"]["quarantined"] += 1
    elif change == "passed":
        quality["checks"][0]["passed"] += 1
    elif change == "empty_group":
        quality["checks"][0]["failed"] = 0
    elif change == "extra":
        quality["checks"][0]["coverage"] = "complete"
    elif change == "duplicate":
        quality["checks"].append(quality["checks"][0])
    else:
        quality["summary"]["last_run_status"] = "failed"
    revise(snapshot, "quality.json", quality)
    with pytest.raises(evidence.SourceEvidenceError):
        export(snapshot, tmp_path / "packet")


def test_zero_recorded_failures_does_not_fabricate_incidents(tmp_path):
    pipeline = Pipeline(tmp_path / "pipeline")
    pipeline.ingest(generate(20, faults=False))
    source = export_snapshot(pipeline, tmp_path / "snapshots", with_predictions=False, generated_at=CLOCK)
    packet = export(source, tmp_path / "packet")
    quality = json.loads((packet / "quality.json").read_bytes())
    assert quality["checks"] == []
    assert quality["summary"]["quarantined"] == 0
    assert json.loads((packet / "incidents.json").read_bytes()) == []


@pytest.mark.parametrize("change", ["count", "identity", "status", "missing", "rule", "boolean"])
def test_incident_parity(snapshot, tmp_path, change):
    incidents = json.loads((snapshot / "incidents.json").read_bytes())
    if change == "count":
        incidents[0]["event_count"] += 1
    elif change == "identity":
        incidents[0]["incident_id"] = "0" * 16
    elif change == "status":
        incidents[0]["status"] = "resolved"
    elif change == "missing":
        incidents.pop()
    elif change == "rule":
        incidents[0]["rule"] = "unrelated"
    else:
        incidents[0]["event_count"] = True
    revise(snapshot, "incidents.json", incidents)
    with pytest.raises(evidence.SourceEvidenceError):
        export(snapshot, tmp_path / "packet")


@pytest.mark.parametrize("change", ["schema", "metric", "lineage_unknown", "cycle", "duplicate_edge", "control"])
def test_catalog_and_declared_lineage(snapshot, tmp_path, change):
    filename = "catalog.json" if change == "schema" else "metrics.json" if change in ("metric", "control") else "lineage.json"
    value = json.loads((snapshot / filename).read_bytes())
    if change == "schema":
        value[0]["columns"][0]["type"] = "wrong"
    elif change == "metric":
        value[0]["table"] = "unknown"
    elif change == "control":
        value[0]["description"] = "unsafe\x00text"
    elif change == "lineage_unknown":
        value[0]["source"] = "unknown"
    elif change == "cycle":
        value.append(dict(source="team_stats", target="matches", transformation="cycle"))
    else:
        value.append(value[0])
    revise(snapshot, filename, value)
    with pytest.raises(evidence.SourceEvidenceError):
        export(snapshot, tmp_path / "packet")


def test_source_change_during_staging_never_publishes(snapshot, tmp_path, monkeypatch):
    original = evidence._capture
    calls = 0

    def changing(root):
        nonlocal calls
        calls += 1
        if calls == 2:
            revise(root, "model_report.json", {"status": "changed"})
        return original(root)

    monkeypatch.setattr(evidence, "_capture", changing)
    with pytest.raises(evidence.SourceEvidenceError, match="changed"):
        export(snapshot, tmp_path / "packet")
    assert not (tmp_path / "packet").exists()
    assert not list(tmp_path.glob(".source-evidence-*"))


def test_naive_early_future_clocks_rejected(snapshot, tmp_path):
    for clock in (CLOCK.replace(tzinfo=None), CLOCK - timedelta(seconds=1), datetime.now(timezone.utc) + timedelta(days=1)):
        with pytest.raises(evidence.SourceEvidenceError):
            evidence.export_source_evidence(snapshot, tmp_path / "packet", generated_at=clock)


def test_cli_requires_no_credentials_and_writes_exact_target(snapshot, tmp_path, capsys):
    target = tmp_path / "packet"
    evidence.main(["--snapshot", str(snapshot), "--output", str(target), "--generated-at", CLOCK.isoformat()])
    assert Path(capsys.readouterr().out.strip()) == target
    assert set(contents(target)) == evidence.PACKAGE_FILES


def test_canonical_invalid_table_even_with_updated_checksums_is_rejected(snapshot, tmp_path):
    target = snapshot / "gold/matches.parquet"
    pq.write_table(pa.table({"match_id": ["x"], "sport": ["football"]}), target)
    revise(snapshot, "gold/matches.parquet", raw=target.read_bytes())
    with pytest.raises(evidence.SourceEvidenceError):
        export(snapshot, tmp_path / "packet")


def test_optional_history_and_offset_clock_preserve_source_bytes(snapshot, tmp_path):
    manifest_path = snapshot / "manifest.json"
    manifest = json.loads(manifest_path.read_bytes())
    (snapshot / "gold/match_history.parquet").unlink()
    del manifest["files"]["gold/match_history.parquet"]
    manifest["generated_at"] = "2026-10-18T02:00:00+02:00"
    manifest_path.write_bytes(json_bytes(manifest))
    catalog = [row for row in json.loads((snapshot / "catalog.json").read_bytes()) if row["name"] != "match_history"]
    revise(snapshot, "catalog.json", catalog)
    packet = export(snapshot, tmp_path / "packet")
    assert (packet / "source-manifest.json").read_bytes() == manifest_path.read_bytes()
    assert len(json.loads((packet / "catalog.json").read_bytes())) == 2


def test_atomic_publish_failure_cleans_only_owned_stage(snapshot, tmp_path, monkeypatch):
    unrelated = tmp_path / ".source-evidence-other"
    unrelated.mkdir()
    (unrelated / "keep").write_text("untouched")
    original = contents(snapshot)

    def fail_rename(stage, target):
        raise OSError("Injected publication failure")

    monkeypatch.setattr(evidence, "_publish", fail_rename)
    with pytest.raises(evidence.SourceEvidenceError):
        export(snapshot, tmp_path / "packet")
    assert contents(snapshot) == original
    assert not (tmp_path / "packet").exists()
    assert list(tmp_path.glob(".source-evidence-*")) == [unrelated]
    assert (unrelated / "keep").read_text() == "untouched"


def test_reparse_ancestor_is_rejected_without_following_it(tmp_path, monkeypatch):
    from types import SimpleNamespace

    original = Path.lstat
    reparse = tmp_path / "reparse"

    def fake_lstat(self, *args, **kwargs):
        if self == reparse:
            return SimpleNamespace(st_mode=0o40755, st_file_attributes=0x400)
        return original(self, *args, **kwargs)

    monkeypatch.setattr(Path, "lstat", fake_lstat)
    with pytest.raises(evidence.SourceEvidenceError, match="reparse"):
        evidence._safe_chain(reparse / "child")


@pytest.mark.parametrize("code", [0, evidence.errno.EEXIST, evidence.errno.ENOSYS, evidence.errno.EINVAL])
def test_linux_no_replace_syscall_has_no_racy_fallback(tmp_path, monkeypatch, code):
    from types import SimpleNamespace

    calls = []

    class Rename:
        def __call__(self, *args):
            calls.append(args)
            evidence.ctypes.set_errno(code)
            return -1 if code else 0

    monkeypatch.setattr(evidence.sys, "platform", "linux")
    monkeypatch.setattr(evidence.ctypes, "CDLL", lambda *args, **kwargs: SimpleNamespace(renameat2=Rename()))
    stage, destination = tmp_path / "stage", tmp_path / "destination"
    if code == 0:
        evidence._publish(stage, destination)
    elif code == evidence.errno.EEXIST:
        with pytest.raises(OSError) as error:
            evidence._publish(stage, destination)
        assert error.value.errno == evidence.errno.EEXIST
    else:
        with pytest.raises(evidence.SourceEvidenceError, match="unavailable"):
            evidence._publish(stage, destination)
    assert len(calls) == 1
    assert calls[0] == (-100, evidence.os.fsencode(stage), -100, evidence.os.fsencode(destination), 1)


def test_unsupported_platform_refuses_publication(tmp_path, monkeypatch):
    monkeypatch.setattr(evidence.sys, "platform", "unsupported")
    with pytest.raises(evidence.SourceEvidenceError, match="unavailable"):
        evidence._publish(tmp_path / "stage", tmp_path / "target")


@pytest.mark.parametrize("change", ["valid", "boolean_probability", "wrong_kind", "wrong_cutoff", "unknown_match"])
def test_prediction_contract_verified_even_when_not_exported(snapshot, tmp_path, change):
    manifest = json.loads((snapshot / "manifest.json").read_bytes())
    match = max((row for row in pq.read_table(snapshot / "gold/matches.parquet").to_pylist()
                 if row["status"] == "scheduled"), key=lambda row: row["start_time"])
    assert evidence._time(match["start_time"]) > evidence._time(manifest["as_of"])
    probabilities = {"a": 0.5, "b": 0.5} if match["sport"] == "valorant" else {"a": 0.4, "draw": 0.2, "b": 0.4}
    prediction = dict(match_id=match["match_id"], sport=match["sport"], probabilities=probabilities,
                      data_kind=manifest["data_kind"], as_of=manifest["as_of"])
    if change == "boolean_probability":
        probabilities.update(a=True, b=False)
    elif change == "wrong_kind":
        prediction["data_kind"] = "api"
    elif change == "wrong_cutoff":
        prediction["as_of"] = match["start_time"]
    elif change == "unknown_match":
        prediction["match_id"] = "absent"
    revise(snapshot, "predictions.json", [prediction])
    if change == "valid":
        packet = export(snapshot, tmp_path / "packet")
        assert "predictions.json" not in contents(packet)
    else:
        with pytest.raises(evidence.SourceEvidenceError):
            export(snapshot, tmp_path / "packet")


def test_unicode_paths_keep_legacy_snapshot_fingerprint(snapshot, tmp_path):
    path = snapshot / "docs/café.md"
    path.write_bytes("# Observed café\n".encode())
    manifest_path = snapshot / "manifest.json"
    manifest = json.loads(manifest_path.read_bytes())
    manifest["files"]["docs/café.md"] = sha(path.read_bytes())
    manifest_path.write_bytes(json_bytes(manifest))
    packet = export(snapshot, tmp_path / "packet")
    exported = json.loads((packet / "manifest.json").read_bytes())
    identity = {key: manifest[key] for key in ("schema_version", "producer_version", "files")}
    ascii_hash = sha(json.dumps(identity, sort_keys=True, separators=(",", ":")).encode())
    unicode_hash = sha(json.dumps(identity, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode())
    assert ascii_hash != unicode_hash
    assert exported["source"]["snapshot_fingerprint"] == ascii_hash


def test_history_catalog_rejects_duplicate_column_names(snapshot, tmp_path):
    path = snapshot / "gold/match_history.parquet"
    table = pq.read_table(path)
    names = table.column_names
    names[-1] = names[0]
    pq.write_table(table.rename_columns(names), path)
    revise(snapshot, "gold/match_history.parquet", raw=path.read_bytes())
    catalog = json.loads((snapshot / "catalog.json").read_bytes())
    for entry in catalog:
        if entry["name"] == "match_history":
            entry["columns"][-1]["name"] = entry["columns"][0]["name"]
    revise(snapshot, "catalog.json", catalog)
    with pytest.raises(evidence.SourceEvidenceError, match="Catalog"):
        export(snapshot, tmp_path / "packet")


@pytest.mark.parametrize("length,accepted", [(120, True), (121, False)])
def test_shared_name_bounds(snapshot, tmp_path, length, accepted):
    metrics = json.loads((snapshot / "metrics.json").read_bytes())
    metrics[0]["unit"] = "u" * length
    revise(snapshot, "metrics.json", metrics)
    if accepted:
        export(snapshot, tmp_path / "packet")
    else:
        with pytest.raises(evidence.SourceEvidenceError):
            export(snapshot, tmp_path / "packet")


@pytest.mark.parametrize("count,accepted", [(0, False), (128, True), (129, False)])
def test_shared_metric_count_bounds(snapshot, tmp_path, count, accepted):
    base = json.loads((snapshot / "metrics.json").read_bytes())[0]
    revise(snapshot, "metrics.json", [dict(base, name=f"metric_{index}") for index in range(count)])
    if accepted:
        export(snapshot, tmp_path / "packet")
    else:
        with pytest.raises(evidence.SourceEvidenceError):
            export(snapshot, tmp_path / "packet")


@pytest.mark.parametrize("length,accepted", [(120, True), (121, False)])
def test_shared_run_id_bounds(snapshot, tmp_path, length, accepted):
    quality = json.loads((snapshot / "quality.json").read_bytes())
    quality["run_id"] = "r" * length
    revise(snapshot, "quality.json", quality)
    if accepted:
        export(snapshot, tmp_path / "packet")
    else:
        with pytest.raises(evidence.SourceEvidenceError):
            export(snapshot, tmp_path / "packet")


def test_shared_lineage_requires_an_edge(snapshot, tmp_path):
    revise(snapshot, "lineage.json", [])
    with pytest.raises(evidence.SourceEvidenceError, match="lineage"):
        export(snapshot, tmp_path / "packet")


@pytest.mark.parametrize("value", ["2026-10-18 00:00:00+00:00", "2026-10-18T00:00:00", "2026-10-18T00:00:00.123456789012345678901Z"])
def test_shared_timestamp_bounds(value):
    with pytest.raises(evidence.SourceEvidenceError):
        evidence._time(value)


def test_shared_source_manifest_file_count_bound(snapshot, tmp_path):
    path = snapshot / "manifest.json"
    manifest = json.loads(path.read_bytes())
    for index in range(257):
        manifest["files"][f"docs/extra-{index}.md"] = "0" * 64
    path.write_bytes(json_bytes(manifest))
    with pytest.raises(evidence.SourceEvidenceError, match="required source payload"):
        export(snapshot, tmp_path / "packet")


@pytest.mark.parametrize("count,accepted", [(128, True), (129, False)])
def test_shared_column_count_bound(snapshot, tmp_path, count, accepted):
    path = snapshot / "gold/matches.parquet"
    table = pq.read_table(path)
    for index in range(count - table.num_columns):
        table = table.append_column(f"additional_{index}", pa.array([None] * table.num_rows, type=pa.string()))
    pq.write_table(table, path)
    revise(snapshot, "gold/matches.parquet", raw=path.read_bytes())
    catalog = json.loads((snapshot / "catalog.json").read_bytes())
    for entry in catalog:
        if entry["name"] == "matches":
            entry["columns"] = [dict(name=field.name, type=str(field.type), description=field.name)
                                for field in table.schema]
    revise(snapshot, "catalog.json", catalog)
    if accepted:
        export(snapshot, tmp_path / "packet")
    else:
        with pytest.raises(evidence.SourceEvidenceError, match="columns"):
            export(snapshot, tmp_path / "packet")


def test_shared_failure_group_count_bound(snapshot, tmp_path):
    quality = json.loads((snapshot / "quality.json").read_bytes())
    quality["checks"] = [dict(rule=f"rule_{index}", table="bronze_events", severity="warning", failed=1,
                              passed=quality["summary"]["valid_events"]) for index in range(129)]
    quality["summary"]["quarantined"] = 129
    revise(snapshot, "quality.json", quality)
    with pytest.raises(evidence.SourceEvidenceError, match="quality checks"):
        export(snapshot, tmp_path / "packet")
