"""Bounded fictional workloads and measured, independently verified scale drills.

This module does not replace the published showcase or claim a measured scale.
Generation is pure Python; Spark is imported only by the explicit scenario command.
"""

import argparse
import hashlib
import json
import re
import sys
import tempfile
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from itertools import islice
from pathlib import Path

from . import __version__
from .schema import MatchEvent, canonical
from .synthetic import TEAMS

WORKLOAD_VERSION = "arena-scale-workload-1.0"
MAX_OBSERVATIONS = 1_000_000
MAX_SHARD_OBSERVATIONS = 100_000
ORIGIN = datetime(2025, 1, 1, tzinfo=timezone.utc)
LIMITATIONS = [
    "Fictional stress workload; dates and outcomes do not represent real sporting events",
    "Local single-writer file observations, not provider push or upstream transaction CDC",
    "Delta tables commit individually; successful verification gates publication",
    "Generation, validation, verification and model work have separate measurement scopes",
    "No prediction training or real-data forecast accuracy is measured",
    "Container memory peak includes the JVM and Python; it is not incremental process memory",
]


@dataclass(frozen=True)
class WorkloadConfig:
    observations: int
    namespace: str
    seed: int = 42
    shard_observations: int = 10_000
    teams_per_sport: int = 64
    shuffle_buffer: int = 1_000
    horizon_days: int = 30

    def __post_init__(self):
        integers = ("observations", "seed", "shard_observations", "teams_per_sport", "shuffle_buffer",
                    "horizon_days")
        if any(type(getattr(self, name)) is not int for name in integers):
            raise ValueError("Workload counts and seed must be integers")
        if not 40 <= self.observations <= MAX_OBSERVATIONS or self.observations % 2:
            raise ValueError("observations must be even and between 40 and 1000000")
        if not isinstance(self.namespace, str) or not re.fullmatch(r"[a-z][a-z0-9-]{0,39}", self.namespace):
            raise ValueError("namespace must be a lowercase ASCII name of 1..40 characters")
        if not 0 <= self.seed <= 2**32 - 1:
            raise ValueError("seed must be between 0 and 4294967295")
        if not 2 <= self.shard_observations <= MAX_SHARD_OBSERVATIONS or self.shard_observations % 2:
            raise ValueError("shard_observations must be even and between 2 and 100000")
        if not 16 <= self.teams_per_sport <= 4096 or self.teams_per_sport % 8:
            raise ValueError("teams_per_sport must be a multiple of eight between 16 and 4096")
        if not 2 <= self.shuffle_buffer <= self.shard_observations or self.shuffle_buffer % 2:
            raise ValueError("shuffle_buffer must be even and bounded by shard_observations")
        if not 1 <= self.horizon_days <= 31:
            raise ValueError("horizon_days must be between 1 and 31")


def _hash(value):
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def _write_json(path, value):
    with Path(path).open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False) + "\n")


def _sha256(path):
    result = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def _pair(config, index):
    """Integer SHA-256 choices avoid random-library/version-dependent sequences."""
    sport = "football" if index % 2 == 0 else "valorant"
    choices = bytes.fromhex(_hash([WORKLOAD_VERSION, config.seed, index, "match"]))
    a, b = choices[0] % 8, choices[1] % 7
    b += b >= a
    cohort = (index // 2) % (config.teams_per_sport // 8)
    match_id = f"synthetic:scale:{config.namespace}:{sport}:{index:07d}"
    seconds = index * (config.horizon_days * 86400 - 14400) // (config.observations // 2 - 1)
    start = ORIGIN + timedelta(seconds=seconds)
    end = start + timedelta(hours=2 if sport == "football" else 3)
    if sport == "football":
        sa, sb = choices[2] % 5, choices[3] % 5
    else:
        sa, sb = (2, choices[3] % 2) if choices[2] % 2 else (choices[3] % 2, 2)
    base = dict(match_id=match_id, provider="synthetic", sport=sport,
                competition=f"Scale {'Football' if sport == 'football' else 'VALORANT'} Circuit {cohort + 1:03d}",
                team_a_id=f"{sport}:scale:{config.namespace}:{cohort * 8 + a:04d}",
                team_b_id=f"{sport}:scale:{config.namespace}:{cohort * 8 + b:04d}",
                team_a=f"{TEAMS[sport][a]} Scale {cohort + 1:03d}",
                team_b=f"{TEAMS[sport][b]} Scale {cohort + 1:03d}",
                start_time=start, best_of=1 if sport == "football" else 3)
    scheduled = MatchEvent(**base, event_id=match_id + ":1", revision=1, status="scheduled",
                           observed_at=start - timedelta(days=2)).row()
    finished = MatchEvent(**base, event_id=match_id + ":2", revision=2, status="finished",
                          completed_at=end, observed_at=end + timedelta(minutes=10),
                          score_a=sa, score_b=sb).row()
    return scheduled, finished


def iter_observations(config: WorkloadConfig):
    """Yield exactly the requested unique observations, buffering at most one block.

    Each block contains complete match pairs. Its order is ascending SHA-256 of
    canonical JSON [version, seed, namespace, "shuffle", block_number, event_id].
    Changing shard size does not change the event sequence or business values.
    """
    def chronological():
        for index in range(config.observations // 2):
            yield from _pair(config, index)
    source = chronological()
    block_number = 0
    while block := list(islice(source, config.shuffle_buffer)):
        block.sort(key=lambda event: _hash([WORKLOAD_VERSION, config.seed, config.namespace,
                                           "shuffle", block_number, event["event_id"]]))
        yield from block
        block_number += 1


def _bounds(config):
    first = _pair(config, 0)[0]
    final_index = config.observations // 2 - 1
    last = _pair(config, final_index)[1]
    # An almost simultaneous preceding VALORANT series can finish after the
    # final football match; kickoff order alone does not order availability.
    observed_end = max(last["observed_at"], _pair(config, final_index - 1)[1]["observed_at"])
    return dict(kickoff_start=first["start_time"], kickoff_end=last["start_time"],
                observation_start=first["observed_at"], observation_end=observed_end)


def write_workload(config: WorkloadConfig, destination: Path):
    """Atomically publish a fresh deterministic package; no generation clock in it."""
    destination = Path(destination)
    if destination.exists():
        raise ValueError("Workload destination must be new; retain previous evidence")
    destination.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=".scale-build-", dir=destination.parent))
    (stage / "shards").mkdir()
    files, shards, total = {}, [], 0
    iterator = iter_observations(config)
    while total < config.observations:
        name = f"shards/{len(shards):06d}.jsonl"
        checksum, rows, size = hashlib.sha256(), 0, 0
        with (stage / name).open("wb") as handle:
            for row in islice(iterator, config.shard_observations):
                line = (canonical(row) + "\n").encode("utf-8")
                handle.write(line)
                checksum.update(line)
                rows += 1
                size += len(line)
        files[name] = checksum.hexdigest()
        shards.append(dict(path=name, observations=rows, bytes=size, sha256=files[name]))
        total += rows
    manifest = dict(workload_version=WORKLOAD_VERSION, config=asdict(config), files=files, shards=shards,
                    received_observations=total, base_unique_observations=total, matches=total // 2,
                    duplicate_observations=0, fault_observations=0, correction_observations=0,
                    **_bounds(config),
                    data_kind="synthetic", algorithm="integer-sha256-and-bounded-hash-sort-v1")
    manifest["dataset_version"] = _hash(manifest)[:16]
    _write_json(stage / "manifest.json", manifest)
    if destination.exists():
        raise ValueError("Workload destination appeared during generation; staging evidence retained")
    stage.replace(destination)
    return manifest


def _manifest(root):
    root = Path(root)
    if root.is_symlink() or not root.is_dir():
        raise ValueError("Workload must be a regular directory")
    path = root / "manifest.json"
    if path.is_symlink() or path.stat().st_size > 1_000_000:
        raise ValueError("Invalid workload manifest")
    manifest = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict) or manifest.get("workload_version") != WORKLOAD_VERSION:
        raise ValueError("Unsupported workload contract")
    config = WorkloadConfig(**manifest["config"])
    identity = {k: v for k, v in manifest.items() if k != "dataset_version"}
    if manifest.get("dataset_version") != _hash(identity)[:16]:
        raise ValueError("Workload manifest identity mismatch")
    expected_shards = (config.observations + config.shard_observations - 1) // config.shard_observations
    if not isinstance(manifest.get("shards"), list) or len(manifest["shards"]) != expected_shards:
        raise ValueError("Workload shard count mismatch")
    files = {}
    for index, shard in enumerate(manifest["shards"]):
        name = f"shards/{index:06d}.jsonl"
        rows = min(config.shard_observations, config.observations - index * config.shard_observations)
        if (set(shard) != {"path", "observations", "bytes", "sha256"} or shard["path"] != name
                or type(shard["observations"]) is not int or shard["observations"] != rows
                or type(shard["bytes"]) is not int or not 1 <= shard["bytes"] <= rows * 8192
                or not isinstance(shard["sha256"], str) or not re.fullmatch(r"[0-9a-f]{64}", shard["sha256"])):
            raise ValueError("Invalid workload shard metadata")
        files[name] = shard["sha256"]
    if manifest.get("files") != files:
        raise ValueError("Workload file inventory mismatch")
    actual = set()
    for item in root.rglob("*"):
        if item.is_symlink():
            raise ValueError("Workload symlinks are forbidden")
        if item.is_file():
            actual.add(item.relative_to(root).as_posix())
    if actual != {"manifest.json", *files}:
        raise ValueError("Workload has missing or extra files")
    for field in ("received_observations", "base_unique_observations"):
        if type(manifest.get(field)) is not int or manifest[field] != config.observations:
            raise ValueError("Workload observation count mismatch")
    if (manifest.get("matches") != config.observations // 2 or manifest.get("data_kind") != "synthetic"
            or any(type(manifest.get(f)) is not int or manifest[f] != 0
                   for f in ("duplicate_observations", "fault_observations", "correction_observations"))):
        raise ValueError("Invalid workload base counts")
    return manifest, config


def iter_batches(root: Path):
    """Hash and parse a bounded shard before yielding any of its observations."""
    root = Path(root)
    manifest, _config = _manifest(root)
    for shard in manifest["shards"]:
        path = root / shard["path"]
        if path.stat().st_size != shard["bytes"]:
            raise ValueError("Workload shard size mismatch")
        checksum, rows = hashlib.sha256(), []
        with path.open("rb") as handle:
            while line := handle.readline(8193):
                if len(line) > 8192 or not line.endswith(b"\n"):
                    raise ValueError("Workload JSONL line exceeds its bound or lacks LF")
                checksum.update(line)
                raw = json.loads(line)
                event = MatchEvent.model_validate(raw).row()
                if raw != event or line != (canonical(event) + "\n").encode("utf-8"):
                    raise ValueError("Workload rows must use the exact canonical wire representation")
                rows.append(event)
                if len(rows) > shard["observations"]:
                    raise ValueError("Workload shard has too many observations")
        if checksum.hexdigest() != shard["sha256"] or len(rows) != shard["observations"]:
            raise ValueError("Workload shard checksum or count mismatch")
        yield rows


def validate_workload(root: Path):
    """Verify all bytes, contracts and deterministic rows without a full dataset list."""
    manifest, config = _manifest(root)
    expected, total = iter_observations(config), 0
    for batch in iter_batches(root):
        for row in batch:
            if row != next(expected):
                raise ValueError("Workload row differs from its deterministic configuration")
            total += 1
    if total != config.observations or next(expected, None) is not None:
        raise ValueError("Workload total does not match its configuration")
    for field, value in {**_bounds(config), "algorithm": "integer-sha256-and-bounded-hash-sort-v1"}.items():
        if manifest.get(field) != value:
            raise ValueError("Workload horizon or algorithm mismatch")
    return manifest


def resource_sample():
    """Measured values where available; missing platform capabilities remain null."""
    result = dict(cgroup_memory_current_bytes=None, cgroup_memory_peak_bytes=None,
                  cgroup_memory_limit_bytes=None, process_max_rss_bytes=None, child_max_rss_bytes=None)
    for field, names in {
        "cgroup_memory_current_bytes": ("memory.current", "memory/memory.usage_in_bytes"),
        "cgroup_memory_peak_bytes": ("memory.peak", "memory/memory.max_usage_in_bytes"),
        "cgroup_memory_limit_bytes": ("memory.max", "memory/memory.limit_in_bytes"),
    }.items():
        for name in names:
            try:
                value = (Path("/sys/fs/cgroup") / name).read_text().strip()
                result[field] = int(value) if value != "max" else None
                break
            except (OSError, ValueError):
                continue
    try:
        import resource
        multiplier = 1 if sys.platform == "darwin" else 1024
        result["process_max_rss_bytes"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * multiplier
        result["child_max_rss_bytes"] = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss * multiplier
    except ImportError:
        pass
    return result


def _disk_bytes(root):
    return sum(p.stat().st_size for p in Path(root).rglob("*") if p.is_file() and not p.is_symlink())


def _runtime_hashes():
    return {p.name: _sha256(p) for p in sorted(Path(__file__).parent.glob("*.py"))}


def _save_report(root, report):
    temporary = root / ".scale-report.tmp"
    _write_json(temporary, report)
    temporary.replace(root / "scale-report.json")


def run_scenario(workload: Path, workspace: Path, *, verification="python", bronze_timeout_seconds=180,
                 crosscheck_sqlite=False):
    """Explicit Spark drill; the small path retains the unchanged full Python oracle."""
    from .incremental import EVENT_COLUMNS, IncrementalPipeline
    from .spark_backend import session

    workspace, workload = Path(workspace), Path(workload)
    if workspace.exists():
        raise ValueError("Scenario workspace must be new; retain all previous reports")
    if workspace.resolve() == workload.resolve() or workspace.resolve().is_relative_to(workload.resolve()):
        raise ValueError("Scenario workspace must be separate from the immutable workload")
    if verification not in ("python", "sqlite"):
        raise ValueError("verification must be python or sqlite")
    manifest, config = _manifest(workload)
    if verification == "python" and config.observations > 10_000:
        raise ValueError("The full Python oracle is limited to 10000 base observations; select sqlite explicitly")
    if type(bronze_timeout_seconds) is not int or not 1 <= bronze_timeout_seconds <= 3600:
        raise ValueError("bronze_timeout_seconds must be between 1 and 3600")
    workspace.mkdir(parents=True)
    start, spark, oracle, pipeline = time.perf_counter(), None, None, None
    report = dict(status="running", package_version=__version__, workload_dataset_version=manifest["dataset_version"],
                  workload_manifest_sha256=_sha256(workload / "manifest.json"), generator_config=asdict(config),
                  runtime_source_sha256=_runtime_hashes(), source_module_file=__file__,
                  verification_backend=verification, sqlite_crosscheck=crosscheck_sqlite,
                  predictions="not_run", runs=[], rejected_submissions=[],
                  received_observations=0,
                  expected_counts=dict(base_unique_observations=config.observations, scenario_setup_observations=1,
                                       correction_observations=4, fault_observations=2,
                                       final_unique_observations=config.observations + 5),
                  submitted_setup_observations=0, submitted_correction_observations=0,
                  submitted_fault_observations=0,
                  shard_read_seconds=0.0, oracle_ingest_seconds=0.0,
                  resource_start=resource_sample(), limitations=LIMITATIONS)
    _save_report(workspace, report)
    try:
        before = time.perf_counter()
        validate_workload(workload)
        report["workload_validation_seconds"] = time.perf_counter() - before
        if verification == "sqlite" or crosscheck_sqlite:
            from .scale_oracle import ScaleOracle
            oracle = ScaleOracle(workspace / "reference" / "oracle.db")
        before = time.perf_counter()
        spark = session()
        spark.sparkContext.setLogLevel("WARN")
        report["spark_start_seconds"] = time.perf_counter() - before
        report["runtime"] = dict(python=sys.version, spark=spark.version, delta="4.4.0",
                                 master=spark.sparkContext.master,
                                 jvm_max_heap_bytes=spark._jvm.java.lang.Runtime.getRuntime().maxMemory(),
                                 bronze_timeout_seconds=bronze_timeout_seconds)

        def create_pipeline():
            return IncrementalPipeline(spark, workspace / "pipeline",
                                       verification=oracle.verify if verification == "sqlite" else None,
                                       bronze_timeout_seconds=bronze_timeout_seconds)

        pipeline = create_pipeline()

        def submit(records, reason, fail_after=None):
            nonlocal pipeline
            report["received_observations"] += len(records)
            before = time.perf_counter()
            if oracle:
                oracle.ingest_batch(records)
            report["oracle_ingest_seconds"] += time.perf_counter() - before
            before = time.perf_counter()
            try:
                result = pipeline.replay(records, reason=reason, fail_after=fail_after)
            finally:
                elapsed = time.perf_counter() - before
                run_id = pipeline.ledger.pending()
                if run_id:
                    result = pipeline.ledger.job(run_id)
                if "result" in locals():
                    payload, attempts = result["payload"], pipeline.ledger.attempts(result["run_id"])
                    report["runs"].append(dict(run_id=result["run_id"], reason=reason, status=result["status"],
                        received=payload["received"], accepted=payload["accepted"], duplicates=payload["duplicates"],
                        changed_matches=len(payload["matches"]), affected_teams=len(payload["teams"]),
                        dispatch_seconds=elapsed, stage_seconds=sum(a["duration_seconds"] or 0 for a in attempts),
                        attempts=attempts))
                    _save_report(workspace, report)
            return result

        def batches():
            iterator = iter_batches(workload)
            while True:
                before = time.perf_counter()
                try:
                    batch = next(iterator)
                except StopIteration:
                    return
                report["shard_read_seconds"] += time.perf_counter() - before
                yield batch

        before = time.perf_counter()
        for batch in batches():
            submit(batch, "base_ingest")
        report["base_ingest_seconds"] = time.perf_counter() - before
        count = pipeline._read("bronze_observations").count()
        if count != config.observations:
            raise AssertionError("Base accepted observations do not match the exact workload budget")
        report["base_unique_observations"] = count
        for batch in batches():
            if submit(batch, "identical_replay")["payload"]["accepted"]:
                raise AssertionError("Identical replay accepted new observations")
        if pipeline._read("bronze_observations").count() != count:
            raise AssertionError("Identical replay changed Bronze count")
        report["identical_replay_idempotent"] = True
        lone = dict(_pair(config, 0)[1], match_id=f"synthetic:scale:{config.namespace}:football:sole",
                    event_id=f"synthetic:scale:{config.namespace}:football:sole:1", revision=1,
                    team_a_id=f"football:scale:{config.namespace}:sole-a", team_a="Scale Solitary Atlas",
                    team_b_id=f"football:scale:{config.namespace}:sole-b", team_b="Scale Solitary Boreal")
        report["submitted_setup_observations"] += 1
        submit([lone], "sole_result_setup")
        watermark = datetime.fromisoformat(manifest["observation_end"].replace("Z", "+00:00"))

        def revise(old, hour, **changes):
            return MatchEvent.model_validate(dict(old, revision=old["revision"] + 1,
                event_id=old["match_id"] + f":{old['revision'] + 1}",
                observed_at=watermark + timedelta(hours=hour), **changes)).row()

        correction = revise(_pair(config, 0)[1], 1, score_a=(_pair(config, 0)[1]["score_a"] + 1))
        original_gold = {(r["sport"], r["team_id"]): r.asDict()
                         for r in pipeline._read("gold_team_stats").collect()}
        report["submitted_correction_observations"] += 1
        result = submit([correction], "late_score_correction")
        impacted = {tuple(t) for t in result["payload"]["teams"]}
        after_gold = {(r["sport"], r["team_id"]): r.asDict()
                      for r in pipeline._read("gold_team_stats").collect()}
        unaffected = [key for key in original_gold if key not in impacted]
        if not unaffected or any(original_gold[key] != after_gold[key] for key in unaffected):
            raise AssertionError("Correction rewrote an unaffected Gold value or audit timestamp")
        report["unaffected_team_rows_preserved"] = len(unaffected)
        cancellation = revise(lone, 2, status="cancelled", completed_at=None, score_a=None, score_b=None)
        report["submitted_correction_observations"] += 1
        submit([cancellation], "cancel_sole_result")
        tombstones = pipeline._read("gold_team_stats").where("deleted_at IS NOT NULL").count()
        if tombstones != 2:
            raise AssertionError("Cancelled sole-result teams must create exactly two tombstones")
        postponed = revise(_pair(config, 2)[1], 3, status="postponed", completed_at=None,
                           score_a=None, score_b=None, start_time=watermark + timedelta(days=7))
        try:
            report["submitted_correction_observations"] += 1
            submit([postponed], "postponed_kickoff", fail_after="silver")
        except RuntimeError as error:
            if str(error) != "Injected failure after silver commit":
                raise
        else:
            raise AssertionError("Stage failure injection did not fail")
        failed_id = pipeline.ledger.pending()
        failed_attempts = pipeline.ledger.attempts(failed_id)
        try:
            pipeline.read_gold()
        except ValueError:
            pass
        else:
            raise AssertionError("An unfinished run exposed active Gold")
        pipeline = create_pipeline()
        before = time.perf_counter()
        resumed = pipeline.resume(failed_id)
        report["recovery_seconds"] = time.perf_counter() - before
        resumed_attempts = pipeline.ledger.attempts(failed_id)
        if any(sum(a["stage"] == stage for a in failed_attempts)
               != sum(a["stage"] == stage for a in resumed_attempts) for stage in ("bronze", "silver")):
            raise AssertionError("Recovery repeated a previously successful stage")
        report["recovered_run"] = dict(run_id=resumed["run_id"], status=resumed["status"], attempts=resumed_attempts)
        report["recovery_scope"] = "New IncrementalPipeline instance; same Python process and Spark session"
        rename = revise(correction, 4, team_a=correction["team_a"] + " United")
        report["submitted_correction_observations"] += 1
        submit([rename], "stable_id_team_rename")
        for fault in (dict(correction, event_id="invalid:scale:score", revision=99, score_a=-1),
                      {"event_id": "invalid:scale:schema", "sport": "football", "unexpected": True}):
            report["received_observations"] += 1
            report["submitted_fault_observations"] += 1
            before = pipeline._read("bronze_observations").count()
            try:
                pipeline.replay([fault], reason="separate_invalid_submission")
            except ValueError as error:
                report["rejected_submissions"].append(dict(error=type(error).__name__, received=1))
            else:
                raise AssertionError("Invalid submission was not rejected")
            if pipeline._read("bronze_observations").count() != before or pipeline.ledger.pending():
                raise AssertionError("Invalid submission changed the canonical pipeline")
        final_count = pipeline._read("bronze_observations").count()
        if final_count != config.observations + 5:
            raise AssertionError("Final observations do not match base plus setup and four revisions")
        report.update(status="verified_locally", final_unique_observations=final_count,
                      scenario_setup_observations=1, correction_observations=4, fault_observations=2,
                      active_gold_rows=pipeline.read_gold().count(), soft_deleted_gold_rows=tombstones,
                      failure_after_silver_recovered=True, rejected_faults_unchanged=True,
                      oracle_parity=True, base_observations_per_second=config.observations / report["base_ingest_seconds"])
        if crosscheck_sqlite and verification == "python":
            before = time.perf_counter()
            oracle.verify(spark, pipeline.paths)
            report["sqlite_crosscheck_seconds"] = time.perf_counter() - before
        if oracle:
            report["independent_oracle"] = oracle.last_report
        if verification == "python":
            from .snapshot import export_snapshot
            class NoModelSnapshot:
                def events(self):
                    return [row.asDict() for row in pipeline._read("bronze_observations")
                            .select(*EVENT_COLUMNS).orderBy("match_id", "revision").collect()]

                def quality(self):
                    return dict(checks=[], summary=dict(last_run_status="succeeded", valid_events=final_count,
                                                        quarantined=0, rejected_candidates=2))
            before = time.perf_counter()
            report["snapshot"] = export_snapshot(NoModelSnapshot(), workspace / "snapshots",
                                                with_predictions=False).name
            report["snapshot_export_seconds"] = time.perf_counter() - before
        return report
    except Exception as error:
        report.update(status="failed", error_type=type(error).__name__, error=str(error))
        if pipeline:
            try:
                if pipeline._exists("bronze_observations"):
                    report["committed_unique_observations_at_failure"] = pipeline._read("bronze_observations").count()
            except Exception:
                pass
        if pipeline and pipeline.ledger.pending():
            run_id = pipeline.ledger.pending()
            report["unfinished_run"] = dict(run_id=run_id, attempts=pipeline.ledger.attempts(run_id))
        raise
    finally:
        try:
            if oracle:
                oracle.close()
            if spark:
                spark.stop()
        finally:
            report["elapsed_seconds"] = time.perf_counter() - start
            report["resource_end"] = resource_sample()
            report["workspace_disk_bytes"] = _disk_bytes(workspace)
            report["workload_disk_bytes"] = _disk_bytes(workload)
            _save_report(workspace, report)


def main():
    parser = argparse.ArgumentParser(description="Isolated fictional workload and measured Spark scale verification")
    sub = parser.add_subparsers(dest="command", required=True)
    generate = sub.add_parser("generate")
    generate.add_argument("--observations", type=int, required=True)
    generate.add_argument("--namespace", required=True)
    generate.add_argument("--output", type=Path, required=True)
    generate.add_argument("--seed", type=int, default=42)
    generate.add_argument("--shard-observations", type=int, default=10_000)
    generate.add_argument("--teams-per-sport", type=int, default=64)
    generate.add_argument("--shuffle-buffer", type=int, default=1_000)
    generate.add_argument("--horizon-days", type=int, default=30)
    validate = sub.add_parser("validate")
    validate.add_argument("--workload", type=Path, required=True)
    scenario = sub.add_parser("scenario")
    scenario.add_argument("--workload", type=Path, required=True)
    scenario.add_argument("--workspace", type=Path, required=True)
    scenario.add_argument("--verification", choices=("python", "sqlite"), default="python")
    scenario.add_argument("--crosscheck-sqlite", action="store_true")
    scenario.add_argument("--bronze-timeout-seconds", type=int, default=180)
    args = parser.parse_args()
    before = time.perf_counter()
    if args.command == "generate":
        config = WorkloadConfig(**{k: v for k, v in vars(args).items() if k not in ("command", "output")})
        manifest = write_workload(config, args.output)
        result = dict(manifest=manifest, generation_seconds=time.perf_counter() - before,
                      disk_bytes=_disk_bytes(args.output), resources=resource_sample())
    elif args.command == "validate":
        result = dict(manifest=validate_workload(args.workload), validation_seconds=time.perf_counter() - before,
                      resources=resource_sample())
    else:
        result = run_scenario(args.workload, args.workspace, verification=args.verification,
                              bronze_timeout_seconds=args.bronze_timeout_seconds,
                              crosscheck_sqlite=args.crosscheck_sqlite)
    print(json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False))


if __name__ == "__main__":
    main()
