"""Incremental Spark/Delta maintenance with persistent stage recovery.

Silver rewrites affected match keys/history only. Gold recomputes only impacted
team keys from all their current finished matches; cancellations produce soft
deletes. An independent Python reconstruction gates success and snapshot export.
"""

import argparse
import json
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path

from .engine import history, latest, team_stats
from .operations import STAGES, StageLedger, bounded_window, utc_now, validated_new_events
from .schema import MatchEvent, canonical
from .spark_backend import aggregate_teams, current_matches, match_schema, session

EVENT_COLUMNS = ["event_id", "match_id", "provider", "sport", "competition", "revision", "team_a_id",
                 "team_a", "team_b_id", "team_b", "start_time", "observed_at", "completed_at", "status",
                 "score_a", "score_b", "best_of"]
METRIC_COLUMNS = ["sport", "team_id", "team_name", "matches", "wins", "draws", "losses", "score_for",
                  "score_against", "last_match_at", "win_rate"]


class IncrementalPipeline:
    def __init__(self, spark, root: Path, *, verification=None, bronze_timeout_seconds=180):
        # Larger isolated benchmarks can supply a disk-backed independent
        # oracle. Normal demonstrations retain the full Python reconstruction.
        if verification is not None and not callable(verification):
            raise TypeError("verification must be an independent verification callable")
        if type(bronze_timeout_seconds) is not int or not 1 <= bronze_timeout_seconds <= 3600:
            raise ValueError("bronze_timeout_seconds must be an integer between 1 and 3600")
        self.verification = verification
        self.bronze_timeout_seconds = bronze_timeout_seconds
        self.spark, self.root = spark, Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.ledger = StageLedger(self.root / "control")
        self.paths = {name: str((self.root / name).resolve()) for name in
                      ("bronze_observations", "silver_matches", "silver_history", "gold_team_stats")}

    def _exists(self, name):
        from delta.tables import DeltaTable
        return DeltaTable.isDeltaTable(self.spark, self.paths[name])

    def _read(self, name):
        return self.spark.read.format("delta").load(self.paths[name])

    def _write(self, name, frame, keys):
        from delta.tables import DeltaTable
        if not self._exists(name):
            frame.write.format("delta").save(self.paths[name])
            return
        updates = {c: f"source.`{c}`" for c in frame.columns if c != "created_at"}
        (DeltaTable.forPath(self.spark, self.paths[name]).alias("target")
         .merge(frame.alias("source"), " AND ".join(f"target.`{k}` = source.`{k}`" for k in keys))
         .whenMatchedUpdate(set=updates).whenNotMatchedInsertAll().execute())

    def replay(self, records, *, fail_after=None, reason="replay"):
        if len(records) > 100_000:
            raise ValueError("A replay batch is limited to 100000 observations")
        with self.ledger.writer():
            if self.ledger.pending():
                raise ValueError("Resume the unfinished run before submitting another batch")
            raw_dir = self.root / "raw_landing"
            raw_dir.mkdir(exist_ok=True)
            candidate_id = uuid.uuid4().hex
            raw_path = raw_dir / f"{candidate_id}.jsonl"
            raw_path.write_text("\n".join(canonical(r) for r in records) + "\n", encoding="utf-8")
            try:
                match_ids = sorted({MatchEvent.model_validate(r).match_id for r in records})
                existing = []
                if match_ids and self._exists("bronze_observations"):
                    keys = self.spark.createDataFrame([(m,) for m in match_ids], "match_id string")
                    existing = [r.asDict() for r in self._read("bronze_observations").join(keys, "match_id")
                                .select(*EVENT_COLUMNS).collect()]
                new = validated_new_events(existing, records)
                if not new and not self._exists("bronze_observations"):
                    raise ValueError("An initial replay requires at least one valid observation")
            except Exception as error:
                quarantine = self.root / "quarantine"
                quarantine.mkdir(exist_ok=True)
                (quarantine / f"{candidate_id}.json").write_text(json.dumps(dict(
                    raw_file=str(raw_path.relative_to(self.root)), error=type(error).__name__,
                    status="entire_candidate_rejected", observed_at=utc_now()), indent=2), encoding="utf-8")
                raise
            teams = sorted({(r["sport"], r[f"team_{side}_id"]) for r in new for side in ("a", "b")})
            payload = dict(events=new, matches=sorted({r["match_id"] for r in new}), teams=teams,
                           timestamp=utc_now(), candidate_id=candidate_id, reason=reason,
                           received=len(records), accepted=len(new), duplicates=len(records) - len(new))
            run_id = self.ledger.create(payload)
            return self._execute(run_id, fail_after)

    def resume(self, run_id=None):
        with self.ledger.writer():
            run_id = run_id or self.ledger.pending()
            if run_id is None:
                raise ValueError("No unfinished run to resume")
            return self._execute(run_id)

    def _execute(self, run_id, fail_after=None):
        result = self.ledger.execute(run_id, {stage: getattr(self, f"_{stage}") for stage in STAGES}, fail_after)
        result["attempts"] = self.ledger.attempts(run_id)
        success = self.ledger.latest_success()
        marker = self.root / "last-success.json"
        try:
            selected = json.loads(marker.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            selected = None
        if isinstance(selected, dict) and selected.get("run_id") == success["run_id"]:
            return result
        # The ledger can commit success just before this marker is interrupted.
        # Rebuild from its latest verified job, even when resuming an older ID.
        completed = self.ledger.attempts(success["run_id"])[-1]["finished_at"]
        temporary = marker.with_suffix(".tmp")
        temporary.write_text(json.dumps(dict(
            run_id=success["run_id"], succeeded_at=completed, accepted=success["payload"]["accepted"],
            changed_matches=len(success["payload"]["matches"]),
            affected_teams=len(success["payload"]["teams"])), indent=2), encoding="utf-8")
        temporary.replace(marker)
        return result

    def _bronze(self, payload):
        from delta.tables import DeltaTable
        from pyspark.sql import functions as F
        if not payload["events"]:
            return
        landing = self.root / "validated_landing"
        landing.mkdir(exist_ok=True)
        target = landing / f"{payload['candidate_id']}.json"
        if not target.exists():
            temp = target.with_suffix(".tmp")
            temp.write_text("\n".join(canonical(r) for r in payload["events"]) + "\n", encoding="utf-8")
            temp.replace(target)
        bronze = self.paths["bronze_observations"]
        if not self._exists("bronze_observations"):
            (self.spark.createDataFrame([], match_schema()).withColumn("created_at", F.lit(None).cast("string"))
             .withColumn("updated_at", F.lit(None).cast("string"))
             .withColumn("deleted_at", F.lit(None).cast("string"))
             .withColumn("source_system", F.lit(None).cast("string"))
             .withColumn("quality_score", F.lit(None).cast("double"))
             .write.format("delta").option("delta.enableChangeDataFeed", "true").save(bronze))

        def insert(batch, _batch_id):
            source = (batch.dropDuplicates(["event_id"])
                      .withColumn("created_at", F.lit(payload["timestamp"]))
                      .withColumn("updated_at", F.lit(payload["timestamp"]))
                      .withColumn("deleted_at", F.lit(None).cast("string"))
                      .withColumn("source_system", F.col("provider"))
                      .withColumn("quality_score", F.lit(1.0)))
            (DeltaTable.forPath(self.spark, bronze).alias("target")
             .merge(source.alias("source"), "target.event_id = source.event_id")
             .whenNotMatchedInsertAll().execute())

        query = (self.spark.readStream.schema(match_schema()).json(str(landing))
                 .writeStream.foreachBatch(insert)
                 .option("checkpointLocation", str(self.root / "checkpoint"))
                 .trigger(availableNow=True).start())
        if not query.awaitTermination(self.bronze_timeout_seconds):
            query.stop()
            raise TimeoutError(f"Bronze AvailableNow exceeded {self.bronze_timeout_seconds} seconds")

    def _silver(self, payload):
        from pyspark.sql import Window, functions as F
        if not payload["matches"]:
            return
        keys = self.spark.createDataFrame([(m,) for m in payload["matches"]], "match_id string")
        events = self._read("bronze_observations").join(keys, "match_id")
        matches = (current_matches(events).withColumn("updated_at", F.lit(payload["timestamp"]))
                   .withColumn("deleted_at", F.when(F.col("status") == "cancelled", F.lit(payload["timestamp"]))))
        self._write("silver_matches", matches, ["match_id"])
        temporal = (events.withColumn("valid_from", F.col("observed_at"))
                    .withColumn("valid_to", F.lead("observed_at").over(Window.partitionBy("match_id").orderBy("revision")))
                    .withColumn("is_current", F.col("valid_to").isNull())
                    .withColumn("updated_at", F.lit(payload["timestamp"])))
        self._write("silver_history", temporal, ["match_id", "revision"])

    def _gold(self, payload):
        from pyspark.sql import functions as F
        if not payload["teams"]:
            return
        teams = self.spark.createDataFrame(payload["teams"], "sport string, team_id string")
        # A team's totals depend on every one of its current matches, not just
        # matches in the newly delivered batch. Team keys scope the computation.
        matches = (self._read("silver_matches").alias("m").join(teams.alias("t"),
            (F.col("m.sport") == F.col("t.sport")) &
            ((F.col("m.team_a_id") == F.col("t.team_id")) | (F.col("m.team_b_id") == F.col("t.team_id"))))
                   .select("m.*").dropDuplicates(["match_id"]))
        aggregates = aggregate_teams(matches).join(teams, ["sport", "team_id"])
        # Left joining every impacted key materializes a tombstone when a
        # cancellation removes the team's final completed result.
        source = (teams.join(aggregates, ["sport", "team_id"], "left")
                  .withColumn("deleted_at", F.when(F.col("matches").isNull(), F.lit(payload["timestamp"])))
                  .withColumn("created_at", F.lit(payload["timestamp"]))
                  .withColumn("updated_at", F.lit(payload["timestamp"]))
                  .withColumn("source_system", F.lit("arena_silver_matches"))
                  .withColumn("quality_score", F.lit(1.0)))
        self._write("gold_team_stats", source, ["sport", "team_id"])

    def _verify(self, _payload):
        if self.verification is not None:
            self.verification(self.spark, self.paths)
            return
        if not self._exists("bronze_observations"):
            return
        events = [r.asDict() for r in self._read("bronze_observations").select(*EVENT_COLUMNS)
                  .orderBy("match_id", "revision").collect()]
        matches = [r.asDict() for r in self._read("silver_matches").select(*EVENT_COLUMNS).orderBy("match_id").collect()]
        expected_matches = latest(events)
        if matches != expected_matches:
            raise AssertionError("Incremental Silver differs from independent full reconstruction")
        actual_history = [r.asDict() for r in self._read("silver_history")
                          .select(*EVENT_COLUMNS, "valid_from", "valid_to").orderBy("match_id", "revision").collect()]
        if actual_history != history(events):
            raise AssertionError("Incremental SCD2 history differs from independent full reconstruction")
        actual = [r.asDict() for r in self._read("gold_team_stats").where("deleted_at IS NULL")
                  .select(*METRIC_COLUMNS).orderBy("sport", "team_id").collect()]
        if actual != team_stats(expected_matches):
            raise AssertionError("Incremental Gold differs from independent full reconstruction")

    def read_gold(self):
        if self.ledger.pending():
            raise ValueError("Gold is unavailable while a stage run is unfinished; resume first")
        return self._read("gold_team_stats").where("deleted_at IS NULL")

    def export(self, output: Path):
        with self.ledger.writer():
            return self._export_locked(output)

    def _export_locked(self, output: Path):
        from .snapshot import export_snapshot
        if self.ledger.pending() or not (self.root / "last-success.json").exists():
            raise ValueError("Publish only after a complete verified stage graph")
        self._verify({})
        pipeline = self
        class VerifiedSnapshot:
            def events(self):
                return [r.asDict() for r in pipeline._read("bronze_observations").select(*EVENT_COLUMNS)
                        .orderBy("match_id", "revision").collect()]

            def quality(self):
                success = json.loads((pipeline.root / "last-success.json").read_text(encoding="utf-8"))
                return dict(run_id=success["run_id"], checks=[], summary=dict(
                    valid_events=pipeline._read("bronze_observations").count(), quarantined=0,
                    rejected_candidates=len(list((pipeline.root / "quarantine").glob("*.json"))),
                    last_run_status="succeeded", execution_backend="spark_delta_verified"))
        # Snapshot contract 2.0 remains stable. Oracle parity has already checked
        # actual Delta metrics; this export serializes that verified canonical
        # state with the shared model and documentation logic.
        return export_snapshot(VerifiedSnapshot(), output)


def scenario(snapshot: Path, root: Path):
    """Measured correction/cancellation/postponement/rename/recovery scenario."""
    from .synthetic import generate
    if (root / "control/stages.db").exists():
        raise ValueError("Scenario needs a fresh output directory; old evidence is retained")
    records = [json.loads(line) for line in (snapshot / "bronze/events.jsonl").read_text(encoding="utf-8").splitlines()]
    # Add a disconnected one-match team pair, then cancel that sole result to
    # prove obsolete aggregates become soft-deleted rather than stale wins.
    lone = next(dict(r) for r in generate(20, faults=False) if r["sport"] == "football" and r["status"] == "finished")
    lone.update(match_id="synthetic:football:sole", event_id="synthetic:football:sole:1", revision=1,
                team_a_id="football:sole-a", team_a="Sole Atlas", team_b_id="football:sole-b", team_b="Sole Boreal")
    records = records + [lone]
    spark = session()
    spark.sparkContext.setLogLevel("WARN")
    start = time.perf_counter()
    pipeline = IncrementalPipeline(spark, root)
    runs = []
    try:
        first = pipeline.replay(records, reason="initial_replay")
        runs.append(first["run_id"])
        count = pipeline._read("bronze_observations").count()
        repeated = pipeline.replay(records, reason="identical_replay")
        runs.append(repeated["run_id"])
        if pipeline._read("bronze_observations").count() != count or repeated["payload"]["accepted"] != 0:
            raise AssertionError("Replay duplicated Bronze")
        completed = next(dict(r) for r in latest(records) if r["sport"] == "football" and r["status"] == "finished"
                         and r["match_id"] != lone["match_id"])
        scheduled = next(dict(r) for r in latest(records) if r["status"] == "scheduled")
        watermark = max(datetime.fromisoformat(r["observed_at"].replace("Z", "+00:00")) for r in records)

        def revision(old, suffix, hour, **changes):
            return MatchEvent.model_validate(dict(old, revision=old["revision"] + 1,
                event_id=old["match_id"] + suffix, observed_at=watermark + timedelta(hours=hour), **changes)).row()

        correction = revision(completed, ":correction", 1, score_a=completed["score_a"] + 1)
        before_correction = {tuple(r[k] for k in ("sport", "team_id")): r.asDict()
                             for r in pipeline._read("gold_team_stats").collect()}
        corrected = pipeline.replay([correction], reason="late_score_correction")
        runs.append(corrected["run_id"])
        impacted = {tuple(t) for t in corrected["payload"]["teams"]}
        after_correction = {tuple(r[k] for k in ("sport", "team_id")): r.asDict()
                            for r in pipeline._read("gold_team_stats").collect()}
        if any(before_correction[k] != after_correction[k] for k in before_correction if k not in impacted):
            raise AssertionError("Incremental correction rewrote an unaffected team's row")
        cancellation = revision(lone, ":cancel", 2, status="cancelled", completed_at=None, score_a=None, score_b=None)
        cancelled = pipeline.replay([cancellation], reason="cancel_sole_result")
        runs.append(cancelled["run_id"])
        tombstones = pipeline._read("gold_team_stats").where("deleted_at IS NOT NULL").count()
        if tombstones != 2:
            raise AssertionError("Cancelled sole-result teams must have two Gold tombstones")
        postponed = revision(scheduled, ":postpone", 3, status="postponed",
                             start_time=datetime.fromisoformat(scheduled["start_time"].replace("Z", "+00:00")) + timedelta(days=7))
        try:
            pipeline.replay([postponed], reason="postponed_kickoff", fail_after="silver")
            raise AssertionError("Fault injection did not fail")
        except RuntimeError as error:
            if "Injected" not in str(error):
                raise
        failed_id = pipeline.ledger.pending()
        try:
            pipeline.read_gold()
            raise AssertionError("Partial run leaked Gold")
        except ValueError:
            pass
        # New pipeline instance uses the persisted job and skips successful
        # Bronze/Silver stages; Spark itself remains up to isolate recovery.
        pipeline = IncrementalPipeline(spark, root)
        resumed = pipeline.resume()
        runs.append(resumed["run_id"])
        rename = revision(correction, ":rename", 4, team_a=correction["team_a"] + " United")
        renamed = pipeline.replay([rename], reason="stable_id_team_rename")
        runs.append(renamed["run_id"])
        # Reprocess a bounded availability window. Every row is already present;
        # the backfill stage graph must leave metric/history state unchanged.
        begin = (watermark - timedelta(days=1)).isoformat()
        end = (watermark + timedelta(days=1)).isoformat()
        accepted = records + [correction, cancellation, postponed, rename]
        backfill = pipeline.replay(bounded_window(accepted, begin, end), reason="bounded_backfill")
        runs.append(backfill["run_id"])
        if backfill["payload"]["accepted"]:
            raise AssertionError("Replaying a completed backfill introduced events")
        exported = pipeline.export(root / "snapshots")
        cdf = pipeline.spark.read.format("delta").option("readChangeFeed", "true").option(
            "startingVersion", 0).load(pipeline.paths["bronze_observations"]).count()
        report = dict(status="verified_locally", spark=spark.version, delta="4.4.0",
            source_module_file=__file__,
            original_events=count, accepted_corrections=4,
            final_events=pipeline._read("bronze_observations").count(), cdf_rows=cdf,
            active_gold_rows=pipeline.read_gold().count(), soft_deleted_gold_rows=tombstones,
            checkpoint_replay_idempotent=True, silver_history_full_oracle_equal=True,
            gold_full_oracle_equal=True, bounded_backfill_idempotent=True,
            failure_after_silver_recovered=True, failed_run_id=failed_id,
            unaffected_team_rows_preserved=True, row_quality_score=1.0,
            elapsed_seconds=time.perf_counter() - start, exported_snapshot=exported.name,
            environment=dict(container_memory_gb=6, cpus=2),
            runs=[dict(run_id=r, reason=pipeline.ledger.job(r)["payload"]["reason"],
                changed_matches=len(pipeline.ledger.job(r)["payload"]["matches"]),
                affected_teams=len(pipeline.ledger.job(r)["payload"]["teams"]),
                attempts=pipeline.ledger.attempts(r)) for r in runs],
            limitations=["File replay is polling-derived observations, not provider push or transaction CDC",
                "Full independent oracle runs at every stage-graph completion; production-scale performance is unclaimed",
                "Delta tables commit individually; consumers are gated until the verified run succeeds",
                "API candidate conflicts reject the entire batch; the reference engine additionally supports per-record quarantine"])
        (root / "incremental-report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(json.dumps(report, indent=2))
        return report
    finally:
        spark.stop()


def benchmark(snapshot: Path, root: Path):
    """A measured file-layout experiment; identical values are mandatory."""
    from pyspark.sql import functions as F
    if root.exists():
        raise ValueError("Benchmark needs a fresh output directory")
    root.mkdir(parents=True)
    spark = session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        frame = spark.read.parquet(str(snapshot / "gold/matches.parquet"))
        tables = {}
        for name, partitions in (("many_files", 16), ("compacted", 2)):
            target = root / name
            start = time.perf_counter()
            frame.repartition(partitions).write.format("delta").save(str(target))
            write_seconds = time.perf_counter() - start
            table = spark.read.format("delta").load(str(target))
            query = table.where("status = 'finished'").groupBy("sport").agg(
                F.count("*").alias("matches"), F.sum("score_a").alias("score_a"), F.sum("score_b").alias("score_b"))
            samples, result = [], None
            for _ in range(3):
                start = time.perf_counter()
                result = [r.asDict() for r in query.orderBy("sport").collect()]
                samples.append(time.perf_counter() - start)
            plan = query._jdf.queryExecution().executedPlan().toString()
            files = list(target.glob("*.parquet"))
            (root / f"{name}-plan.txt").write_text(plan, encoding="utf-8")
            tables[name] = dict(files=len(files), bytes=sum(p.stat().st_size for p in files),
                                write_seconds=write_seconds, query_seconds=samples, result=result)
        if tables["many_files"]["result"] != tables["compacted"]["result"]:
            raise AssertionError("Compaction changed results")
        import statistics
        before, after = (statistics.median(tables[n]["query_seconds"]) for n in ("many_files", "compacted"))
        report = dict(status="verified_locally", spark=spark.version, delta="4.4.0",
            dataset_version=json.loads((snapshot / "manifest.json").read_text())["dataset_version"],
            rows=frame.count(), tables=tables, result_equal=True,
            median_query_speedup=before / after, improvement_observed=after < before,
            protocol="16 vs 2 partitions; 3 sequential cold/warm mixed queries per layout; local 2-core container",
            limitations="Small fixture and OS/JVM caches; exploratory measurement, not a controlled scale claim. No timings fabricated.")
        (root / "benchmark-report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(json.dumps(report, indent=2))
        return report
    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser(description="Actual incremental Spark/Delta stage runner")
    parser.add_argument("--workspace", type=Path, default=Path("data/spark-incremental"))
    sub = parser.add_subparsers(dest="command", required=True)
    for command in ("scenario", "benchmark"):
        task = sub.add_parser(command)
        task.add_argument("--snapshot", type=Path, required=True)
    replay = sub.add_parser("replay")
    replay.add_argument("file", type=Path)
    replay.add_argument("--fail-after", choices=STAGES)
    backfill = sub.add_parser("backfill")
    backfill.add_argument("file", type=Path)
    backfill.add_argument("--from", dest="begin", required=True)
    backfill.add_argument("--to", dest="end", required=True)
    backfill.add_argument("--max-events", type=int, default=100_000)
    resume = sub.add_parser("resume")
    resume.add_argument("--run-id")
    export = sub.add_parser("export")
    export.add_argument("--output", type=Path, default=Path("data/spark-snapshots"))
    args = parser.parse_args()
    if args.command in ("scenario", "benchmark"):
        globals()[args.command](args.snapshot, args.workspace)
        return
    spark = session()
    spark.sparkContext.setLogLevel("WARN")
    pipeline = IncrementalPipeline(spark, args.workspace)
    try:
        if args.command == "resume":
            result = pipeline.resume(args.run_id)
        elif args.command == "export":
            print(pipeline.export(args.output))
            return
        else:
            records = [json.loads(line) for line in args.file.read_text(encoding="utf-8").splitlines() if line.strip()]
            if args.command == "backfill":
                records = bounded_window(records, args.begin, args.end, args.max_events)
            result = pipeline.replay(records, fail_after=getattr(args, "fail_after", None), reason=args.command)
        print(json.dumps(dict(run_id=result["run_id"], status=result["status"],
                             accepted=result["payload"]["accepted"], attempts=result["attempts"]), indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
