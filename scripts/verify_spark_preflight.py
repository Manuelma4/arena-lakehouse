"""Native Spark/Delta event ownership acceptance over an isolated synthetic store."""

import argparse
import hashlib
import json
import time
from contextlib import closing
from datetime import datetime, timedelta
from pathlib import Path

from arena_lakehouse.incremental import IncrementalPipeline
from arena_lakehouse.spark_backend import session


def verify(workload, workspace):
    if workspace.exists():
        raise ValueError("Use a fresh workspace; retain earlier reports")
    records = []
    for shard in sorted((workload / "shards").glob("*.jsonl")):
        with shard.open(encoding="utf-8") as source:
            for line in source:
                row = json.loads(line)
                if row.get("provider") != "synthetic":
                    raise ValueError("This isolated acceptance requires synthetic workload records")
                if row["status"] == "finished" and row["match_id"] not in {r["match_id"] for r in records}:
                    records.append(row)
                if len(records) == 2:
                    break
        if len(records) == 2:
            break
    if len(records) != 2:
        raise ValueError("Workload requires two distinct finished synthetic matches")
    workspace.mkdir(parents=True)
    report = dict(status="running", scope="Two synthetic matches; native event identity acceptance, not scale throughput",
        predictions="not_run", provider_requests=0, model_requests=0, rejected=[])
    spark = None
    started = time.perf_counter()
    try:
        spark = session()
        spark.sparkContext.setLogLevel("WARN")
        report["runtime"] = dict(spark=spark.version, master=spark.sparkContext.master,
            driver_heap_bytes=spark._jvm.java.lang.Runtime.getRuntime().maxMemory())
        pipeline = IncrementalPipeline(spark, workspace / "pipeline", bronze_timeout_seconds=300)
        pipeline.replay(records, reason="native_preflight_setup")
        # Retain an older revision so ownership cannot be inferred only from current state.
        old = records[0]
        revised = {**old, "revision":old["revision"] + 1, "event_id":old["event_id"] + ":next",
            "observed_at":(datetime.fromisoformat(old["observed_at"].replace("Z", "+00:00")) + timedelta(hours=1)).isoformat()}
        pipeline.replay([revised], reason="native_preflight_newer_revision")

        def state():
            from delta.tables import DeltaTable
            result = {}
            for name in pipeline.paths:
                rows = [row.asDict() for row in pipeline._read(name).collect()]
                encoded = sorted(json.dumps(row,sort_keys=True,default=str) for row in rows)
                version = DeltaTable.forPath(spark,pipeline.paths[name]).history(1).select("version").first()[0]
                result[name] = dict(rows=len(rows),version=version,
                    sha256=hashlib.sha256("\n".join(encoded).encode()).hexdigest())
            with closing(pipeline.ledger.connect()) as db:
                result["jobs"] = db.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
            return result

        baseline = state()
        borrowed = {**records[1], "event_id":old["event_id"], "match_id":records[1]["match_id"] + ":foreign"}
        candidates = [
            ("retained_historical_id_borrowed_by_another_match", [borrowed]),
            ("one_new_id_reused_within_candidate_batch", [
                {**records[0], "match_id":old["match_id"] + ":candidate-a", "event_id":"synthetic:preflight:collision:1"},
                {**records[1], "match_id":records[1]["match_id"] + ":candidate-b", "event_id":"synthetic:preflight:collision:1"},
            ]),
        ]
        for name, candidate in candidates:
            try:
                pipeline.replay(candidate,reason=name)
            except ValueError:
                report["rejected"].append(name)
            else:
                raise AssertionError("Event ownership conflict was accepted")
            if state() != baseline or pipeline.ledger.pending():
                raise AssertionError("Rejected candidate changed a Delta table or created a job")
        duplicate = pipeline.replay([old],reason="native_preflight_exact_historical_retry")
        after_retry = state()
        if duplicate["payload"]["accepted"] != 0 or duplicate["payload"]["duplicates"] != 1:
            raise AssertionError("Exact historical retry lost duplicate semantics")
        if any(after_retry[name] != baseline[name] for name in pipeline.paths):
            raise AssertionError("Exact historical retry changed Delta state")
        report.update(status="verified_locally", historical_collision_rejected=True,
            candidate_collision_rejected=True, no_job_for_rejected_candidates=True,
            exact_historical_retry_idempotent=True, delta_state=after_retry,
            quarantine_records=len(list((pipeline.root / "quarantine").glob("*.json"))))
        return report
    except Exception as error:
        report.update(status="failed", error_type=type(error).__name__, error=str(error))
        raise
    finally:
        try:
            if spark is not None:
                spark.stop()
        finally:
            report["elapsed_seconds"] = time.perf_counter() - started
            (workspace / "native-preflight-report.json").write_text(
                json.dumps(report,indent=2,allow_nan=False),encoding="utf-8")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workload", type=Path, required=True)
    parser.add_argument("--workspace", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(verify(args.workload,args.workspace),indent=2))
