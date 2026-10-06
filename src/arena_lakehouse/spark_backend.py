"""Real Spark/Delta replay, CDF and checkpoint smoke with an independent oracle."""

import argparse
import json
import time
from pathlib import Path

def session():
    from pyspark.sql import SparkSession
    return (SparkSession.builder.appName("arena-lakehouse").master("local[2]")
            .config("spark.driver.memory", "2g")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.databricks.delta.snapshotPartitions", "2")
            .config("spark.jars.packages", "io.delta:delta-spark_4.1_2.13:4.4.0")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate())


def match_schema():
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType
    integers = {"revision", "score_a", "score_b", "best_of"}
    fields = ["event_id", "match_id", "provider", "sport", "competition", "revision", "team_a_id",
              "team_a", "team_b_id", "team_b", "start_time", "observed_at", "completed_at", "status",
              "score_a", "score_b", "best_of"]
    return StructType([StructField(name, IntegerType() if name in integers else StringType(), True) for name in fields])


def current_matches(events):
    from pyspark.sql import Window, functions as F
    return (events.withColumn("_rank", F.row_number().over(Window.partitionBy("match_id").orderBy(F.desc("revision"))))
            .where("_rank = 1").drop("_rank"))


def aggregate_teams(matches):
    from pyspark.sql import functions as F
    parts = []
    for side, opponent in (("a", "b"), ("b", "a")):
        parts.append(matches.select("sport", "match_id", "observed_at", "status",
            F.col(f"team_{side}_id").alias("team_id"),
            F.col(f"team_{side}").alias("team_name"), F.col(f"score_{side}").alias("score_for"),
            F.col(f"score_{opponent}").alias("score_against"), "completed_at"))
    union = parts[0].unionByName(parts[1])
    from pyspark.sql import Window
    names = (union.withColumn("_name_rank", F.row_number().over(Window.partitionBy("sport", "team_id")
              .orderBy(F.desc("observed_at"), F.desc("match_id"))))
             .where("_name_rank = 1").select("sport", "team_id", "team_name"))
    return (union.where("status = 'finished'").groupBy("sport", "team_id").agg(
        F.count("*").alias("matches"),
        F.sum((F.col("score_for") > F.col("score_against")).cast("long")).alias("wins"),
        F.sum((F.col("score_for") == F.col("score_against")).cast("long")).alias("draws"),
        F.sum((F.col("score_for") < F.col("score_against")).cast("long")).alias("losses"),
        F.sum("score_for").alias("score_for"), F.sum("score_against").alias("score_against"),
        F.max("completed_at").alias("last_match_at"))
        .withColumn("win_rate", F.col("wins") / F.col("matches"))
        .join(names, ["sport", "team_id"]))


def run(snapshot: Path, root: Path):
    from delta.tables import DeltaTable
    from pyspark.sql import Window, functions as F
    from .engine import latest, team_stats

    records = [json.loads(x) for x in (snapshot / "bronze/events.jsonl").read_text(encoding="utf-8").splitlines()]
    if not records:
        raise ValueError("Spark smoke requires a nonempty validated snapshot")
    root.mkdir(parents=True, exist_ok=True)
    landing = root / "landing"
    landing.mkdir(exist_ok=True)
    original = landing / "0001.json"
    original.write_text("\n".join(json.dumps(r) for r in records) + "\n", encoding="utf-8")
    spark = session()
    spark.sparkContext.setLogLevel("WARN")
    start = time.perf_counter()
    bronze = str((root / "bronze").resolve())
    try:
        if not DeltaTable.isDeltaTable(spark, bronze):
            spark.createDataFrame([], match_schema()).write.format("delta").option(
                "delta.enableChangeDataFeed", "true").save(bronze)

        def merge(batch, _batch_id):
            (DeltaTable.forPath(spark, bronze).alias("target")
             .merge(batch.dropDuplicates(["event_id"]).alias("source"), "target.event_id = source.event_id")
             .whenNotMatchedInsertAll().execute())

        def consume():
            query = (spark.readStream.schema(match_schema()).json(str(landing))
                     .writeStream.foreachBatch(merge).option("checkpointLocation", str(root / "checkpoint"))
                     .trigger(availableNow=True).start())
            if not query.awaitTermination(180):
                query.stop()
                raise TimeoutError("AvailableNow did not finish within 180 seconds")

        consume()
        before = spark.read.format("delta").load(bronze).count()
        consume()
        after = spark.read.format("delta").load(bronze).count()
        if before != after or before != len(records):
            raise AssertionError("Checkpoint replay introduced duplicates or lost records")
        # A second file represents a late source correction, not a made-up live feed.
        correction = next(dict(r) for r in latest(records) if r["sport"] == "football" and r["status"] == "finished")
        from datetime import datetime, timedelta
        correction["revision"] += 1
        correction["event_id"] = correction["match_id"] + ":late-correction"
        correction["score_a"] += 1
        correction["observed_at"] = (datetime.fromisoformat(correction["observed_at"].replace("Z", "+00:00"))
                                     + timedelta(days=10)).isoformat(timespec="microseconds").replace("+00:00", "Z")
        (landing / "0002.json").write_text(json.dumps(correction) + "\n", encoding="utf-8")
        consume()
        accepted = records + [correction]
        events = spark.read.format("delta").load(bronze)
        matches = current_matches(events)
        gold = aggregate_teams(matches)
        actual = [r.asDict() for r in gold.orderBy("sport", "team_id").collect()]
        expected = team_stats(latest(accepted))
        if actual != expected:
            raise AssertionError("Spark team aggregates differ from the independent Python oracle")
        matches.write.format("delta").mode("overwrite").save(str(root / "silver_matches"))
        gold.write.format("delta").mode("overwrite").save(str(root / "gold_team_stats"))
        window = Window.partitionBy("match_id").orderBy("revision")
        (events.withColumn("valid_from", F.col("observed_at"))
         .withColumn("valid_to", F.lead("observed_at").over(window))
         .write.format("delta").mode("overwrite").save(str(root / "silver_match_history")))
        cdf_rows = spark.read.format("delta").option("readChangeFeed", "true").option(
            "startingVersion", 0).load(bronze).count()
        report = dict(status="verified_locally", spark=spark.version, delta="4.4.0",
                      original_events=before, after_checkpoint_restart=after,
                      after_late_correction=events.count(), cdf_rows=cdf_rows,
                      gold_rows=len(actual), oracle_equal=True, elapsed_seconds=time.perf_counter() - start,
                      limitations="Full Gold rebuild; incremental impacted-team recomputation is a later milestone")
        (root / "smoke-report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(json.dumps(report, indent=2))
    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=Path("data/spark-smoke"))
    args = parser.parse_args()
    if (args.output / "checkpoint").exists():
        parser.error("Use a fresh smoke output directory; previous evidence is kept intact")
    run(args.snapshot, args.output)


if __name__ == "__main__":
    main()
