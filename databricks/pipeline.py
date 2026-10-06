"""Optional Lakeflow Spark Declarative Pipelines profile.

STATUS: pending Databricks workspace verification. Upstream contract-validation
is mandatory; API payloads must not be deposited directly in this input volume.
The managed engine controls refresh/checkpoints; do not execute this as a script.
"""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType, StructField, StructType

from arena_lakehouse.managed_contract import all_quality_rules
from arena_lakehouse.spark_backend import aggregate_teams, match_schema

spark = SparkSession.getActiveSession()
SOURCE = spark.conf.get("arena.source_volume_path")
if not SOURCE.startswith("/Volumes/"):
    raise ValueError("Configure an existing Unity Catalog Volume directory")


@dp.table(name="bronze_observations", table_properties={"delta.enableChangeDataFeed": "true"})
def bronze_observations():
    raw = (spark.readStream.format("cloudFiles").option("cloudFiles.format", "json")
           .option("cloudFiles.schemaEvolutionMode", "rescue").option("rescuedDataColumn", "_rescued_data")
           .schema(StructType(match_schema().fields + [StructField("_rescued_data", StringType(), True)]))
           .load(SOURCE))
    return (raw.withColumn("created_at", F.current_timestamp().cast("string"))
            .withColumn("updated_at", F.col("created_at"))
            .withColumn("deleted_at", F.lit(None).cast("string"))
            .withColumn("source_system", F.col("provider"))
            .withColumn("source_file", F.col("_metadata.file_path"))
            .withColumn("quality_score", F.when(F.expr(all_quality_rules()), F.lit(1.0)).otherwise(F.lit(0.0))))


@dp.table(name="quarantine_observations")
def quarantine_observations():
    return spark.readStream.table("bronze_observations").where("quality_score < 1.0")


@dp.temporary_view(name="valid_observations")
@dp.expect_or_drop("canonical_match_contract", "quality_score = 1.0")
def valid_observations():
    return spark.readStream.table("bronze_observations").drop("_rescued_data", "source_file")


dp.create_streaming_table(name="silver_match_history")
dp.create_auto_cdc_flow(
    target="silver_match_history", source="valid_observations", keys=["match_id"],
    sequence_by=F.struct("revision", "observed_at"), stored_as_scd_type=2,
    # A cancellation is retained as a match state. Treating it as a hard CDC
    # delete would lose the evidence that explains a removed completed result.
    ignore_null_updates=False,
)


@dp.materialized_view(name="gold_matches")
def gold_matches():
    return (spark.read.table("silver_match_history").where("__END_AT IS NULL")
            .drop("__START_AT", "__END_AT")
            .withColumn("deleted_at", F.when(F.col("status") == "cancelled", F.col("updated_at"))))


@dp.materialized_view(name="gold_team_stats")
@dp.expect_or_fail("nonempty_completed_summary", "matches > 0 AND wins + draws + losses = matches")
def gold_team_stats():
    # Reuse the exact aggregation function verified by the local Delta scenario.
    return (aggregate_teams(spark.read.table("gold_matches"))
            .withColumn("quality_score", F.lit(1.0))
            .withColumn("created_at", F.current_timestamp().cast("string"))
            .withColumn("updated_at", F.col("created_at"))
            .withColumn("deleted_at", F.lit(None).cast("string"))
            .withColumn("source_system", F.lit("arena_managed_gold_matches")))
