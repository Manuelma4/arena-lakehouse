# Databricks notebook source
# MAGIC %md
# MAGIC # Arena Gold contract verification
# MAGIC Status: pending workspace verification. This task follows the pipeline
# MAGIC update and reads only published Gold datasets. No snapshot release is
# MAGIC authorized by this notebook; the full acceptance report remains separate.

# COMMAND ----------
import re

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "main")  # noqa: F821
dbutils.widgets.text("schema", "arena_demo")  # noqa: F821
catalog = dbutils.widgets.get("catalog")  # noqa: F821
schema = dbutils.widgets.get("schema")  # noqa: F821
if not all(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name) for name in (catalog, schema)):
    raise ValueError("Catalog/schema parameters must be unquoted SQL identifiers")
prefix = f"`{catalog}`.`{schema}`"
matches = spark.read.table(f"{prefix}.gold_matches")  # noqa: F821
stats = spark.read.table(f"{prefix}.gold_team_stats")  # noqa: F821
if matches.groupBy("match_id").count().where("count <> 1").limit(1).count():
    raise AssertionError("Gold match keys are not unique")
if stats.groupBy("sport", "team_id").count().where("count <> 1").limit(1).count():
    raise AssertionError("Gold team keys are not unique")
bad = stats.where("matches <= 0 OR wins + draws + losses <> matches OR "
                  "win_rate < 0 OR win_rate > 1 OR quality_score <> 1")
if bad.limit(1).count():
    raise AssertionError("Gold team metrics violate the contract")
actual_totals = stats.groupBy("sport").agg(F.sum("matches").alias("team_match_count"))
expected_totals = matches.where("status = 'finished'").groupBy("sport").count()
if actual_totals.join(expected_totals, "sport", "full").where(
    "coalesce(team_match_count,0) <> 2 * coalesce(count,0)"
).limit(1).count():
    raise AssertionError("Finished-match and team-match totals disagree")
print({"status": "verified_in_databricks", "gold_matches": matches.count(), "gold_teams": stats.count()})
