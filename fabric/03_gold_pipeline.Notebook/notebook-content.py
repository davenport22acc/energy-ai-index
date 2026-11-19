# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # 03 Gold: pipeline views
#
# | Table | Use |
# | --- | --- |
# | `gold_pipeline_by_state` | MW and units by technology, storage type, state and status |
# | `gold_projects` | units in planning >= `MIN_KW`, with operator (companies only) and permit, for map and profiles |
# | `gold_status_transitions` | monthly status changes from the history, e.g. planning to operation |
#
# Plus a JSON export of the small tables to `Files/exports` for the static website.

# PARAMETERS CELL ********************

MIN_KW = 1000  # size threshold for gold tables

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- shared setup: identical in every notebook (checked in CI) ---
import os
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.window import Window

IN_FABRIC = os.path.exists("/lakehouse/default")
if IN_FABRIC:
    FILES = "/lakehouse/default/Files"  # local file API path of the default lakehouse
    SPARK_FILES = "Files"                # same folder for Spark
else:
    _root = os.path.abspath(os.environ.get("LAKE_ROOT", "lake"))
    FILES = f"{_root}/Files"
    SPARK_FILES = FILES
    os.makedirs(FILES, exist_ok=True)

try:
    spark  # provided by Fabric
except NameError:
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    _builder = (
        SparkSession.builder.appName("energy-pipeline-radar")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "6g"))
        .config("spark.sql.session.timeZone", "UTC")
    )
    spark = configure_spark_with_delta_pip(_builder).getOrCreate()

from delta.tables import DeltaTable


def _path(name):
    return f"{_root}/Tables/{name}"


def table_exists(name):
    if IN_FABRIC:
        return spark.catalog.tableExists(name)
    return DeltaTable.isDeltaTable(spark, _path(name))


def read_table(name):
    return spark.read.table(name) if IN_FABRIC else spark.read.format("delta").load(_path(name))


def delta_table(name):
    return DeltaTable.forName(spark, name) if IN_FABRIC else DeltaTable.forPath(spark, _path(name))


def write_table(df, name, mode="overwrite", partition_by=None, replace_where=None):
    w = df.write.format("delta").mode(mode).option("mergeSchema", "true")
    if partition_by:
        w = w.partitionBy(partition_by)
    if replace_where and table_exists(name):
        w = w.option("replaceWhere", replace_where)
    elif mode == "overwrite":
        w = w.option("overwriteSchema", "true")
    if IN_FABRIC:
        w.saveAsTable(name)
    else:
        w.save(_path(name))


def col(df, source, alias, cast=None):
    """Select a source column if present, else NULL. Keeps notebooks robust to MaStR schema changes."""
    e = F.col(source) if source in df.columns else F.lit(None).cast("string")
    return (e.cast(cast) if cast else e).alias(alias)
# --- end shared setup ---

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

units = read_table("silver_units").where(F.col("net_kw") >= MIN_KW)
operators = read_table("silver_operators")

by_state = (
    units.groupBy("technology", "storage_technology", "state", "status")
    .agg(F.count("*").alias("units"), F.round(F.sum("net_kw") / 1000, 1).alias("mw"))
)
write_table(by_state, "gold_pipeline_by_state")

projects = (
    units.where(F.col("status") == "In Planung")
    .join(operators.select("operator_id", "company_name", "legal_form"), "operator_id", "left")
    .withColumn("unit_name", F.when(F.col("company_name").isNotNull(), F.col("unit_name")))  # free-text names only for companies
    .withColumn("net_mw", F.round(F.col("net_kw") / 1000, 2))
)
if table_exists("silver_permits"):
    permits = read_table("silver_permits").select("permit_id", "permit_date", "authority", "permit_type")
    projects = projects.join(permits, "permit_id", "left")
projects = projects.drop("net_kw", "gross_kw")
write_table(projects, "gold_projects")

hist = read_table("silver_units_history").where(F.col("net_kw") >= MIN_KW)
w = Window.partitionBy("unit_id").orderBy("valid_from")
transitions = (
    hist.withColumn("from_status", F.lag("status").over(w))
    .where(F.col("from_status").isNotNull() & (F.col("from_status") != F.col("status")))
    .groupBy(F.trunc("valid_from", "month").alias("month"), "technology", "from_status", F.col("status").alias("to_status"))
    .agg(F.count("*").alias("units"), F.round(F.sum("net_kw") / 1000, 1).alias("mw"))
)
write_table(transitions, "gold_status_transitions")

for t in ["gold_pipeline_by_state", "gold_projects", "gold_status_transitions"]:
    print(t, read_table(t).count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Export for the website

# CELL ********************

import json

export_dir = f"{FILES}/exports"
os.makedirs(export_dir, exist_ok=True)
for t in ["gold_pipeline_by_state", "gold_status_transitions"]:
    rows = [r.asDict() for r in read_table(t).collect()]
    with open(f"{export_dir}/{t}.json", "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, default=str)
    print(f"{export_dir}/{t}.json: {len(rows)} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
