# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # 02 Silver: units, history, operators, permits
#
# Reads one `load_date` from bronze and builds clean, typed tables:
#
# | Table | Content |
# | --- | --- |
# | `silver_units` | wind, PV and storage in one schema, codes decoded via Katalogwerte, current snapshot |
# | `silver_units_history` | SCD type 2: a new version whenever status, capacity, dates or operator change |
# | `silver_operators` | companies only, no natural persons |
# | `silver_permits` | permits (Genehmigungen) |
#
# No size filter here. Filters belong in gold.

# PARAMETERS CELL ********************

LOAD_DATE = ""  # YYYYMMDD, empty = newest load_date in bronze

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

# MARKDOWN ********************

# ## Helpers: latest snapshot and catalog decoding

# CELL ********************

def snapshot(table):
    df = read_table(table)
    d = LOAD_DATE or df.agg(F.max("load_date")).first()[0]
    return df.where(F.col("load_date") == d), d


katalog, LOAD_DATE = snapshot("bronze_mastr_katalogwerte")
katalog = katalog.select("Id", "Wert").dropDuplicates(["Id"])
print("load_date:", LOAD_DATE, "| catalog values:", katalog.count())

# MaStR stores many attributes as catalog ids. Katalogwerte ids are unique across categories.
CODED = [
    "EinheitBetriebsstatus", "Bundesland", "Land", "Energietraeger", "Lage", "Technologie",
    "Batterietechnologie", "Hersteller", "Einspeisungsart", "Art", "Personenart", "Rechtsform",
    "Hauptausrichtung", "Nutzungsbereich", "Marktfunktion",
]


def decode(df):
    for c in CODED:
        if c in df.columns:
            k = F.broadcast(katalog.select(F.col("Id").alias(f"_id_{c}"), F.col("Wert").alias(f"_v_{c}")))
            df = (
                df.join(k, df[c] == k[f"_id_{c}"], "left")
                .withColumn(c, F.coalesce(F.col(f"_v_{c}"), F.col(c)))
                .drop(f"_id_{c}", f"_v_{c}")
            )
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## silver_units

# CELL ********************

SOURCES = {
    "wind": "bronze_mastr_einheitenwind",
    "solar": "bronze_mastr_einheitensolar",
    "storage": "bronze_mastr_einheitenstromspeicher",
}


def units_of(tech, table):
    df, _ = snapshot(table)
    df = decode(df)
    storage = tech == "storage"
    return df.select(
        col(df, "EinheitMastrNummer", "unit_id"),
        F.lit(tech).alias("technology"),
        col(df, "NameStromerzeugungseinheit", "unit_name"),
        col(df, "EinheitBetriebsstatus", "status"),
        col(df, "Nettonennleistung", "net_kw", "double"),
        col(df, "Bruttoleistung", "gross_kw", "double"),
        col(df, "Bundesland", "state"),
        col(df, "Land", "country"),
        col(df, "Gemeinde", "municipality"),
        col(df, "Postleitzahl", "postcode"),
        col(df, "Breitengrad", "lat", "double"),
        col(df, "Laengengrad", "lon", "double"),
        col(df, "AnlagenbetreiberMastrNummer", "operator_id"),
        col(df, "Inbetriebnahmedatum", "commissioning_date", "date"),
        col(df, "GeplantesInbetriebnahmedatum", "planned_commissioning_date", "date"),
        col(df, "DatumEndgueltigeStilllegung", "decommissioning_date", "date"),
        col(df, "Registrierungsdatum", "registered_date", "date"),
        col(df, "DatumLetzteAktualisierung", "last_update", "timestamp"),
        col(df, "GenMastrNummer", "permit_id"),
        col(df, "EegMastrNummer", "eeg_id"),
        col(df, "LokationMastrNummer", "location_id"),
        (col(df, "Lage", "wind_location") if tech == "wind" else F.lit(None).cast("string").alias("wind_location")),
        (col(df, "Technologie", "storage_technology") if storage else F.lit(None).cast("string").alias("storage_technology")),
        (col(df, "Batterietechnologie", "battery_technology") if storage else F.lit(None).cast("string").alias("battery_technology")),
        F.to_date(F.lit(LOAD_DATE), "yyyyMMdd").alias("snapshot_date"),
    )


units = None
for tech, table in SOURCES.items():
    if table_exists(table):
        part = units_of(tech, table)
        units = part if units is None else units.unionByName(part)

units = units.cache()
write_table(units, "silver_units")
units.groupBy("technology", "status").agg(F.count("*").alias("units"), F.round(F.sum("net_kw") / 1000).alias("mw")).orderBy("technology", "status").show(50, False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## silver_units_history (SCD type 2)
#
# Every unit gets a row hash over the tracked attributes. A changed hash closes the current version and opens a new one.
# Units that disappear from the register are closed. Running the same `load_date` twice changes nothing.

# CELL ********************

TRACKED = ["status", "net_kw", "operator_id", "planned_commissioning_date", "commissioning_date", "decommissioning_date"]
HIST = "silver_units_history"
day = F.to_date(F.lit(LOAD_DATE), "yyyyMMdd")

snap = units.withColumn(
    "row_hash",
    F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("~")) for c in TRACKED]), 256),
)

if not table_exists(HIST):
    first = snap.withColumn("valid_from", day).withColumn("valid_to", F.lit(None).cast("date")).withColumn("is_current", F.lit(True))
    write_table(first, HIST)
else:
    current = read_table(HIST).where("is_current").select("unit_id", F.col("row_hash").alias("_old_hash"))
    changed = snap.join(current, "unit_id", "left").where(
        F.col("_old_hash").isNull() | (F.col("_old_hash") != F.col("row_hash"))
    )
    existing_changed = changed.where(F.col("_old_hash").isNotNull())
    staged = (
        changed.drop("_old_hash").withColumn("merge_key", F.col("unit_id"))
        .unionByName(existing_changed.drop("_old_hash").withColumn("merge_key", F.lit(None).cast("string")))
    )
    insert_values = {c: f"s.{c}" for c in snap.columns}
    insert_values.update({"valid_from": f"to_date('{LOAD_DATE}', 'yyyyMMdd')", "valid_to": "CAST(NULL AS DATE)", "is_current": "true"})
    (
        delta_table(HIST).alias("h")
        .merge(staged.alias("s"), "h.unit_id = s.merge_key AND h.is_current = true")
        .whenMatchedUpdate(
            condition="h.row_hash <> s.row_hash",
            set={"is_current": "false", "valid_to": f"to_date('{LOAD_DATE}', 'yyyyMMdd')"},
        )
        .whenNotMatchedInsert(values=insert_values)
        .execute()
    )
    # units no longer in the register
    (
        delta_table(HIST).alias("h")
        .merge(snap.select("unit_id").alias("s"), "h.unit_id = s.unit_id")
        .whenNotMatchedBySourceUpdate(
            condition="h.is_current = true",
            set={"is_current": "false", "valid_to": f"to_date('{LOAD_DATE}', 'yyyyMMdd')"},
        )
        .execute()
    )

h = read_table(HIST)
print("history rows:", h.count(), "| current:", h.where("is_current").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## silver_operators and silver_permits

# CELL ********************

actors, _ = snapshot("bronze_mastr_marktakteure")
actors = decode(actors)
operators = actors.select(
    col(actors, "MastrNummer", "operator_id"),
    col(actors, "Personenart", "person_type"),
    col(actors, "Firmenname", "company_name"),
    col(actors, "Rechtsform", "legal_form"),
    col(actors, "Land", "country"),
    col(actors, "Bundesland", "state"),
    col(actors, "Postleitzahl", "postcode"),
    col(actors, "Ort", "city"),
    col(actors, "Registergericht", "register_court"),
    col(actors, "Registernummer", "register_number"),
    col(actors, "Marktfunktion", "market_function"),
    F.to_date(F.lit(LOAD_DATE), "yyyyMMdd").alias("snapshot_date"),
).where(
    # companies only: no natural persons in any downstream table
    F.col("company_name").isNotNull()
    & ~F.lower(F.coalesce(F.col("person_type"), F.lit(""))).contains("natürlich")
)
write_table(operators, "silver_operators")
print("operators (companies):", operators.count())

if table_exists("bronze_mastr_einheitengenehmigung"):
    gen, _ = snapshot("bronze_mastr_einheitengenehmigung")
    gen = decode(gen)
    permits = gen.select(
        col(gen, "GenMastrNummer", "permit_id"),
        col(gen, "Art", "permit_type"),
        col(gen, "Datum", "permit_date", "date"),
        col(gen, "Behoerde", "authority"),
        col(gen, "Aktenzeichen", "file_reference"),
        col(gen, "Frist", "deadline", "date"),
        col(gen, "Registrierungsdatum", "registered_date", "date"),
        F.to_date(F.lit(LOAD_DATE), "yyyyMMdd").alias("snapshot_date"),
    )
    write_table(permits, "silver_permits")
    print("permits:", permits.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Data quality

# CELL ********************

u = read_table("silver_units")
checks = {
    "units not empty": u.count() > 0,
    "unit_id never null": u.where(F.col("unit_id").isNull()).count() == 0,
    "unit_id unique": u.count() == u.select("unit_id").distinct().count(),
    "no negative capacity": u.where(F.col("net_kw") < 0).count() == 0,
    "status decoded (no pure numbers)": u.where(F.col("status").rlike("^[0-9]+$")).count() == 0,
}
for name, ok in checks.items():
    print(("OK   " if ok else "FAIL ") + name)
if not all(checks.values()):
    raise AssertionError("silver data quality failed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
