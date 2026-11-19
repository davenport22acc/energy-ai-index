# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # 01 Bronze: Marktstammdatenregister
#
# Downloads the full MaStR export (about 3 GB, DL-DE-BY-2.0) into `Files/raw/mastr`, parses the XML files of the selected tables
# and writes them 1:1 into Delta tables `bronze_mastr_<table>`.
#
# - every column as string, no filters, no cleaning
# - one partition per `load_date`, a rerun of the same date replaces only that partition
# - `MASTR_ZIP` points to an existing zip (tests), `MAX_FILES_PER_TABLE` limits the XML files per table for quick runs

# PARAMETERS CELL ********************

LOAD_DATE = ""            # YYYYMMDD of the export, empty = newest available
MASTR_ZIP = ""            # path to an existing export zip, empty = download
MAX_FILES_PER_TABLE = 0   # > 0 limits XML files per table (quick test run)
TABLES = [
    "EinheitenWind", "EinheitenSolar", "EinheitenStromSpeicher",
    "EinheitenGenehmigung", "Marktakteure", "Katalogwerte", "Katalogkategorien",
]

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

# ## Download

# CELL ********************

import time
from datetime import timedelta

import requests

UA = {"User-Agent": "accilium-energy-pipeline-radar/0.3 (research)"}
BASE = "https://download.marktstammdatenregister.de"


def candidate_urls(day):
    yy, stamp = day.year % 100, day.strftime("%Y%m%d")
    for version in (f"{yy}.2", f"{yy}.1", f"{yy - 1}.2", f"{yy + 1}.1"):
        yield stamp, f"{BASE}/Gesamtdatenexport_{stamp}_{version}.zip"


def download(url, target, attempts=8):
    """Download with retries and HTTP resume. The MaStR server drops connections now and then."""
    part = target + ".part"
    for i in range(attempts):
        done = os.path.getsize(part) if os.path.exists(part) else 0
        headers = dict(UA, Range=f"bytes={done}-") if done else UA
        try:
            with requests.get(url, stream=True, headers=headers, timeout=120) as r:
                if r.status_code not in (200, 206):
                    raise requests.RequestException(f"HTTP {r.status_code}")
                with open(part, "ab" if r.status_code == 206 else "wb") as f:
                    for chunk in r.iter_content(chunk_size=8 << 20):
                        f.write(chunk)
            os.replace(part, target)
            return
        except requests.RequestException as exc:
            print(f"Attempt {i + 1} failed: {exc}")
            time.sleep(30 * (i + 1))
    raise RuntimeError(f"Download failed: {url}")


if MASTR_ZIP:
    zip_path = MASTR_ZIP
    LOAD_DATE = LOAD_DATE or datetime.now(timezone.utc).strftime("%Y%m%d")
else:
    raw_dir = f"{FILES}/raw/mastr"
    os.makedirs(raw_dir, exist_ok=True)
    if LOAD_DATE:
        days = [datetime.strptime(LOAD_DATE, "%Y%m%d")]
    else:
        days = [datetime.now(timezone.utc) - timedelta(days=d) for d in range(3)]
    zip_path = None
    for day in days:
        for stamp, url in candidate_urls(day):
            target = f"{raw_dir}/Gesamtdatenexport_{stamp}.zip"
            if os.path.exists(target):
                zip_path, LOAD_DATE = target, stamp
                break
            try:
                probe = requests.get(url, stream=True, headers=UA, timeout=60)
                probe.close()
            except requests.RequestException:
                continue
            if probe.status_code == 200:
                print("Downloading", url)
                download(url, target)
                zip_path, LOAD_DATE = target, stamp
                break
        if zip_path:
            break
    if not zip_path:
        raise RuntimeError("No MaStR export found for the last days")

print("Export:", zip_path, "| load_date:", LOAD_DATE)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Parse XML to Parquet staging

# CELL ********************

import re
import shutil
import zipfile

import pyarrow as pa
import pyarrow.parquet as pq
from lxml import etree

STAGING = f"{FILES}/staging/mastr/{LOAD_DATE}"
shutil.rmtree(STAGING, ignore_errors=True)


def table_of(member):
    return re.sub(r"(_\d+)?\.xml$", "", os.path.basename(member))


def records(stream):
    """Yield one dict per record element (children of the root), streaming, constant memory."""
    for _, el in etree.iterparse(stream, events=("end",), huge_tree=True):
        parent = el.getparent()
        if parent is not None and parent.getparent() is None:
            yield {etree.QName(c).localname: c.text for c in el}
            el.clear()
            while el.getprevious() is not None:
                del parent[0]


def flush(rows, table, part):
    cols = sorted({k for r in rows for k in r})
    data = {c: pa.array([r.get(c) for r in rows], type=pa.string()) for c in cols}
    os.makedirs(f"{STAGING}/{table}", exist_ok=True)
    pq.write_table(pa.table(data), f"{STAGING}/{table}/part-{part:05d}.parquet")


counts, files_seen, parts = {}, {}, {}
with zipfile.ZipFile(zip_path) as z:
    members = sorted(m for m in z.namelist() if m.endswith(".xml") and table_of(m) in TABLES)
    for m in members:
        t = table_of(m)
        files_seen[t] = files_seen.get(t, 0) + 1
        if MAX_FILES_PER_TABLE and files_seen[t] > MAX_FILES_PER_TABLE:
            continue
        rows = []
        with z.open(m) as f:
            for rec in records(f):
                rows.append(rec)
                if len(rows) >= 200_000:
                    flush(rows, t, parts.get(t, 0))
                    parts[t] = parts.get(t, 0) + 1
                    counts[t] = counts.get(t, 0) + len(rows)
                    rows = []
        if rows:
            flush(rows, t, parts.get(t, 0))
            parts[t] = parts.get(t, 0) + 1
            counts[t] = counts.get(t, 0) + len(rows)
        print(f"{m}: ok")

missing = set(TABLES) - set(counts)
print("Rows per table:", counts)
if missing:
    print("WARNING, not in export:", sorted(missing))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Load into bronze Delta tables

# CELL ********************

for t in sorted(counts):
    df = (
        spark.read.option("mergeSchema", "true")
        .parquet(f"{SPARK_FILES}/staging/mastr/{LOAD_DATE}/{t}")
        .withColumn("load_date", F.lit(LOAD_DATE))
        .withColumn("source_file", F.lit(os.path.basename(zip_path)))
    )
    name = f"bronze_mastr_{t.lower()}"
    write_table(df, name, partition_by="load_date", replace_where=f"load_date = '{LOAD_DATE}'")
    print(f"{name}: {counts[t]:,} rows for {LOAD_DATE}")

shutil.rmtree(STAGING, ignore_errors=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
