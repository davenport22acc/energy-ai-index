"""Assert the expected lake state after two fixture months (see make_mastr_fixture.py)."""
from __future__ import annotations

import os
import sys

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

root = os.path.abspath(os.environ.get("LAKE_ROOT", "lake"))
spark = configure_spark_with_delta_pip(
    SparkSession.builder.master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
).getOrCreate()


def t(name):
    return spark.read.format("delta").load(f"{root}/Tables/{name}")


failures = []


def check(label, ok):
    print(("OK   " if ok else "FAIL ") + label)
    if not ok:
        failures.append(label)


bronze_wind = t("bronze_mastr_einheitenwind")
check("bronze keeps both months", {r[0] for r in bronze_wind.select("load_date").distinct().collect()} == {"20261101", "20261201"})
check("bronze wind month 1 has 3 rows (two XML files)", bronze_wind.where("load_date = '20261101'").count() == 3)

units = t("silver_units")
check("silver_units is month 2 snapshot", units.select("snapshot_date").distinct().count() == 1 and units.count() == 7)
check("status decoded", units.where("unit_id = 'W2'").first()["status"] == "In Betrieb")
check("state decoded", units.where("unit_id = 'W2'").first()["state"] == "Brandenburg")
check("storage type decoded", units.where("unit_id = 'P1'").first()["storage_technology"] == "Pumpspeicher")

hist = t("silver_units_history")
w2 = hist.where("unit_id = 'W2'").orderBy("valid_from").collect()
check("W2 has two versions", len(w2) == 2)
check("W2 old version closed", w2[0]["is_current"] is False and str(w2[0]["valid_to"]) == "2026-12-01")
s2 = hist.where("unit_id = 'S2'").collect()
check("S2 removed from register is closed", len(s2) == 1 and s2[0]["is_current"] is False)
check("B2 new in month 2", hist.where("unit_id = 'B2' AND is_current").count() == 1)
check("W1 unchanged keeps one version", hist.where("unit_id = 'W1'").count() == 1)

ops = t("silver_operators")
check("no natural persons in operators", ops.where("operator_id = 'ABR3'").count() == 0 and ops.count() == 2)

trans = t("gold_status_transitions")
row = trans.where("technology = 'wind' AND from_status = 'In Planung' AND to_status = 'In Betrieb'").collect()
check("gold transition planning to operation for W2", len(row) == 1 and row[0]["units"] == 1)

projects = t("gold_projects")
check("gold_projects only in planning >= 1 MW", projects.where("status <> 'In Planung'").count() == 0 and projects.where("unit_id = 'S2'").count() == 0)
check("gold_projects joins permit", projects.where("unit_id = 'W3'").first()["authority"] == "GAA Oldenburg")

check("JSON export written", os.path.exists(f"{root}/Files/exports/gold_pipeline_by_state.json"))

sys.exit(1 if failures else 0)
