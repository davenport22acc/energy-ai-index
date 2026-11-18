"""Load the German Marktstammdatenregister (MaStR) and build the DE pipeline base.

Usage: python -m epr.ingest.mastr [out_dir]

Downloads the full public bulk export via open-mastr (DL-DE-BY-2.0), then writes:
  out/units_pipeline.parquet   wind, solar, storage units >= 1 MW not yet in operation
  out/units_operating.parquet  same technologies >= 1 MW in operation (for history and models)
  out/permits.parquet          permits (EinheitenGenehmigung)
  out/operators.parquet        market actors referenced by the units above
  data/reports/mastr_summary.md  counts and MW by technology, status and state
Column names differ between open-mastr versions, so columns are resolved defensively
and the report lists what was found.
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pandas as pd
from open_mastr import Mastr
from sqlalchemy import inspect

MIN_KW = 1000
TECH_TABLES = {"wind": "wind_extended", "solar": "solar_extended", "storage": "storage_extended"}

CANDIDATES = {
    "unit_id": ["EinheitMastrNummer"],
    "name": ["NameStromerzeugungseinheit", "NameWindpark", "NameStromspeicher"],
    "status": ["EinheitBetriebsstatus"],
    "kw": ["Nettonennleistung", "Bruttoleistung"],
    "state": ["Bundesland"],
    "plz": ["Postleitzahl"],
    "municipality": ["Gemeinde"],
    "lat": ["Breitengrad"],
    "lon": ["Laengengrad"],
    "operator_id": ["AnlagenbetreiberMastrNummer"],
    "commissioning": ["Inbetriebnahmedatum"],
    "planned_commissioning": ["GeplantesInbetriebnahmedatum"],
    "registered": ["Registrierungsdatum"],
    "permit_id": ["GenMastrNummer", "GenehmigungMastrNummer"],
    "eeg_id": ["EegMastrNummer"],
    "location_id": ["LokationMaStRNummer", "LokationMastrNummer"],
}


def pick(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, str]]:
    found = {}
    for key, options in CANDIDATES.items():
        for col in options:
            if col in df.columns:
                found[key] = col
                break
    return df[list(found.values())].rename(columns={v: k for k, v in found.items()}), found


def main(out: str = "out") -> None:
    out_dir = Path(out)
    out_dir.mkdir(parents=True, exist_ok=True)
    db = Mastr()
    db.download(method="bulk", data=["wind", "solar", "storage", "permit", "market"])
    tables = set(inspect(db.engine).get_table_names())

    frames, found_cols = [], {}
    for tech, table in TECH_TABLES.items():
        if table not in tables:
            continue
        raw = pd.read_sql_table(table, db.engine)
        df, found = pick(raw)
        found_cols[tech] = found
        df["technology"] = tech
        df["kw"] = pd.to_numeric(df.get("kw"), errors="coerce")
        frames.append(df[df["kw"] >= MIN_KW])
    units = pd.concat(frames, ignore_index=True)

    operating = units["status"].astype(str).str.contains("In Betrieb", na=False)
    units[~operating].to_parquet(out_dir / "units_pipeline.parquet", index=False)
    units[operating].to_parquet(out_dir / "units_operating.parquet", index=False)

    if "permit" in tables:
        pd.read_sql_table("permit", db.engine).to_parquet(out_dir / "permits.parquet", index=False)
    if "market_actors" in tables and "operator_id" in units:
        actors = pd.read_sql_table("market_actors", db.engine)
        key = "MastrNummer" if "MastrNummer" in actors.columns else actors.columns[0]
        actors[actors[key].isin(units["operator_id"].dropna().unique())].to_parquet(
            out_dir / "operators.parquet", index=False
        )

    summary = (
        units.assign(mw=units["kw"] / 1000)
        .groupby(["technology", "status"], dropna=False)
        .agg(units=("kw", "size"), mw=("mw", "sum"))
        .reset_index()
        .sort_values(["technology", "mw"], ascending=[True, False])
    )
    by_state = (
        units[~operating]
        .assign(mw=lambda d: d["kw"] / 1000)
        .groupby(["state", "technology"], dropna=False)["mw"]
        .sum()
        .unstack(fill_value=0)
        .round(0)
    )
    by_state = by_state.loc[by_state.sum(axis=1).sort_values(ascending=False).index]
    lines = [
        f"# MaStR DE, Stand {date.today().isoformat()}",
        "",
        f"Einheiten Wind, PV und Speicher ab {MIN_KW / 1000:.0f} MW. Quelle: Marktstammdatenregister, Datenlizenz Deutschland 2.0.",
        "",
        "## Nach Technologie und Status",
        "",
        summary.to_markdown(index=False, floatfmt=".0f"),
        "",
        "## Pipeline (nicht in Betrieb) nach Bundesland, MW",
        "",
        by_state.to_markdown(),
        "",
        "## Gefundene Spalten",
        "",
    ] + [f"- {tech}: {', '.join(f'{k}={v}' for k, v in cols.items())}" for tech, cols in found_cols.items()]
    report = Path("data/reports/mastr_summary.md")
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines[:30]))


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "out")
