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
TECH_TABLES = {
    "wind": ["wind_extended", "units_wind", "EinheitenWind", "einheitenwind"],
    "solar": ["solar_extended", "units_solar", "EinheitenSolar", "einheitensolar"],
    "storage": ["storage_extended", "units_electricity_storage", "EinheitenStromSpeicher", "einheitenstromspeicher"],
}
PERMIT_TABLES = ["permit", "permits", "EinheitenGenehmigung", "einheitengenehmigung"]
ACTOR_TABLES = ["market_actors", "Marktakteure", "marktakteure"]


def first(names: list[str], available: set[str]) -> str | None:
    lower = {a.lower(): a for a in available}
    for n in names:
        if n in available:
            return n
        if n.lower() in lower:
            return lower[n.lower()]
    return None

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


def prefetch(db: Mastr, attempts: int = 8) -> str:
    """Download the full export ourselves, with retries and resume.

    The MaStR server sometimes drops HEAD and range requests, which made open-mastr's
    own download fail. With the zip already in place, open-mastr skips its download.
    Returns the export date (YYYYMMDD) to pass on to db.download.
    """
    import os
    import time as _time
    from datetime import datetime, timedelta, timezone

    import requests
    from open_mastr.xml_download.utils_download_bulk import gen_xml_download_url

    ua = {"User-Agent": "accilium-energy-pipeline-radar/0.2 (research)"}
    folder = Path(db.output_dir) / "data" / "xml_download"
    folder.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc)
    for day in (now, now - timedelta(days=1)):
        stamp = day.strftime("%Y%m%d")
        for version in ("current", "before", "after"):
            url = gen_xml_download_url(day.timetuple(), use_version=version)
            try:
                probe = requests.get(url, stream=True, headers=ua, timeout=60)
                probe.close()
            except requests.RequestException:
                continue
            if probe.status_code != 200:
                continue
            target = folder / f"Gesamtdatenexport_{stamp}.zip"
            part = target.with_suffix(".part")
            for i in range(attempts):
                done = part.stat().st_size if part.exists() else 0
                headers = dict(ua, Range=f"bytes={done}-") if done else ua
                try:
                    with requests.get(url, stream=True, headers=headers, timeout=120) as r:
                        if r.status_code not in (200, 206):
                            raise requests.RequestException(f"HTTP {r.status_code}")
                        mode = "ab" if r.status_code == 206 else "wb"
                        with part.open(mode) as f:
                            for chunk in r.iter_content(chunk_size=8 << 20):
                                f.write(chunk)
                    os.replace(part, target)
                    print(f"Downloaded {url} ({target.stat().st_size / 1e9:.1f} GB)")
                    return stamp
                except requests.RequestException as exc:
                    print(f"Attempt {i + 1} failed: {exc}")
                    _time.sleep(30 * (i + 1))
    raise RuntimeError("MaStR export could not be downloaded")


def read_units(table: str, engine) -> pd.DataFrame:
    """Read only units >= MIN_KW in SQL to keep memory low (PV has millions of rows).

    Works for tables and views. Falls back to a full read if the capacity column has another name.
    """
    for col in ("Nettonennleistung", "Bruttoleistung", "net_capacity", "gross_capacity"):
        try:
            return pd.read_sql_query(f'SELECT * FROM "{table}" WHERE "{col}" >= {MIN_KW}', engine)
        except Exception:
            continue
    return pd.read_sql_query(f'SELECT * FROM "{table}"', engine)


def main(out: str = "out") -> None:
    out_dir = Path(out)
    out_dir.mkdir(parents=True, exist_ok=True)
    db = Mastr()
    stamp = prefetch(db)
    db.download(method="bulk", data=["wind", "solar", "storage", "permit", "market"], date=stamp)
    insp = inspect(db.engine)
    tables = set(insp.get_table_names()) | set(insp.get_view_names())
    print("Tables and views:", sorted(tables))

    frames, found_cols = [], {}
    for tech, options in TECH_TABLES.items():
        table = first(options, tables)
        if table is None:
            print(f"No table for {tech}")
            continue
        raw = read_units(table, db.engine)
        df, found = pick(raw)
        found_cols[tech] = found
        df["technology"] = tech
        df["kw"] = pd.to_numeric(df.get("kw"), errors="coerce")
        frames.append(df[df["kw"] >= MIN_KW])
    if not frames:
        raise RuntimeError(f"No unit tables found. Available: {sorted(tables)}")
    units = pd.concat(frames, ignore_index=True)

    operating = units["status"].astype(str).str.contains("In Betrieb", na=False)
    units[~operating].to_parquet(out_dir / "units_pipeline.parquet", index=False)
    units[operating].to_parquet(out_dir / "units_operating.parquet", index=False)

    permit_table = first(PERMIT_TABLES, tables)
    if permit_table:
        pd.read_sql_query(f'SELECT * FROM "{permit_table}"', db.engine).to_parquet(out_dir / "permits.parquet", index=False)
    actor_table = first(ACTOR_TABLES, tables)
    if actor_table and "operator_id" in units:
        actors = pd.read_sql_query(f'SELECT * FROM "{actor_table}"', db.engine)
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
