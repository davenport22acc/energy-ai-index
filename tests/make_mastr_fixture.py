"""Build two tiny MaStR-like export zips (UTF-16 XML, catalog codes) for CI.

Usage: python tests/make_mastr_fixture.py <out_dir>
Writes Gesamtdatenexport_20261101.zip and Gesamtdatenexport_20261201.zip.
Month 2 changes: wind W2 goes from planning to operation, solar S2 disappears, storage B2 is new.
"""
from __future__ import annotations

import sys
import zipfile
from pathlib import Path

KATALOG = {
    "35": "In Betrieb", "31": "In Planung", "37": "Endgültig stillgelegt",
    "1400": "Bayern", "1401": "Brandenburg", "1402": "Niedersachsen",
    "84": "Deutschland", "524": "Batterie", "525": "Pumpspeicher",
    "517": "Organisation", "518": "Natürliche Person", "888": "an Land", "1": "BImSchG",
}


def xml(root: str, record: str, rows: list[dict]) -> bytes:
    body = "".join(
        f"<{record}>" + "".join(f"<{k}>{v}</{k}>" for k, v in r.items() if v is not None) + f"</{record}>"
        for r in rows
    )
    return f'<?xml version="1.0" encoding="UTF-16"?><{root}>{body}</{root}>'.encode("utf-16")


def unit(uid, status, kw, state, op, **extra):
    return {
        "EinheitMastrNummer": uid, "EinheitBetriebsstatus": status, "Nettonennleistung": kw,
        "Bruttoleistung": kw, "Bundesland": state, "Land": "84", "AnlagenbetreiberMastrNummer": op,
        "NameStromerzeugungseinheit": f"Projekt {uid}", "Registrierungsdatum": "2025-03-01", **extra,
    }


def build(month: int) -> dict[str, bytes]:
    wind = [
        unit("W1", "35", "4200", "1402", "ABR1", Lage="888", Inbetriebnahmedatum="2020-05-01"),
        unit("W2", "35" if month == 2 else "31", "5600", "1401", "ABR1", Lage="888",
             GeplantesInbetriebnahmedatum="2026-11-15", GenMastrNummer="G1",
             **({"Inbetriebnahmedatum": "2026-11-20"} if month == 2 else {})),
    ]
    wind2 = [unit("W3", "31", "6000", "1402", "ABR2", Lage="888", GenMastrNummer="G2")]
    solar = [unit("S1", "35", "12000", "1400", "ABR2")] + ([unit("S2", "31", "800", "1400", "ABR3")] if month == 1 else [])
    storage = [unit("B1", "31", "100000", "1401", "ABR1", Technologie="524", Batterietechnologie="Lithium")]
    if month == 2:
        storage.append(unit("B2", "31", "50000", "1400", "ABR2", Technologie="524"))
    storage.append(unit("P1", "35", "1060000", "1400", "ABR2", Technologie="525"))
    actors = [
        {"MastrNummer": "ABR1", "Personenart": "517", "Firmenname": "Nordwind Projekt GmbH", "Land": "84"},
        {"MastrNummer": "ABR2", "Personenart": "517", "Firmenname": "Sonne und Speicher AG", "Land": "84"},
        {"MastrNummer": "ABR3", "Personenart": "518", "Firmenname": None, "Land": "84"},
    ]
    permits = [
        {"GenMastrNummer": "G1", "Art": "1", "Datum": "2024-02-01", "Behoerde": "LfU Brandenburg"},
        {"GenMastrNummer": "G2", "Art": "1", "Datum": "2025-07-01", "Behoerde": "GAA Oldenburg"},
    ]
    return {
        "Katalogwerte.xml": xml("Katalogwerte", "Katalogwert", [{"Id": k, "Wert": v} for k, v in KATALOG.items()]),
        "Katalogkategorien.xml": xml("Katalogkategorien", "Katalogkategorie", [{"Id": "1", "Name": "Demo"}]),
        "EinheitenWind_1.xml": xml("EinheitenWind", "EinheitWind", wind),
        "EinheitenWind_2.xml": xml("EinheitenWind", "EinheitWind", wind2),
        "EinheitenSolar.xml": xml("EinheitenSolar", "EinheitSolar", solar),
        "EinheitenStromSpeicher.xml": xml("EinheitenStromSpeicher", "EinheitStromSpeicher", storage),
        "Marktakteure.xml": xml("Marktakteure", "Marktakteur", actors),
        "EinheitenGenehmigung.xml": xml("EinheitenGenehmigung", "EinheitGenehmigung", permits),
    }


def main(out: str) -> None:
    out_dir = Path(out)
    out_dir.mkdir(parents=True, exist_ok=True)
    for month, stamp in ((1, "20261101"), (2, "20261201")):
        with zipfile.ZipFile(out_dir / f"Gesamtdatenexport_{stamp}.zip", "w", zipfile.ZIP_DEFLATED) as z:
            for name, data in build(month).items():
                z.writestr(name, data)
    print("fixtures in", out_dir)


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "tests/fixtures")
