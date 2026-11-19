# Energy Pipeline Radar (accilium)

Public website showing which wind, PV and storage projects are being developed in Austria, Germany and Italy, who develops them, when they are likely to connect, and where battery storage pays off. Built on Microsoft Fabric as a learning project (data engineering, data science, agentic coding).

Plan: Claude Doc "Energy Pipeline Radar: Plan".

## Modules

- **A Pipeline Radar**: registers, auctions, permits, grid capacity. AI extracts projects from permit documents and resolves project companies to developers.
- **B BESS Siting**: price spreads per bidding zone (ENTSO-E, GME) combined with free grid capacity.

## Sources (phase 0)

| Source | Country | Status |
| --- | --- | --- |
| Marktstammdatenregister (open-mastr, DL-DE-BY-2.0) | DE | `src/epr/ingest/mastr.py`, weekly |
| BNetzA auction results | DE | next |
| Terna MACSE results | IT | next |
| Free grid capacity per substation (quarterly PDFs) | AT | next |
| ENTSO-E Transparency prices | AT, DE, IT | needs API token |

## Run

Automatic: GitHub Actions runs `ingest-mastr` monthly and on every push to `src/epr/ingest/mastr.py`. Parquet files are attached to the run as an artifact, the summary lands in `data/reports/mastr_summary.md`.

Locally:

```
pip install -e .
python -m epr.ingest.mastr out
```

The first local run downloads the full MaStR export (several GB) and takes a while.

## Rules

- Only open, official or regulatory data. No job boards or commercial portals.
- No persons. Companies and project companies only.

## Legacy

`legacy/` holds the earlier job-postings prototype (Energy AI Index). Its workflow is disabled.
