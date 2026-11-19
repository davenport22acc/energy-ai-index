# Fabric: medallion for MaStR

Three notebooks, one lakehouse, Git-synced from this folder.

| Notebook | Layer | Writes |
| --- | --- | --- |
| `01_bronze_mastr` | Bronze | `bronze_mastr_<table>`: MaStR XML 1:1, all strings, partition `load_date` |
| `02_silver_units` | Silver | `silver_units`, `silver_units_history` (SCD2), `silver_operators`, `silver_permits` |
| `03_gold_pipeline` | Gold | `gold_pipeline_by_state`, `gold_projects`, `gold_status_transitions`, JSON in `Files/exports` |

## Set up in Fabric (once)

1. Workspace on a Fabric capacity (trial or F-SKU), you as admin.
2. New Lakehouse `epr_lakehouse` (with or without schemas).
3. Workspace settings > Git integration > GitHub: repo `davenport22acc/energy-ai-index`, branch `main`, folder `fabric`. Connect with a fine-grained GitHub token (Contents read/write on this repo). Sync: the three notebooks appear.
4. Open each notebook once > Lakehouses > add `epr_lakehouse` as default. Commit the change back to Git.
5. Run `01`, `02`, `03` in order. The first `01` run downloads about 3 GB and parses for well over an hour on small capacities. For a quick try set `MAX_FILES_PER_TABLE = 2`.
6. Later: a Data Pipeline that runs the three notebooks monthly, a semantic model on the gold tables, a Power BI report.

## Run outside Fabric

Same code, local Spark with Delta (Java 17 needed):

```
pip install "pyspark==3.5.3" "delta-spark==3.2.1" lxml pyarrow requests
export LAKE_ROOT=$PWD/lake
python scripts/run_notebook.py fabric/01_bronze_mastr.Notebook --param MAX_FILES_PER_TABLE=2
python scripts/run_notebook.py fabric/02_silver_units.Notebook
python scripts/run_notebook.py fabric/03_gold_pipeline.Notebook
```

Windows: use WSL, or set `LAKE_ROOT` to a folder and run the same commands in PowerShell.

## Tests

`lake-ci` runs all three notebooks on two tiny fixture months (`tests/make_mastr_fixture.py`), reruns month 2 to prove idempotency and checks the result (`tests/check_lake.py`): decoding, SCD2 versions, companies only, gold transitions.

## Rules

- Bronze never filters or cleans. Silver never filters by size. Thresholds live in gold.
- No natural persons beyond bronze: `silver_operators` keeps companies only, unit names in gold only for company-operated units.
- The shared setup block must stay identical in all notebooks (CI checks it).
