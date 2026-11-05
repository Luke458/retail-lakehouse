# Week 5 (30 Nov - 6 Dec): Terraform on Azure, code only

From the career plan (Phase 1): infrastructure as code for Azure (ADLS Gen2,
Databricks), and the Spark jobs running there unchanged.

**Done when:** one cloud run, then `destroy`. **Code half done
2026-11-05.** The infrastructure code validates and the Spark jobs take
their paths from configuration. The cloud run waits on a subscription and
budget (Questions in `GOAL.md`).

## What's built

- OpenTofu 1.13.8, installed in `~/.local/opt/tofu-1.13.8`. The archive's
  sha256 matched the release's `SHA256SUMS`, and `SHA256SUMS` has a good
  signature from OpenTofu's key `E3E6 E43D 84CB 852E ADB0 051D 0C0A F313
  E5FD 9F80`. `scripts/env.sh` puts it on `PATH`.
- `infra/` (README there):
  - resource group;
  - ADLS Gen2 account with no account keys, `files` and `tables`
    containers, and a cool tier for old drops;
  - Databricks workspace, and an access connector with Storage Blob Data
    Contributor on the lake;
  - Unity Catalog storage credential, external locations, catalog, schema,
    and an external volume over `files`;
  - the `transform-favorita` job: per feed bronze → silver → reconcile on a
    single-node job cluster, the wheel from the volume, and the same
    retries as Airflow (none for reconcile);
  - tags on everything, local state, a remote-state example, and a cost
    note.
- Spark code, unchanged commands on Databricks:
  - `TABLE_ROOT` (an `abfss://` URI) holds the Delta tables, and
    `LAKE_ROOT` (a Unity Catalog volume, an ordinary path) holds files;
  - `spark_session()` takes the runtime's session on Databricks and doesn't
    stop it;
  - the wheel's `retail-lakehouse-spark` entry point is the job's
    `python_wheel_task`.
- CI stage `infra` (`tofu fmt -check`, `validate`), in `scripts/ci.sh`
  and the workflow.

## Proof (2026-11-05)

| Check | Result |
|---|---|
| `tofu init` / `fmt -check` / `validate` | providers azurerm 5.12.0, databricks 1.141.0 (lock file kept); fmt clean; "The configuration is valid." |
| `scripts/ci.sh all` (now with `infra`) | exit 0; 29 Airflow-side tests (4 new path tests), 7 Spark, dbt 33/33, infra valid |
| `TABLE_ROOT` elsewhere, the real bronze job on the fixture lake | Delta table written under the separate `TABLE_ROOT`; raw read from `LAKE_ROOT` (`"action": "loaded", "rows": 378`) |
| `abfss://` root | `table_path` keeps `abfss://tables@...dfs.core.windows.net/bronze/favorita/sales` (pathlib would make it `abfss:/`) |
| Wheel | `entry_points.txt` has `retail-lakehouse-spark = retail_lakehouse.spark.__main__:main` |

**Not proven:** nothing has been planned or applied. So the azurerm 5.x and
databricks arguments are checked by `validate` (schema) only, not against
the APIs, and the Spark jobs have not run on a Databricks runtime. tflint
isn't installed, so it wasn't run.

## Next: Week 6 (7 Dec)

Package: README with the architecture, a runbook, and a fresh-clone run.
