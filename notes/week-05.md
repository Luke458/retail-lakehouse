# Week 5 (30 Nov - 6 Dec): Terraform on Azure

From the career plan (Phase 1): infrastructure as code for Azure (ADLS Gen2,
Databricks), and the Spark jobs running there unchanged.

**Done when:** one cloud run, then `destroy`. **Done 2026-11-09.** Code
half 2026-11-05; the cloud run on 2026-11-09, after the author opened a
pay-as-you-go subscription and set a $20 limit for it.

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

At the time nothing had been planned or applied, so the arguments were
checked by `validate` (schema) only. The cloud run below checks them
against the APIs. tflint isn't installed, so it wasn't run.

## Cloud run (2026-11-09)

Region `australiaeast`, Databricks premium, runtime 18 LTS (Spark 4.1,
picked by `databricks_spark_version { long_term_support = true }`),
single node `Standard_D4ds_v6`. Week ending 2016-05-24 (the latest local
drop): its raw files, about 5 MB, and the wheel, uploaded to the `files`
container.

| Step (UTC) | Result |
|---|---|
| 23:46-23:50 (8 Nov) apply, Azure half | 9 resources: resource group, lake and containers, lifecycle policy, workspace, access connector, two role assignments |
| 01:10 apply, Databricks half | 7 resources: storage credential, two external locations, catalog `retail_dev`, schema, volume, job |
| 01:12-01:42 run 1 | failed before any work: `CLOUD_PROVIDER_RESOURCE_STOCKOUT`, no `Standard_D4ds_v4` capacity. No VM started |
| 02:15-02:32 run 2 (`Standard_D4ds_v6`) | all six tasks succeeded; about 15 min cluster start and library install, then about 1 min for the six tasks |
| 02:33-02:42 destroy | 16 destroyed; only the subscription's own sign-up and Network Watcher groups remain |

Task output on Databricks, compared with the local run of the same week
(`data/reports/reconcile/`):

| | Databricks | Local |
|---|---|---|
| bronze sales / transactions | 664,947 / 371 loaded, 0 quarantined | same |
| silver sales / transactions | 664,947 / 371 inserted, version 1 | same rows |
| reconcile sales | 4 checks pass; Spark sum 5,201,537.249000012 | 4 checks pass; 5,201,537.248999988 |
| reconcile transactions | 4 checks pass; sum 578,065 | same |

The sales sums differ by 2.4e-8, floating-point addition in a different
order; the check's tolerance is 1e-9 of the total (5e-3 here). The
reconcile reports were written to the volume, read back with
`az storage blob download`, and the Delta logs were under
`abfss://tables@…/silver/favorita/sales/_delta_log` as expected. So the
same wheel and the same commands ran unchanged against `abfss://` tables
and a Unity Catalog volume.

### What got in the way

1. **Unregistered resource providers** on the new subscription; registered
   with `az provider register`.
2. **VM size:** DDSv5 (the default) had 0 vCPU quota. DDSv4 had quota but
   Azure had no capacity (run 1). Choosing by quota, SKU restrictions and
   the workspace's node-type list found `Standard_D4ds_v6`, now the default.
3. **Personal Microsoft account:** for 25 minutes the workspace API
   answered "Did not find workspace with specified org ID". Opening the URL
   directly signed in to Microsoft's consumer tenant, not the
   subscription's. Launching the workspace from the portal fixed both; the
   author also made a directory admin account for this.
4. **No account keys** on the lake, so uploads need a data role for the
   person running tofu: `azurerm_role_assignment.deployer_lake`
   (`grant_deployer_data_access`, default on).
5. My first run watcher piped JSON through zsh's `echo`, which turns `\n`
   in the logs into control characters, so it waited out its hour instead
   of seeing the failure at 01:42. Rewritten in Python.

### Cost

Resources were up for about 2 h 56 min; the cluster for about 17 min.
Estimate, by line: the workspace's NAT gateway and public IP for about 3 h,
one 4-vCPU VM and its jobs-compute DBUs for about 17 min, and a few MB of
storage. Under $1 in total, well inside the $20 limit. The measured figure
comes from Cost Management, which lags by up to a day (and rate-limited
the query on the day); check it on 2026-11-10.

## Next: Week 6 (7 Dec)

Package: README with the architecture, a runbook, and a fresh-clone run.
