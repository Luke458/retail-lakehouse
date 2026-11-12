# Week 8: the transform on AWS Glue, triggered by Airflow (GOAL-3 Part B)

From `notes/GOAL-3.md`: the Week 5 pattern on AWS. OpenTofu provisions S3,
the Glue Data Catalog, a least-privilege Glue role, a Glue Spark job and an
Athena workgroup. Local Airflow starts the job for one week; Athena checks
silver against the local run; everything is destroyed the same day.

**Done when:** one week runs bronze → silver → reconcile on Glue from
Airflow, matches the local run row for row, and is destroyed. **Done
2026-11-12**, within the author's US$10 limit (estimate under US$0.20,
below).

## What's built

- **Glue version check (B1):** the plan assumed Glue 5.x (Spark 3.5). Glue
  6.0 runs Spark 4.1.1, Python 3.13 and Delta Lake 4.2.0. In `.venv-glue`
  with exactly those versions (`scripts/setup.sh glue`), the Spark tests
  passed (8) and the CI fixture pipeline built gold (33/33). The only
  change: `requires-python` widened to `<3.14`, so the wheel installs on
  Glue.
- **`infra/aws/` (README there), 17 resources:**
  - the lake bucket: versioned, public access blocked, SSE-S3, owner
    enforced, old versions expired after a day;
  - an Athena workgroup that cancels any query scanning more than 1 GiB,
    and its results bucket (7-day expiry);
  - a Glue database;
  - a Glue role that can read and write only the lake bucket and its own
    logs;
  - one Glue 6.0 job (G.1X × 2, 30-minute timeout, no retries) that runs
    every step, chosen with `--step`;
  - a log group with 7-day retention.
- **`retail_lakehouse.glue`, the entry point:** Glue has no mount for the
  files side, as Databricks had a Unity Catalog volume.
  1. Before a step, it copies the plain files the step reads with Python
     (the week's drop and its `_INGEST.json`) from S3 to the driver's disk.
  2. Spark reads the raw CSV from S3 (`spark_uri`), because Glue's executors
     cannot see the driver's disk.
  3. After the step, it uploads `reports/`, also when the step fails.

  The Delta tables need none of this: `TABLE_ROOT` is an `s3://` URI.
- **`transform_favorita_aws`, the Airflow DAG:** started by hand with a
  `week` parameter. Per feed, side by side: `upload_raw` → `bronze` →
  `silver` → `reconcile` → `check`. The middle three are `GlueJobOperator`
  calls; `check` reads Glue's reconcile report from S3. Retries match the
  local DAG.
- **Credentials:** `aws login` on a named profile gives temporary
  credentials, with no access keys anywhere. boto3 needs `botocore[crt]` to
  read such a profile, so `.venv` now has it. `scripts/aws.sh` supplies the
  profile, Airflow's connection, and the bucket and job names from
  OpenTofu's outputs.

## The run (2026-11-12, UTC)

Account: the author's own AWS Organization, its member account (not the
management account). Region `ap-southeast-2`.

| Time | Step | Result |
|---|---|---|
| 03:31-03:32 | `tofu apply` (saved plan) | 17 added; two S3 lifecycle rules took about a minute each |
| 03:32 | wheel and Glue entry uploaded | `retail_lakehouse-0.1.0` (35 KB) |
| 03:33:10 | DAG triggered, week 2016-05-24 | both `upload_raw` in 3 s |
| 03:33-03:35 | `bronze` × 2 on Glue | 89 s and 97 s execution |
| 03:34-03:35 | `silver` × 2, first try | rejected by Glue: `ConcurrentRunsExceededException` (below) |
| 03:35-03:38 | `silver` × 2, retry | 121 s and 123 s |
| 03:38-03:39 | `reconcile` × 2 | 68 s and 77 s; every check passes |
| 03:39:47 | DAG run | **success** (6 min 37 s) |
| 03:40 | Athena check | below |
| 03:41:01-03:41:07 | `tofu destroy` | 17 destroyed |
| 03:41 | cleanup | Glue's three default log groups (created outside OpenTofu on first run) deleted; account left with no buckets, Glue jobs, Glue databases, project roles or log groups |

## Proof

Glue's reconcile reports (the same DuckDB-against-Spark checks as locally),
compared with the local run and the Week 5 Databricks run:

| | Glue | Local | Databricks (Week 5) |
|---|---|---|---|
| sales raw / bronze / quarantined / silver | 664,947 / 664,947 / 0 / 664,947 | same | same |
| transactions | 371 / 371 / 0 / 371 | same | same |
| sales sum, Spark side | 5,201,537.249000026 | 5,201,537.248999988 | 5,201,537.249000012 |

The sums differ only in the ninth decimal place, from floating-point
addition order, and are within the check's 1e-9 relative tolerance.

**Row for row, via Athena.** Athena read silver on S3 as Delta tables
(`table_type = DELTA`) and matched local silver for the week. The checks
were rows, distinct keys, the sum, and an order-independent fingerprint:
the sum of the first 48 bits of `md5(keys | row_hash)` over every row,
where `row_hash` covers the row's values. It changes if any row's key or
value differs.

| | rows | distinct keys | sum | fingerprint |
|---|---|---|---|---|
| sales: S3 via Athena | 664,947 | 664,947 | 5,201,537.249000026 | 93642434167769384299 |
| sales: local DuckDB | 664,947 | 664,947 | 5,201,537.249000002 | 93642434167769384299 |
| transactions: S3 via Athena | 371 | 371 | 578,065 | 53428816963174102 |
| transactions: local DuckDB | 371 | 371 | 578,065 | 53428816963174102 |

Athena scanned 9.8 MB for this.

## Cost

| Item | Usage | Estimate (US$) |
|---|---|---|
| Glue 6.0 (Sydney, US$0.308 per DPU-hour, from the AWS Price List API) | 6 runs, 1,151 DPU-seconds = 0.32 DPU-hours | 0.10 |
| Athena (US$5 per TB, 10 MB minimum per query) | 2 queries | < 0.01 |
| S3, CloudWatch Logs | about 9 minutes of a few MB, a few thousand requests | < 0.05 |
| **Total** | | **under US$0.20** |

This is an estimate until Cost Explorer shows the day's charges, usually by
the next day. A US$5 monthly budget with email alerts covers the account.

## What went wrong

- **`ConcurrentRunsExceededException` on `silver`.** The job allowed 2 runs
  at once, and the two feeds use exactly 2. But Glue went on counting a run
  for a short while after it reported SUCCEEDED, so each `silver` task,
  started a second after its `bronze` finished, was rejected. That was true
  even when no run was active, and the account quotas (2,000 runs) were not
  the limit. Airflow's retry a minute later went through. The limit is now 4,
  which costs nothing, since billing is per running worker. It was applied
  in place during the run, before the retries.
- **`aws login` profiles need `botocore[crt]`.** Without it, boto3 (and so
  Airflow) failed with `MissingDependencyException`, while the AWS CLI worked.
- **The account was a member of an Organization.** It turned out to be the
  author's own Organization, but that was checked before anything was
  created: a member account's usage is billed to the management account.

## Left for later

- Not scheduled: the AWS run is a one-week proof, like Week 5, and the DAG
  has no schedule.
- Gold and the gate stay local. Running dbt against Athena (dbt-athena), or
  the gate on Glue, would be the next step for a standing AWS deployment.
