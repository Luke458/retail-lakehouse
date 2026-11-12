# Goal 3: dbt on Snowflake, and the pipeline on AWS

Repo `~/Projects/retail-lakehouse`. Written 2026-11-09 from the Sydney
market research (career-ops
`data/research/reports/Sydney data engineering market 2026.md`) and
career-ops `upskill.mjs`. After CV v2 the remaining real gaps are
**Snowflake** (about a quarter of Sydney DE ad snippets, 3 evaluated
reports) and **AWS** (about a third of snippets, 5 reports; tied with
Azure). The project already covers Databricks, Azure, dbt, Terraform-style
IaC and CI/CD. Each part below adds one platform without rewriting the
pipeline, and ends with a proof table and a destroy.

Each iteration:
1. Read this file and do the first unchecked task, within the guardrails.
2. Tick it and add one line to the log at the bottom.
3. Continue. Stop when every task is ticked, or the rest are blocked.

Claude writes all the code, at the author's request. **Status: Part A in
progress (A1-A4 done; A5 wrap-up open: destroy by the last trial day). Part B started
2026-11-11 at the author's request, ahead of January ("we definitely need some AWS");
B1 done, the rest waits on B0 and the author's go with a spend limit.**

## Why these two, in this order

- **Snowflake first (December).** It costs no money: the trial gives
  credits and needs no card. It reuses the dbt project as it is, and
  "dbt on Snowflake" is the most common modern-stack pairing in Sydney ads
  (Qantas, Xero). About 2 loop days.
- **AWS second (January), and only if it's still worth it.** Do it if, after
  the Christmas break, AWS shops are still at least a third of the
  shortlist (career plan: "AWS certs unless AWS shops dominate"). It costs a
  few dollars. The ASX ad shows the target vocabulary: S3, Glue, Glue Data
  Catalog, Athena, Airflow, Terraform. About 3 loop days.
- **Not doing:** Kafka/streaming (a different job; research says
  deprioritise), Redshift (cost, and Athena covers SQL on S3), MWAA (about
  US$1/hour minimum; Airflow stays local and triggers AWS).

## Guardrails (in addition to GOAL-2's)

- **Data:** public or synthetic only, no employer or supplier names.
- **Git:**
  - Branch per part (`snowflake`, `aws`); reach `main` through a PR with
    green CI.
  - Never force-push.
  - CI stays credential-free: the new targets are tested locally;
    CI keeps running dbt on DuckDB.
- **Secrets:**
  - No passwords, keys or account identifiers in the repo.
  - Snowflake uses key-pair auth, with the private key in
    `~/.config/retail-lakehouse/` (mode 600) and only an env var name in
    `profiles.yml`.
  - AWS uses an SSO or named profile in `~/.aws`, never access keys in
    files.
- **Spend:**
  - **Part A: $0 cash.** Trial credits only, and a resource monitor
    suspends the warehouse at 10 credits.
  - **Part B: at most US$10**, region `ap-southeast-2`, with an AWS Budget
    alert at US$5, and everything destroyed the same day.
  - Before any apply, show the plan to the author. A new spend limit needs
    the author's word, as in Week 5.
- **Permission system:** if it blocks an action, don't work around it;
  give the author the exact command.
- **Never run podman.** At most 2 Spark JVMs at a time. Avoid ports
  8000, 8080 and 8096.
- **The CV is not edited from here.** After each part, the new claims go
  to career-ops through its `add` mode, which needs the author's approval.

## Part A: dbt gold on Snowflake

**Design.** Silver stays in Delta on the local lake. A small loader exports
the gate-approved weeks of silver (plus the reference tables) to Parquet,
PUTs them to an internal stage, and runs `COPY INTO` into a `SILVER` schema.
The same dbt models build `GOLD` on Snowflake from a new `snowflake` target.
Where DuckDB and Snowflake SQL differ, `adapter.dispatch` macros cover it;
there is no second copy of the models. Snowflake objects are created with
OpenTofu (the `snowflakedb/snowflake` provider) in `infra/snowflake/`, so
the IaC story covers a second platform.

- [x] **A0. Author (blocking):**
  - Sign up for a Snowflake trial: Enterprise edition, on AWS
    `ap-southeast-2` (Sydney), so it lives in the same region as Part B.
  - Run the one-off key-pair registration command I'll give you
    (`ALTER USER … SET RSA_PUBLIC_KEY`) as ACCOUNTADMIN.
  - Tell me the account identifier, via `~/.config/retail-lakehouse/snowflake.env`,
    not in chat.
- [x] **A1. OpenTofu for Snowflake** (`infra/snowflake/`):
  - database `RETAIL_LAKEHOUSE` with schemas `SILVER` and `GOLD`;
  - warehouse `TRANSFORM_XS` (X-Small, `AUTO_SUSPEND = 60`,
    `AUTO_RESUME`);
  - roles `TRANSFORMER` and `LOADER`, and a service user for dbt;
  - a resource monitor at 10 credits that suspends the warehouse.

  `tofu validate` joins `scripts/ci.sh infra`; plan and apply run locally only.
- [x] **A2. Loader:** `retail_lakehouse.snowflake_load`. It exports approved
  weeks with DuckDB's Delta reader to Parquet, then PUT, COPY INTO and an
  idempotent MERGE on the business keys, so a rerun loads nothing twice.
  A row-count and sum check against silver goes in a reconcile report, like
  the Spark one.
- [x] **A3. dbt on Snowflake:**
  - a `snowflake` target in `dbt/profiles.yml` (env vars only);
  - dispatch macros for the SQL differences (date functions, `QUALIFY`,
    casts);
  - the incremental models use `merge` on Snowflake.

  `dbt build --target snowflake` must pass all 22 tests, including the
  custom gold-equals-silver test.
- [x] **A4. Proof and cost:**
  - Gold on Snowflake equals gold on DuckDB for every served week. Compare
    row counts and sums per model, written to a report.
  - Credits used, from `ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY`, logged
    in `notes/week-07-snowflake.md`.
  - A Snowsight screenshot is the author's to take.
- [ ] **A5. Wrap up:**
  - Suspend the warehouse.
  - Decide whether the trial objects stay until the trial ends; they cost
    nothing once suspended. Run `tofu destroy` at the latest on the last
    trial day.
  - Update the README, `notes/evidence.md` and `notes/decisions.md`
    ("Why Snowflake as a second gold target, what breaks at 100x").
  - PR to `main`.

**CV line this would support** (draft, for career-ops `add`, after
approval): "dbt gold models built on both DuckDB and Snowflake from one
codebase; Snowflake provisioned with OpenTofu, with a resource monitor
capping spend."

## Part B: the transform on AWS (S3, Glue, Athena), triggered by Airflow

**Design.** This is the Week 5 pattern on AWS:
- **Infrastructure:** OpenTofu provisions an S3 lake bucket (versioned,
  public access blocked, SSE-S3), a Glue Data Catalog database, an IAM role
  for Glue with least privilege on that bucket, one Glue Spark job per
  step, and an Athena workgroup with a results bucket and a bytes-scanned
  limit.
- **Code:** the wheel runs on Glue unchanged except for session setup.
  Glue's Delta support (`--datalake-formats delta`) keeps the Delta code;
  tables are registered in the Glue catalog so Athena can query silver.
- **Orchestration:** local Airflow triggers the job through the Amazon
  provider's `GlueJobOperator`, in a new `transform_favorita_aws` DAG, so
  "Airflow orchestrating AWS Glue" is real.

- [x] **B0. Author (blocking):**
  - Create the AWS account: root MFA, plus an IAM Identity Center user with
    admin rights for the setup.
  - Create a Budget alert at US$5.
  - Configure `aws configure sso` with a profile named `retail-lakehouse`.
  - Give the go in chat, with the spend limit.
- [x] **B1. Glue version check, local, before any cloud work.** (Rewritten
  2026-11-11.) The plan assumed Glue 5.x, which runs Spark 3.5. AWS has since
  released **Glue 6.0: Spark 4.1.1, Python 3.13, Java 17, Delta Lake 4.2.0**,
  with S3A as its only S3 connector. The code targets Spark 4.x, so the
  check became Python 3.13 with PySpark 4.1.1 and delta-spark 4.2.0 in
  `.venv-glue` (`scripts/setup.sh glue`). `requires-python` widened to
  `<3.14` so the wheel installs on Glue.
- [ ] **B2. OpenTofu for AWS** (`infra/aws/`):
  - the resources above;
  - tags on everything;
  - local state;
  - `validate` added to CI.
- [x] **B3. Glue entry point and session:**
  - `on_glue()` next to `on_databricks()`;
  - `TABLE_ROOT=s3://…/tables`, `LAKE_ROOT=s3://…/files`. The `files`
    side needs S3 I/O for drops and reports, since there is no FUSE
    mount; it goes through a small filesystem shim (`fsspec`/`s3fs`).
- [x] **B4. Airflow:** a `transform_favorita_aws` DAG using
  `GlueJobOperator` for bronze → silver → reconcile, with the same
  dependencies and retries as the local DAG. AWS credentials come from the
  SSO profile through an Airflow connection; nothing is stored in the repo.
- [ ] **B5. One week on AWS:**
  - apply;
  - upload one week of raw data and the wheel;
  - trigger from Airflow;
  - query silver from Athena and compare it with the local run row for
    row;
  - `tofu destroy`;
  - log timings and cost in `notes/week-08-aws.md`.

  Cost comes from Cost Explorer the next day, and is marked as an
  estimate until then.
- [ ] **B6. Wrap up:**
  - README, `evidence.md`, `decisions.md` ("Glue vs EMR Serverless vs
    Databricks on AWS; what breaks at 100x").
  - PR to `main`.

**CV line this would support** (draft, for career-ops `add`, after
approval): "Same Spark jobs run on Azure Databricks and AWS Glue (S3, Glue
Data Catalog, Athena), provisioned with OpenTofu and triggered by Airflow;
each cloud run matched the local run row for row and was destroyed."

## Also worth doing alongside (author's call, not part of the loop)

- **Certifications:**
  - Databricks DE Associate first: the CV lists it as expected February
    2027, so book it.
  - The AWS Data Engineer Associate (DEA-C01) only if Part B is done and
    AWS ads still dominate. The project and the cert cover the same ground,
    so pick one unless an employer asks for the cert.
- **Talking points:** after each part, read the new `decisions.md`
  section until you can explain it without notes. Interviews test exactly
  this for AI-assisted projects.

## Log

- 2026-11-09: written. Not started.
- 2026-11-10: A0 prep: key pair, register_key.sql and snowflake.env template written to ~/.config/retail-lakehouse/ (mode 600). Waiting on the author's trial sign-up and key registration.
- 2026-11-11: A0 done. Trial on AWS ap-southeast-2; key-pair login works (the admin user object is stored in lowercase, so SQL must double-quote its name). Two service-user key pairs added beside the admin key.
- 2026-11-11: A1 written on branch `snowflake`: `infra/snowflake/` (provider snowflakedb ~> 2.23), `scripts/snowflake.sh` (connection from ~/.config), CI validates both roots. `tofu plan`: 21 to add. Waiting on the author's go to apply.
- 2026-11-11: A1 applied with the author's go: 21 added; re-plan shows no changes; service-user logins, role boundaries, warehouse (suspended, cap attached) and monitor (10 credits, 0 used) checked. infra/snowflake/README.md written.
- 2026-11-11: A2 done: `retail_lakehouse.snowflake_load` (DuckDB delta_scan → Parquet → PUT → COPY INTO a temp table → MERGE on business keys, plus deletes within loaded weeks). Full load of 113 approved weeks: 58,896,896 sales rows (73 s), all 113 weeks reconcile on counts and sums; a full rerun inserts, updates and deletes nothing. A tampered row and a stray row were repaired by the next run (1 updated, 1 deleted). First full reconcile flagged 2014-10-21: same row count, sums split by rounding to 3 dp after different float addition order; now unrounded with a 1e-9 relative tolerance. dbt-snowflake 1.12.5 added to .venv-dbt.
- 2026-11-11: A3 done: `snowflake` target in dbt/profiles.yml (env vars only, key-pair as LAKEHOUSE_DBT/TRANSFORMER); sources point at SILVER (gate decisions at SILVER.GATE_DECISIONS); two dispatch macros (`any_true`: bool_or vs boolor_agg; `ordered_string_agg`: string_agg vs listagg within group). Everything else (GROUP BY ALL, lateral aliases, FULL JOIN USING) ran unchanged. `dbt build --target snowflake`: PASS=33 (10 models, 23 tests, including gold_units_match_silver) on the first full build, and again incrementally for one week. Kept delete+insert by week rather than the planned merge: unique_key is week_ending, which is not unique per row. DuckDB gold rebuilt from current silver, PASS=33.
- 2026-11-11: A4 done: `retail_lakehouse.gold_compare`: 5 models × 113 weeks, 35 checks per week, Snowflake equals DuckDB; a negative control (one value changed, one row deleted in a copy of DuckDB gold) fails on exactly those. Credits: 0.096 on TRANSFORM_XS for the session (INFORMATION_SCHEMA metering), monitor 0.10/10. Storage 416 MB. `notes/week-07-snowflake.md`.
- 2026-11-11: A5 partly done: warehouse suspended (auto), stage empty; README, evidence.md (19-21), decisions.md (Week 7) updated. Open: PR to main; `tofu destroy` by the last trial day (about 2026-12-10), the author's call on keeping the objects until then.
- 2026-11-11: Part B started at the author's request. B1 done: Glue 6.0 (Spark 4.1.1, Python 3.13, Delta 4.2.0) replaces the planned Spark 3.5 check. In `.venv-glue` with those versions: Spark tests 8 passed (1 skipped, as in CI), and the CI fixture pipeline (raw → bronze → silver → dbt gold) PASS=33. No compatibility layer needed; `requires-python` widened to <3.14. Note for B3: S3A is Glue 6.0's only S3 connector.
- 2026-11-12: B0 done: the author's own AWS Organization; the project uses its member account (not the management account), CLI through `aws login` on a named profile (temporary credentials, no keys). Root MFA on the management account; a US$5 monthly budget with email alerts (80% forecast, 100% actual) created in the member account. Spend limit for Part B: US$10 (author, 2026-11-12).
- 2026-11-12: B2 written: `infra/aws/` (lake bucket: versioned, public access blocked, SSE-S3, owner-enforced, 1-day noncurrent expiry; Athena results bucket and workgroup with a 1 GiB scan cutoff; Glue database; least-privilege Glue role; one Glue 6.0 job, G.1X x2, 30-min timeout, no retries; log group with 7-day retention). Plan: 17 to add. Waiting on the author's go to apply.
- 2026-11-12: B3 done: `retail_lakehouse.glue` stages the files a step reads with Python from S3 to the driver's disk and uploads `reports/` after (also on failure); Spark reads raw CSVs from S3 (`spark_uri`); `on_managed_spark()` covers Databricks and Glue. 5 new tests with a fake S3; Spark tests unchanged (8 passed). One Glue job for every step (`--step`), as the Databricks job had one task per step, instead of one job per step.
- 2026-11-12: B4 done: `transform_favorita_aws` (manual, `week` param): per feed upload_raw -> bronze -> silver -> reconcile (GlueJobOperator) -> check (reads the reconcile report from S3); retries as the local DAG. Amazon provider 9.36.0 and botocore[crt] (needed for `aws login` profiles) added to .venv via the Airflow constraints. `scripts/aws.sh` wraps profile, connection and the OpenTofu outputs.
