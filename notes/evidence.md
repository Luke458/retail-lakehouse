# Evidence sheet

Source material for the author's CV v2 and interviews. Not CV text. Each
line is a claim the repository supports, with where the proof is. Numbers
are from real runs on 2026-11-04, 2026-11-05, (the cloud run)
2026-11-09, (Snowflake) 2026-11-11 and (AWS) 2026-11-12.

## Authorship (read first)

The career plan (career-ops, Phase 1) set the scope and stack. The author
ran the project and set its guardrails. Claude Code wrote the code, tests,
configuration and notes under the author's direction, including
`notes/decisions.md`, and the author asked for it that way. On 2026-11-05
the author also delegated the open questions, and Claude decided them
(`notes/GOAL-2.md`). Any CV or interview wording has to be true to that.
"Built" should not imply hand-written. The design reasoning is in
`decisions.md`; it becomes the author's once they can defend it.

## Orchestration (Airflow 3)

1. **Four DAGs** for weekly drops (ingest, transform, publish) plus
   reference data. They use TaskFlow, data-interval timetables, reschedule
   sensors, mapped task groups, branching, pools,
   `max_active_tis_per_dag`, `depends_on_past`, and SMTP alerts.
   `dags/`, `notes/week-01.md`.
2. **Backfill:** a 10-week backfill is green. A corrupt drop failed 3 tries
   and alerted. A late drop waited on its sensor. A republish and rerun left
   raw byte-identical. `notes/week-01.md`.
3. **History:** 113 weeks of history backfilled in rerunnable chunks. That
   ran into, and fixed, SQLite scheduler crashes (restart supervisor), a
   daylight-saving off-by-one, and a backfill-creation race.
   `notes/week-03.md`, "What went wrong".

## Spark and Delta Lake

4. **Bronze:** schema contract, typed load, and a quarantine with reasons
   (unparseable rows up to 1%).
5. **Silver:** dedup and an incremental MERGE (change detection by row
   hash, deletes for vanished rows). Rerunning a week is a no-op, with no
   new version. `notes/week-02.md`.
6. **Reconciliation:** DuckDB independently matches Spark on every
   feed-week: 226 reports over 113 weeks, all passing (2026-11-05).
7. **Fault handling:**
   - duplicates: 5,135 dropped;
   - bad rows: 674 quarantined;
   - schema drift: failed before writing, then recovered;
   - restatement: 2,587 rows updated, matching an independent count.

   `notes/week-02.md`.
8. **MERGE performance fix:** found and fixed a MERGE that rewrote the
   whole table every week. Now 1 file instead of 112, 659,753 rows copied
   instead of 58,231,949, 5.7 s instead of 67 s. Silver on disk went from
   38.9 GB to 0.7 GB after VACUUM. `notes/week-06.md`, `notes/GOAL-2.md`.

## dbt (gold)

9. **Models:** dbt-duckdb over the silver Delta tables: 10 models, 22 tests,
   source freshness, and incremental builds by week.
10. **Gate-aware gold:** gold reads only gate-approved weeks, and a custom
    test checks that every served week equals silver. It caught a
    deliberately broken filter. `notes/week-03.md`, `notes/week-04.md`.

## Data quality gate (verity)

11. **The gate:** verity runs between silver and gold. Its own Delta table
    holds one version per week, it sits in a DAG, and every decision is
    recorded.
12. **Quarantine:** a blocked week isn't published; gold keeps its last
    approved version; an alert says why. A person can release a week, with
    a name and a reason. `notes/week-03.md`, `notes/week-06.md`.
13. **Measured honestly:**
    - a missing store (5.6% of units) is caught;
    - a 0.04% restatement passes as immaterial;
    - false alarms: 9 of 20 clean weeks blocked, each traced to a real
      cause in the data (a 2015 catalogue gap, Carnaval moving a week, the
      April 2016 earthquake).

    `notes/week-03.md`.

## CI/CD

14. **Workflows:** GitHub Actions for lint, two test suites (Spark on JDK
    21), `dbt build` on a fixture lake built by the production code, and
    `tofu validate`. Deploy packages a versioned bundle.
15. **Local run:** the same stages run locally in about 75-90 s. Three
    deliberately broken changes each fail their stage. On GitHub: a PR
    that broke gold failed CI and was closed, and a valid PR passed (about
    2 minutes) and merged; deploy then packaged the bundle.
    `notes/week-04.md`.

## Infrastructure as code (Azure)

16. **OpenTofu:**
    - resource group;
    - ADLS Gen2 with no account keys;
    - Databricks workspace;
    - managed-identity access connector;
    - Unity Catalog credential, external locations and volume;
    - the transform job.

    Applied once (16 resources) and destroyed on 2026-11-09.
    `infra/`, `notes/week-05.md`.
17. **Same commands on Databricks:** the Spark jobs take their table and
    file roots from config (`abfss://` and a UC volume), so the same
    commands run there. On Databricks (runtime 18 LTS), one week ran
    bronze → silver → reconcile and matched the local run row for row
    (664,947 sales, 371 transactions). Cost under $1 (estimate).
    `notes/week-05.md`.

## Second gold target (Snowflake)

19. **One dbt project, two warehouses:** the same models build gold on
    DuckDB and on Snowflake. Two dispatch macros cover the SQL that differs.
    On Snowflake: 10 models and 23 data tests pass, including
    gold-equals-silver. Gold on the two engines is equal for every model
    and every one of 113 weeks (3,955 per-week checks); a deliberately
    altered copy fails on exactly the altered values.
    `notes/week-07-snowflake.md`.
20. **Idempotent loader:** approved silver weeks go to Snowflake through
    Parquet, `PUT`, `COPY INTO` and a `MERGE` on business keys. Full load:
    113 weeks, 58.9M sales rows, every week reconciled. A rerun changes
    nothing; a tampered row and a stray row were repaired by the next run.
    `src/retail_lakehouse/snowflake_load.py`.
21. **Provisioned and capped:** OpenTofu created the database, schemas,
    warehouse, roles, key-pair service users, least-privilege grants and a
    10-credit resource monitor (21 objects; a re-plan is clean). The whole
    session used 0.096 credits, from trial credit. `infra/snowflake/`.

## The transform on AWS (Glue, S3, Athena)

22. **Same Spark jobs on a third platform:** the wheel that runs locally and
    on Azure Databricks ran bronze → silver → reconcile on AWS Glue 6.0
    (Spark 4.1), started by Airflow through `GlueJobOperator`. For week
    2016-05-24, Glue's reconcile reports match the local and Databricks
    counts (664,947 sales, 371 transactions). `notes/week-08-aws.md`.
23. **Row for row via Athena:** Athena read silver on S3 as Delta tables and
    matched local silver on rows, distinct keys, sums and an
    order-independent md5 fingerprint over every row's keys and values.
24. **Provisioned, capped and destroyed:** OpenTofu created the S3 lake
    (versioned, no public access, encrypted), a least-privilege Glue role,
    the Glue job, a Glue database and an Athena workgroup with a 1 GiB scan
    cutoff (17 resources). Run, checked and destroyed in about 9 minutes;
    1,151 DPU-seconds, under US$0.20 (estimate). Temporary credentials only
    (`aws login`), no access keys. `infra/aws/`.

## Packaging

18. **Fresh clone:** README, architecture diagram and runbook. A fresh clone
    runs end to end from the README: setup, a sha256-checked data download,
    CI, and a 30-week demo through the gate. That run found and fixed three
    first-install bugs. `notes/week-06.md`.

## What it does not show (yet)

- A scheduled cloud deployment: the cloud run was one week, triggered by
  hand, with Airflow, dbt and the gate still local.
- Containers: Airflow runs from a local `uv` install, not Podman.
- A scheduled Snowflake load: the loader and the Snowflake dbt build run by
  hand, not from Airflow.
- A standing AWS deployment: the AWS run was one week, started by hand, with
  gold and the gate still local.
- Kafka, Scala: the other gaps in the upskill map (career-ops
  `node upskill.mjs`, 2026-11-05). The project addresses its CI/CD,
  Terraform and dbt gaps.
