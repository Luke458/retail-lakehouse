# Week 2 (9 - 15 Nov): PySpark bronze to silver on Delta

From the career plan (Phase 1): schema enforcement, dedup, incremental
`MERGE`, idempotent reruns.

**Done when:** rerunning a week changes nothing; SQL and PySpark outputs
reconcile. **Done 2026-11-04** (early). Reasoning in `decisions.md`.

## What's built

- `dags/transform_favorita.py`: per week, per feed (mapped):
  `wait_for_raw` -> `bronze` -> `silver` -> `reconcile`. Spark tasks share a
  2-slot `spark` pool, and the Delta writers run one at a time.
- `src/retail_lakehouse/spark/`:
  - `bronze.py`: raw CSV to Delta with an explicit schema, header contract
    check, quarantine with reasons, and a row count reconciled against the
    manifest;
  - `silver.py`: dedup, `MERGE` with change detection and per-week deletes,
    a load control table, and the reference dimensions;
  - `reconcile.py`: DuckDB SQL over the raw file against Spark's layers.
- `ingest_reference` now also builds the silver `stores`, `items` and
  `holidays_events` tables.
- Simulator content faults: `duplicate`, `schema`, `bad-rows`, `restate`.
- Spark 4.2.0 and Delta 4.4.1 in `.venv-spark`, on a local JDK 21.
- Tests: `scripts/test.sh` runs both suites. 17 Airflow-side and 7 Spark
  tests pass (about 40 s).

## Proof (2026-11-04)

| Check | Result |
|---|---|
| 12-week transform backfill (weeks ending 2016-01-05 .. 2016-03-22) | 12/12 runs `success`, at most 2 JVMs at once |
| SQL (DuckDB on raw) vs PySpark (bronze, quarantine, silver) | 30/30 feed-weeks reconcile: raw = bronze + quarantine, parsed = bronze, distinct keys = silver, sums equal |
| Rerun an unchanged week (2016-02-02) | bronze `unchanged`, silver `unchanged`, no new Delta version |
| `--fault duplicate` (2016-03-29) | 5,135 exact duplicates dropped; silver 657,284 = DuckDB distinct keys |
| `--fault bad-rows` (2016-04-05) | 674 rows quarantined as `unparseable` (0.1%, under the 1% limit); 689,007 loaded |
| `--fault schema` (2016-04-12) | bronze failed (`missing ['unit_sales'], extra ['units']`), retried, alerted; nothing written. Republished clean: `success` |
| `--fault restate` (2016-03-15) | raw replaced (`replaces` recorded); silver MERGE updated 2,587 rows, inserted 0, deleted 0. An independent count of the revised rows gives 2,587 |

Reproduce a fault, for example a restatement:

```bash
. scripts/env.sh
python -m retail_lakehouse.drops publish 2016-03-15 --fault restate --force
airflow tasks clear ingest_favorita -s 2016-03-09 -e 2016-03-09 -y
airflow tasks clear transform_favorita -s 2016-03-09 -e 2016-03-09 -y
```

## What went wrong on the way

- **The first backfill ran 10 weeks at once.** `airflow backfill create
  --max-active-runs` defaults to 10 and overrides the DAG's
  `max_active_runs=1`. That meant 20 JVMs, a load average of 68, and a
  concurrent-MERGE failure on silver (the `silver failed for 2016-01-06`
  alert). I stopped it, cleared the layers and rebuilt. Concurrency now lives
  on the tasks (a pool plus `max_active_tis_per_dag`), so no CLI flag can
  undo it.
- **A no-op MERGE still committed a Delta version.** Fixed with the
  `control/silver_loads` table: an unchanged rerun skips the MERGE.
- **DuckDB 2.0 removed `fetchone()` on relations.** Fixed.

## Next: Week 3 (16 Nov)

dbt gold models and dbt tests, with `verity` as a DAG task between silver and
gold that quarantines a bad load. The restate and duplicate faults are ready
as inputs for the gate.
