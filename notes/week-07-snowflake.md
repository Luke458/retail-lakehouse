# Week 7: dbt gold on Snowflake (GOAL-3 Part A)

From `notes/GOAL-3.md`: the same dbt project builds gold on Snowflake as
well as DuckDB, with Snowflake provisioned by OpenTofu and spend capped by a
resource monitor. Trial credits only, no cash.

**Done when:** gold on Snowflake equals gold on DuckDB for every served
week, and the credits used are logged. **Done 2026-11-11**, the day the
trial account was opened (Enterprise, AWS `ap-southeast-2`, Sydney).

## What's built

- `infra/snowflake/` (README there), applied with OpenTofu, 21 objects:
  - database `RETAIL_LAKEHOUSE` with schemas `SILVER` and `GOLD`;
  - warehouse `TRANSFORM_XS` (X-Small, 60 s auto-suspend, created
    suspended);
  - resource monitor `RETAIL_LAKEHOUSE_CAP`: 10 credits a month on the
    warehouse, with notifications at 50% and 75%, suspend at 90% and an
    immediate stop at 100%;
  - roles `LOADER` (writes `SILVER` only) and `TRANSFORMER` (reads
    `SILVER`, writes `GOLD`), under `SYSADMIN`;
  - key-pair-only service users `LAKEHOUSE_LOADER` and `LAKEHOUSE_DBT`.
- `scripts/snowflake.sh`: puts the connection from
  `~/.config/retail-lakehouse/` (mode 600) into the environment for one
  command. The repo holds no account identifier, user name or key.
- `retail_lakehouse.snowflake_load`, for each table:
  1. DuckDB reads the silver Delta table (weekly tables: approved weeks
     only) and writes Parquet.
  2. `PUT` uploads it to an internal stage, and `COPY INTO` fills a
     temporary table.
  3. A `MERGE` on the business keys inserts new rows and updates only rows
     whose values changed; a `DELETE` removes rows silver no longer has,
     within the loaded weeks.
  4. A reconcile compares counts and sums per week with silver and writes
     a report.

  It loads the gate's decisions too, so dbt's approved-weeks filter works
  unchanged on Snowflake.
- dbt: a `snowflake` target (env vars only), sources pointed at `SILVER`,
  and two dispatch macros for the SQL that differs:
  - `any_true`: `bool_or` on DuckDB, `boolor_agg` on Snowflake;
  - `ordered_string_agg`: `string_agg(… order by …)` on DuckDB,
    `listagg(…) within group (order by …)` on Snowflake.

  `GROUP BY ALL`, lateral column aliases and `FULL JOIN … USING` ran
  unchanged.
- `retail_lakehouse.gold_compare`: for every gold model and every week,
  the row count plus one aggregate per column (numbers summed, booleans
  counted true, text counted and its length summed), on both engines.
- CI validates `infra/snowflake/` with no credentials. 11 new unit tests
  cover the loader's and the comparison's SQL and logic without a
  connection.

## Proof (2026-11-11)

| Check | Result |
|---|---|
| `tofu apply` | 21 added; a second plan shows no changes |
| Service users | each logs in with its own role; neither has a password |
| Least privilege | `LOADER` cannot create in `GOLD`; `TRANSFORMER` cannot create in `SILVER` but sees a table the loader created (future grant) |
| Trial load, one week (2016-05-24) | 664,947 sales and 371 transactions, the same counts as the Week 5 Databricks run; reconcile passes |
| Rerun of that week | 0 inserted, 0 updated, 0 deleted |
| One row changed and one stray row added in Snowflake, then a rerun | 1 updated, 1 deleted; reconcile passes |
| Full load, 113 approved weeks | 58,896,896 sales rows (58,231,949 new; 73 s for sales), 39,104 transactions, the three reference tables, 126 gate decisions |
| Reconcile, full load | all 113 weeks pass on counts and sums (after the fix below) |
| Full rerun | 0 inserted, 0 updated, 0 deleted, for every table |
| `dbt build --target snowflake` | PASS=33: 10 models and 23 data tests, including `gold_units_match_silver`; again for one week incrementally (delete+insert) |
| DuckDB gold, rebuilt from the same silver | PASS=33 |
| Gold on Snowflake vs gold on DuckDB | 5 models × 113 weeks, 35 checks per week (3,955 in all): **equal** |
| Negative control: one gold value +1 and one row deleted in a copy of DuckDB gold | the comparison fails on exactly those: `sum(units)` for one week of one model; rows and three sums for one week of another |

Reports: `data/reports/snowflake/load-*.json` and `gold-compare-*.json`
(local, not committed, like the other run reports).

## Cost

Credits on `TRANSFORM_XS` for the whole session, from
`INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY` at about 12:10 UTC:
**0.096 credits** (all compute), plus 0.0006 credits of cloud services. The
resource monitor shows 0.10 of 10 used. It came out of trial credits, so
the cash cost is $0. `ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY` lags by up
to three hours, so the figure above is the table function's.

Warehouse time by query tag, in seconds of elapsed query time: the loader
237 (including the PUT uploads), dbt 39, the comparison 1. Most of the
session's warehouse time was the loader; the dbt build of all 113 weeks took
about 7 seconds on an X-Small.

## What went wrong

- **Registering the admin key.** The trial's admin user is stored with a
  lowercase name, while its login name is an email address. `ALTER USER`
  needs the object name, double-quoted: unquoted, Snowflake uppercases it,
  and the answer is "does not exist or not authorized", which reads like a
  permissions problem. `SHOW USERS` showed the stored name.
- **`count(distinct (a, b))`.** DuckDB accepts a row in `count(distinct)`;
  Snowflake does not. The reconcile counts distinct keys with a subquery
  now, the same SQL on both engines.
- **Rounding split two equal sums.** The first full reconcile flagged week
  2014-10-21: the same 490,040 rows, but the sums, rounded to 3 decimals,
  were .935 and .936. The two engines add floats in a different order and
  the totals sat either side of a rounding boundary. The reconcile now
  compares unrounded sums with a relative tolerance of 1e-9 (as the Spark
  reconcile does), and counts exactly.
- **The provider warns on `SNOWFLAKE_ACCOUNT`** (v3 will read it as a
  fallback). The wrapper splits it into organisation and account names and
  unsets it.

## Left for later

- **Teardown:** the trial ends about 10 December. Suspended, the warehouse
  costs nothing; storage (416 MB, 412 MB of it silver sales, plus a day of Time
  Travel) uses trial credits only. `tofu
  destroy` by the last trial day (GOAL-3 A5).
- A Snowsight screenshot of the gold tables and the monitor is the
  author's to take.
- Not built: Airflow tasks for the Snowflake load and build. The loader and
  dbt run by hand, like the Week 5 cloud run.
