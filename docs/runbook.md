# Runbook

Every command runs from the repo root after `. scripts/env.sh` (scripts
source it themselves). Week arguments are week endings, which are Tuesdays.
Airflow's logical date for a week is the Wednesday six days earlier (the
start of its interval): week ending 2016-03-15 is logical date 2016-03-09.

## Start and stop

```bash
scripts/mail.sh &        # alerts: data/mail/new/ (one file per email)
scripts/airflow.sh &     # http://localhost:8088, user admin
scripts/stop.sh          # stops the supervisor first, then the components
```

`airflow.sh` runs the scheduler, DAG processor, API server and triggerer,
each in a restart loop: a component that dies is back in 10 s (log line
`<component> | exited, restarting`). The metadata DB is SQLite in
`.airflow/`, so only one copy of this repo's Airflow may run at a time.

Health: `curl -s localhost:8088/api/v2/monitor/health`.

## A normal week

A new drop arrives in `data/inbox/favorita/<feed>/week_ending=W/` (here
from the simulator: `python -m retail_lakehouse.drops publish W`). The three
weekly DAGs run in sequence on their own:

1. `ingest_favorita` waits for both manifests, verifies them, and lands raw.
2. `transform_favorita` waits for raw, then runs bronze, silver and
   reconcile per feed.
3. `publish_favorita` waits for both reconcile reports to pass, runs the
   gate, then builds gold or quarantines.

To run one week by hand, all three in order: `scripts/replay.sh W`. For a
range: `scripts/backfill.sh <dag> <first logical date> <last logical date>
[weeks per chunk]`. It's chunked (one large backfill locks SQLite) and
rerunnable: finished chunks are skipped.

## Replay a week

```bash
scripts/replay.sh 2016-03-15           # republish clean, rerun all three DAGs
scripts/replay.sh 2016-03-15 restate   # republish with a fault
```

Every layer is idempotent. An unchanged drop is a no-op in raw, bronze and
silver (no new Delta version), and the gate reuses its decision when the
week's rows are unchanged. A changed drop replaces the week in each layer
and is gated again, as a revision of that week.

## A week is quarantined

The email subject is `week ending W quarantined: verity <STATUS>`. The
email gives the likely cause and the findings, largest first, with
materially missing stores or products before everything else.

1. Read the report: `<report>/report.md` (its path is in the email, and in
   `data/control/gate/week_ending=W/decision-*.json`).
2. Decide which of these it is:
   - **The data is wrong** (a missing store, a partial file). Get a
     corrected drop from the vendor and `scripts/replay.sh W`. The gate
     assesses the redelivery.
   - **The data is right and the world changed** (a holiday that moved, a
     store opening, an earthquake). Release it:

     ```bash
     "$QC_PYTHON" -m retail_lakehouse.gate release --week W --by "<your name>" --reason "<why>"
     airflow tasks clear publish_favorita -s <logical date> -e <logical date>T23:59:59 -t decide -d -y
     ```

     The release is recorded next to verity's decision, which is kept
     unchanged. `decide` then builds gold for the week.
3. Check gold: `python -m retail_lakehouse.gold fingerprint` (rows and an
   md5 per table) before and after.

While a week is quarantined, gold keeps serving everything else, including
the week's last approved version if it had one. Later weeks still gate
normally: the quarantined week's rows are in the gate table, so the next
week is compared with what actually arrived.

## Break a week on purpose

Simulator faults (`python -m retail_lakehouse.drops publish W --force --fault F`,
or `scripts/replay.sh W F`):

| Fault | Expected |
|---|---|
| `late` | data file, no manifest: `ingest` waits (sensor). Publish again without the fault to finish |
| `corrupt` | the manifest's sha256 doesn't match: `ingest` fails, retries, alerts. Nothing lands |
| `schema` | a renamed column: `bronze` fails before writing, alerts |
| `duplicate` | 1% of rows sent twice: silver deduplicates, and counts them in the result |
| `bad-rows` | 0.1% of quantities unparseable: quarantined with a reason, the rest loads |
| `restate` | 0.4% of rows revised up 10%: silver updates exactly those rows; the gate passes it (immaterial) |
| `missing-store` | store 44 left out: the gate blocks (MISSING_STORES) and gold is unchanged |

Undo with a clean `scripts/replay.sh W`.

## When something fails

- **A task failed and alerted.** Its log link is in the email. `ingest`,
  `bronze` and `silver` retry once. `reconcile` never retries: a mismatch
  between DuckDB and Spark is a finding to investigate, not a flake. After
  fixing the cause: `airflow tasks clear <dag> -s <logical> -e <logical>T23:59:59 -y`.
- **Runs sit queued and nothing runs.** Check health. The supervisor
  restarts a dead scheduler, so look for `scheduler | exited` in the Airflow
  log for the reason.
- **`backfill.sh` stops with "failed runs".** Fix the failed run, then rerun
  the same command; finished chunks are skipped.
- **The gate failed (not quarantined).** verity crashed (exit 1) or was
  locked (exit 75): there's no decision and no gold build. The retry
  assesses the same table versions (`control/gate/week_ending=W/pending.json`).
- **Machine restarted mid-run.** `scripts/airflow.sh &` again. Runs that
  were in flight are marked failed. Clear them, or rerun the backfill.

## Housekeeping

- **Delta history.** Old file versions stay for time travel. Reclaim
  space after a week or more with `VACUUM` (default retention: 7 days), for
  example in `.venv-spark`:

  ```python
  from delta.tables import DeltaTable
  from retail_lakehouse.spark.common import spark_session, table_path

  spark = spark_session()
  DeltaTable.forPath(spark, table_path("silver", "sales")).vacuum()
  ```

  Observed with Delta 4.4 here: `VACUUM ... RETAIN <n> HOURS DRY RUN`
  found 0 files for every n > 0, even hours after the last commit, while
  `RETAIN 0 HOURS` listed them. To reclaim space now, run `vacuum(0)`, with
  `spark.databricks.delta.retentionDurationCheck.enabled=false`, only
  while Airflow is idle (nothing may be reading or writing the table). That
  is how silver went from 38.9 GB to 0.7 GB on 2026-11-05.
- **Gold rebuild.** `"$DBT" build --full-refresh` rebuilds gold from approved
  weeks only. A week whose latest decision blocks it is dropped from gold, not
  frozen at an earlier version.
- **Alerts.** `data/mail/new/` grows by one file per email. Delete freely.
