# Design decisions

Short answers to "why did you do it this way". Newest week last.

## Week 1: orchestration and raw landing

**The run owns an explicit week.** `CronDataIntervalTimetable("0 0 * * 3")`
gives each run a Wednesday-to-Wednesday data interval. The week it loads is
`data_interval_end - 1 day`, a Tuesday, checked in `landing.week_ending`. The
date a run happened to start never decides what it loads. Airflow 3's
default `@weekly` is a trigger timetable with no interval, which would leave
the week to be inferred.

**History is replayed, not faked as live.** `end_date` is the day after the
source ends (2017-08-15), and `catchup=False`. Every run is an explicit
backfill or a trigger, so nothing quietly schedules 2013-2017 on first deploy.

**The manifest is the trigger, not the data file.** The vendor writes
`_MANIFEST.json` last, so a sensor watching for the CSV could see a
half-written file. The manifest also carries row counts and sha256s, which
makes "the file arrived" and "the file is what was sent" the same check.

**Sensor in `reschedule` mode.** Drops can be hours late. In `poke` mode the
sensor holds a worker slot the whole time. `reschedule` frees the slot between
checks (every 30 s, giving up after 6 h). A deferrable trigger would free it
completely, but a filesystem check is cheap, and reschedule needs no triggerer
code. Switch if many feeds wait at once.

**Verify, then land, as separate tasks.** A checksum failure is a bad file,
not a flaky network, so the retry is mostly there to cover a vendor fixing
the drop within the minute. The real recovery is the alert, a republish, and
clearing the task. Keeping verify separate means the failure names the bad
file, and raw is never touched.

**Retries: 2, 30 s with exponential backoff, then an email.** Short delays
because the inputs are local files. Against a remote source I'd use minutes.
The alert is a task-level `SmtpNotifier` in `default_args`, so it fires once,
after the last retry, and names the task and map index.
`tests/test_dag_integrity.py` fails any DAG with a task that doesn't retry or
alert.

**Mapped over feeds.** `ingest.expand(feed=WEEKLY_FEEDS)` runs one task group
per feed. A new weekly feed is one entry in a tuple, and a late feed holds
back only its own branch (seen with `--fault late`: transactions landed while
sales waited).

**Raw is an exact copy, landed idempotently.**
- The drop is copied unchanged, with a `_INGEST.json` recording the run, the
  time, the source checksums and row counts.
- A rerun with the same checksums is a no-op and leaves even the metadata
  alone. Proof: republishing all 11 weeks and rerunning every run left the
  raw zone byte-identical, with 22 of 22 feed-weeks `unchanged`.
- A different file for the same week replaces the drop as a whole. It is
  staged next to the target and swapped in by rename, so a reader sees the
  old drop or the new one, never a mix. `replaces` records the old checksums.

**Deterministic gzip.** The first version had a bug: republishing identical
data changed every checksum, because gzip writes a timestamp and the file
name into its header. Writing with `mtime=0` and no name makes the same rows
give the same bytes. Without that, idempotency is a lie at the first
republish.

**Reference tables are their own DAG.** Stores, items and holidays arrive
once and change rarely, so `ingest_reference` has no schedule and is triggered
after a reference drop. Both DAGs emit file `Asset`s, which Week 2's
bronze-to-silver DAG can be scheduled on instead of on a clock.

**Local, not containers, for now.** Airflow 3.3.2 runs in a `uv` venv pinned
to the official constraints file, with LocalExecutor on SQLite. That's fine
for one machine and one user. Postgres comes in when the scheduler needs real
concurrency, or in the cloud week.

## What breaks at 100x

- **SQLite metadata DB:** one writer. Move to Postgres before raising
  parallelism.
- **Full-file checksum and row count in the worker:** about 0.5 s a week now.
  At 100x, verify streams and the manifest check moves to object-store
  metadata (ETag or content-MD5).
- **Rename-swap in raw:** atomic on a local filesystem, not on object storage.
  There, land into a new versioned prefix and point a manifest or table
  version at it. Delta does this in Week 2.
- **One task group per feed:** fine for tens of feeds. For hundreds of
  vendors, map over a feed registry in config.

## Week 2: bronze and silver on Delta

**Spark gets its own venv.** `delta-spark` pins `importlib-metadata` below
the version Airflow's constraints require. Rather than loosen Airflow's
constraints, the Spark jobs live in `.venv-spark`, and Airflow runs them as
subprocesses (`python -m retail_lakehouse.spark <job>`), one JVM per task.
That's also the shape they take as Databricks jobs in Week 5: the
orchestrator submits, it doesn't host the compute.

**JDK 21, not the system Java.** Spark 4.2 doesn't support Java 27. A
Temurin 21 tarball in `~/.local/opt` (checksum-verified), selected by
`JAVA_HOME` in `scripts/env.sh`, leaves the system untouched.

**Bronze is raw with types, not raw cleaned.**
- The CSV is read with an explicit schema, never inferred.
- A header that isn't the contract (`SchemaDrift`) fails before anything is
  written. Guessing that `units` means `unit_sales` is how bad data gets in
  quietly.
- Rows that don't parse, have a null key, or fall outside the week go to
  `quarantine/<feed>` with the raw line and a reason, so nothing is dropped
  without a record.
- Over 1% bad rows fails the load, after quarantining, so the rows can be
  inspected.
- Bronze plus quarantine must equal the manifest's row count, or the load
  fails.

**Bronze writes are partition replaces.** `replaceWhere _week_ending = X`
replaces exactly one week, in bronze and quarantine together. A week already
loaded from the same raw checksum is skipped, with no new table version.

**Silver is one row per natural key, MERGEd a week at a time.**
- Duplicates collapse to one row. On a conflict (same key, different values)
  the highest vendor `id` wins, taken as the later correction. Exact and
  conflicting duplicates are counted separately, because the second is a
  data-quality signal and the first usually isn't.
- `WHEN MATCHED AND row_hash differs THEN UPDATE` rewrites only rows whose
  values changed. A restatement of 0.5% of rows touches 0.5% of rows.
- `WHEN NOT MATCHED BY SOURCE` deletes rows the vendor stopped sending, but
  only inside that week's partition. The predicate is the difference between
  "restated week" and "deleted history".
- Partitioned by `week_ending`, and the MERGE condition names the week, so a
  merge reads one partition, not the table.

**A no-op MERGE still commits.** Delta writes an empty version even when the
MERGE changes no rows. So each merge is recorded in `control/silver_loads`
with the bronze checksum, and a rerun from the same checksum skips the MERGE
entirely. "Rerunning a week changes nothing" then holds for the table
version too, not just the data. If silver itself is missing, the control
table is ignored.

**Unknown stays null.** `onpromotion` is null for every row before
2014-04-01, because the vendor didn't track it. Silver keeps
`on_promotion` null rather than defaulting it to false, which would invent a
fact.

**Reconciliation uses a second engine.** DuckDB reads the raw CSV with every
column as text, applies `TRY_CAST`, deduplicates in its own SQL, and sums.
Spark's bronze, quarantine and silver must match it on row counts, distinct
keys and the measure's sum. If Spark checked itself, a bug in the shared
logic would pass. Reconcile never retries, because a mismatch is a finding,
not a flake.

**Cross-DAG dependency on data, not on task state.** `transform_favorita`
waits for the week's `_INGEST.json` in raw, not for `ingest_favorita`'s task
to succeed (`ExternalTaskSensor`). Either DAG can be rerun or backfilled on
its own. Asset scheduling was the other option, but asset-triggered runs have
no data interval, so they can't be backfilled by week.

**Concurrency is set on the tasks, not on the backfill command.** The first
backfill ran 10 weeks at once: `airflow backfill create --max-active-runs`
defaults to 10 and overrides the DAG's `max_active_runs=1`. That meant 20
JVMs, a load average of 68, and concurrent MERGEs into the same table. The
fix doesn't depend on the person typing the command:
- every Spark task takes a slot in a 2-slot `spark` pool;
- `bronze` and `silver` set `max_active_tis_per_dag=1`, so one week at a time
  writes each layer.

### What breaks at 100x (Week 2)

- **One JVM per task:** the 10-15 s startup dominates at this size and is
  noise at 100x. On a cluster, submit jobs to a long-lived cluster or job
  compute, and stop starting a Spark per task.
- **`max_active_tis_per_dag=1` serializes writers:** fine for one table per
  feed. At 100x, partition-scoped writes (`replaceWhere`, MERGE on the
  partition) are safe in parallel only if Delta's conflict detection sees
  disjoint partitions, so make the partition predicate explicit, as it is
  here.
- **DuckDB reconciliation reads the whole raw week:** at 100x, reconcile on
  aggregates both engines can produce cheaply (per-store counts and sums),
  not row-level.
