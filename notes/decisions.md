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

## Week 3: dbt gold and the verity gate

**dbt-duckdb locally, dbt-databricks in the cloud.** Gold is a DuckDB database
(`data/gold/favorita.duckdb`). dbt reads the silver Delta tables through
DuckDB's `delta` extension (`delta_scan`), with no Spark and no Thrift
server. The silver tables use the basic Delta protocol (reader v1), which
`delta_scan` reads. Sums match the reconcile reports. dbt gets its own venv
(`.venv-dbt`), like Spark, so the three toolchains never negotiate a shared
dependency set. In Week 5 the same models run on Databricks under
`dbt-databricks`. Only the source definitions and a few dialect macros differ.

**Gold is incremental by week, and only approved weeks enter it.** The gate
sits between silver and gold, but silver has already merged the week by the
time the gate runs. If gold were rebuilt from all of silver, a quarantined
week would leak in on the next good week's build. So each gold model
processes only the run's week (`delete+insert` on `week_ending`), and only
when the gate approved it. A blocked week leaves gold exactly as it was,
including that week's last good version if it had one. A
`dbt build --full-refresh` rebuilds from approved weeks only. A week whose
latest decision is "blocked" is then dropped rather than frozen, which is
written into the runbook.

**What the gate blocks, and what it doesn't.** verity's status is the
verdict. `PASS` and `PASS_WITH_EXPLANATION` publish. `INVESTIGATE`,
`DATA_CONTRACT_FAILURE` and `INCOMPLETE` quarantine. verity failing to run
(exit 1) or being locked by another run (exit 75) is not a verdict. The gate
task fails and retries, records no decision, and gold isn't built, because
`decide` has nothing to read. Treating a crash as a quarantine would bury an
infrastructure fault among data findings, and treating it as a pass would
publish unchecked data.

**Why block rather than warn.** The gold tables feed weekly reporting. A
restated or half-delivered week that reaches gold is consumed before anyone
reads an alert, and correcting a published number costs more than publishing
a day late. The trade-off is false alarms. verity flagged about 30 of 100
clean Favorita weeks in its own evaluation, so some clean weeks will wait for
a person. That is why `release` exists: a named person records an approval
with a reason, verity's status is kept unchanged next to it, and the next
`decide` builds the week.

**The gate keeps its own table.** verity compares two Delta versions, but
silver commits several versions per week (each feed's MERGE, reference
loads). The gate writes one version per gated week to
`gate/favorita_weekly`, at verity's grain (store × item × week net units), so
each assessment is "this week arrived" against the version before it. A run
whose rows are unchanged reuses its decision instead of writing a new version
that verity would compare with itself. Weeks loaded before the gate went live
are written once, as version 0, and recorded as approved `PRE_GATE` history,
not as assessed.

**Weeks are gated in order** (`depends_on_past` on `gate`): each version
pair assumes the previous week is already in the table. A quarantined week
still counts as done for that chain. Its rows are in the gate table and its
decision is recorded, so later weeks proceed and are compared against what
actually arrived.

**Supervised Airflow components instead of `airflow standalone`.** On the
SQLite metadata DB, creating a backfill while the scheduler is updating the
same runs can kill the scheduler (`database is locked`, then
`StaleDataError`). It happened twice during the history backfill.
`standalone` never restarts a dead component, so runs sat queued with
nothing running them. `scripts/airflow.sh` now runs the scheduler, DAG
processor, API server and triggerer each in its own restart loop. The root
cause is SQLite's single writer. At 100x, or with more than one person, the
metadata DB is Postgres and the components run as separate services under a
process manager (systemd, or Kubernetes in the cloud). The restart loop is
the local stand-in for that.

**Measured, not tuned.** On 20 clean weeks the gate blocked 9 (45%). It
caught a missing store (5.6% of units) and passed a 0.04% restatement as
immaterial. I left verity's configuration at its defaults rather than tune
it to the weeks I measured it on. Both its expectation (seasonal-naive, so
2016 is compared with an early 2015 that is missing about 600 products) and
its materiality (0.1% of the whole compared history) are design questions
for verity, written down for its author. The pipeline side is what this
project controls: a blocked week costs a person's review, never a wrong
number in gold.

### What breaks at 100x (Week 3)

- **The gate table.** It holds every week at store × item grain (8.3M rows
  for 92 weeks, 42 MB), and each assessment reads the whole of it: 56 s
  today. At 100x the stores and items, verity would compare a rolling
  window (a year or two) instead of all history, with older weeks
  compacted out. That also tightens the history-revision materiality,
  which is a share of the compared total.
- **One gate run at a time.** `depends_on_past` keeps the version chain in
  order, so a backlog drains one week per minute. With more feeds, each
  dataset gets its own gate table and chain, and they run in parallel.
- **Human release at a 45% false-alarm rate** is a part-time job. At scale,
  approvals need to be cheap and remembered: verity's approval records
  (approved closures, expected events) rather than a release command per
  week. So does a lower false-alarm rate, which is verity's to fix.
- **Gold in a DuckDB file.** One writer, one machine. At 100x, gold is a
  Delta table on Databricks (`dbt-databricks`, Week 5). The per-week
  `delete+insert` and the approved-weeks filter carry over unchanged.

## Week 4: CI/CD

**CI runs the production code on a fixture, not a mock.** The fixture lake
(3 stores, 24 items, 2 weeks) goes through the real simulator, landing, and
Spark bronze and silver jobs, so `dbt build` in CI reads silver produced the
way production produces it. The stage takes about 30 s, most of it a Spark JVM. The gate
is the one stage CI does not run: verity has its own CI, and an assessment
needs 26+ weeks of history. The fixture records its weeks as `FIXTURE`
approvals, so gold's approved-weeks filter is still exercised.

**A test that would have caught a real class of bug.** Most dbt tests
(unique, not null, relationships, ranges) pass on wrong totals. The
singular test `gold_units_match_silver` compares each served week's units
with silver's. A filter or a join that drops or duplicates rows fails it.
That is the broken change used in the proof.

**One job per venv.** Airflow, Spark and dbt never share a venv (Week 2), so
CI installs one per job and runs them in parallel. A change to the Spark
pins doesn't rebuild the Airflow environment.

**Deploy is an artifact for now.** Merge to `main`, then CI green, then a
versioned bundle (wheel, DAGs, dbt, config, commit manifest). Week 5 turns
the same bundle into a Databricks deployment.

### What breaks at 100x (Week 4)

- The Spark and dbt jobs each start a JVM and download Delta's jars on a
  cold cache. At more jobs, a prebuilt CI image with the JDK and jars saves
  most of the setup time.
- The fixture is hand-built. With more feeds and edge cases, fixtures should
  be generated from contracts (each feed's schema and the faults it must
  survive), and the fault matrix runs in CI against the fixture.

## Week 5: Azure and Databricks (code only)

**OpenTofu, not Terraform.** Same language and providers (azurerm,
databricks), open-source licence, installable without an account. The code
runs under either.

**Files and tables have separate roots.** On Databricks, Delta tables live
at `abfss://` URIs, but the Python code also reads and writes plain files
(drops, manifests, raw CSVs, reports) with `pathlib`. A Unity Catalog
volume gives those files an ordinary path (`/Volumes/...`), so `LAKE_ROOT`
stays a path and only the tables move, via `TABLE_ROOT`. Locally both are
the same folder. The change touched one function (`table_path`), because
every Delta read and write already went through it.

**Identity, not keys.** The storage account has shared keys disabled.
Databricks reaches it as the access connector's managed identity, through a
Unity Catalog storage credential, so no secret sits in a cluster config.

**The schedule stays in Airflow.** The Databricks job has no schedule of
its own. Airflow triggers it per week with a `week` parameter, as it runs
the Spark commands locally, so retries, alerts and the gate keep one
orchestrator.

**Apply, run, destroy.** The workspace's managed networking has an hourly
cost whether or not a job runs, so the cloud proof is a single session, not
a standing environment.

### What breaks at 100x (Week 5)

- A single-node job cluster per week. At 100x the data, a small autoscaling
  job cluster, partitioned silver tables, and the two feeds as parallel
  tasks.
- One workspace and local state. Several environments need remote state per
  environment, CI deploying with OIDC federation (no stored secrets), and
  workspace configuration in modules shared across environments.
