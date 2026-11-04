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
