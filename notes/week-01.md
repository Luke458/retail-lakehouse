# Week 1 (2 - 8 Nov): orchestration and raw landing

From the career plan (Phase 1): Airflow 3 with TaskFlow, retries, sensors,
backfill and dynamic task mapping, landing raw weekly drops.

**Done when:** a 10-week backfill runs green, and a failed task retries and
alerts. **Done 2026-11-04.** The reasons behind each choice are in
`decisions.md`.

## What's built

- `dags/ingest_favorita.py`: one run per Wednesday-Tuesday week. Per feed,
  mapped: `wait_for_manifest` (reschedule sensor) -> `verify` (sha256 and
  rows) -> `land` (raw, idempotent). Then `publish` updates the raw assets.
- `dags/ingest_reference.py`: stores, items, holidays; triggered, not
  scheduled.
- `src/retail_lakehouse/landing.py`: verify and land logic, unit-tested.
- `src/retail_lakehouse/drops.py`: vendor simulator with `--fault late|corrupt`.
- Alerts: `SmtpNotifier` to the local catcher (`scripts/mail.sh`), with
  messages in `data/mail/new/`.
- Tests: 17 pass. The DAG house rules: imports clean, every task retries,
  every DAG alerts.

## Proof (2026-11-04)

| Check | Result |
|---|---|
| 10-week backfill, weeks ending 2016-01-05 .. 2016-03-08 | 10/10 runs `success` |
| Corrupt drop (week ending 2016-03-15) | `verify` failed 3 tries (`sha256 373f106c5d36 != manifest 000000000000`), one alert email naming the task and log URL, raw untouched |
| Republish clean, clear failed tasks | run `success` |
| Late drop (week ending 2016-03-22) | sales sensor `up_for_reschedule` while transactions landed; published the manifest, run `success` |
| Republish all 11 weeks unchanged, rerun every run | raw byte-identical, 22/22 feed-weeks `unchanged` |

Reproduce:

```bash
. scripts/env.sh
python -m retail_lakehouse.drops reference && airflow dags trigger ingest_reference
python -m retail_lakehouse.drops publish 2016-01-05 --count 10
airflow backfill create --dag-id ingest_favorita --from-date 2015-12-30 --to-date 2016-03-02
python -m retail_lakehouse.drops publish 2016-03-15 --fault corrupt
airflow backfill create --dag-id ingest_favorita --from-date 2016-03-09 --to-date 2016-03-09
```

`--from-date` is the interval start (a Wednesday). A run for the week ending
Tuesday T has logical date T - 6 days, and its run id is named for T + 1.

## Not done / open

- **Podman (your call).** Rootless Podman is broken on this machine. Every
  command fails with `unable to create a new pause process ...
  conmon.pid: no such file or directory`. `podman system migrate` didn't fix
  it. The remaining fixes (`migrate` once it works, or a reboot) restart your
  running containers, so I left it. Nothing through Week 3 needs containers.
- **No UI screenshot.** The Playwright browser isn't installed. The run table
  above comes from `airflow dags list-runs`.

## Next: Week 2 (9 Nov)

PySpark bronze to silver on Delta: schema enforcement, dedup, incremental
`MERGE`, idempotent reruns, scheduled on the raw assets. Needs Java 17 and
`delta-spark` in the venv.
