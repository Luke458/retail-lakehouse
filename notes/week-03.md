# Week 3 (16 - 22 Nov): dbt gold behind a verity quality gate

From the career plan (Phase 1): dbt models from silver to gold, with tests
and freshness; verity as the gate between silver and gold; a bad load is
quarantined and the last good gold stays served.

**Done when:** a fault the gate should catch blocks gold, the alert goes
out, and gold is unchanged; a clean week then passes and gold updates. The
clean-week false-alarm rate is reported as measured. **Done 2026-11-05.**
Reasoning in `decisions.md` (Week 3).

## What's built

- `dbt/` (dbt-core 1.12.6, dbt-duckdb 1.11.1, in `.venv-dbt`): silver Delta
  tables read through DuckDB's `delta_scan`; 5 staging views, 4 incremental
  gold facts and 1 mart (promo uplift by family); 22 tests (unique, not
  null, relationships, accepted ranges) and source freshness. Gold reads only
  weeks whose latest gate decision approves them (`macros/weeks.sql`).
- `src/retail_lakehouse/gate.py` (verity from retail-qc `main` `6526a44`, in
  `.venv-qc`): its own Delta table `gate/favorita_weekly` at verity's
  Favorita grain (store × item × week net units), one version per gated
  week; `qc weekly` on the previous version against the new one; one
  decision record per run under `control/gate/`; `bootstrap` for history
  and `release` for a person's approval.
- `dags/publish_favorita.py`: `wait_for_silver` (both reconcile reports
  pass) → `gate` (in order, `depends_on_past`) → `decide` → `build_gold` or
  `quarantine` (email with verity's status, likely cause, and the findings
  for review: materially missing stores or products first).
- `src/retail_lakehouse/gold.py`: rows and md5 per gold table, to show
  whether a run changed gold.
- `scripts/replay.sh <week> [fault]`: republish a week and run ingest,
  transform and publish for it in order. `scripts/backfill.sh`: long ranges
  in chunks.
- `scripts/airflow.sh`: each Airflow component in its own restart loop,
  replacing `airflow standalone` (see "What went wrong").
- Simulator: new content fault `missing-store` (store 44, the largest, 5.6%
  of units).
- Tests: 25 Airflow-side (gate bookkeeping, alert summary, simulator) and 7
  Spark tests pass; `ruff` clean.

## Proof (2026-11-05)

History: 107 weeks in silver (2014-04-01 .. 2016-04-12, then new weeks up to
2016-05-24), all reconcile reports passing. The 92 weeks up to 2015-12-29
are the gate's pre-gate history (version 0, 8,326,821 rows), approved as
`PRE_GATE`. Gold rebuilt from approved weeks only equalled the gate table
(400,956,964.816 units). A gate run takes 56 s median (verity over 107 weeks
of store × item rows), and a one-week gold build takes 7 s.

| Check | Result |
|---|---|
| 15 clean weeks gated (2016-01-05 .. 2016-04-12) | 8 PASS, 7 INVESTIGATE; every week's gate rows equal the Favorita source |
| Quarantined week rerun (2016-01-12) | alert sent; `build_gold` skipped; gold fingerprint identical; no new gate version or decision for unchanged data |
| `restate` on an approved week (2016-03-15) | **not caught**: PASS. Revision +2,058 units (0.04% of the week); verity's materiality is 0.1% of the whole compared history (482,096 units). Gold rebuilt with the restated week, equal to silver |
| `missing-store` on a new week (2016-04-19) | **caught**: INVESTIGATE, likely cause MISSING_STORES, "1 store absent, expected 302,515 (5.64%) vs limit 0.10%: material". Gold unchanged; alert names store S44 first |
| Same week redelivered clean | INVESTIGATE, HISTORICAL_CORRECTION (week 172 revised +394,624). Not served |
| New clean weeks 2016-04-26, 05-03 | INVESTIGATE: Manabí +58k units after the 16 April earthquake (185,447 vs 102,559 the week before); school supplies, a family that sold nothing in 2015 |
| New clean weeks 2016-05-10, 05-17, 05-24 | PASS; gold updated with those 3 weeks, equal to silver. The 3 blocked weeks before them are not served. Gold: 103 weeks |

**Clean-week false alarms: 9 of 20 clean weeks blocked (45%).** verity's
own Favorita evaluation reports about 30 of 100. The faults and the clean
redelivery are not counted. Every blocked clean week has a cause visible in
the data:

- 5 weeks (2016-01-05 .. 01-26, 02-16), national units 50-60% above
  expected. The expectation is the same week a year earlier
  (seasonal-naive), and early 2015 in the public dataset is missing about
  600 products (2,837 → 2,233 at week 106, recovering by week 128).
  2016-02-16's base week is also 2015's Carnaval. More history does not
  help: the forecast looks one year back.
- 2016-02-09, Bolívar +35%: Carnaval was in this week in 2016 and a week
  later in 2015.
- 2016-02-02, 8 products gone from the source (0.47% of units). A real
  change in the feed, and a fair question for a person.
- 2016-04-26 and 05-03, the earthquake and the new school-supplies family
  (above).

**Faults: 1 of 2 caught.** A missing store is caught at once. A small
restatement is below verity's materiality and publishes. The materiality
scales with the length of the compared history, so at 107 weeks a single
week has to move by about 9% to be reviewed. That is a question for verity's
author, not tuned here.

None of the blocked weeks was released. Release is a person's decision
(`python -m retail_lakehouse.gate release --week W --by NAME --reason ...`,
then clear `decide`), and it is listed in `GOAL.md` under Questions.

**Follow-up 2026-11-05 (`notes/GOAL-2.md`):** at the author's request I
decided this myself and released all 10 blocked clean weeks: the 9 above
plus 2016-04-19's clean redelivery. Each one was released only after
checking that silver equals the Favorita source in rows and units, and that
evidence is the recorded reason. verity's status stays on each decision,
and the false-alarm rate above is unchanged. Gold now serves all 113 silver
weeks (`gold_units_match_silver` PASS).

Reproduce a fault:

```bash
scripts/replay.sh 2016-04-19 missing-store   # blocked: store S44 missing
python -m retail_lakehouse.gold fingerprint  # unchanged
scripts/replay.sh 2016-04-19                 # redeliver clean
```

## What went wrong on the way

- The machine rebooted overnight during the history backfill. Each piece is
  rerunnable, so it resumed from where it stopped.
- `scripts/backfill.sh` counted weeks in local time. Sydney's daylight
  saving change made an 8-week chunk look like 7, and the ingest backfill
  waited forever for a count it had already passed. It now counts in UTC.
- The scheduler died twice on the SQLite metadata DB when a backfill was
  created while it was updating the same runs (`database is locked`, then
  `StaleDataError`). `airflow standalone` never restarts it, so runs sat
  queued with nothing running them. Components now run under restart loops;
  a `kill -9` of the scheduler is back in 10 s. The first version didn't
  restart: it piped output through `sed`, and the dead scheduler's children
  held the pipe open.
- A backfill whose create came before the previous one was marked complete
  was refused. The retry logic then took one overlapping run as "this chunk
  exists". It now retries for 10 minutes and waits only when every run in
  the chunk exists.
- DuckDB 2.0's `.arrow()` returns a stream, not a table. The first gate
  bootstrap wrote the table and then crashed; it now uses `.to_arrow_table()`.
- The gate's first findings summary was a guess at verity's output. I read
  the source and rewrote it against `report.json`, then extended it when the
  missing-store run showed that missing entities are events, not findings.

## Next: Week 4 (23 Nov)

CI/CD: the workflows, `scripts/ci.sh` and the fixture lake are drafted; run
them locally, and show a broken change failing. Running it on GitHub waits
on the repo question.
