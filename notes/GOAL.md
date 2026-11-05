# Goal: finish the flagship (Weeks 3-6 of the career plan)

Repo `~/Projects/retail-lakehouse`. Plan: career-ops
`data/career-plan-2026-10-27.md`, Phase 1. Weeks 1-2 are done
(`notes/week-01.md`, `notes/week-02.md`). Each iteration:
1. Read this file and do the first unchecked task, within the guardrails.
2. Tick it and add one line to the log at the bottom.
3. Continue, or stop if blocked.

Claude writes all the code, at the author's request. The author wants it
built and proven, not handed back.

## Guardrails (never break these)

- **Public or synthetic data only:** Favorita and the simulator's faults.
  Nothing from work, and no employer or supplier names anywhere.
- **Git:**
  - One branch per week, cut from the previous week's branch (`week-03`
    from `week-02`, and so on).
  - Commit on it when the week's tests pass (`scripts/test.sh` and
    `ruff check .`).
  - Never push, never create a GitHub repo, never merge into `main`. These
    need the author (see Questions).
- **No spend, no cloud:** no Azure or other cloud logins, no `terraform apply`,
  nothing that costs money. Write the code, validate it locally, then stop
  at the question.
- **The machine is shared with the author's services:**
  - Don't touch Podman or any container (the author's own
    services).
  - Don't use ports 8000, 8080 or 8096.
  - At most 2 Spark JVMs at once (the `spark` pool).
  - `pkill -f` patterns must not match your own shell. Use
    `scripts/stop.sh`, PIDs, or `[x]yz` bracket patterns.
- **verity (`~/Projects/retail-qc`):**
  - Use it as an installed library, from `main`, in a venv here.
  - Don't edit retail-qc. If it needs a change, write it up under Questions.
  - Its Timestral branch stays untouched.
- **Installs:** user-space only (`~/.local`, venvs), with checksums
  verified. No sudo.
- **Long runs:** wrap in `systemd-inhibit --what=sleep:idle`. They must be
  rerunnable. Wait on them; don't poll in a tight loop.
- **Don't fake proof.** Every "done when" needs a real run and the numbers in
  that week's `notes/week-NN.md`. If something fails, say so in the notes.
- **Design reasoning:** short, in the existing voice, in `notes/decisions.md`
  under a heading for the week, including "what breaks at 100x".
- **Author's judgement:** anything that needs it goes under "Questions for
  the author". Don't decide it. Keep working on what isn't blocked.

## Tasks

### Week 3: dbt gold, with verity as the gate

- [x] 1. Choose the dbt adapter and write the choice up. Default:
  `dbt-duckdb` reading the silver Delta tables (its `delta` extension), with
  gold as DuckDB or Parquet. Week 5 swaps in `dbt-databricks`. Install it in
  its own venv if it conflicts.
- [x] 2. Gold models:
  - weekly sales by store x family (the department) and by state;
  - weekly store transactions;
  - holiday-flagged days;
  - one mart that answers a retail question (for example promo uplift by
    family).
  - Add dbt tests (unique, not_null, relationships to the dimensions,
    accepted ranges) and source freshness.
- [x] 3. verity gate:
  - Install retail-qc's `qc` package into a venv here.
  - Run it as a DAG task between silver and gold on the week's silver
    snapshot against the previous one. Delta versions are the snapshots.
  - Read retail-qc's README and docs for the API and configuration. Favorita
    runs on units only.
  - Decide what status blocks gold (at least `DATA_CONTRACT_FAILURE` and
    `INVESTIGATE`), and write it up.
- [x] 4. Quarantine a bad load: when the gate blocks, gold is not built for
  that week, the week is marked quarantined (a control record), an alert goes
  out, and the previous gold stays served.
- [x] 5. Proof: inject faults the gate should catch (the simulator's
  `restate`, plus anything verity's fault families suggest, such as a missing
  store or product), and show gate blocks, gold unchanged, alert sent. Then a
  clean week passes and gold updates. Record the numbers, plus any clean week
  the gate wrongly blocked (verity has a known false-alarm rate on Favorita;
  report it honestly). Notes in `notes/week-03.md`, decisions, commit.

### Week 4: CI/CD (local half)

- [x] 6. GitHub Actions workflows in `.github/workflows/`:
  - on PR: ruff, both pytest suites (Spark on JDK 21 via
    `actions/setup-java`), `dbt build` on a small fixture lake;
  - on merge to main: a deploy job (for now, build and upload an artifact).
  - Keep CI runtime reasonable: a tiny fixture, cached venvs.
- [x] 7. Run the workflow steps locally (the same commands, through a
  `scripts/ci.sh` the workflow also calls) and show a deliberately broken
  change failing. Notes in `notes/week-04.md`, commit. The real
  "broken PR fails, valid PR merges" proof needs a GitHub repo (Questions).

### Week 5: Terraform on Azure (code only)

- [x] 8. Install Terraform or OpenTofu in user space (verified checksum).
  Write `infra/`:
  - resource group, ADLS Gen2 storage account, Databricks workspace, and a
    Databricks job that runs the Spark jobs;
  - remote state described, with a local backend for now;
  - variables, outputs, tags, and a cost note.
  - Run `fmt`, `validate`, and `tflint` if available. No `plan` against
    real credentials, no `apply`.
- [x] 9. Make the Spark jobs runnable on Databricks unchanged: paths from
  config (`abfss://` vs local), and a job definition. Notes in
  `notes/week-05.md`, commit. Stop the Week 5 cloud run at the Azure
  question.

### Week 6: package

- [x] 10. README rewrite: problem, architecture diagram (Mermaid or SVG),
  quality gate, CI/CD, cost, what changes at scale. Write a runbook
  (`docs/runbook.md`): start, stop, replay a week, handle a quarantined week,
  rotate a fault. A stranger must be able to run it from a fresh clone:
  prove that with a clean clone into the scratchpad and the README steps
  only. Commit.
- [x] 11. Update the career-plan status line (career-ops
  `data/career-plan-2026-10-27.md`, Phase 1). Write the author a summary:
  what's proven, and what waits on them. Then stop the loop. The walkthrough
  video and CV v2 (Week 7) are the author's.

## Questions for the author

- Merge `week-02` (and later branches) into `main`?
- Week 4: create a GitHub repo (name, public or private) and push, so CI
  runs for real?
- Week 5: Azure subscription and budget for one cloud run, then
  `terraform destroy`?
- verity materiality: history revisions are material above 0.1% of the
  whole compared history (`materiality_ratio`), so at 107 weeks a single
  week must move by about 9% before verity reviews it. The simulator's
  restate (0.04% of a week) passes. Should the gate use a per-week ratio,
  a shorter window, or is this intended? (Not tuned here.)
- Nine clean weeks are quarantined (false alarms, reasons in
  `notes/week-03.md`), plus 2016-04-19's clean redelivery. Release them
  (`docs/runbook.md`, "A week is quarantined") or leave them as the record
  of the gate's false alarms?
- Disk: silver's old file versions take about 38 GB (the pre-fix MERGE
  rewrote the table every week; the live table is 0.7 GB). VACUUM with the
  default 7-day retention after 2026-11-12, or sooner with a shorter
  retention (irreversible, and no time travel before that)?
- Podman: still broken (see `notes/week-01.md`). Fixing it restarts your
  containers.

## Log

- 2026-11-04: goal written. Weeks 1-2 done (`936aa6e` on main, `296a09b` on
  `week-02`).
- 2026-11-04: task 1 done. dbt-duckdb 1.11.1 (dbt-core 1.12.6) in `.venv-dbt`, reading silver via `delta_scan`. Gold is incremental by week and approved weeks only (decisions.md).
- 2026-11-04: task 2 done. 5 staging views, 5 incremental gold models, 22 dbt tests (corrected 2026-11-05: first logged as 27), source freshness; `dbt build` 32/32 in 3.5 s. Gold units = silver units (81,135,726.257, 15 weeks).
- 2026-11-04: task 3 in progress. verity `main` (`6526a44`) installed in `.venv-qc`. Gate module, verity config, `publish_favorita` DAG, gate tests written. verity needs at least 26 weeks of history, so history backfill 2014-04-01..2015-12-29 is running (`scripts/backfill.sh`, chunked: one large backfill locked SQLite and crashed the scheduler; restarted). While waiting: task 6 files drafted (workflows, `scripts/ci.sh`, fixture lake, package script), not yet run.
- 2026-11-05: machine rebooted overnight mid-backfill (history landed through 2014-12-23). Fixed `scripts/backfill.sh`: week counts in local time lost a week across the DST change (Sydney), and a chunk whose create lost the SQLite lock race now waits on its existing runs. Resumed. `gate.run` now reads verity's real output (`qc weekly --json` → `report_dir/report.json` findings with review dispositions); crash or lock exits fail and retry against the same version pair instead of quarantining.
- 2026-11-05: the scheduler died again at 09:22 (SQLAlchemy `StaleDataError` on `dag_run` while two backfills were being created on SQLite), so 21 runs were queued with nothing running them for 30 min. `scripts/airflow.sh` no longer uses `airflow standalone`: each component runs in a restart loop, and `stop.sh` stops the supervisor first. Kill test: `kill -9` the scheduler, and it is back within 10 s. The first version used a pipe to `sed`, which didn't restart because the dead scheduler's children held the pipe open. Backfills now run one after another (ingest, then transform), not in parallel.
- 2026-11-05: history ingest done (53 weeks through 2015-12-29). Transform stalled after one chunk, 10:09 to 10:31. Its next create came before Airflow had marked the previous backfill complete, which it does a minute or two after the last run. That create was refused, and the fallback took one overlapping successful run as "this chunk exists". Fixed: it retries for 10 minutes, and waits on existing runs only when all of them exist. Resumed.
- 2026-11-05: history transform done. 107 weeks in silver (2014-04-01..2016-04-12), and all 214 reconcile reports pass. Week 2's fault weeks (2016-03-15 restate, 03-29 duplicates, 04-05 bad rows) were republished clean and rerun; silver equals the Favorita source for all three (rows and units). Gate bootstrap: version 0, 92 weeks, 8,326,821 rows, 92 `PRE_GATE` decisions. It failed once because DuckDB 2.0's `.arrow()` returns a reader; it now uses `.to_arrow_table()`. Gold full refresh: approved weeks only, 92 weeks, 400,956,964.816 units, equal to the gate table. First verity assessment, week ending 2016-01-05 (v0→v1, 49 s): **INVESTIGATE**, national units 6,095,794 vs expected 3,941,876 (+54.6%). The gate data equals the source, so this is a false alarm. The expectation is the same week a year earlier, and with 92 weeks of history verity sees one prior year. Not released by me: release is the author's call (Questions). `publish_favorita` unpaused and backfilling 2016-01-05..2016-04-12. Week 1 quarantined: gold skipped, alert email received with the finding and the release command.
- 2026-11-05: task 3 done. 15 weeks of 2016 gated through Airflow: 8 PASS, 7 INVESTIGATE, all of them clean data (each week's gate rows equal the source). Causes: 5 national-units flags whose base year (early 2015) is missing about 600 products in the public data, products 2,837→2,233 at week 106; one Bolívar +35% in the 2016 Carnaval week (Carnaval fell a week later in 2015); one week with 8 products gone from the source (0.47% of units), a real source change. verity's forecast is seasonal-naive (same week last year), so more history would not fix the first cluster. Gold = 92 pre-gate + 8 passed weeks and equals silver.
- 2026-11-05: task 4 done. Quarantined week 2016-01-12 rerun through `publish_favorita`: alert email sent (14 total), `build_gold` skipped, gold fingerprint (`python -m retail_lakehouse.gold fingerprint`: rows + md5 per table) identical before and after (`data/reports/proofs/gold-*-quarantine.json`). No new gate version or decision for unchanged data. The "last good version stays served" half is in task 5's restate of an approved week.
- 2026-11-05: task 5, restate of approved week 2016-03-15 (`scripts/replay.sh 2016-03-15 restate`, new): verity **PASS** (v15→v16, NO_NEW_WEEK). History revised +2,058 units, and verity's materiality is 0.1% of the whole compared history (482,096 units over 107 weeks), so this 0.04%-of-a-week revision is immaterial. Gold rebuilt with the restated week: 5,123,421.015 = silver; three sales tables changed, the other two the same. No alert. Not tuned: a per-week materiality is the author's call in verity (Questions).
- 2026-11-05: task 5, `missing-store` fault (new simulator fault: store 44, the largest, 5.6% of units, dropped from the drop) on new week 2016-04-19: verity **INVESTIGATE**, likely cause MISSING_STORES. "1 store absent, expected 302,515 (5.64%) vs limit 0.10%: material". Gold fingerprint unchanged, alert sent (15th email). Two fixes found on the way: `replay.sh` needed `--reprocess-behavior` for the gate's `depends_on_past` DAG, and the alert's findings now lead with materially missing entities (verity reports them as events, and the blocking finding names none). Now replaying 04-19 clean, then 04-26 and 05-03 (post-earthquake weeks, 16 Apr 2016).
- 2026-11-05: task 5 done. Clean redelivery of 2016-04-19: INVESTIGATE (historical correction). 2016-04-26 and 05-03: INVESTIGATE (Manabí after the earthquake; school supplies, a family with no 2015 sales). 2016-05-10, 05-17, 05-24: PASS, and gold updated (103 weeks, equal to silver; the blocked weeks not served). Totals: clean-week false alarms 9/20 (45%; verity's own Favorita evaluation reports about 30/100), faults caught 1/2 (missing store yes, 0.04% restate no). Notes in `notes/week-03.md`, decisions updated.
- 2026-11-05: Week 3 committed (`dccc202` on `week-03`). Tasks 6 and 7 done on `week-04`. `scripts/ci.sh all` exit 0 in 72 s. A new dbt test, `gold_units_match_silver`, passes on the real lake (103 weeks) and on the fixture, and `ci.sh` checks that fixture gold isn't empty. Three broken changes each fail their stage: gold excluding returns, `FAIL gold_units_match_silver`; `retries=0` on the gate, the DAG integrity test; an unused import, ruff F401. `package.sh` bundle checked. The workflows have not run on GitHub (Questions). Notes in `notes/week-04.md`.
- 2026-11-05: Week 4 committed (`d64ef12` on `week-04`). Tasks 8 and 9 done on `week-05`. OpenTofu 1.13.8 installed in user space (sha256 and GPG signature verified). `infra/` validates (azurerm 5.12.0, databricks 1.141.0): ADLS Gen2, Databricks workspace, access connector, Unity Catalog storage, and the weekly transform job. Spark jobs take `TABLE_ROOT` (abfss) for tables and `LAKE_ROOT` (a UC volume) for files, use the runtime session on Databricks, and have a wheel entry point. Proven locally with the real bronze job writing to a separate `TABLE_ROOT`. CI has an `infra` stage. No plan or apply (Questions: Azure). Notes in `notes/week-05.md`.
- 2026-11-05: task 10 in progress on `week-06`. While writing the README I found silver's MERGE rewrote the whole table every week: `WHEN NOT MATCHED BY SOURCE` disables partition pruning. That is why silver took 39 GB with a 0.7 GB live table. Fixed with tombstones: a new test reproduces it, and all 8 Spark tests pass. Real lake, 2016-03-15 redelivered clean: 1 file instead of 112, 659,753 rows copied instead of 58,231,949, 5.7 s instead of 67 s, 2,587 rows reverted (Week 2's restate count). Gate PASS, gold back to 5,121,362.755. Old versions not vacuumed (author's call, runbook). README, runbook, `get-data.sh` (pinned sha256s; the Hugging Face copy of the Kaggle data) and `demo.sh` written. DuckDB pinned to 2.0.0 in all three venvs. Ports configurable. Next: the fresh-clone run.
- 2026-11-05: fresh-clone run in the scratchpad (main Airflow and mail stopped, so the clone used the default ports). README steps only. `setup.sh` 7 s (uv cache), with verity installed from github.com/Luke458/verity at `6526a44`. `get-data.sh` downloaded 862 MB in 21 s, and all five sha256s matched. `ci.sh all` first failed: ruff formats the runbook's Python block, and I had written the runbook after the last CI run, so main failed too. Fixed (`bd0a276`), then exit 0 in 93 s. `demo.sh` now waits for DAG registration before unpausing (`04776da`). Demo running.
- 2026-11-05: task 10 done. Fresh clone, README steps only: setup, data (862 MB, sha256 ok), CI (exit 0, 93 s after the runbook-format fix), Airflow, `demo.sh` (after the gold-folder fix: 30 weeks, 4 gated, PASS ×4, gold 30 weeks). Runbook quarantine procedure on the clone's demo data: a served week redelivered without store 44 was quarantined (revision −295,512, material); gold kept the last approved version (5,121,362.755, 53 stores) while silver held the bad one; release plus clearing `decide` rebuilt gold = silver. Alerts now state the size of material revisions. Main Airflow and mail restarted. Notes in `notes/week-06.md`.
- 2026-11-05: task 11 done. Career-plan status line added (career-ops `data/career-plan-2026-10-27.md`, Phase 1, uncommitted there). It says Claude wrote the code and notes. Every task is ticked, and what remains needs the author (Questions). Loop stopped.
