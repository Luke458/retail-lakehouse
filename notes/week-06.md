# Week 6 (7 - 13 Dec): package

From the career plan (Phase 1): a README a stranger can follow, an
architecture diagram, a runbook, and proof it runs from a fresh clone.

**Done when:** a clean clone, using the README's steps only, runs end to
end. **Done 2026-11-05.**

## What's built

- `README.md`:
  - the problem, an architecture diagram (Mermaid), and what each layer
    does;
  - what each week proved, with the gate's false alarms stated;
  - how to run it, and the layout.
- `docs/runbook.md`: start and stop, a normal week, replay, handling a
  quarantined week (fix the feed, or release it), the fault table, failure
  recovery, and housekeeping (VACUUM, full refresh).
- `scripts/get-data.sh`: the five Favorita parquet files from the Hugging
  Face copy of the Kaggle data, with pinned sha256s.
- `scripts/demo.sh`: 30 weeks end to end through the real DAGs. 26 weeks
  are pre-gate history and the last 4 go through the gate. It ends with a
  verdict and gold table.
- Ports (`AIRFLOW_PORT`, `MAIL_PORT`) and the data location
  (`FAVORITA_SOURCE`, default `data/source/favorita`) are configurable.
  DuckDB is pinned to 2.0.0 in the three venvs that share the gold file.

## Fixed on the way

- **Silver's MERGE rewrote the whole table every week**, a Week 2 defect
  found while measuring disk use for the README. `WHEN NOT MATCHED BY
  SOURCE` makes every target row a candidate, so Delta can't prune to the
  week's partition. It is why silver took 39 GB on disk with a 0.7 GB live
  table. The week's vanished keys now arrive as tombstones in the same
  MERGE. Real lake, 2016-03-15 redelivered:

  | | before (v119) | after (v120) |
  |---|---|---|
  | files rewritten | 112 | 1 |
  | rows copied | 58,231,949 | 659,753 (one week) |
  | bytes written | 696 MB | 8.7 MB |
  | MERGE time | 67 s | 5.7 s |

  A new Spark test reproduces it: merging one week must leave another
  week's files untouched. The old versions (about 38 GB) aren't vacuumed;
  that's the author's call, and the runbook shows how.
- The fresh clone found three more:
  - ruff formats Python code blocks in Markdown, and the runbook was
    written after the last CI run;
  - `demo.sh` could unpause DAGs before a fresh Airflow had registered
    them;
  - a fresh lake has no `gold/` folder, and DuckDB won't create one, which
    would also have failed the publish DAG's first gold build.
- Alerts now state the size of a material history revision. verity's
  blocking finding for a revision carries impact 0.

## Proof: fresh clone (2026-11-05)

`git clone -b week-06` into the scratchpad. The main Airflow and mail
catcher were stopped so the clone used the README's default ports. Then the
README's commands, in order:

| Step | Result |
|---|---|
| `scripts/setup.sh` | exit 0, 7 s (uv cache warm); verity installed from github.com/Luke458/verity at `6526a44` |
| `scripts/get-data.sh` | 862 MB downloaded in 21 s; all five sha256s match |
| `scripts/ci.sh all` | first run failed (runbook formatting, fixed in `557ddc0`); then exit 0 in 93 s: lint, 29 + 8 tests, dbt 33/33, infra valid |
| `scripts/mail.sh &`, `scripts/airflow.sh &` | healthy; the admin password file written on first start |
| `scripts/demo.sh` | first run stopped at the first gold build (no `gold/` folder, fixed in `e1e5bc8`). Rerun: exit 0, finished chunks skipped. Ingest and transform of 30 weeks about 28 min, gate bootstrap 26 weeks (3,107,058 rows), then 4 weeks gated: **PASS ×4, gold serves 30 weeks** |

The runbook's quarantine procedure, on the clone's demo data:

| Step | Result |
|---|---|
| `scripts/replay.sh 2016-03-15 missing-store` (a served week, redelivered without store 44) | INVESTIGATE, HISTORICAL_CORRECTION, revision −295,512 units (material); 1 alert |
| Gold while quarantined | still the last approved version: 5,121,362.755 units, 53 stores. Silver holds the bad redelivery: 4,825,850.689, 52 stores |
| `gate release` + `airflow tasks clear ... -t decide -d` | `decide` reran, `build_gold` success, `quarantine` skipped; gold = silver (4,825,850.689, 52 stores) |

That release was a test of the procedure on throwaway demo data. No week in
the project's own lake has been released.

## Next

Task 11 in `GOAL.md`: the career-plan status line and a summary for the
author. The walkthrough video and CV v2 (Week 7) are the author's.
