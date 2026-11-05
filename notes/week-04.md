# Week 4 (23 - 29 Nov): CI/CD, local half

From the career plan (Phase 1): GitHub Actions on every PR (lint, tests,
`dbt build` on a fixture), a deploy job on merge to main.

**Done when:** a broken PR fails and a valid one merges. **Local half done
2026-11-05.** The same commands the workflows run pass locally, and three
deliberately broken changes each fail at the right stage. Running it on
GitHub waits on the repo question in `GOAL.md`.

## What's built

- `.github/workflows/ci.yml`, on pull requests and pushes to `main`, three
  parallel jobs:
  - lint + Airflow-side tests;
  - Spark tests on Temurin 21 (`actions/setup-java`);
  - `dbt build` on a fixture lake.
  Each job installs only its venv (`scripts/setup.sh <venv>`). uv's cache
  is keyed on `setup.sh`, and the Delta jars (ivy) are cached.
- `.github/workflows/deploy.yml`: after `ci` succeeds on `main`, it packages
  the wheel, DAGs, dbt project and verity config with a commit manifest, and
  uploads it as an artifact. Week 5 replaces the upload with a Databricks
  deployment.
- `scripts/ci.sh [lint|airflow|spark|dbt|all]`: the stages the workflows
  call, runnable locally.
- `src/retail_lakehouse/fixture.py`: a lake of 3 stores, 24 items and 2
  weeks, built with the real simulator, landing and Spark jobs, so CI's
  `dbt build` reads silver produced by production code. Gate decisions are
  written as `FIXTURE` approvals: CI doesn't run verity, which has its own CI.
- `dbt/tests/gold_units_match_silver.sql`: every week served in gold
  carries exactly silver's units. It runs in every `dbt build`, in CI and in
  production.
- After `dbt build`, `ci.sh` checks that the fixture's gold actually holds
  its 2 weeks: tests on an empty gold would pass.
- `scripts/package.sh <dir>`: the deploy job's bundle.

## Proof (2026-11-05)

| Check | Result |
|---|---|
| `scripts/ci.sh all` on `week-04` | exit 0 in 72 s: ruff clean; 25 Airflow-side tests; 7 Spark tests (41 s); fixture lake + `dbt build` 33/33, fixture gold 2 weeks |
| New gold test on the real lake | `gold_units_match_silver` PASS over 103 served weeks (0.14 s) |
| Broken gold: `and not s.is_return` added to `fct_sales_store_family_week` ("exclude returns") | `ci.sh dbt` exit 1: `FAIL 2 gold_units_match_silver`, 5 downstream nodes skipped (`--fail-fast`) |
| Broken DAG: `retries=0` on `publish_favorita.gate`, with no reason | `ci.sh airflow` exit 1: `test_every_task_retries_or_says_why_not`, `['publish_favorita.gate']` |
| Broken lint: an unused import in `landing.py` | `ci.sh lint` exit 1: ruff `F401` |
| `scripts/package.sh` | wheel + 45-file bundle + `MANIFEST.json` (commit, files); no local or cache files |

Logs of the three failures: `data/reports/proofs/ci/broken-*.log`. Each
change was reverted from git afterwards.

**Not proven yet:** the workflows themselves have not run on GitHub, so
runner details (uv cache keys, the ivy cache path, the Java setup) are
untested there. That needs a repository (Questions in `GOAL.md`), then a PR
with one of the broken changes above and one without.

## Next: Week 5 (30 Nov)

Terraform for Azure (code only, `validate` without credentials), and Spark
job paths that run on Databricks unchanged.
