#!/usr/bin/env bash
# The CI pipeline, runnable locally. GitHub Actions calls the same stages.
#
#   scripts/ci.sh [lint|airflow|spark|dbt|all]
#
# Each stage uses the venvs from scripts/setup.sh. `dbt` builds a tiny fixture
# lake with the real simulator, landing and Spark jobs, then runs `dbt build`
# (all models and tests) against it.
set -euo pipefail
. "$(dirname "$0")/env.sh"
cd "$REPO"
stage=${1:-all}

lint() {
  .venv/bin/ruff check .
  .venv/bin/ruff format --check .
}
airflow_tests() { .venv/bin/python -m pytest -q -m "not spark"; }
spark_tests() { "$SPARK_PYTHON" -m pytest -q -m spark -p no:cacheprovider; }
dbt_build() {
  work=$(mktemp -d)
  trap 'rm -rf "$work"' RETURN
  "$SPARK_PYTHON" -m retail_lakehouse.fixture "$work"
  mkdir -p "$work/lake/gold"
  LAKE_ROOT="$work/lake" DBT_TARGET_PATH="$work/dbt/target" DBT_LOG_PATH="$work/dbt/logs" \
    "$DBT" build --fail-fast
  # Tests on an empty gold pass trivially: the fixture's two weeks must be there.
  "$SPARK_PYTHON" - "$work/lake/gold/favorita.duckdb" <<'PY'
import sys

import duckdb

con = duckdb.connect(sys.argv[1], read_only=True)
weeks, rows = con.sql("select count(distinct week_ending), count(*) from fct_sales_store_family_week").fetchall()[0]
assert weeks == 2 and rows > 0, f"fixture gold has {weeks} weeks and {rows} rows, expected 2 weeks"
print(f"fixture gold: {weeks} weeks, {rows} store-family rows")
PY
}

case $stage in
  lint) lint ;;
  airflow) airflow_tests ;;
  spark) spark_tests ;;
  dbt) dbt_build ;;
  all) lint && airflow_tests && spark_tests && dbt_build ;;
  *) echo "unknown stage $stage" >&2; exit 2 ;;
esac
