#!/usr/bin/env bash
# Create the four venvs. Each toolchain gets its own, because their pins conflict:
#   .venv        Airflow 3.3.2, pinned to its constraints file
#   .venv-spark  Spark 4.2 + Delta 4.4 + DuckDB (bronze, silver, reconcile)
#   .venv-dbt    dbt-duckdb and dbt-snowflake (gold), and the Snowflake loader
#   .venv-qc     verity, pinned to a commit (the gate)
# DuckDB is pinned to one version in the three venvs that use it: they share the gold file.
set -euo pipefail
cd "$(dirname "$0")/.."
PY=3.12
AIRFLOW_VERSION=3.3.2
CONSTRAINTS="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PY}.txt"
VERITY="git+https://github.com/Luke458/verity@6526a443f24dd5e4cb178a8f66fe85b9ccb365cc"
only=${1:-all}

want() { [[ $only == all || $only == "$1" ]]; }
if want airflow; then
  uv venv --python "$PY" .venv
  uv pip install --python .venv/bin/python --constraint "$CONSTRAINTS" \
    "apache-airflow[smtp,amazon]==${AIRFLOW_VERSION}" "botocore[crt]" pyarrow pandas pytest ruff aiosmtpd
  uv pip install --python .venv/bin/python --no-deps -e .
fi
if want spark; then
  uv venv --python "$PY" .venv-spark
  uv pip install --python .venv-spark/bin/python "pyspark==4.2.0" "delta-spark==4.4.1" "duckdb==2.0.0" pyarrow \
    "pandas<3" pytest ruff
  uv pip install --python .venv-spark/bin/python --no-deps -e .
fi
if want dbt; then
  uv venv --python "$PY" .venv-dbt
  uv pip install --python .venv-dbt/bin/python "dbt-duckdb==1.11.1" "dbt-snowflake==1.12.5" "dbt-core==1.12.6" "duckdb==2.0.0"
  uv pip install --python .venv-dbt/bin/python --no-deps -e .
fi
# Optional, not in "all": the AWS Glue 6.0 runtime's versions (Python 3.13, Spark 4.1.1, Delta 4.2.0),
# to test the Spark code against Glue before deploying: SPARK_PYTHON=$PWD/.venv-glue/bin/python scripts/ci.sh spark
if [[ $only == glue ]]; then
  uv venv --python 3.13 .venv-glue
  uv pip install --python .venv-glue/bin/python "pyspark==4.1.1" "delta-spark==4.2.0" "duckdb==2.0.0" pyarrow \
    "pandas<3" pytest
  uv pip install --python .venv-glue/bin/python --no-deps -e .
fi
if want qc; then
  uv venv --python "$PY" .venv-qc
  uv pip install --python .venv-qc/bin/python "verity[delta] @ ${VERITY}" "duckdb==2.0.0"
  uv pip install --python .venv-qc/bin/python --no-deps -e .
fi
