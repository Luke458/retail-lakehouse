#!/usr/bin/env bash
# Create .venv with Airflow pinned to its constraints file, plus this package.
set -euo pipefail
cd "$(dirname "$0")/.."
AIRFLOW_VERSION=3.3.2
PYTHON_VERSION=3.12
CONSTRAINTS="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
uv venv --python "$PYTHON_VERSION" .venv
uv pip install --python .venv/bin/python --constraint "$CONSTRAINTS" \
  "apache-airflow[smtp]==${AIRFLOW_VERSION}" pyarrow pandas pytest ruff aiosmtpd
uv pip install --python .venv/bin/python --no-deps -e .
