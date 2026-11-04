#!/usr/bin/env bash
# Two venvs, two test runs: Airflow side (DAGs, landing, simulator), then Spark side.
set -euo pipefail
. "$(dirname "$0")/env.sh"
cd "$REPO"
"$REPO/.venv/bin/python" -m pytest -q -m "not spark" "$@"
"$SPARK_PYTHON" -m pytest -q -m spark -p no:cacheprovider "$@"
