#!/usr/bin/env bash
# Run Airflow locally (API server on :8088, scheduler, DAG processor, triggerer).
# The admin password is printed on first start and saved in .airflow/.
set -euo pipefail
. "$(dirname "$0")/env.sh"
exec airflow standalone
