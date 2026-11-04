#!/usr/bin/env bash
# Run Airflow locally (API server on :8088, scheduler, DAG processor, triggerer).
# The admin password is printed on first start and saved in .airflow/.
set -euo pipefail
. "$(dirname "$0")/env.sh"
airflow db migrate >/dev/null
# Each Spark task is one local JVM (4 GB driver); two at a time fits this machine.
airflow pools set spark 2 "local Spark JVMs" >/dev/null
exec airflow standalone
