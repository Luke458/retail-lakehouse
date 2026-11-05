#!/usr/bin/env bash
# Run Airflow locally: API server on :8088, scheduler, DAG processor, triggerer.
# Stop with scripts/stop.sh.
#
# Each component runs in its own restart loop rather than under
# `airflow standalone`, which never restarts a component that dies. On the
# SQLite metadata DB the scheduler can die when a backfill is created while it
# is updating the same runs (StaleDataError); it then restarts in 10 seconds
# and picks the queued runs up again.
#
# The admin password is in .airflow/simple_auth_manager_passwords.json.generated,
# written on the API server's first start.
set -euo pipefail
. "$(dirname "$0")/env.sh"
airflow db migrate >/dev/null
# Each Spark task is one local JVM (4 GB driver); two at a time fits this machine.
airflow pools set spark 2 "local Spark JVMs" >/dev/null

pidfile="$AIRFLOW_HOME/supervisor.pid"
echo $$ >"$pidfile"
trap 'kill $(jobs -p) 2>/dev/null; rm -f "$pidfile"; exit 0' TERM INT

supervise() {  # supervise <label> <airflow args...>
  local name=$1
  shift
  while :; do
    # Not a pipe: a dead scheduler's executor children keep a pipe open, and
    # the loop would wait on them instead of restarting it.
    airflow "$@" > >(sed -u "s/^/$name | /") 2>&1 || true
    echo "$(date -u +%FT%TZ) $name | exited, restarting in 10 s"
    sleep 10
  done
}

supervise scheduler scheduler &
supervise dag-proc dag-processor &
supervise api-server api-server &
supervise triggerer triggerer &
wait
