#!/usr/bin/env bash
# Stop the local Airflow processes started by scripts/airflow.sh:
# SIGTERM, then SIGKILL whatever is left after 30 seconds.
REPO="$(cd "$(dirname "$0")/.." && pwd)"
find_pids() { pgrep -f "$REPO/.venv/bin/airflow|^airflow (api_server|serve-logs|worker)"; }
pids=$(find_pids)
[ -z "$pids" ] && { echo "airflow not running"; exit 0; }
kill $pids 2>/dev/null
for _ in $(seq 1 30); do
  [ -z "$(find_pids)" ] && { echo stopped; exit 0; }
  sleep 1
done
pids=$(find_pids)
[ -n "$pids" ] && kill -9 $pids 2>/dev/null
sleep 1
[ -z "$(find_pids)" ] && echo "stopped (forced)" || { echo "still running: $(find_pids | tr '\n' ' ')" >&2; exit 1; }
