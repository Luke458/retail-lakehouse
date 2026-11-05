#!/usr/bin/env bash
# Replay one week through ingest, transform and publish, optionally with a fault.
#
#   scripts/replay.sh <week ending, a Tuesday> [fault]
#
# Republishes the week's drop (forced, so a fault can be added or taken away),
# then runs each DAG for that week in order and waits for it: an existing run
# is cleared, a week never run before gets a one-week backfill. Stops at the
# first DAG whose run fails. A quarantine is a successful publish run; read
# the gate decision for the verdict (python -m retail_lakehouse.gate, or
# data/control/gate/week_ending=<week>/).
set -euo pipefail
. "$(dirname "$0")/env.sh"
week=$1 fault=${2:-}
logical=$(date -u -d "$week - 6 days" +%F) # the Wednesday that opens the week's interval

"$REPO/.venv/bin/python" -m retail_lakehouse.drops publish "$week" --force ${fault:+--fault "$fault"}

state() {
  airflow dags list-runs "$1" --start-date "$logical" --end-date "${logical}T23:59:59" -o plain 2>/dev/null |
    awk -v d="$1" '$1 == d {print $3}'
}

for dag in ingest_favorita transform_favorita publish_favorita; do
  if [[ -n "$(state "$dag")" ]]; then
    airflow tasks clear "$dag" -s "$logical" -e "${logical}T23:59:59" -y >/dev/null 2>&1
  else
    # --reprocess-behavior is required for a DAG with depends_on_past (the gate)
    airflow backfill create --dag-id "$dag" --from-date "$logical" --to-date "$logical" \
      --reprocess-behavior failed >/dev/null 2>&1
  fi
  # A cleared run can read "success" until the scheduler requeues it.
  for _ in $(seq 1 12); do [[ "$(state "$dag")" != success ]] && break; sleep 5; done
  until s=$(state "$dag"); [[ "$s" == success || "$s" == failed ]]; do sleep 10; done
  echo "$(date +%T) $dag $week${fault:+ ($fault)}: $s"
  [[ "$s" == success ]] || exit 1
done
