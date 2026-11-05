#!/usr/bin/env bash
# Backfill a weekly DAG over a long range in chunks, waiting for each to finish.
#
#   scripts/backfill.sh <dag_id> <from Wednesday> <to Wednesday> [weeks per chunk]
#
# Why chunks: one big `airflow backfill create` inserts every run in a single
# transaction, which on the SQLite metadata DB collides with the scheduler
# ("database is locked"). Airflow also allows one active backfill per DAG, so
# chunks run one after another. Rerunnable: chunks whose runs all succeeded are
# skipped; a chunk with failed runs stops the script.
set -euo pipefail
. "$(dirname "$0")/env.sh"
dag=$1 from=$2 to=$3 chunk=${4:-8}

states() {  # run states for logical dates in [$1, $2]
  airflow dags list-runs "$dag" --start-date "$1" --end-date "$2T23:59:59" -o plain 2>/dev/null |
    awk -v d="$dag" '$1 == d {print $3}'
}

start=$from
while [[ ! "$start" > "$to" ]]; do
  end=$(date -u -d "$start + $(( (chunk - 1) * 7 )) days" +%F)
  [[ "$end" > "$to" ]] && end=$to
  # UTC: in local time a DST change makes a week an hour short and drops one
  expected=$(( ( $(date -u -d "$end" +%s) - $(date -u -d "$start" +%s) ) / 604800 + 1 ))
  done_runs=$(states "$start" "$end" | grep -c '^success$' || true)
  if (( done_runs >= expected )); then
    echo "$(date +%T) $dag $start..$end: already done"
  else
    # Refusals are transient: the SQLite lock, or the previous chunk's backfill
    # not yet marked complete (the scheduler does that a minute or two after its
    # last run). Retry for up to 10 minutes.
    for attempt in $(seq 1 30); do
      if airflow backfill create --dag-id "$dag" --from-date "$start" --to-date "$end" \
           --reprocess-behavior failed >/dev/null 2>&1; then break; fi
      # Every run in the chunk already exists (an earlier attempt created it): wait on those.
      (( $(states "$start" "$end" | grep -vc '^failed$' || true) >= expected )) && break
      (( attempt == 30 )) && { echo "$dag $start..$end: could not create backfill" >&2; exit 1; }
      sleep 20
    done
    while :; do
      s=$(states "$start" "$end")
      if grep -q '^failed$' <<<"$s"; then
        echo "$(date +%T) $dag $start..$end: failed runs, stopping" >&2; exit 1
      fi
      (( $(grep -c '^success$' <<<"$s" || true) >= expected )) && break
      sleep 30
    done
    echo "$(date +%T) $dag $start..$end: $expected runs succeeded"
  fi
  start=$(date -u -d "$end + 7 days" +%F)
done
echo "$(date +%T) $dag $from..$to: done"
