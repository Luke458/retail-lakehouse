#!/usr/bin/env bash
# End to end on a short history, through the real DAGs. Airflow must be
# running (scripts/airflow.sh) and the source data present (scripts/get-data.sh).
#
#   scripts/demo.sh
#
# 1. Publish the reference drop and 30 weekly drops (weeks ending 2015-08-25
#    .. 2016-03-15) into the inbox.
# 2. ingest_reference, then ingest_favorita and transform_favorita for those
#    weeks (backfills).
# 3. Gate bootstrap: the first 26 weeks become pre-gate history (verity
#    needs 26 weeks), and gold is built from them.
# 4. publish_favorita for the last 4 weeks: verity gates each one, and gold
#    is built for the approved ones; a blocked week sends an alert to
#    data/mail/new/.
#
# About 40 minutes, mostly Spark (about a minute per week). Rerunnable.
set -euo pipefail
. "$(dirname "$0")/env.sh"
cd "$REPO"
first=2015-08-25 history_end=2016-02-16 last=2016-03-15
logical() { date -u -d "$1 - 6 days" +%F; } # the Wednesday that opens a week's interval
gated_first=$(date -u -d "$history_end + 7 days" +%F)

echo "== 1. drops"
python -m retail_lakehouse.drops reference
python -m retail_lakehouse.drops publish "$first" --count 30

for dag in ingest_reference ingest_favorita transform_favorita publish_favorita; do
  airflow dags unpause "$dag" >/dev/null 2>&1
done

echo "== 2. ingest and transform"
ref_state() { airflow dags list-runs ingest_reference -o plain 2>/dev/null | awk '$1 == "ingest_reference" {print $3}' | head -1; }
if [[ "$(ref_state)" != success ]]; then
  airflow dags trigger ingest_reference >/dev/null 2>&1
  until s=$(ref_state); [[ $s == success || $s == failed ]]; do sleep 10; done
  [[ $s == success ]] || { echo "ingest_reference failed" >&2; exit 1; }
fi
echo "ingest_reference: success"
scripts/backfill.sh ingest_favorita "$(logical "$first")" "$(logical "$last")" 13
scripts/backfill.sh transform_favorita "$(logical "$first")" "$(logical "$last")" 8

echo "== 3. gate history and gold"
"$QC_PYTHON" -m retail_lakehouse.gate bootstrap --through "$history_end" --run-id demo
"$DBT" build --full-refresh --quiet

echo "== 4. gate and publish the last 4 weeks"
scripts/backfill.sh publish_favorita "$(logical "$gated_first")" "$(logical "$last")" 4

"$QC_PYTHON" - "$gated_first" "$last" <<'PY'
import sys
from datetime import date, timedelta

import duckdb

from retail_lakehouse.gate import latest_decision
from retail_lakehouse.landing import lake_root

week, last = (date.fromisoformat(a) for a in sys.argv[1:])
gold = duckdb.connect(str(lake_root() / "gold/favorita.duckdb"), read_only=True)
served = {w for (w,) in gold.sql("select distinct week_ending from fct_sales_store_family_week").fetchall()}
print(f"\n{'week ending':12} {'verity':24} gold")
while week <= last:
    d = latest_decision(week)
    print(f"{week!s:12} {d['status']:24} {'served' if week in served else 'not served (quarantined)'}")
    week += timedelta(weeks=1)
print(f"\ngold: {len(served)} weeks. Reports: data/reports/gate/; alerts: data/mail/new/")
PY
