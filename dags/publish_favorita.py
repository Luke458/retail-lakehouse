"""Weekly silver to gold, behind the verity gate.

Same Wednesday-Tuesday intervals as the other weekly DAGs:

    week -> wait_for_silver -> gate -> decide -> build_gold
                                              -> quarantine

- ``wait_for_silver`` waits until both feeds' reconcile reports for the week
  exist and pass: silver is complete and verified.
- ``gate`` runs verity (``python -m retail_lakehouse.gate run`` in
  ``.venv-qc``) and records a decision. Its snapshots form a chain (each
  version is compared with the one before), so it runs one week at a time and
  in order (``depends_on_past``).
- ``build_gold`` runs ``dbt build`` for that week only. Gold models read only
  weeks the gate approved, so a quarantined week never leaks in later.
- ``quarantine`` leaves gold as it was and emails the findings. A quarantine
  is a handled outcome, not a failure, so the run succeeds. To release a
  week, record a person's approval with ``python -m retail_lakehouse.gate
  release`` and clear ``decide`` for that week (see the runbook).
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta

from airflow.providers.smtp.notifications.smtp import SmtpNotifier
from airflow.sdk import Asset, PokeReturnValue, dag, task
from airflow.timetables.interval import CronDataIntervalTimetable

from retail_lakehouse import landing

GOLD = Asset(f"file://{landing.lake_root() / 'gold/favorita.duckdb'}")
ONCALL = "data-oncall@localhost"


@dag(
    schedule=CronDataIntervalTimetable("0 0 * * 3", timezone="UTC"),
    start_date=datetime(2013, 1, 2),
    end_date=datetime(2017, 8, 16),
    catchup=False,
    default_args={
        "owner": "data-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "on_failure_callback": SmtpNotifier(
            to=ONCALL,
            subject="[retail-lakehouse] {{ ti.dag_id }}.{{ ti.task_id }} failed for {{ ds }}",
            html_content=(
                "<p>{{ ti.dag_id }}.{{ ti.task_id }} failed after {{ ti.try_number }} tries.</p>"
                "<p>Run {{ run_id }}.</p><p>Log: {{ ti.log_url }}</p>"
            ),
        ),
    },
    tags=["favorita", "gate", "gold"],
    doc_md=__doc__,
)
def publish_favorita():
    @task
    def week(data_interval_end=None) -> str:
        return landing.week_ending(data_interval_end).isoformat()

    @task.sensor(poke_interval=60, timeout=timedelta(hours=12), mode="reschedule")
    def wait_for_silver(week: str) -> PokeReturnValue:
        reports = [
            landing.lake_root() / "reports/reconcile" / feed / f"week_ending={week}" / "reconcile.json"
            for feed in landing.WEEKLY_FEEDS
        ]
        ready = all(p.exists() and all(json.loads(p.read_text())["checks"].values()) for p in reports)
        return PokeReturnValue(is_done=ready)

    @task.bash(depends_on_past=True, max_active_tis_per_dag=1)
    def gate(week: str, run_id=None) -> str:
        return f"\"$QC_PYTHON\" -m retail_lakehouse.gate run --week {week} --run-id '{run_id}'"

    @task.branch
    def decide(week: str) -> str:
        from datetime import date

        from retail_lakehouse.gate import latest_decision

        decision = latest_decision(date.fromisoformat(week))
        print(f"{week}: {decision['status']}, approved={decision['approved']} ({decision['reason']})")
        return "build_gold" if decision["approved"] else "quarantine"

    @task.bash(max_active_tis_per_dag=1, outlets=[GOLD])
    def build_gold(week: str) -> str:
        # DuckDB does not create the folder of a new database file.
        return (
            f'mkdir -p "$LAKE_ROOT/gold" && "$DBT" build --select gold --vars \'{{week_ending: "{week}"}}\''
            ' && "$DBT" source freshness'
        )

    @task
    def quarantine(week: str) -> str:
        from datetime import date

        from airflow.providers.smtp.hooks.smtp import SmtpHook

        from retail_lakehouse.gate import latest_decision

        decision = latest_decision(date.fromisoformat(week))
        findings = "".join(
            f"<li><code>{json.dumps(f)}</code></li>" for f in (decision.get("findings") or [])[:10]
        )
        with SmtpHook(smtp_conn_id="smtp_default") as smtp:
            smtp.send_email_smtp(
                to=ONCALL,
                subject=f"[retail-lakehouse] week ending {week} quarantined: verity {decision['status']}",
                html_content=(
                    f"<p>Gold was not built for the week ending {week}; it keeps serving the last "
                    f"approved data.</p><p>verity status: <b>{decision['status']}</b>, likely cause: "
                    f"{decision.get('likely_cause')}. Report: {decision.get('report')}/report.md</p>"
                    f"<p>Findings for review, largest impact first:</p><ul>{findings}</ul>"
                    f"<p>Release after review: <code>python -m retail_lakehouse.gate release "
                    f"--week {week} --by NAME --reason ...</code>, then clear <code>decide</code>.</p>"
                ),
            )
        return decision["status"]

    this_week = week()
    branch = decide(this_week)
    wait_for_silver(this_week) >> gate(this_week) >> branch
    branch >> [build_gold(this_week), quarantine(this_week)]


publish_favorita()
