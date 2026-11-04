"""Favorita reference tables (stores, items, holidays): verify and land to raw.

The vendor sends these once and republishes them only on change, so there is
no schedule: trigger it after a reference drop. Same checks and landing as the
weekly feeds; a republished drop replaces raw as a whole, and the silver
dimensions are rebuilt from it.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.providers.smtp.notifications.smtp import SmtpNotifier
from airflow.sdk import Asset, dag, task

from retail_lakehouse import landing

REFERENCE = Asset(f"file://{landing.drop_path('raw', 'reference')}")
SILVER_REFERENCE = Asset(f"file://{landing.lake_root() / 'silver/favorita/reference'}")


@dag(
    schedule=None,
    start_date=datetime(2013, 1, 1),
    catchup=False,
    default_args={
        "owner": "data-platform",
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        "on_failure_callback": SmtpNotifier(
            to="data-oncall@localhost",
            subject="[retail-lakehouse] {{ ti.dag_id }}.{{ ti.task_id }} failed",
            html_content="<p>Run {{ run_id }}. Log: {{ ti.log_url }}</p>",
        ),
    },
    tags=["favorita", "raw"],
    doc_md=__doc__,
)
def ingest_reference():
    @task
    def verify() -> str:
        folder = landing.drop_path("inbox", "reference")
        landing.verify(folder)
        return str(folder)

    @task(outlets=[REFERENCE])
    def land(folder: str, run_id=None) -> str:
        from pathlib import Path

        record = landing.land(Path(folder), landing.drop_path("raw", "reference"), run_id)
        return record["action"]

    @task.bash(pool="spark", outlets=[SILVER_REFERENCE])
    def silver(run_id=None) -> str:
        # Small dimensions: replaced whole from the latest raw drop (Spark venv, one JVM).
        return f"\"$SPARK_PYTHON\" -m retail_lakehouse.spark reference --run-id '{run_id}'"

    land(verify()) >> silver()


ingest_reference()
