"""Weekly raw to bronze to silver on Delta, reconciled against an independent engine.

Same Wednesday-Tuesday intervals as ``ingest_favorita``. Per feed (mapped):

    wait_for_raw -> bronze -> silver -> reconcile

- ``wait_for_raw`` waits for the week's ``_INGEST.json`` in the raw zone.
  The dependency is on the data, not on the other DAG's task state, so
  either DAG can be rerun or backfilled on its own.
- The Spark jobs run as subprocesses in the Spark venv (``$SPARK_PYTHON``),
  one JVM per task, the way they will run as Databricks jobs later.
- Concurrency is set on the tasks, not left to whoever starts a backfill
  (``airflow backfill create`` has its own ``--max-active-runs``, default 10,
  which overrides the DAG's). Every Spark task takes a slot in the ``spark``
  pool (2 slots, one JVM each). ``bronze`` and ``silver`` run one task at a
  time across all runs (``max_active_tis_per_dag=1``), because weeks write
  the same Delta tables and concurrent writes to one table conflict.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.providers.smtp.notifications.smtp import SmtpNotifier
from airflow.sdk import Asset, PokeReturnValue, dag, task, task_group
from airflow.timetables.interval import CronDataIntervalTimetable

from retail_lakehouse import landing

SILVER_ASSETS = [
    Asset(f"file://{landing.lake_root() / 'silver/favorita' / feed}") for feed in landing.WEEKLY_FEEDS
]


def spark_job(job: str, feed: str, week: str, run_id: str | None = None) -> str:
    run = f" --run-id '{run_id}'" if run_id else ""
    # Spark's own logging goes to stderr; the last stdout line is the job's JSON result.
    return f'"$SPARK_PYTHON" -m retail_lakehouse.spark {job} --feed {feed} --week {week}{run}'


@dag(
    schedule=CronDataIntervalTimetable("0 0 * * 3", timezone="UTC"),
    start_date=datetime(2013, 1, 2),
    end_date=datetime(2017, 8, 16),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "on_failure_callback": SmtpNotifier(
            to="data-oncall@localhost",
            subject="[retail-lakehouse] {{ ti.dag_id }}.{{ ti.task_id }} failed for {{ ds }}",
            html_content=(
                "<p>{{ ti.dag_id }}.{{ ti.task_id }} (map index {{ ti.map_index }}) failed after "
                "{{ ti.try_number }} tries.</p><p>Run {{ run_id }}.</p><p>Log: {{ ti.log_url }}</p>"
            ),
        ),
    },
    tags=["favorita", "bronze", "silver"],
    doc_md=__doc__,
)
def transform_favorita():
    @task
    def week(data_interval_end=None) -> str:
        return landing.week_ending(data_interval_end).isoformat()

    @task_group
    def transform(feed: str, week: str):
        @task.sensor(poke_interval=60, timeout=timedelta(hours=12), mode="reschedule")
        def wait_for_raw(feed: str, week: str) -> PokeReturnValue:
            from datetime import date

            folder = landing.drop_path("raw", feed, date.fromisoformat(week))
            return PokeReturnValue(is_done=(folder / landing.INGEST).exists())

        @task.bash(pool="spark", max_active_tis_per_dag=1)
        def bronze(feed: str, week: str, run_id=None) -> str:
            return spark_job("bronze", feed, week, run_id)

        @task.bash(pool="spark", max_active_tis_per_dag=1)
        def silver(feed: str, week: str, run_id=None) -> str:
            return spark_job("silver", feed, week, run_id)

        @task.bash(pool="spark", retries=0, doc_md="No retry: a mismatch is a finding, not a flake.")
        def reconcile(feed: str, week: str) -> str:
            return spark_job("reconcile", feed, week)

        wait_for_raw(feed, week) >> bronze(feed, week) >> silver(feed, week) >> reconcile(feed, week)

    @task(outlets=SILVER_ASSETS)
    def publish(week: str) -> str:
        return week

    this_week = week()
    transform.partial(week=this_week).expand(feed=list(landing.WEEKLY_FEEDS)) >> publish(this_week)


transform_favorita()
