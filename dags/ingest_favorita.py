"""Weekly Favorita drops: wait for the vendor's manifest, verify, land to raw.

One run per Wednesday-Tuesday week. The data interval is Wednesday 00:00 to
the next Wednesday 00:00 UTC, so the week a run owns is explicit, not
inferred from when it happened to run. The schedule ends with the source data
(2017-08-15); every run is a backfill or a manual trigger. This replays
history, it does not pretend to be live.

Per feed (dynamically mapped, so a new feed is one entry in ``WEEKLY_FEEDS``):

    wait_for_manifest -> verify -> land

and ``publish`` marks the raw assets updated once every feed has landed.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.providers.smtp.notifications.smtp import SmtpNotifier
from airflow.sdk import Asset, PokeReturnValue, dag, task, task_group
from airflow.timetables.interval import CronDataIntervalTimetable

from retail_lakehouse import landing

RAW_ASSETS = [Asset(f"file://{landing.drop_path('raw', feed)}") for feed in landing.WEEKLY_FEEDS]

alert = SmtpNotifier(
    to="data-oncall@localhost",
    subject="[retail-lakehouse] {{ ti.dag_id }}.{{ ti.task_id }} failed for {{ ds }}",
    html_content=(
        "<p>{{ ti.dag_id }}.{{ ti.task_id }} (map index {{ ti.map_index }}) failed after "
        "{{ ti.try_number }} tries.</p><p>Run {{ run_id }}, interval {{ data_interval_start }} to "
        "{{ data_interval_end }}.</p><p>Log: {{ ti.log_url }}</p>"
    ),
)


@dag(
    schedule=CronDataIntervalTimetable("0 0 * * 3", timezone="UTC"),
    start_date=datetime(2013, 1, 2),
    end_date=datetime(2017, 8, 16),
    catchup=False,
    max_active_runs=4,
    default_args={
        "owner": "data-platform",
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        "retry_exponential_backoff": True,
        "on_failure_callback": alert,
    },
    tags=["favorita", "raw"],
    doc_md=__doc__,
)
def ingest_favorita():
    @task_group
    def ingest(feed: str):
        @task.sensor(poke_interval=30, timeout=timedelta(hours=6), mode="reschedule")
        def wait_for_manifest(feed: str, data_interval_end=None) -> PokeReturnValue:
            folder = landing.drop_path("inbox", feed, landing.week_ending(data_interval_end))
            return PokeReturnValue(is_done=(folder / landing.MANIFEST).exists(), xcom_value=str(folder))

        @task
        def verify(folder: str) -> str:
            from pathlib import Path

            manifest = landing.verify(Path(folder))
            print(f"verified {[(f['file'], f['rows']) for f in manifest['files']]}")
            return folder

        @task
        def land(folder: str, feed: str, run_id=None, data_interval_end=None) -> dict:
            from pathlib import Path

            target = landing.drop_path("raw", feed, landing.week_ending(data_interval_end))
            record = landing.land(Path(folder), target, run_id)
            print(f"{record['action']}: {target}")
            return {"feed": feed, "action": record["action"], "rows": record["rows"]}

        return land(verify(wait_for_manifest(feed)), feed)

    @task(outlets=RAW_ASSETS)
    def publish(results: list[dict]) -> None:
        for result in results:
            print(f"{result['feed']}: {result['action']} {result['rows']}")

    publish(ingest.expand(feed=list(landing.WEEKLY_FEEDS)))


ingest_favorita()
