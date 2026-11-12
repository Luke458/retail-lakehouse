"""One week through bronze, silver and reconcile on AWS Glue, started by hand.

    scripts/aws.sh airflow dags trigger transform_favorita_aws --conf '{"week": "2016-05-24"}'

Per feed, side by side:

    upload_raw -> bronze -> silver -> reconcile -> check

- ``upload_raw`` copies the week's raw drop (CSV and ``_INGEST.json``) from
  the local lake to the files side in S3.
- ``bronze``, ``silver`` and ``reconcile`` start the one Glue job
  (``infra/aws/glue.tf``) with ``--step``, and wait for it. The steps, their
  order and their retries are ``transform_favorita``'s; only where the Spark
  runs changes.
- ``check`` reads the reconcile report the Glue run wrote to S3 and fails the
  run if any check failed.

The bucket and the job come from ``scripts/aws.sh`` (OpenTofu's outputs), and
the AWS credentials from the ``aws_retail_lakehouse`` connection, which uses a
named profile: nothing account-specific is in the repo. Not scheduled: the
AWS run is a one-week proof, applied and destroyed the same day.
"""

from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.smtp.notifications.smtp import SmtpNotifier
from airflow.sdk import Param, TaskGroup, dag, task

from retail_lakehouse import landing

CONN = "aws_retail_lakehouse"
REGION = os.environ.get("AWS_REGION", "ap-southeast-2")
GLUE_JOB = os.environ.get("RETAIL_LAKEHOUSE_GLUE_JOB", "retail-lakehouse-dev-spark")
FILES = "files"  # the files side's prefix in the lake bucket (LAKE_URI)


def lake_bucket() -> str:
    return os.environ["RETAIL_LAKEHOUSE_LAKE_BUCKET"]


def glue_step(step: str, feed: str, **kwargs) -> GlueJobOperator:
    return GlueJobOperator(
        task_id=step,
        job_name=GLUE_JOB,
        script_args={
            "--step": step,
            "--feed": feed,
            "--week": "{{ params.week }}",
            "--run-id": "{{ run_id }}",
        },
        aws_conn_id=CONN,
        region_name=REGION,
        wait_for_completion=True,
        stop_job_run_on_kill=True,
        **kwargs,
    )


@task
def upload_raw(feed: str, params=None) -> int:
    folder = landing.drop_path("raw", feed, date.fromisoformat(params["week"]))
    files = sorted(p for p in folder.iterdir() if p.is_file())
    if not any(p.name == landing.INGEST for p in files):
        raise FileNotFoundError(f"{folder}: no {landing.INGEST}, the week has not been ingested")
    hook = S3Hook(aws_conn_id=CONN)
    for path in files:
        key = f"{FILES}/{path.relative_to(landing.lake_root()).as_posix()}"
        hook.load_file(str(path), key=key, bucket_name=lake_bucket(), replace=True)
    return len(files)


@task(retries=0, doc_md="No retry: a failed check is a finding, not a flake.")
def check(feed: str, params=None) -> dict:
    key = f"{FILES}/reports/reconcile/{feed}/week_ending={params['week']}/reconcile.json"
    report = json.loads(S3Hook(aws_conn_id=CONN).read_key(key, bucket_name=lake_bucket()))
    failed = [name for name, ok in report["checks"].items() if not ok]
    if failed:
        raise ValueError(f"{feed} {params['week']}: reconcile failed on Glue: {failed}")
    return report


@dag(
    schedule=None,
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    params={"week": Param("2016-05-24", type="string", pattern=r"^\d{4}-\d{2}-\d{2}$")},
    default_args={
        "owner": "data-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "on_failure_callback": SmtpNotifier(
            to="data-oncall@localhost",
            subject="[retail-lakehouse] {{ ti.dag_id }}.{{ ti.task_id }} failed for {{ params.week }}",
            html_content=(
                "<p>{{ ti.dag_id }}.{{ ti.task_id }} failed after {{ ti.try_number }} tries.</p>"
                "<p>Run {{ run_id }}.</p><p>Log: {{ ti.log_url }}</p>"
            ),
        ),
    },
    tags=["favorita", "aws", "glue"],
    doc_md=__doc__,
)
def transform_favorita_aws():
    for feed in landing.WEEKLY_FEEDS:
        with TaskGroup(feed):
            reconcile = glue_step(
                "reconcile", feed, retries=0, doc_md="No retry: a mismatch is a finding, not a flake."
            )
            upload_raw(feed) >> glue_step("bronze", feed) >> glue_step("silver", feed) >> reconcile
            reconcile >> check(feed)


transform_favorita_aws()
