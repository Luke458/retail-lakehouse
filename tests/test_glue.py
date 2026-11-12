"""The Glue entry point's staging, against a fake S3 (no AWS needed)."""

from datetime import date
from pathlib import Path

import pytest

from retail_lakehouse import glue
from retail_lakehouse.spark import common

LAKE_URI = "s3://lake-bucket/files"
ARGS = ["--JOB_NAME", "retail-lakehouse-dev-spark", "--TABLE_ROOT", "s3://lake-bucket/tables",
        "--LAKE_URI", LAKE_URI, "--step", "bronze", "--feed", "sales", "--week", "2016-05-24",
        "--run-id", "glue-test"]  # fmt: skip


class FakeS3:
    """Just enough of boto3's S3 client: a dict of key -> bytes."""

    def __init__(self, objects: dict[str, bytes]):
        self.objects = dict(objects)

    def get_paginator(self, _name):
        fake = self

        class Paginator:
            def paginate(self, Bucket, Prefix):
                assert Bucket == "lake-bucket"
                yield {"Contents": [{"Key": k} for k in sorted(fake.objects) if k.startswith(Prefix)]}

        return Paginator()

    def download_file(self, bucket, key, filename):
        Path(filename).write_bytes(self.objects[key])

    def upload_file(self, filename, bucket, key):
        self.objects[key] = Path(filename).read_bytes()


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for name in ("RETAIL_LAKEHOUSE_RUNTIME", "LAKE_ROOT", "LAKE_URI", "TABLE_ROOT"):
        monkeypatch.delenv(name, raising=False)


def test_split_uri():
    assert glue.split_uri("s3://lake-bucket/files") == ("lake-bucket", "files")
    assert glue.split_uri("s3://lake-bucket") == ("lake-bucket", "")
    with pytest.raises(ValueError):
        glue.split_uri("/tmp/lake")


def test_each_step_stages_only_what_it_reads_with_python():
    assert glue.inputs("bronze", "sales", "2016-05-24") == ["raw/favorita/sales/week_ending=2016-05-24/"]
    assert glue.inputs("reconcile", "transactions", "2016-05-24") == [
        "raw/favorita/transactions/week_ending=2016-05-24/"
    ]
    assert glue.inputs("reference", None, None) == ["raw/favorita/reference/"]
    assert glue.inputs("silver", "sales", "2016-05-24") == []


def test_run_stages_the_drop_runs_the_step_and_uploads_reports(tmp_path):
    drop = "files/raw/favorita/sales/week_ending=2016-05-24/"
    s3 = FakeS3({drop + "sales.csv.gz": b"gz", drop + "_INGEST.json": b"{}",
                 "files/raw/favorita/sales/week_ending=2016-05-17/sales.csv.gz": b"other week"})  # fmt: skip
    seen = {}

    def step(argv):
        seen["argv"] = argv
        seen["env"] = {k: glue.os.environ[k] for k in ("RETAIL_LAKEHOUSE_RUNTIME", "LAKE_ROOT", "TABLE_ROOT")}
        seen["staged"] = sorted(
            p.relative_to(tmp_path).as_posix() for p in tmp_path.rglob("*") if p.is_file()
        )
        seen["spark_reads"] = common.spark_uri(common.raw_file("sales", date(2016, 5, 24)))
        out = tmp_path / "reports/reconcile/sales/week_ending=2016-05-24/reconcile.json"
        out.parent.mkdir(parents=True)
        out.write_text('{"passed": true}')

    glue.run(ARGS, s3=s3, local=tmp_path, step_main=step)

    assert seen["argv"] == ["bronze", "--feed", "sales", "--week", "2016-05-24", "--run-id", "glue-test"]
    assert seen["env"] == {"RETAIL_LAKEHOUSE_RUNTIME": "glue", "LAKE_ROOT": str(tmp_path),
                           "TABLE_ROOT": "s3://lake-bucket/tables"}  # fmt: skip
    # Only this week's drop is staged, not other weeks.
    assert seen["staged"] == ["raw/favorita/sales/week_ending=2016-05-24/_INGEST.json",
                              "raw/favorita/sales/week_ending=2016-05-24/sales.csv.gz"]  # fmt: skip
    # Spark reads the raw file from S3, not from the driver's local copy.
    assert (
        seen["spark_reads"] == "s3://lake-bucket/files/raw/favorita/sales/week_ending=2016-05-24/sales.csv.gz"
    )
    assert (
        s3.objects["files/reports/reconcile/sales/week_ending=2016-05-24/reconcile.json"]
        == b'{"passed": true}'
    )


def test_reports_are_uploaded_even_when_the_step_fails(tmp_path):
    s3 = FakeS3({})

    def failing(argv):
        out = tmp_path / "reports/reconcile/sales/week_ending=2016-05-24/reconcile.json"
        out.parent.mkdir(parents=True)
        out.write_text('{"passed": false}')
        raise RuntimeError("reconcile mismatch")

    with pytest.raises(RuntimeError):
        glue.run(ARGS, s3=s3, local=tmp_path, step_main=failing)
    assert "files/reports/reconcile/sales/week_ending=2016-05-24/reconcile.json" in s3.objects


def test_spark_uri_is_the_plain_path_off_glue(tmp_path, monkeypatch):
    monkeypatch.setenv("LAKE_ROOT", str(tmp_path))
    path = tmp_path / "raw/favorita/sales/week_ending=2016-05-24/sales.csv.gz"
    assert common.spark_uri(path) == str(path)
    assert not common.on_managed_spark()
