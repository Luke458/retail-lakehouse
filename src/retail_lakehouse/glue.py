"""Run one Spark step on AWS Glue (``infra/aws/glue.tf``).

Glue has no mount for the lake's files side, as Databricks had a Unity Catalog
volume. So before the step runs, the plain files it reads with Python (the
week's raw drop and its ingest manifest, or the reference drop) are copied
from ``$LAKE_URI`` in S3 to the driver's local disk, which becomes
``LAKE_ROOT``. Spark itself reads the raw CSV from S3 (``spark_uri`` in
``spark/common.py``), because Glue's executors cannot see the driver's disk.
After the step, whatever it wrote under ``reports/`` is uploaded back,
including when the step fails, so a failed reconcile still leaves its report.

The Delta tables need none of this: ``TABLE_ROOT`` is an ``s3://`` URI.

Glue calls it with the job's arguments, for example
``--step bronze --feed sales --week 2016-05-24 --run-id manual``, plus the
job's defaults (``--TABLE_ROOT``, ``--LAKE_URI``) and Glue's own
(``--JOB_NAME`` and others), which are ignored.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

LOCAL_ROOT = Path("/tmp/lake")
RUNTIME = "glue"
OUTPUTS = ("reports",)


def split_uri(uri: str) -> tuple[str, str]:
    """``s3://bucket/some/prefix`` -> (``bucket``, ``some/prefix``)."""
    if not uri.startswith(("s3://", "s3a://")):
        raise ValueError(f"not an S3 URI: {uri}")
    bucket, _, prefix = uri.split("://", 1)[1].partition("/")
    return bucket, prefix.strip("/")


def inputs(step: str, feed: str | None, week: str | None) -> list[str]:
    """Prefixes under the files side that a step reads with plain Python."""
    if step in ("bronze", "reconcile"):
        return [f"raw/favorita/{feed}/week_ending={week}/"]
    if step == "reference":
        return ["raw/favorita/reference/"]
    return []  # silver reads only Delta tables


def download(s3, bucket: str, root: str, rel: str, local: Path) -> int:
    """Copy every object under ``root/rel`` to ``local/rel``. Returns the number of files."""
    n = 0
    prefix = f"{root}/{rel}" if root else rel
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue
            target = local / key[len(root) :].lstrip("/") if root else local / key
            target.parent.mkdir(parents=True, exist_ok=True)
            s3.download_file(bucket, key, str(target))
            n += 1
    return n


def upload(s3, bucket: str, root: str, local: Path, rel: str) -> int:
    """Copy every file under ``local/rel`` to ``root/rel`` in the bucket."""
    n = 0
    base = local / rel
    if not base.exists():
        return 0
    for path in sorted(p for p in base.rglob("*") if p.is_file()):
        key = "/".join(filter(None, [root, path.relative_to(local).as_posix()]))
        s3.upload_file(str(path), bucket, key)
        n += 1
    return n


def parse(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(add_help=False)
    p.add_argument("--step", required=True, choices=["bronze", "silver", "reconcile", "reference"])
    p.add_argument("--feed")
    p.add_argument("--week")
    p.add_argument("--run-id", default="manual")
    p.add_argument("--TABLE_ROOT", required=True)
    p.add_argument("--LAKE_URI", required=True)
    args, _glue_args = p.parse_known_args(argv)
    return args


def spark_argv(args: argparse.Namespace) -> list[str]:
    out = [args.step]
    if args.feed:
        out += ["--feed", args.feed]
    if args.week:
        out += ["--week", args.week]
    return out + ["--run-id", args.run_id]


def run(argv: list[str], s3=None, local: Path = LOCAL_ROOT, step_main=None) -> None:
    args = parse(argv)
    os.environ.update(
        RETAIL_LAKEHOUSE_RUNTIME=RUNTIME,
        LAKE_ROOT=str(local),
        LAKE_URI=args.LAKE_URI.rstrip("/"),
        TABLE_ROOT=args.TABLE_ROOT.rstrip("/"),
    )
    if s3 is None:
        import boto3

        s3 = boto3.client("s3")
    if step_main is None:
        from retail_lakehouse.spark.__main__ import main as step_main

    bucket, root = split_uri(args.LAKE_URI)
    staged = sum(download(s3, bucket, root, rel, local) for rel in inputs(args.step, args.feed, args.week))
    print(f"glue: staged {staged} files from {args.LAKE_URI} to {local}", flush=True)
    try:
        step_main(spark_argv(args))
    finally:
        sent = sum(upload(s3, bucket, root, local, rel) for rel in OUTPUTS)
        print(f"glue: uploaded {sent} files to {args.LAKE_URI}", flush=True)
