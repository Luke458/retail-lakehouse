"""Vendor drop simulator: publishes Favorita as weekly files, one week at a time.

    python -m retail_lakehouse.drops weeks                  # list publishable weeks
    python -m retail_lakehouse.drops reference              # stores, items, holidays
    python -m retail_lakehouse.drops publish 2016-01-05     # one week
    python -m retail_lakehouse.drops publish 2016-01-05 --count 10
    python -m retail_lakehouse.drops publish 2016-03-15 --fault late     # no manifest yet
    python -m retail_lakehouse.drops publish 2016-03-15 --fault corrupt --force

This stands in for the source system, not the pipeline. The pipeline should
only ever see the inbox, the way it would see an SFTP folder or a bucket.

A week runs Wednesday to Tuesday and is named by its Tuesday (the source ends
on Tuesday 2017-08-15). Each publish writes, under ``$LAKE_ROOT/inbox``:

    favorita/sales/week_ending=YYYY-MM-DD/sales.csv.gz
    favorita/transactions/week_ending=YYYY-MM-DD/transactions.csv.gz
    favorita/<feed>/week_ending=YYYY-MM-DD/_MANIFEST.json

The manifest goes last. It carries row counts and sha256s, so a sensor that
waits for it never reads a half-written file. Data files are written to a
temporary name and renamed, so a reader never sees a partial file either.
Publishing a week that is already there is a no-op unless ``--force``.

Source: the Kaggle Favorita parquet files (``$FAVORITA_SOURCE``; default is
retail-qc's copy). Public data only.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.parquet as pq

FIRST_DAY = date(2013, 1, 2)  # first Wednesday; 2013-01-01 is a lone Tuesday
LAST_DAY = date(2017, 8, 15)
REFERENCE = ("stores", "items", "holidays_events")


def source_dir() -> Path:
    default = Path.home() / "Projects/retail-qc/data/real/favorita"
    return Path(os.environ.get("FAVORITA_SOURCE", default))


def lake_root() -> Path:
    return Path(os.environ.get("LAKE_ROOT", Path(__file__).resolve().parents[2] / "data"))


def week_endings() -> list[date]:
    first = FIRST_DAY + timedelta(days=6)
    return [first + timedelta(weeks=i) for i in range((LAST_DAY - first).days // 7 + 1)]


def parse_week(text: str) -> date:
    week = date.fromisoformat(text)
    if week not in set(week_endings()):
        raise SystemExit(f"{text} is not a week ending (a Tuesday from {week_endings()[0]} to {LAST_DAY})")
    return week


def read_week(name: str, week: date) -> pa.Table:
    """Rows of a daily source table whose date falls in the week ending ``week``."""
    start, end = (week - timedelta(days=6)).isoformat(), week.isoformat()
    table = pq.read_table(
        source_dir() / f"{name}.parquet",
        filters=[("date", ">=", start), ("date", "<=", end)],
    )
    keep = [c for c in table.column_names if not c.startswith("__index")]
    return table.select(keep).sort_by([("date", "ascending")])


def write_csv_gz(table: pa.Table, path: Path) -> dict[str, object]:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    # No name or timestamp in the gzip header: the same rows always give the same bytes,
    # so a republish of unchanged data has the same sha256 and lands as a no-op.
    with (
        tmp.open("wb") as raw,
        gzip.GzipFile(filename="", mode="wb", compresslevel=6, fileobj=raw, mtime=0) as out,
    ):
        pcsv.write_csv(table, out)
    tmp.rename(path)
    return {"file": path.name, "rows": table.num_rows, "sha256": sha256(path)}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def write_manifest(folder: Path, feed: str, files: list[dict[str, object]], **extra: object) -> Path:
    manifest = {
        "feed": feed,
        "published_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "files": files,
        **extra,
    }
    path = folder / "_MANIFEST.json"
    tmp = folder / "._MANIFEST.json.tmp"
    tmp.write_text(json.dumps(manifest, indent=1) + "\n")
    tmp.rename(path)
    return path


FAULTS = ("late", "corrupt")


def publish_week(week: date, force: bool = False, fault: str | None = None) -> list[Path]:
    """Publish both feeds for a week; return the manifests written.

    ``fault`` breaks the sales drop on purpose, for testing the pipeline:
    ``late`` writes the data file but no manifest (publish again without the
    fault to finish it); ``corrupt`` writes a manifest whose sha256 does not
    match the file.
    """
    if fault not in (None, *FAULTS):
        raise ValueError(f"unknown fault {fault!r}")
    published = []
    for feed, source in (("sales", "train"), ("transactions", "transactions")):
        folder = lake_root() / "inbox/favorita" / feed / f"week_ending={week.isoformat()}"
        if (folder / "_MANIFEST.json").exists() and not force:
            continue
        table = read_week(source, week)
        files = [write_csv_gz(table, folder / f"{feed}.csv.gz")]
        if feed == "sales" and fault == "late":
            continue
        if feed == "sales" and fault == "corrupt":
            files[0]["sha256"] = "0" * 64
        start = (week - timedelta(days=6)).isoformat()
        published.append(write_manifest(folder, feed, files, week_start=start, week_ending=week.isoformat()))
    return published


def publish_reference(force: bool = False) -> Path | None:
    folder = lake_root() / "inbox/favorita/reference"
    if (folder / "_MANIFEST.json").exists() and not force:
        return None
    files = []
    for name in REFERENCE:
        table = pq.read_table(source_dir() / f"{name}.parquet")
        keep = [c for c in table.column_names if not c.startswith("__index")]
        files.append(write_csv_gz(table.select(keep), folder / f"{name}.csv.gz"))
    return write_manifest(folder, "reference", files)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("weeks", help="list publishable week endings")
    ref = sub.add_parser("reference", help="publish stores, items and holidays once")
    ref.add_argument("--force", action="store_true")
    pub = sub.add_parser("publish", help="publish one or more weeks")
    pub.add_argument("week", type=parse_week, help="week ending (a Tuesday)")
    pub.add_argument("--count", type=int, default=1, help="publish this many consecutive weeks")
    pub.add_argument("--force", action="store_true", help="republish weeks already in the inbox")
    pub.add_argument("--fault", choices=FAULTS, help="break the sales drop on purpose")
    args = parser.parse_args(argv)

    if args.command == "weeks":
        weeks = week_endings()
        print(f"{len(weeks)} weeks: {weeks[0]} .. {weeks[-1]}")
    elif args.command == "reference":
        path = publish_reference(args.force)
        print(path or "reference already published (use --force)")
    else:
        for i in range(args.count):
            week = args.week + timedelta(weeks=i)
            if week > LAST_DAY:
                break
            paths = publish_week(week, args.force, args.fault)
            print(f"{week}: " + (", ".join(str(p.parent.parent.name) for p in paths) or "already published"))


if __name__ == "__main__":
    main()
