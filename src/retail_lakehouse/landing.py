"""Inbox to raw zone: check a vendor drop against its manifest and land it unchanged.

The raw zone is an exact, immutable copy of what the vendor sent, plus one
``_INGEST.json`` per drop saying when it was landed, by which run, and from
which source checksums. Everything downstream reads raw, never the inbox.

Landing is idempotent. If raw already holds the same bytes (same sha256s), a
rerun changes nothing, not even the metadata. If the vendor republished a
different file for the same week, the drop is replaced as a whole, and the
new ``_INGEST.json`` records the checksums it replaced.
"""

from __future__ import annotations

import gzip
import json
import os
import shutil
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from retail_lakehouse.drops import sha256

WEEKLY_FEEDS = ("sales", "transactions")
MANIFEST = "_MANIFEST.json"
INGEST = "_INGEST.json"


class DropError(ValueError):
    """The drop does not match its manifest. Retrying will not fix the file itself."""


def lake_root() -> Path:
    return Path(os.environ.get("LAKE_ROOT", Path(__file__).resolve().parents[2] / "data"))


def week_ending(data_interval_end: datetime) -> date:
    """The Tuesday that closes a Wednesday-to-Wednesday data interval."""
    week = data_interval_end.date() - timedelta(days=1)
    if week.weekday() != 1:
        raise ValueError(f"interval ending {data_interval_end} does not close a Wednesday-Tuesday week")
    return week


def drop_path(zone: str, feed: str, week: date | None = None) -> Path:
    path = lake_root() / zone / "favorita" / feed
    return path / f"week_ending={week.isoformat()}" if week else path


def read_manifest(folder: Path) -> dict[str, Any]:
    return json.loads((folder / MANIFEST).read_text())


def count_csv_rows(path: Path) -> int:
    with gzip.open(path, "rb") as handle:
        return sum(block.count(b"\n") for block in iter(lambda: handle.read(1 << 20), b"")) - 1


def verify(folder: Path) -> dict[str, Any]:
    """Check every file in a drop against its manifest; return the manifest."""
    manifest = read_manifest(folder)
    problems = []
    for entry in manifest["files"]:
        path = folder / entry["file"]
        if not path.exists():
            problems.append(f"{entry['file']}: missing")
            continue
        if (actual := sha256(path)) != entry["sha256"]:
            problems.append(f"{entry['file']}: sha256 {actual[:12]} != manifest {entry['sha256'][:12]}")
        elif (rows := count_csv_rows(path)) != entry["rows"]:
            problems.append(f"{entry['file']}: {rows} rows != manifest {entry['rows']}")
    if problems:
        raise DropError(f"{folder}: " + "; ".join(problems))
    return manifest


def land(source: Path, target: Path, run_id: str) -> dict[str, Any]:
    """Copy a verified drop to the raw zone. Returns the ``_INGEST.json`` record."""
    manifest = read_manifest(source)
    checksums = {entry["file"]: entry["sha256"] for entry in manifest["files"]}
    if (target / INGEST).exists():
        current = json.loads((target / INGEST).read_text())
        if current["source_sha256"] == checksums:
            return {**current, "action": "unchanged"}
    else:
        current = None

    staging = target.with_name(f".{target.name}.staging")
    shutil.rmtree(staging, ignore_errors=True)
    staging.mkdir(parents=True)
    for name in [*checksums, MANIFEST]:
        shutil.copy2(source / name, staging / name)
    record = {
        "run_id": run_id,
        "landed_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "source": str(source),
        "source_sha256": checksums,
        "rows": {entry["file"]: entry["rows"] for entry in manifest["files"]},
        "replaces": current["source_sha256"] if current else None,
    }
    (staging / INGEST).write_text(json.dumps(record, indent=1) + "\n")

    # Swap the whole drop in; a reader sees the old drop or the new one, never a mix.
    old = target.with_name(f".{target.name}.old")
    shutil.rmtree(old, ignore_errors=True)
    if target.exists():
        target.rename(old)
    staging.rename(target)
    shutil.rmtree(old, ignore_errors=True)
    return {**record, "action": "replaced" if current else "landed"}
