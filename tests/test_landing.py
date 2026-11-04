import json
from datetime import UTC, date, datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from retail_lakehouse import drops, landing


@pytest.fixture
def lake(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    days = [f"2016-01-{d:02d}" for d in range(6, 13)]
    pq.write_table(
        pa.table(
            {
                "id": list(range(7)),
                "date": days,
                "store_nbr": [1] * 7,
                "item_nbr": [7] * 7,
                "unit_sales": [1.5] * 7,
                "onpromotion": [False] * 7,
            }
        ),
        source / "train.parquet",
    )
    pq.write_table(
        pa.table({"date": days, "store_nbr": [1] * 7, "transactions": [10] * 7}),
        source / "transactions.parquet",
    )
    monkeypatch.setenv("FAVORITA_SOURCE", str(source))
    monkeypatch.setenv("LAKE_ROOT", str(tmp_path / "lake"))
    return tmp_path / "lake"


WEEK = date(2016, 1, 12)


def test_week_ending_is_the_tuesday_before_the_interval_end():
    assert landing.week_ending(datetime(2016, 1, 13, tzinfo=UTC)) == WEEK
    with pytest.raises(ValueError):
        landing.week_ending(datetime(2016, 1, 14, tzinfo=UTC))


def test_verify_counts_rows_and_checks_checksums(lake):
    drops.publish_week(WEEK)
    manifest = landing.verify(landing.drop_path("inbox", "sales", WEEK))
    assert manifest["files"][0]["rows"] == 7


def test_verify_rejects_a_corrupt_drop(lake):
    drops.publish_week(WEEK, fault="corrupt")
    with pytest.raises(landing.DropError, match="sha256"):
        landing.verify(landing.drop_path("inbox", "sales", WEEK))


def test_verify_rejects_a_row_count_mismatch(lake):
    drops.publish_week(WEEK)
    folder = landing.drop_path("inbox", "sales", WEEK)
    manifest = json.loads((folder / landing.MANIFEST).read_text())
    manifest["files"][0]["rows"] = 8
    (folder / landing.MANIFEST).write_text(json.dumps(manifest))
    with pytest.raises(landing.DropError, match="rows"):
        landing.verify(folder)


def test_landing_twice_changes_nothing(lake):
    drops.publish_week(WEEK)
    source, target = landing.drop_path("inbox", "sales", WEEK), landing.drop_path("raw", "sales", WEEK)
    first = landing.land(source, target, "run-1")
    before = {p.name: p.read_bytes() for p in target.iterdir()}
    second = landing.land(source, target, "run-2")
    assert (first["action"], second["action"]) == ("landed", "unchanged")
    assert {p.name: p.read_bytes() for p in target.iterdir()} == before
    assert sorted(before) == ["_INGEST.json", "_MANIFEST.json", "sales.csv.gz"]


def test_a_republished_drop_replaces_raw_and_records_what_it_replaced(lake):
    drops.publish_week(WEEK)
    source, target = landing.drop_path("inbox", "sales", WEEK), landing.drop_path("raw", "sales", WEEK)
    first = landing.land(source, target, "run-1")
    (source / landing.MANIFEST).unlink()
    pq.write_table(
        pa.table(
            {
                "id": [0],
                "date": ["2016-01-06"],
                "store_nbr": [1],
                "item_nbr": [7],
                "unit_sales": [2.0],
                "onpromotion": [True],
            }
        ),
        lake.parent / "source/train.parquet",
    )
    drops.publish_week(WEEK, force=True)
    second = landing.land(source, target, "run-2")
    assert second["action"] == "replaced"
    assert second["replaces"] == first["source_sha256"]
    assert second["rows"] == {"sales.csv.gz": 1}
    assert not list(target.parent.glob(".*"))  # no staging or old copies left
