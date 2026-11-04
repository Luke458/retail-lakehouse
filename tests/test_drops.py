import gzip
import json
from datetime import date

import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.parquet as pq
import pytest

from retail_lakehouse import drops


@pytest.fixture
def lake(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    days = [f"2016-01-{d:02d}" for d in range(1, 13)]  # Fri 1 Jan .. Tue 12 Jan
    pq.write_table(
        pa.table(
            {
                "id": list(range(len(days))),
                "date": days,
                "store_nbr": [1] * len(days),
                "item_nbr": [7] * len(days),
                "unit_sales": [1.5] * len(days),
                "onpromotion": [False] * len(days),
            }
        ),
        source / "train.parquet",
    )
    pq.write_table(
        pa.table({"date": days, "store_nbr": [1] * len(days), "transactions": [10] * len(days)}),
        source / "transactions.parquet",
    )
    for name in drops.REFERENCE:
        pq.write_table(pa.table({"key": [1, 2]}), source / f"{name}.parquet")
    monkeypatch.setenv("FAVORITA_SOURCE", str(source))
    monkeypatch.setenv("LAKE_ROOT", str(tmp_path / "lake"))
    return tmp_path / "lake"


def read_drop(folder):
    manifest = json.loads((folder / "_MANIFEST.json").read_text())
    with gzip.open(folder / manifest["files"][0]["file"]) as handle:
        return manifest, pcsv.read_csv(handle)


def test_weeks_run_wednesday_to_tuesday_and_end_on_the_last_source_day():
    weeks = drops.week_endings()
    assert {w.weekday() for w in weeks} == {1}
    assert weeks[0] == date(2013, 1, 8) and weeks[-1] == drops.LAST_DAY


def test_a_week_holds_exactly_its_seven_days_and_the_manifest_matches(lake):
    drops.publish_week(date(2016, 1, 12))
    folder = lake / "inbox/favorita/sales/week_ending=2016-01-12"
    manifest, table = read_drop(folder)
    assert table["date"].to_pylist() == [date(2016, 1, d) for d in range(6, 13)]
    assert manifest["files"][0]["rows"] == table.num_rows == 7
    assert manifest["files"][0]["sha256"] == drops.sha256(folder / "sales.csv.gz")
    assert (manifest["week_start"], manifest["week_ending"]) == ("2016-01-06", "2016-01-12")
    assert not list(folder.glob(".*"))  # no temporary files left behind


def test_republishing_is_a_no_op_unless_forced(lake):
    assert len(drops.publish_week(date(2016, 1, 12))) == 2
    assert drops.publish_week(date(2016, 1, 12)) == []
    assert len(drops.publish_week(date(2016, 1, 12), force=True)) == 2


def test_reference_publishes_once(lake):
    assert drops.publish_reference() is not None
    assert drops.publish_reference() is None


def test_only_tuesdays_in_range_parse():
    with pytest.raises(SystemExit):
        drops.parse_week("2016-01-13")
    with pytest.raises(SystemExit):
        drops.parse_week("2017-08-22")


def test_a_late_drop_has_data_but_no_manifest_until_published_again(lake):
    drops.publish_week(date(2016, 1, 12), fault="late")
    folder = lake / "inbox/favorita/sales/week_ending=2016-01-12"
    assert (folder / "sales.csv.gz").exists() and not (folder / "_MANIFEST.json").exists()
    drops.publish_week(date(2016, 1, 12))
    assert (folder / "_MANIFEST.json").exists()


def test_a_corrupt_drop_has_a_checksum_that_does_not_match(lake):
    drops.publish_week(date(2016, 1, 12), fault="corrupt")
    folder = lake / "inbox/favorita/sales/week_ending=2016-01-12"
    manifest, _ = read_drop(folder)
    assert manifest["files"][0]["sha256"] != drops.sha256(folder / "sales.csv.gz")
    other = lake / "inbox/favorita/transactions/week_ending=2016-01-12"
    assert read_drop(other)[0]["files"][0]["sha256"] == drops.sha256(other / "transactions.csv.gz")


def test_republishing_unchanged_data_gives_identical_bytes(lake):
    drops.publish_week(date(2016, 1, 12))
    folder = lake / "inbox/favorita/sales/week_ending=2016-01-12"
    first = drops.sha256(folder / "sales.csv.gz")
    drops.publish_week(date(2016, 1, 12), force=True)
    assert drops.sha256(folder / "sales.csv.gz") == first
