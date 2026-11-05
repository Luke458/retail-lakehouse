"""Bronze, silver and reconcile on a tiny lake, with a real local Spark. Runs in .venv-spark."""

from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from retail_lakehouse import drops, landing  # noqa: E402
from retail_lakehouse.spark import bronze, reconcile, silver  # noqa: E402
from retail_lakehouse.spark.common import SchemaDrift, spark_session, table_path  # noqa: E402

pytestmark = pytest.mark.spark
WEEK = date(2016, 1, 12)
N = 2048  # rows; ids are 0..N-1, so the simulator's id-based faults hit a fixed set


@pytest.fixture(scope="module")
def spark():
    session = spark_session("tests")
    yield session
    session.stop()


@pytest.fixture
def lake(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    days = [f"2016-01-{6 + i % 7:02d}" for i in range(N)]
    pq.write_table(
        pa.table(
            {
                "id": list(range(N)),
                "date": days,
                "store_nbr": [1 + i // 1024 for i in range(N)],
                "item_nbr": [i % 1024 for i in range(N)],  # with store_nbr, unique per row
                "unit_sales": [float(i % 13) - 1.0 for i in range(N)],
                "onpromotion": [None if i % 3 == 0 else i % 2 == 0 for i in range(N)],
            }
        ),
        source / "train.parquet",
    )
    pq.write_table(
        pa.table({"date": days[:7], "store_nbr": [1] * 7, "transactions": list(range(7))}),
        source / "transactions.parquet",
    )
    monkeypatch.setenv("FAVORITA_SOURCE", str(source))
    monkeypatch.setenv("LAKE_ROOT", str(tmp_path / "lake"))
    return tmp_path / "lake"


def deliver(fault=None, force=False):
    """Publish a week and land it in raw, as the ingest DAG would."""
    drops.publish_week(WEEK, force=force, fault=fault)
    for feed in landing.WEEKLY_FEEDS:
        landing.land(landing.drop_path("inbox", feed, WEEK), landing.drop_path("raw", feed, WEEK), "test")


def run(spark, feed="sales"):
    loaded = bronze.load_week(spark, feed, WEEK, "test")
    merged = silver.merge_week(spark, feed, WEEK, "test")
    return loaded, merged, reconcile.reconcile_week(spark, feed, WEEK)


def silver_rows(spark, feed="sales"):
    frame = spark.read.format("delta").load(table_path("silver", feed))
    return {
        tuple(r)
        for r in frame.select("date", "store_nbr", "item_nbr", "unit_sales", "on_promotion").collect()
    }


def test_a_clean_week_loads_reconciles_and_keeps_unknown_promotions_null(spark, lake):
    deliver()
    loaded, merged, report = run(spark)
    assert (loaded["rows"], loaded["quarantined"], merged["inserted"]) == (N, 0, N)
    assert all(report["checks"].values())
    nulls = (
        spark.read.format("delta").load(table_path("silver", "sales")).where("on_promotion IS NULL").count()
    )
    assert nulls == len(range(0, N, 3))


def test_rerunning_a_week_changes_nothing(spark, lake):
    deliver()
    run(spark)
    before = silver_rows(spark)
    versions = {layer: delta_version(spark, layer) for layer in ("bronze", "silver")}
    assert bronze.load_week(spark, "sales", WEEK, "rerun")["action"] == "unchanged"
    assert silver.merge_week(spark, "sales", WEEK, "rerun")["action"] == "unchanged"
    assert {layer: delta_version(spark, layer) for layer in ("bronze", "silver")} == versions
    assert silver_rows(spark) == before


def delta_version(spark, layer, feed="sales"):
    from delta.tables import DeltaTable

    return DeltaTable.forPath(spark, table_path(layer, feed)).history(1).first()["version"]


def test_duplicates_collapse_to_one_row_per_key(spark, lake):
    deliver(fault="duplicate")
    loaded, merged, report = run(spark)
    sent_twice = len(range(0, N, 128))
    assert loaded["rows"] == N + sent_twice
    assert (merged["duplicates_exact"], merged["duplicates_conflicting"], merged["inserted"]) == (
        sent_twice,
        0,
        N,
    )
    assert all(report["checks"].values())


def test_unparseable_rows_are_quarantined_not_dropped(spark, lake):
    deliver(fault="bad-rows")
    loaded, _, report = run(spark)
    bad = len(range(0, N, 1024))
    assert (loaded["rows"], loaded["quarantined"], loaded["reasons"]) == (N - bad, bad, {"unparseable": bad})
    assert (
        report["spark"]["quarantined_rows"] == report["duckdb"]["raw_rows"] - report["duckdb"]["parsed_rows"]
    )


def test_a_renamed_column_fails_before_writing_anything(spark, lake):
    deliver(fault="schema")
    with pytest.raises(SchemaDrift, match="missing \\['unit_sales'\\], extra \\['units'\\]"):
        bronze.load_week(spark, "sales", WEEK, "test")
    assert not (lake / "bronze").exists()


def test_a_restatement_updates_only_the_revised_rows(spark, lake):
    deliver()
    run(spark)
    before = silver_rows(spark)
    deliver(fault="restate", force=True)
    loaded, merged, report = run(spark)
    revised = [i for i in range(0, N, 256) if float(i % 13) - 1.0 != 0.0]  # 0 * 1.1 is still 0
    assert loaded["action"] == "loaded"
    assert (merged["inserted"], merged["updated"], merged["deleted"]) == (0, len(revised), 0)
    assert len(silver_rows(spark) - before) == len(revised)
    assert all(report["checks"].values())


def test_rows_the_vendor_stops_sending_are_deleted_from_that_week_only(spark, lake):
    deliver()
    run(spark)
    table = pq.read_table(lake.parent / "source/train.parquet")
    pq.write_table(table.slice(0, N - 10), lake.parent / "source/train.parquet")
    deliver(force=True)
    _, merged, report = run(spark)
    assert (merged["inserted"], merged["updated"], merged["deleted"]) == (0, 0, 10)
    assert all(report["checks"].values())


def test_merging_a_week_rewrites_only_that_weeks_files(spark, lake):
    # Two weeks in silver, then the first week redelivered with 10 rows fewer.
    source = lake.parent / "source/train.parquet"
    first = pq.read_table(source)
    days = first.column("date").to_pylist()
    second = first.set_column(1, "date", pa.array([f"2016-01-{int(d[-2:]) + 7:02d}" for d in days]))
    second = second.set_column(0, "id", pa.array([i + N for i in range(N)]))
    pq.write_table(pa.concat_tables([first, second]), source)
    for week in (WEEK, date(2016, 1, 19)):
        drops.publish_week(week)
        for feed in landing.WEEKLY_FEEDS:
            landing.land(landing.drop_path("inbox", feed, week), landing.drop_path("raw", feed, week), "test")
        bronze.load_week(spark, "sales", week, "test")
        silver.merge_week(spark, "sales", week, "test")

    def files(week):
        return set(
            spark.read.format("delta")
            .load(table_path("silver", "sales"))
            .where(f"week_ending = '{week}'")
            .inputFiles()
        )

    other = files("2016-01-19")
    pq.write_table(pa.concat_tables([first.slice(0, N - 10), second]), source)
    deliver(force=True)
    loaded, merged, _ = run(spark)
    assert merged["deleted"] == 10
    assert files("2016-01-19") == other  # the other week's files were not rewritten
