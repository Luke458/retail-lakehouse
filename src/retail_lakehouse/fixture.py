"""A tiny Favorita-shaped lake for CI: source, inbox, raw, bronze, silver, gate decisions.

    python -m retail_lakehouse.fixture /tmp/lake      # in .venv-spark

Three stores in two states, 24 items in three families, two weeks (ending
2016-01-12 and 2016-01-19), plus a holiday. It runs the real simulator,
landing and Spark jobs on it, so `dbt build` in CI reads silver produced by
the same code as production. The gate decisions are written as approved
fixtures (``status: FIXTURE``); CI does not run verity, which has its own CI.
"""

from __future__ import annotations

import os
import sys
from datetime import date, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

WEEKS = (date(2016, 1, 12), date(2016, 1, 19))


def write_source(source: Path) -> None:
    source.mkdir(parents=True, exist_ok=True)
    days = [WEEKS[0] - timedelta(days=6) + timedelta(days=d) for d in range(14)]
    rows = [
        (store, item, day)
        for day in days
        for store in (1, 2, 3)
        for item in range(100, 124)
        if (store + item + day.toordinal()) % 4  # not every item sells every day
    ]
    pq.write_table(
        pa.table(
            {
                "id": list(range(len(rows))),
                "date": [day.isoformat() for _, _, day in rows],
                "store_nbr": [store for store, _, _ in rows],
                "item_nbr": [item for _, item, _ in rows],
                "unit_sales": [float((store * item + day.day) % 9) - 0.5 for store, item, day in rows],
                "onpromotion": [(item + day.day) % 5 == 0 for _, item, day in rows],
            }
        ),
        source / "train.parquet",
    )
    pq.write_table(
        pa.table(
            {
                "date": [day.isoformat() for day in days for _ in (1, 2, 3)],
                "store_nbr": [store for _ in days for store in (1, 2, 3)],
                "transactions": [900 + store * 10 + day.day for day in days for store in (1, 2, 3)],
            }
        ),
        source / "transactions.parquet",
    )
    pq.write_table(
        pa.table(
            {
                "store_nbr": [1, 2, 3],
                "city": ["Quito", "Quito", "Guayaquil"],
                "state": ["Pichincha", "Pichincha", "Guayas"],
                "type": ["D", "C", "A"],
                "cluster": [13, 8, 4],
            }
        ),
        source / "stores.parquet",
    )
    pq.write_table(
        pa.table(
            {
                "item_nbr": list(range(100, 124)),
                "family": [("GROCERY I", "BEVERAGES", "PRODUCE")[i % 3] for i in range(24)],
                "class": [1000 + i for i in range(24)],
                "perishable": [int(i % 3 == 2) for i in range(24)],
            }
        ),
        source / "items.parquet",
    )
    pq.write_table(
        pa.table(
            {
                "date": ["2016-01-06", "2016-01-15"],
                "type": ["Holiday", "Event"],
                "locale": ["National", "Local"],
                "locale_name": ["Ecuador", "Quito"],
                "description": ["Fixture national holiday", "Fixture local event"],
                "transferred": [False, False],
            }
        ),
        source / "holidays_events.parquet",
    )


def build(root: Path) -> None:
    os.environ["LAKE_ROOT"] = str(root / "lake")
    os.environ["FAVORITA_SOURCE"] = str(root / "source")
    from retail_lakehouse import drops, gate, landing
    from retail_lakehouse.spark import bronze, silver
    from retail_lakehouse.spark.common import spark_session

    write_source(root / "source")
    drops.publish_reference()
    landing.land(landing.drop_path("inbox", "reference"), landing.drop_path("raw", "reference"), "fixture")
    spark = spark_session("fixture")
    try:
        silver.load_reference(spark, "fixture")
        for week in WEEKS:
            drops.publish_week(week)
            for feed in landing.WEEKLY_FEEDS:
                folder = landing.drop_path("inbox", feed, week)
                landing.verify(folder)
                landing.land(folder, landing.drop_path("raw", feed, week), "fixture")
                bronze.load_week(spark, feed, week, "fixture")
                silver.merge_week(spark, feed, week, "fixture")
            gate.write_decision(week, approved=True, status="FIXTURE", reason="CI fixture, not assessed")
    finally:
        spark.stop()


if __name__ == "__main__":
    build(Path(sys.argv[1]))
