"""Reconcile a week: DuckDB SQL over the raw file against Spark's bronze, quarantine and silver.

DuckDB reads the raw CSV on its own (every column as text, ``TRY_CAST`` to
type), deduplicates with its own SQL, and sums. If both engines agree, the
Spark pipeline neither dropped nor invented rows. Checks per week:

- raw rows             = bronze rows + quarantined rows
- raw rows that parse  = bronze rows
- distinct parsed keys = silver rows
- sum of the measure   = silver sum (relative tolerance 1e-9)

The report is written to ``reports/reconcile/<feed>/week_ending=.../`` whether
or not it passes. A mismatch raises.
"""

from __future__ import annotations

import json
from datetime import date
from typing import Any

from retail_lakehouse.landing import lake_root
from retail_lakehouse.spark.common import KEYS, raw_file, table_path

MEASURE = {"sales": "unit_sales", "transactions": "transactions"}
TYPES = {
    "sales": {
        "id": "BIGINT",
        "date": "DATE",
        "store_nbr": "INTEGER",
        "item_nbr": "INTEGER",
        "unit_sales": "DOUBLE",
    },
    "transactions": {"date": "DATE", "store_nbr": "INTEGER", "transactions": "INTEGER"},
}


class Mismatch(ValueError):
    pass


def duckdb_side(feed: str, week: date) -> dict[str, Any]:
    import duckdb

    typed = ", ".join(f"TRY_CAST({c} AS {t}) AS {c}, {c} AS raw_{c}" for c, t in TYPES[feed].items())
    # Same rule as Spark: keys must parse; any other field may be empty (null) but not garbage.
    parsed = " AND ".join(
        f"{c} IS NOT NULL" if c in KEYS[feed] else f"({c} IS NOT NULL OR raw_{c} IS NULL)"
        for c in TYPES[feed]
    )
    keys = ", ".join(KEYS[feed])
    order = "ORDER BY id DESC" if "id" in TYPES[feed] else ""
    measure = MEASURE[feed]
    sql = f"""
        WITH raw AS (SELECT * FROM read_csv('{raw_file(feed, week)}', all_varchar = true, header = true)),
        typed AS (SELECT {typed} FROM raw),
        ok AS (SELECT * FROM typed WHERE {parsed}
               AND date BETWEEN DATE '{week}' - INTERVAL 6 DAY AND DATE '{week}'),
        latest AS (SELECT * FROM ok QUALIFY row_number() OVER (PARTITION BY {keys} {order}) = 1)
        SELECT (SELECT count(*) FROM raw), (SELECT count(*) FROM ok),
               (SELECT count(*) FROM latest), (SELECT sum({measure}) FROM latest)
    """
    raw, ok, distinct, total = duckdb.sql(sql).fetchall()[0]
    return {"raw_rows": raw, "parsed_rows": ok, "distinct_keys": distinct, "sum": float(total or 0)}


def spark_side(spark, feed: str, week: date) -> dict[str, Any]:
    from pyspark.sql import functions as F

    def rows(layer: str, column: str = "_week_ending") -> int:
        return spark.read.format("delta").load(table_path(layer, feed)).where(f"{column} = '{week}'").count()

    silver = spark.read.format("delta").load(table_path("silver", feed)).where(f"week_ending = '{week}'")
    agg = silver.agg(F.count("*").alias("n"), F.sum(MEASURE[feed]).alias("total")).first()
    return {
        "bronze_rows": rows("bronze"),
        "quarantined_rows": rows("quarantine"),
        "silver_rows": agg["n"],
        "sum": float(agg["total"] or 0),
    }


def reconcile_week(spark, feed: str, week: date) -> dict[str, Any]:
    duck, sp = duckdb_side(feed, week), spark_side(spark, feed, week)
    checks = {
        "raw = bronze + quarantine": duck["raw_rows"] == sp["bronze_rows"] + sp["quarantined_rows"],
        "parsed = bronze": duck["parsed_rows"] == sp["bronze_rows"],
        "distinct keys = silver": duck["distinct_keys"] == sp["silver_rows"],
        "sum matches": abs(duck["sum"] - sp["sum"]) <= 1e-9 * max(1.0, abs(duck["sum"])),
    }
    report = {"feed": feed, "week_ending": str(week), "duckdb": duck, "spark": sp, "checks": checks}
    out = lake_root() / "reports/reconcile" / feed / f"week_ending={week}" / "reconcile.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=1) + "\n")
    if not all(checks.values()):
        failed = [name for name, ok in checks.items() if not ok]
        raise Mismatch(f"{feed} {week}: {failed}; duckdb {duck}, spark {sp}")
    return {**report, "action": "reconciled"}
