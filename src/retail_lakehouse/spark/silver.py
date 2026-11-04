"""Bronze to silver: one row per natural key, merged incrementally a week at a time.

Silver tables are partitioned by ``week_ending`` and keyed on the feed's
natural key. Each week is MERGEd from bronze:

- duplicates in the drop collapse to one row per key. On a conflict (same
  key, different values) the row with the highest vendor ``id`` wins, taken
  as the later correction. Both counts are reported;
- matched rows are rewritten only when their values changed (``row_hash``),
  so a rerun of an unchanged week updates nothing;
- rows the vendor no longer sends for that week are deleted
  (``WHEN NOT MATCHED BY SOURCE``, restricted to the week's partition);
- each merge is recorded in ``control/silver_loads`` with the bronze source
  checksum. A rerun from the same checksum skips the MERGE, which would
  otherwise commit an empty table version.

Sales nulls stay null: ``onpromotion`` is unknown before 2014-04, not false.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from retail_lakehouse.spark.common import KEYS, RAW_SCHEMAS, raw_file, table_path

VALUES = {
    "sales": ["unit_sales", "on_promotion", "is_return"],
    "transactions": ["transactions"],
}
REFERENCE = ("stores", "items", "holidays_events")


def shape(frame, feed: str):
    from pyspark.sql import functions as F

    if feed == "sales":
        frame = frame.withColumnRenamed("onpromotion", "on_promotion").withColumn(
            "is_return", F.col("unit_sales") < 0
        )
    return frame


def deduplicate(frame, feed: str):
    """One row per key; returns (rows, exact duplicates dropped, conflicting duplicates dropped)."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    keys, values = KEYS[feed], VALUES[feed]
    order = [F.col("id").desc()] if "id" in frame.columns else [F.lit(1)]
    window = Window.partitionBy(*keys).orderBy(*order)
    ranked = frame.withColumn("_rank", F.row_number().over(window)).withColumn(
        "_variants", F.size(F.collect_set(F.struct(*values)).over(Window.partitionBy(*keys)))
    )
    dropped = ranked.where("_rank > 1")
    counts = dropped.groupBy((F.col("_variants") > 1).alias("conflict")).count().collect()
    by_kind = {row["conflict"]: row["count"] for row in counts}
    kept = ranked.where("_rank = 1").drop("_rank", "_variants")
    return kept, by_kind.get(False, 0), by_kind.get(True, 0)


def row_hash(values: list[str]):
    from pyspark.sql import functions as F

    return F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("∅")) for c in values]), 256)


def merge_week(spark, feed: str, week: date, run_id: str) -> dict[str, Any]:
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    keys, values = KEYS[feed], VALUES[feed]
    bronze = spark.read.format("delta").load(table_path("bronze", feed)).where(f"_week_ending = '{week}'")
    checksums = [row[0] for row in bronze.select("_source_sha256").distinct().collect()]
    if len(checksums) != 1:
        raise ValueError(f"bronze {feed} week ending {week}: expected one source, found {checksums}")
    path = table_path("silver", feed)
    if DeltaTable.isDeltaTable(spark, path) and last_load(spark, feed, week) == checksums[0]:
        return {"feed": feed, "week_ending": str(week), "action": "unchanged"}
    source_rows = bronze.count()
    rows, exact, conflicting = deduplicate(shape(bronze, feed), feed)
    source = rows.select(
        *keys,
        *values,
        F.col("_week_ending").alias("week_ending"),
        row_hash(values).alias("row_hash"),
        F.lit(run_id).alias("_run_id"),
        F.current_timestamp().alias("_updated_at"),
    )

    if not DeltaTable.isDeltaTable(spark, path):
        source.limit(0).write.format("delta").partitionBy("week_ending").save(path)
    target = DeltaTable.forPath(spark, path)
    before = target.history(1).select("version").first()[0]
    on = " AND ".join([f"t.week_ending = '{week}'", *[f"t.{k} = s.{k}" for k in keys]])
    (
        target.alias("t")
        .merge(source.alias("s"), on)
        .whenMatchedUpdateAll(condition="t.row_hash <> s.row_hash")
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete(condition=f"t.week_ending = '{week}'")
        .execute()
    )
    last = target.history(1).select("version", "operationMetrics").first()
    metrics = last["operationMetrics"] if last["version"] != before else {}
    changed = {
        name: int(metrics.get(f"numTargetRows{name.title()}", 0))
        for name in ("inserted", "updated", "deleted")
    }
    record_load(spark, feed, week, checksums[0], last["version"], run_id)
    return {
        "feed": feed,
        "week_ending": str(week),
        "bronze_rows": source_rows,
        "duplicates_exact": exact,
        "duplicates_conflicting": conflicting,
        **changed,
        "version": last["version"],
        "action": "unchanged" if not any(changed.values()) else "merged",
    }


def last_load(spark, feed: str, week: date) -> str | None:
    """The bronze checksum silver last merged for this week, if any."""
    from delta.tables import DeltaTable

    path = table_path("control", "silver_loads")
    if not DeltaTable.isDeltaTable(spark, path):
        return None
    latest = (
        spark.read.format("delta")
        .load(path)
        .where(f"feed = '{feed}' AND week_ending = '{week}'")
        .orderBy("loaded_at", ascending=False)
        .select("source_sha256")
        .first()
    )
    return latest[0] if latest else None


def record_load(spark, feed: str, week: date, checksum: str, version: int, run_id: str) -> None:
    from pyspark.sql import functions as F

    row = spark.createDataFrame(
        [(feed, week, checksum, version, run_id)],
        "feed STRING, week_ending DATE, source_sha256 STRING, silver_version LONG, run_id STRING",
    ).withColumn("loaded_at", F.current_timestamp())
    row.write.format("delta").mode("append").save(table_path("control", "silver_loads"))


def load_reference(spark, run_id: str) -> dict[str, Any]:
    """Small dimensions: replace each silver table from the latest raw reference drop."""
    from retail_lakehouse.spark.common import check_header

    counts = {}
    for name in REFERENCE:
        path = raw_file(name)
        check_header(path, name)
        frame = spark.read.csv(str(path), schema=RAW_SCHEMAS[name], header=True, mode="FAILFAST")
        frame.write.format("delta").mode("overwrite").save(table_path("silver", name))
        counts[name] = frame.count()
    return {"action": "loaded", "rows": counts}
