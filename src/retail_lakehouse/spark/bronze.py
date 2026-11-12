"""Raw to bronze: enforce the contract, quarantine what doesn't parse, keep the rest as sent.

Bronze is raw with types: one Delta table per feed, partitioned by
``_week_ending``, with lineage columns (source checksum, run id, load time).
Nothing is cleaned or deduplicated here; that is silver's job.

- Header not the contract: ``SchemaDrift``, nothing written. A renamed column
  is a conversation with the vendor, not something to guess around.
- Rows that don't parse, have no key, or fall outside the week go to
  ``quarantine/<feed>`` with the raw line and a reason. More than
  ``MAX_BAD_FRACTION`` fails the load after quarantining, so they can be inspected.
- Loaded + quarantined must equal the manifest's row count.
- Idempotent: a week already loaded from the same source checksum is skipped;
  otherwise that week's partition is replaced (``replaceWhere``), in both tables.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from functools import reduce
from operator import or_
from typing import Any

from retail_lakehouse.landing import INGEST
from retail_lakehouse.spark.common import KEYS, RAW_SCHEMAS, check_header, raw_file, spark_uri, table_path

MAX_BAD_FRACTION = 0.01


class TooManyBadRows(ValueError):
    pass


def existing_checksum(spark, path: str, week: date) -> str | None:
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, path):
        return None
    rows = (
        spark.read.format("delta")
        .load(path)
        .where(f"_week_ending = '{week}'")
        .select("_source_sha256")
        .distinct()
        .collect()
    )
    return rows[0][0] if len(rows) == 1 else None


def load_week(spark, feed: str, week: date, run_id: str) -> dict[str, Any]:
    from pyspark.sql import functions as F

    source = raw_file(feed, week)
    ingest = json.loads((source.parent / INGEST).read_text())
    checksum, expected_rows = ingest["source_sha256"][source.name], ingest["rows"][source.name]
    bronze, quarantine = table_path("bronze", feed), table_path("quarantine", feed)
    if existing_checksum(spark, bronze, week) == checksum:
        return {"feed": feed, "week_ending": str(week), "action": "unchanged"}

    check_header(source, feed)
    raw = spark.read.csv(
        spark_uri(source),
        schema=f"{RAW_SCHEMAS[feed]}, _corrupt_record STRING",
        header=True,
        mode="PERMISSIVE",
        columnNameOfCorruptRecord="_corrupt_record",
    ).cache()  # Spark refuses to filter on the corrupt-record column of an uncached CSV scan

    start = week - timedelta(days=6)
    reason = (
        F.when(F.col("_corrupt_record").isNotNull(), F.lit("unparseable"))
        .when(reduce(or_, [F.col(k).isNull() for k in KEYS[feed]]), F.lit("missing key"))
        .when(~F.col("date").between(F.lit(start), F.lit(week)), F.lit("outside week"))
    )
    lineage = [
        F.lit(week).alias("_week_ending"),
        F.lit(checksum).alias("_source_sha256"),
        F.lit(run_id).alias("_run_id"),
        F.current_timestamp().alias("_loaded_at"),
    ]
    tagged = raw.withColumn("_reason", reason)
    good = tagged.where("_reason IS NULL").drop("_reason", "_corrupt_record").select("*", *lineage)
    bad = tagged.where("_reason IS NOT NULL").select(
        F.coalesce("_corrupt_record", F.to_json(F.struct(*raw.columns[:-1]))).alias("raw_line"),
        F.col("_reason").alias("reason"),
        *lineage,
    )

    n_good, n_bad = good.count(), bad.count()
    if n_good + n_bad != expected_rows:
        raise ValueError(f"{source}: read {n_good} + {n_bad} rows, manifest says {expected_rows}")

    def write(frame, path):
        (
            frame.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"_week_ending = '{week}'")
            .partitionBy("_week_ending")
            .save(path)
        )

    write(bad, quarantine)
    if n_bad > MAX_BAD_FRACTION * expected_rows:
        raise TooManyBadRows(f"{source}: {n_bad}/{expected_rows} rows quarantined (> {MAX_BAD_FRACTION:.0%})")
    write(good, bronze)
    reasons = {r["reason"]: r["count"] for r in bad.groupBy("reason").count().collect()} if n_bad else {}
    raw.unpersist()
    return {
        "feed": feed,
        "week_ending": str(week),
        "action": "loaded",
        "rows": n_good,
        "quarantined": n_bad,
        "reasons": reasons,
    }
