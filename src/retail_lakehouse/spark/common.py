"""Spark session, lake paths and the raw contracts shared by the Spark jobs."""

from __future__ import annotations

import gzip
import os
from pathlib import Path

from retail_lakehouse.landing import drop_path, lake_root

# The raw contract: exact column names and order the vendor agreed to send.
RAW_SCHEMAS = {
    "sales": "id BIGINT, date DATE, store_nbr INT, item_nbr INT, unit_sales DOUBLE, onpromotion BOOLEAN",
    "transactions": "date DATE, store_nbr INT, transactions INT",
    "stores": "store_nbr INT, city STRING, state STRING, type STRING, cluster INT",
    "items": "item_nbr INT, family STRING, class INT, perishable INT",
    "holidays_events": (
        "date DATE, type STRING, locale STRING, locale_name STRING, description STRING, transferred BOOLEAN"
    ),
}
# Columns a row cannot be keyed without.
KEYS = {
    "sales": ["date", "store_nbr", "item_nbr"],
    "transactions": ["date", "store_nbr"],
}


class SchemaDrift(ValueError):
    """The file's header is not the agreed contract. Nothing was written."""


def columns(feed: str) -> list[str]:
    return [part.strip().split(" ")[0] for part in RAW_SCHEMAS[feed].split(",")]


def check_header(path: Path, feed: str) -> None:
    with gzip.open(path, "rt") as handle:
        header = [name.strip().strip('"') for name in handle.readline().strip().split(",")]
    if header != (expected := columns(feed)):
        missing = [c for c in expected if c not in header]
        extra = [c for c in header if c not in expected]
        raise SchemaDrift(
            f"{path}: header {header} != contract {expected} (missing {missing}, extra {extra})"
        )


def on_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def on_glue() -> bool:
    """Set by retail_lakehouse.glue, the Glue job's entry point."""
    return os.environ.get("RETAIL_LAKEHOUSE_RUNTIME") == "glue"


def on_managed_spark() -> bool:
    """A platform that owns the Spark session (Delta configured by the job, not by us)."""
    return on_databricks() or on_glue()


def spark_uri(path: Path) -> str:
    """Where Spark reads a lake file: the path itself, or with ``$LAKE_URI`` set (Glue) the same
    file in S3, because Glue's executors cannot see the driver's local copy of the files side."""
    uri = os.environ.get("LAKE_URI")
    if not uri:
        return str(path)
    rel = Path(path).relative_to(lake_root())
    return f"{uri.rstrip('/')}/{rel.as_posix()}"


def table_root() -> str:
    """Where the Delta tables live: ``$TABLE_ROOT`` (``abfss://tables@<account>.dfs.core.windows.net/``
    on Databricks), else the lake folder. Files (drops, raw, reports) stay under ``lake_root()``,
    which on Databricks is a Unity Catalog volume: an ordinary path for Python and Spark alike."""
    return os.environ.get("TABLE_ROOT") or str(lake_root())


def table_path(layer: str, name: str) -> str:
    # A string join: pathlib would collapse "abfss://" to "abfss:/".
    return f"{table_root().rstrip('/')}/{layer}/favorita/{name}"


def raw_file(feed: str, week=None) -> Path:
    folder = drop_path("raw", "reference" if week is None else feed, week)
    return folder / f"{feed}.csv.gz"


def spark_session(app: str = "retail-lakehouse"):
    """A local Spark with Delta, or on Databricks and Glue the platform's own session (Delta built in)."""
    from pyspark.sql import SparkSession

    if on_managed_spark():
        return SparkSession.builder.appName(app).getOrCreate()

    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.master("local[4]")
        .appName(app)
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "4")
        # Delta replays its log with 50 tasks by default; these tables are small.
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
