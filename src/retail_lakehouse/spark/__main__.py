"""Spark job entry point, one JVM per call. Airflow runs it with the Spark venv's python.

    python -m retail_lakehouse.spark bronze    --feed sales --week 2016-01-05 --run-id manual
    python -m retail_lakehouse.spark silver    --feed sales --week 2016-01-05 --run-id manual
    python -m retail_lakehouse.spark reconcile --feed sales --week 2016-01-05
    python -m retail_lakehouse.spark reference --run-id manual

The last line on stdout is the job's result as JSON (Airflow keeps it as XCom).
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("job", choices=["bronze", "silver", "reconcile", "reference"])
    parser.add_argument("--feed", choices=["sales", "transactions"])
    parser.add_argument("--week", type=date.fromisoformat)
    parser.add_argument("--run-id", default="manual")
    args = parser.parse_args(argv)
    if args.job != "reference" and not (args.feed and args.week):
        parser.error(f"{args.job} needs --feed and --week")

    from retail_lakehouse.spark import bronze, reconcile, silver
    from retail_lakehouse.spark.common import spark_session

    spark = spark_session(f"{args.job}-{args.feed or 'reference'}-{args.week or ''}")
    try:
        if args.job == "bronze":
            result = bronze.load_week(spark, args.feed, args.week, args.run_id)
        elif args.job == "silver":
            result = silver.merge_week(spark, args.feed, args.week, args.run_id)
        elif args.job == "reconcile":
            result = reconcile.reconcile_week(spark, args.feed, args.week)
        else:
            result = silver.load_reference(spark, args.run_id)
    finally:
        spark.stop()
    sys.stdout.write(json.dumps(result, default=str) + "\n")


if __name__ == "__main__":
    main()
