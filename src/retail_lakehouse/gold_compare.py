"""Compare gold on Snowflake with gold on DuckDB, model by model and week by week.

    scripts/snowflake.sh .venv-dbt/bin/python -m retail_lakehouse.gold_compare

For every gold model and every week: the row count, and one aggregate per
column (numbers summed, booleans counted true, text counted and its total
length summed, dates counted). Counts must be equal; sums equal within float
addition-order noise. Writes a report to $LAKE_ROOT/reports/snowflake/.
Reads Snowflake as the dbt service user (role TRANSFORMER).
"""

from __future__ import annotations

import json
import os
import sys
from datetime import UTC, datetime

from retail_lakehouse.snowflake_load import lake_root, sums_match

MODELS = (
    "fct_sales_store_family_week",
    "fct_sales_state_week",
    "fct_store_transactions_week",
    "fct_store_holiday_days",
    "mart_promo_uplift_family_week",
)
NUMERIC = ("TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "DOUBLE", "FLOAT", "DECIMAL")


def aggregates(columns: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """(label, SQL) per column, written so DuckDB and Snowflake compute the same value."""
    out = [("rows", "count(*)")]
    for name, dtype in columns:
        if name == "week_ending":
            continue
        t = dtype.upper()
        if t.startswith(NUMERIC):
            out.append((f"sum({name})", f"sum({name})"))
        elif t == "BOOLEAN":
            out.append((f"true({name})", f"sum(case when {name} then 1 else 0 end)"))
        elif t == "VARCHAR":
            out.append((f"count({name})", f"count({name})"))
            out.append((f"len({name})", f"sum(length({name}))"))
        else:
            out.append((f"count({name})", f"count({name})"))
    return out


def compare_sql(model: str, aggs: list[tuple[str, str]], qualified: str) -> str:
    cols = ", ".join(sql for _, sql in aggs)
    return f"select cast(week_ending as varchar), {cols} from {qualified} group by 1 order by 1"


def compare(local: dict, remote: dict, labels: list[str]) -> list[dict]:
    diffs = []
    for week in sorted(set(local) | set(remote)):
        a, b = local.get(week), remote.get(week)
        if a is None or b is None:
            diffs.append({"week": week, "only_in": "duckdb" if b is None else "snowflake"})
            continue
        for label, x, y in zip(labels, a, b, strict=True):
            exact = label == "rows" or label.startswith(("count(", "true(", "len("))
            if exact:
                same = x == y or (x is not None and y is not None and int(x) == int(y))
            else:
                same = sums_match(x, y)
            if not same:
                diffs.append({"week": week, "check": label, "duckdb": x, "snowflake": y})
    return diffs


def main() -> int:
    import duckdb
    import snowflake.connector

    gold = lake_root() / "gold" / "favorita.duckdb"
    con = duckdb.connect(str(gold), read_only=True)
    sf = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT_IDENTIFIER"],
        user="LAKEHOUSE_DBT",
        role="TRANSFORMER",
        private_key_file=os.environ["SNOWFLAKE_DBT_KEY_PATH"],
        authenticator="SNOWFLAKE_JWT",
        warehouse="TRANSFORM_XS",
        database="RETAIL_LAKEHOUSE",
        schema="GOLD",
        session_parameters={"QUERY_TAG": "retail-lakehouse:gold_compare"},
        server_session_keep_alive=False,
    )
    results, passed = [], True
    try:
        for model in MODELS:
            rel = con.sql(f"select * from {model} limit 0")
            columns = [(c, str(t)) for c, t in zip(rel.columns, rel.types, strict=True)]
            aggs = aggregates(columns)
            local = {r[0]: r[1:] for r in con.sql(compare_sql(model, aggs, model)).fetchall()}
            cur = sf.cursor()
            cur.execute(compare_sql(model, aggs, f"GOLD.{model}"))
            remote = {r[0]: r[1:] for r in cur.fetchall()}
            labels = [label for label, _ in aggs]
            diffs = compare(local, remote, labels)
            rows = sum(int(v[0]) for v in local.values())
            passed &= not diffs
            results.append(
                {"model": model, "weeks": len(local), "rows": rows, "checks": labels, "differences": diffs}
            )
            print(
                f"{model:30} {len(local):>4} weeks {rows:>9,} rows {len(labels):>2} checks/week: "
                f"{'equal' if not diffs else f'{len(diffs)} DIFFERENCES'}"
            )
    finally:
        sf.close()
    out_dir = lake_root() / "reports" / "snowflake"
    out_dir.mkdir(parents=True, exist_ok=True)
    now = datetime.now(UTC)
    path = out_dir / f"gold-compare-{now.strftime('%Y%m%dT%H%M%S')}.json"
    path.write_text(
        json.dumps({"at": now.isoformat(), "duckdb": str(gold), "passed": passed, "models": results},
                   indent=2, default=str) + "\n"
    )  # fmt: skip
    print(f"report: {path} ({'passed' if passed else 'FAILED'})")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
