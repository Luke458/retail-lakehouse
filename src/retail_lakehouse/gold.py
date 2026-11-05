"""Fingerprint the gold tables, to show that a run did or did not change them.

    python -m retail_lakehouse.gold fingerprint > before.json

One line per table: row count, and an md5 over its rows in a fixed order, so
two fingerprints are equal only if every row is. Opens the DuckDB file
read-only. Runs in any venv with duckdb (``.venv-spark``, ``.venv-qc``).
"""

from __future__ import annotations

import json
import sys

from retail_lakehouse.landing import lake_root


def fingerprint() -> dict[str, dict[str, object]]:
    import duckdb

    con = duckdb.connect(str(lake_root() / "gold/favorita.duckdb"), read_only=True)
    tables = [
        name
        for (name,) in con.sql(
            "select table_name from information_schema.tables where table_type = 'BASE TABLE' order by 1"
        ).fetchall()
    ]
    result = {}
    for table in tables:
        rows, digest = con.sql(
            f"select count(*), md5(coalesce(string_agg(r, '\n' order by r), '')) "
            f"from (select cast(t as varchar) as r from {table} t)"
        ).fetchall()[0]
        result[table] = {"rows": rows, "md5": digest}
    return result


if __name__ == "__main__":
    if sys.argv[1:] != ["fingerprint"]:
        raise SystemExit(__doc__)
    sys.stdout.write(json.dumps(fingerprint(), indent=1) + "\n")
