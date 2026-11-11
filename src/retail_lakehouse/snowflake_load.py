"""Load gate-approved weeks of silver into Snowflake (a second gold target).

    scripts/snowflake.sh .venv-dbt/bin/python -m retail_lakehouse.snowflake_load [--weeks W1,W2]

For each table: DuckDB reads the silver Delta table and writes Parquet (weekly
tables: approved weeks only), PUT uploads it to an internal stage, COPY INTO
fills a temporary table, and a MERGE on the business keys applies it, so a
rerun inserts and updates nothing. Rows silver no longer has are deleted
(within the loaded weeks, for the weekly tables). Then a reconcile compares
row counts and sums per week between silver and Snowflake and writes a
report to $LAKE_ROOT/reports/snowflake/.

Runs as LAKEHOUSE_LOADER (role LOADER, which can write only SILVER).
Connection settings come from the environment (scripts/snowflake.sh).
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

DATABASE = "RETAIL_LAKEHOUSE"
SCHEMA = "SILVER"
WAREHOUSE = "TRANSFORM_XS"
STAGE = "LOAD_STAGE"
USER = "LAKEHOUSE_LOADER"
ROLE = "LOADER"

# DuckDB type -> Snowflake type. Anything else is loaded as text.
TYPES = {
    "DATE": "DATE",
    "INTEGER": "NUMBER(10,0)",
    "BIGINT": "NUMBER(19,0)",
    "DOUBLE": "FLOAT",
    "BOOLEAN": "BOOLEAN",
    "VARCHAR": "VARCHAR",
    "TIMESTAMP WITH TIME ZONE": "TIMESTAMP_TZ",
    "TIMESTAMP": "TIMESTAMP_NTZ",
}


@dataclass(frozen=True)
class Table:
    name: str
    keys: tuple[str, ...]
    weekly: bool = False  # filtered to approved weeks, reconciled per week
    measure: str | None = None  # summed in the reconcile
    drop: tuple[str, ...] = ()  # silver columns not shipped
    source: str | None = None  # DuckDB relation; default: the silver Delta table

    @property
    def target(self) -> str:
        return self.name.upper()


TABLES = (
    # row_hash is silver's own change detection; the MERGE compares the columns instead,
    # which saves shipping a 32-character string per row.
    Table("sales", ("date", "store_nbr", "item_nbr"), weekly=True, measure="unit_sales", drop=("row_hash",)),
    Table("transactions", ("date", "store_nbr"), weekly=True, measure="transactions", drop=("row_hash",)),
    Table("stores", ("store_nbr",)),
    Table("items", ("item_nbr",)),
    Table("holidays_events", ("date", "locale", "locale_name", "description", "type")),
    Table("gate_decisions", ("week_ending", "decided_at"), source="gate"),
)


def snowflake_type(duckdb_type: str) -> str:
    return TYPES.get(duckdb_type.upper(), "VARCHAR")


def create_table_sql(table: Table, columns: list[tuple[str, str]]) -> str:
    cols = ",\n  ".join(f"{name} {snowflake_type(t)}" for name, t in columns)
    return f"create table if not exists {SCHEMA}.{table.target} (\n  {cols}\n)"


def merge_sql(table: Table, columns: list[str], staging: str) -> str:
    """MERGE staging into the target: insert new keys, update keys whose values changed."""
    on = " and ".join(f"t.{k} = s.{k}" for k in table.keys)
    values = [c for c in columns if c not in table.keys and not c.startswith("_")]
    changed = " or ".join(f"not equal_null(t.{c}, s.{c})" for c in values) or "false"
    update = ", ".join(f"{c} = s.{c}" for c in columns if c not in table.keys)
    cols = ", ".join(columns)
    src = ", ".join(f"s.{c}" for c in columns)
    return (
        f"merge into {SCHEMA}.{table.target} t using {staging} s on {on}\n"
        f"when matched and ({changed}) then update set {update}\n"
        f"when not matched then insert ({cols}) values ({src})"
    )


def delete_sql(table: Table, staging: str) -> str:
    """Delete rows silver no longer has: within the loaded weeks, or anywhere for a full table."""
    match = " and ".join(f"s.{k} = t.{k}" for k in table.keys)
    scope = f"t.week_ending in (select distinct week_ending from {staging}) and " if table.weekly else ""
    return (
        f"delete from {SCHEMA}.{table.target} t where {scope}"
        f"not exists (select 1 from {staging} s where {match})"
    )


def reconcile_sql(table: Table, weeks_filter: str) -> str:
    """Per-week (weekly tables) or whole-table counts, sums and distinct keys. Same SQL on both sides."""
    # Unrounded: rounding in SQL can split two sums that differ only by float addition order.
    measure = f"sum({table.measure})" if table.measure else "null"
    keys = ", ".join(table.keys)
    if table.weekly:
        return (
            f"select cast(week_ending as varchar) as week, count(*) as n, {measure} as total "
            f"from {{rel}} where {weeks_filter} group by 1 order by 1"
        )
    distinct = f"(select count(*) from (select distinct {keys} from {{rel}}))"
    return f"select 'all' as week, count(*) as n, {distinct} as total from {{rel}}"


@dataclass
class Result:
    table: str
    exported: int = 0
    files: int = 0
    inserted: int = 0
    updated: int = 0
    deleted: int = 0
    seconds: float = 0.0
    mismatches: list[dict] = field(default_factory=list)
    compared: int = 0


def lake_root() -> Path:
    return Path(os.environ.get("LAKE_ROOT", "data"))


def duck():
    import duckdb

    con = duckdb.connect()
    con.sql("load delta")
    return con


def gate_glob() -> str:
    return str(lake_root() / "control" / "gate" / "*" / "decision-*.json")


def relation(table: Table) -> str:
    if table.source == "gate":
        return f"read_json_auto('{gate_glob()}', union_by_name = true, format = 'auto')"
    return f"delta_scan('{lake_root() / 'silver' / 'favorita' / table.name}')"


def approved_weeks(con) -> list[str]:
    """The latest decision per week approves it (the same rule as dbt's approved_weeks())."""
    rows = con.sql(f"""
        select week_ending from (
          select cast(week_ending as date) as week_ending, approved,
                 row_number() over (partition by week_ending order by decided_at desc) as latest
          from read_json_auto('{gate_glob()}', union_by_name = true, format = 'auto'))
        where latest = 1 and approved order by 1""").fetchall()
    return [str(r[0]) for r in rows]


def weeks_clause(weeks: list[str]) -> str:
    return "week_ending in (" + ", ".join(f"date '{w}'" for w in weeks) + ")" if weeks else "false"


def export(con, table: Table, weeks: list[str], out: Path) -> tuple[list[tuple[str, str]], int, int]:
    """Write the table's rows to Parquet files under out/. Returns columns, row count, file count."""
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)
    rel = con.sql(f"select * from {relation(table)}")
    columns = [(c, str(t)) for c, t in zip(rel.columns, rel.types, strict=True) if c not in table.drop]
    # Types Snowflake has no mapping for here (JSON, all-null columns) travel as text.
    columns = [(c, t if t.upper() in TYPES else "VARCHAR") for c, t in columns]
    select = ", ".join(f'cast("{c}" as {t}) as "{c}"' for c, t in columns)
    where = f"where {weeks_clause(weeks)}" if table.weekly else ""
    con.sql(
        f"copy (select {select} from {relation(table)} {where}) to '{out}' "
        # Big tables split into ~128 MB files, so PUT and COPY run in parallel; small ones stay one file.
        f"(format parquet, compression zstd, per_thread_output {str(table.weekly).lower()}, "
        f"file_size_bytes '128MB')"
    )
    n = con.sql(f"select count(*) from read_parquet('{out}/**/*.parquet')").fetchall()[0][0]
    files = len(list(out.rglob("*.parquet")))
    return columns, n, files


def snowflake():
    import snowflake.connector

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT_IDENTIFIER"],
        user=USER,
        role=ROLE,
        private_key_file=os.environ["SNOWFLAKE_LOADER_KEY_PATH"],
        authenticator="SNOWFLAKE_JWT",
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA,
        session_parameters={"QUERY_TAG": "retail-lakehouse:snowflake_load"},
        server_session_keep_alive=False,
    )


def load(sf, table: Table, columns: list[tuple[str, str]], out: Path) -> tuple[int, int, int]:
    cur = sf.cursor()
    staging = f"{SCHEMA}._LOAD_{table.target}"
    names = [c for c, _ in columns]
    cur.execute(create_table_sql(table, columns))
    cur.execute(f"create or replace temporary table {staging} like {SCHEMA}.{table.target}")
    prefix = f"@{SCHEMA}.{STAGE}/{table.name}/"
    cur.execute(f"remove {prefix}")
    for part in sorted(out.rglob("*.parquet")):
        cur.execute(f"put 'file://{part}' {prefix} auto_compress = false overwrite = true parallel = 8")
    cur.execute(
        f"copy into {staging} from {prefix} "
        f"file_format = (type = parquet use_logical_type = true) "
        f"match_by_column_name = case_insensitive purge = true on_error = abort_statement"
    )
    cur.execute(merge_sql(table, names, staging))
    inserted, updated = cur.fetchone()[:2]
    cur.execute(delete_sql(table, staging))
    deleted = cur.fetchone()[0]
    cur.execute(f"drop table if exists {staging}")
    return inserted, updated, deleted


def sums_match(a, b, rel: float = 1e-9) -> bool:
    """Equal within float addition-order noise (counts are compared exactly)."""
    if a is None or b is None:
        return a is None and b is None
    a, b = float(a), float(b)
    return abs(a - b) <= rel * max(1.0, abs(a), abs(b))


def reconcile(con, sf, table: Table, weeks: list[str]) -> tuple[int, list[dict]]:
    sql = reconcile_sql(table, weeks_clause(weeks))
    local = {r[0]: r[1:] for r in con.sql(sql.format(rel=relation(table))).fetchall()}
    cur = sf.cursor()
    cur.execute(sql.format(rel=f"{SCHEMA}.{table.target}"))
    remote = {r[0]: r[1:] for r in cur.fetchall()}
    mismatches = []
    for week in sorted(set(local) | set(remote)):
        a, b = local.get(week), remote.get(week)
        same = a is not None and b is not None and a[0] == b[0] and sums_match(a[1], b[1])
        if not same:
            silver, snow = (list(x) if x else None for x in (a, b))
            mismatches.append({"week": week, "silver": silver, "snowflake": snow})
    return len(set(local) | set(remote)), mismatches


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--weeks", help="comma-separated week endings (default: every approved week)")
    p.add_argument("--tables", help="comma-separated subset of " + ", ".join(t.name for t in TABLES))
    p.add_argument("--keep-export", action="store_true", help="keep the Parquet export under $LAKE_ROOT")
    args = p.parse_args(argv)

    con = duck()
    approved = approved_weeks(con)
    weeks = approved
    if args.weeks:
        asked = [w.strip() for w in args.weeks.split(",") if w.strip()]
        unapproved = sorted(set(asked) - set(approved))
        if unapproved:
            print(f"not approved by the gate, not loaded: {', '.join(unapproved)}", file=sys.stderr)
        weeks = [w for w in asked if w in set(approved)]
    tables = [t for t in TABLES if not args.tables or t.name in args.tables.split(",")]

    started = datetime.now(UTC)
    export_root = lake_root() / "snowflake" / "export"
    results: list[Result] = []
    with snowflake() as sf:
        sf.cursor().execute(f"create stage if not exists {SCHEMA}.{STAGE} file_format = (type = parquet)")
        for table in tables:
            t0 = time.monotonic()
            r = Result(table.name)
            out = export_root / table.name
            columns, r.exported, r.files = export(con, table, weeks, out)
            r.inserted, r.updated, r.deleted = load(sf, table, columns, out)
            r.compared, r.mismatches = reconcile(con, sf, table, weeks)
            r.seconds = round(time.monotonic() - t0, 1)
            if not args.keep_export:
                shutil.rmtree(out, ignore_errors=True)
            results.append(r)
            status = "ok" if not r.mismatches else f"{len(r.mismatches)} MISMATCHES"
            print(
                f"{table.name:16} exported {r.exported:>11,} in {r.files} files | inserted {r.inserted:>11,} "
                f"updated {r.updated:>9,} deleted {r.deleted:>7,} | reconcile {r.compared} "
                f"{'weeks' if table.weekly else 'table'}: {status} | {r.seconds}s"
            )

    report = {
        "started_at": started.isoformat(),
        "finished_at": datetime.now(UTC).isoformat(),
        "target": f"{DATABASE}.{SCHEMA}",
        "weeks": {"count": len(weeks), "first": min(weeks, default=None), "last": max(weeks, default=None)},
        "tables": [r.__dict__ for r in results],
        "passed": all(not r.mismatches for r in results),
    }
    out_dir = lake_root() / "reports" / "snowflake"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"load-{started.strftime('%Y%m%dT%H%M%S')}.json"
    path.write_text(json.dumps(report, indent=2, default=str) + "\n")
    print(f"report: {path} ({'passed' if report['passed'] else 'FAILED'})")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
