"""The Snowflake loader's SQL, checked without a Snowflake connection (CI has none)."""

from retail_lakehouse import snowflake_load as s

SALES = next(t for t in s.TABLES if t.name == "sales")
STORES = next(t for t in s.TABLES if t.name == "stores")
SALES_COLS = ["date", "store_nbr", "item_nbr", "unit_sales", "on_promotion", "is_return", "week_ending",
              "_run_id", "_updated_at"]  # fmt: skip


def test_every_table_has_keys_and_weekly_tables_have_a_measure():
    for t in s.TABLES:
        assert t.keys, t.name
        if t.weekly:
            assert t.measure and "week_ending" not in t.keys, t.name


def test_types_map_and_unknown_types_load_as_text():
    assert s.snowflake_type("DATE") == "DATE"
    assert s.snowflake_type("TIMESTAMP WITH TIME ZONE") == "TIMESTAMP_TZ"
    assert s.snowflake_type("DOUBLE") == "FLOAT"
    assert s.snowflake_type("JSON") == "VARCHAR"


def test_merge_matches_on_the_business_keys_and_updates_only_changed_rows():
    sql = s.merge_sql(SALES, SALES_COLS, "SILVER._LOAD_SALES")
    assert "on t.date = s.date and t.store_nbr = s.store_nbr and t.item_nbr = s.item_nbr" in sql
    # Change detection compares business columns (null-safe), never the run metadata.
    assert "not equal_null(t.unit_sales, s.unit_sales)" in sql
    assert "equal_null(t._run_id" not in sql and "equal_null(t._updated_at" not in sql
    # But an update carries the new metadata along, and never rewrites a key.
    update = sql.split("update set ")[1].split("\n")[0]
    assert "_updated_at = s._updated_at" in update and "store_nbr =" not in update


def test_deletes_stay_inside_the_loaded_weeks_for_weekly_tables():
    weekly = s.delete_sql(SALES, "SILVER._LOAD_SALES")
    assert "t.week_ending in (select distinct week_ending from SILVER._LOAD_SALES)" in weekly
    full = s.delete_sql(STORES, "SILVER._LOAD_STORES")
    assert "week_ending" not in full and "s.store_nbr = t.store_nbr" in full


def test_reconcile_runs_the_same_sql_on_both_sides():
    sql = s.reconcile_sql(SALES, s.weeks_clause(["2016-05-17", "2016-05-24"]))
    assert "{rel}" in sql and "sum(unit_sales)" in sql and "round(" not in sql
    assert "date '2016-05-17', date '2016-05-24'" in sql
    # Multi-column distinct counts: a subquery, since Snowflake has no count(distinct (a, b)).
    multi = next(t for t in s.TABLES if len(t.keys) > 1 and not t.weekly)
    assert "select distinct " + ", ".join(multi.keys) in s.reconcile_sql(multi, "true")


def test_no_weeks_selects_nothing():
    assert s.weeks_clause([]) == "false"


def test_create_table_uses_unquoted_identifiers():
    sql = s.create_table_sql(SALES, [("date", "DATE"), ("unit_sales", "DOUBLE")])
    assert sql.startswith("create table if not exists SILVER.SALES")
    assert "date DATE" in sql and "unit_sales FLOAT" in sql and '"' not in sql


def test_sums_match_tolerates_addition_order_but_not_a_lost_row():
    # 2014-10-21 on the first full load: same 490,040 rows; rounded to 3 dp, 4644675.935 vs .936.
    assert s.sums_match(4644675.9354999, 4644675.9355001)
    assert not s.sums_match(4644675.9355, 4644675.9355 - 1.0)
    assert s.sums_match(None, None) and not s.sums_match(None, 0.0)
