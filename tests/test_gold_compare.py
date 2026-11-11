"""Gold comparison logic, without DuckDB or Snowflake."""

from retail_lakehouse import gold_compare as g


def test_one_aggregate_per_column_type():
    aggs = dict(g.aggregates([("week_ending", "DATE"), ("units", "DOUBLE"), ("is_holiday", "BOOLEAN"),
                              ("holidays", "VARCHAR"), ("date", "DATE")]))  # fmt: skip
    assert aggs == {
        "rows": "count(*)",
        "sum(units)": "sum(units)",
        "true(is_holiday)": "sum(case when is_holiday then 1 else 0 end)",
        "count(holidays)": "count(holidays)",
        "len(holidays)": "sum(length(holidays))",
        "count(date)": "count(date)",
    }


def test_counts_are_exact_and_sums_tolerate_addition_order():
    labels = ["rows", "sum(units)"]
    # The Week 5 Spark and DuckDB sums of one week of sales: same rows, different addition order.
    a, b = {"2016-05-24": (10, 5201537.249000012)}, {"2016-05-24": (10, 5201537.248999988)}
    assert g.compare(a, b, labels) == []
    diffs = g.compare({"2016-05-24": (10, 1.0)}, {"2016-05-24": (11, 1.0)}, labels)
    assert diffs == [{"week": "2016-05-24", "check": "rows", "duckdb": 10, "snowflake": 11}]


def test_a_week_on_one_side_only_is_a_difference():
    assert g.compare({"2016-05-24": (1,)}, {}, ["rows"]) == [{"week": "2016-05-24", "only_in": "duckdb"}]
