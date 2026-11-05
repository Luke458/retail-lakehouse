"""Lake paths: local folders, or a Unity Catalog volume and an abfss:// tables root on Databricks."""

from datetime import date

import pytest

from retail_lakehouse.landing import drop_path
from retail_lakehouse.spark import common


@pytest.fixture(autouse=True)
def lake(tmp_path, monkeypatch):
    monkeypatch.setenv("LAKE_ROOT", str(tmp_path))
    monkeypatch.delenv("TABLE_ROOT", raising=False)
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
    return tmp_path


def test_tables_live_in_the_lake_folder_by_default(lake):
    assert common.table_path("silver", "sales") == f"{lake}/silver/favorita/sales"


def test_an_abfss_table_root_keeps_its_double_slash(monkeypatch):
    monkeypatch.setenv("TABLE_ROOT", "abfss://tables@stretaillakedev.dfs.core.windows.net/")
    assert common.table_path("bronze", "sales") == (
        "abfss://tables@stretaillakedev.dfs.core.windows.net/bronze/favorita/sales"
    )


def test_files_stay_under_lake_root_when_tables_move(monkeypatch, lake):
    monkeypatch.setenv("TABLE_ROOT", "abfss://tables@x.dfs.core.windows.net")
    assert common.raw_file("sales", date(2016, 1, 5)).is_relative_to(lake)
    assert drop_path("raw", "sales", date(2016, 1, 5)).is_relative_to(lake)


def test_databricks_is_detected_from_the_runtime_variable(monkeypatch):
    assert not common.on_databricks()
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "17.3")
    assert common.on_databricks()
