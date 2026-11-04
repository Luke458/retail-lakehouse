"""House rules for every DAG in dags/. Skipped until the first DAG exists."""

from pathlib import Path

import pytest

DAGS = Path(__file__).resolve().parents[1] / "dags"
pytestmark = pytest.mark.skipif(not list(DAGS.rglob("*.py")), reason="no DAGs yet")


@pytest.fixture(scope="module")
def dagbag():
    from airflow.dag_processing.dagbag import DagBag

    return DagBag(dag_folder=str(DAGS))


def test_dags_import_cleanly(dagbag):
    assert dagbag.import_errors == {}
    assert dagbag.dags


def test_every_task_retries(dagbag):
    no_retries = [f"{d.dag_id}.{t.task_id}" for d in dagbag.dags.values() for t in d.tasks if not t.retries]
    assert no_retries == []


def test_every_dag_alerts_on_failure(dagbag):
    silent = [
        d.dag_id
        for d in dagbag.dags.values()
        if not d.on_failure_callback and not all(t.on_failure_callback for t in d.tasks)
    ]
    assert silent == []
