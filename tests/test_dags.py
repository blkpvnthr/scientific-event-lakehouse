"""DAG import validation.

Requires Apache Airflow. Skipped automatically where Airflow is not installed
(the main Spark test environment); run in the dedicated Airflow environment or
inside the Airflow Docker image. See docs/LOCAL_DEVELOPMENT.md.
"""

from __future__ import annotations

from pathlib import Path

import pytest

# Skip unless the real apache-airflow package is installed. A local ``airflow/``
# directory exists at the repo root, so probe a real submodule, not the name.
pytest.importorskip("airflow.models")

from airflow.models import DagBag

DAGS_DIR = Path(__file__).resolve().parents[1] / "airflow" / "dags"
EXPECTED_DAGS = {
    "historical_processing",
    "incremental_ingestion",
    "recovery_replay",
}


def test_dags_import_without_errors():
    dagbag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    assert dagbag.import_errors == {}, dagbag.import_errors
    assert EXPECTED_DAGS.issubset(set(dagbag.dag_ids))


def test_dags_have_retries_and_timeouts():
    dagbag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    for dag_id in EXPECTED_DAGS:
        # Use the in-memory parsed DAGs (avoids the Airflow metadata DB).
        dag = dagbag.dags[dag_id]
        for task in dag.tasks:
            assert task.retries >= 1
            assert task.execution_timeout is not None
