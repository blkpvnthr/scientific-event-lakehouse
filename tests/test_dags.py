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


def test_quality_gate_precedes_delta_merge():
    """Watermark protection: the quality gate must run before the Delta merge
    (which advances the watermark) in every DAG that has both."""
    dagbag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    for dag_id in ("historical_processing", "incremental_ingestion"):
        dag = dagbag.dags[dag_id]
        quality = dag.get_task("quality_gate")
        merge = dag.get_task("merge_delta")
        # merge_delta is downstream of quality_gate (transitively).
        assert "merge_delta" in quality.get_flat_relative_ids(upstream=False)
        assert "quality_gate" in merge.get_flat_relative_ids(upstream=True)


def test_watermark_commits_after_gold_and_reconcile():
    """End-to-end watermark guarantee: commit_watermark runs only after the merge,
    Gold, and reconciliation — so any downstream failure leaves it unchanged."""
    dagbag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    gold_task = {
        "historical_processing": "build_gold",
        "incremental_ingestion": "refresh_gold",
    }
    for dag_id in ("historical_processing", "incremental_ingestion"):
        dag = dagbag.dags[dag_id]
        commit = dag.get_task("commit_watermark")
        upstream = commit.get_flat_relative_ids(upstream=True)
        # merge, gold, and reconcile all precede the watermark commit.
        assert "merge_delta" in upstream
        assert gold_task[dag_id] in upstream
        assert "reconcile" in upstream
        # commit_watermark has no downstream tasks that could fail after it in
        # the incremental DAG (it is terminal there).
        if dag_id == "incremental_ingestion":
            assert commit.get_flat_relative_ids(upstream=False) == set()
