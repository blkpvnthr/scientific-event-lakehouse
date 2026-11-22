"""Recovery and replay DAG.

Reprocesses an already-archived ingestion run from its immutable raw files. Used
to recover from a failed downstream run or to rebuild derived tables. Because
every stage is idempotent (checksum-verified Bronze, fingerprint-keyed Delta
merges), replaying a run does not create duplicate records.

Trigger with:
    airflow dags trigger recovery_replay --conf '{"run_id": "<run_id>"}'
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from airflow import DAG
from src.orchestration import tasks

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="recovery_replay",
    description="Replay an archived run to recover derived tables",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    params={"run_id": Param("", type="string")},
    tags=["lakehouse", "recovery"],
) as dag:
    run_id = "{{ params.run_id }}"

    validate = PythonOperator(
        task_id="validate_manifests",
        python_callable=tasks.verify_run_ready,
        op_kwargs={"run_id": run_id},
    )
    bronze = PythonOperator(
        task_id="reprocess_bronze",
        python_callable=tasks.run_bronze,
        op_kwargs={"run_id": run_id},
    )
    silver = PythonOperator(
        task_id="reprocess_silver",
        python_callable=tasks.run_silver,
        op_kwargs={"run_id": run_id},
    )
    delta = PythonOperator(
        task_id="reconcile_delta",
        python_callable=tasks.run_delta,
        op_kwargs={"run_id": run_id},
    )
    report = PythonOperator(
        task_id="publish_recovery_report",
        python_callable=tasks.publish_execution_report,
        op_kwargs={"run_id": run_id},
    )

    validate >> bronze >> silver >> delta >> report
