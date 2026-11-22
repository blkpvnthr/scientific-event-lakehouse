"""Historical processing DAG.

Processes an already-ingested historical run through the full medallion pipeline:
validate -> Bronze -> Silver -> Delta -> Gold -> quality gate -> report.

Trigger with a run id, e.g.:
    airflow dags trigger historical_processing \
        --conf '{"run_id": "3d8d9a251f33489e807356890c692789"}'

All tasks are idempotent, so retries never create duplicate records.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from airflow import DAG
from src.orchestration import tasks

DEFAULT_RUN_ID = "3d8d9a251f33489e807356890c692789"

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="historical_processing",
    description="Process a historical ingestion run through the lakehouse",
    schedule=None,  # triggered manually with a run_id
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    params={"run_id": Param(DEFAULT_RUN_ID, type="string")},
    tags=["lakehouse", "historical"],
) as dag:
    run_id = "{{ params.run_id }}"

    validate = PythonOperator(
        task_id="validate_run",
        python_callable=tasks.verify_run_ready,
        op_kwargs={"run_id": run_id},
    )
    bronze = PythonOperator(
        task_id="process_bronze",
        python_callable=tasks.run_bronze,
        op_kwargs={"run_id": run_id},
    )
    silver = PythonOperator(
        task_id="normalize_silver",
        python_callable=tasks.run_silver,
        op_kwargs={"run_id": run_id},
    )
    delta = PythonOperator(
        task_id="merge_delta",
        python_callable=tasks.run_delta,
        op_kwargs={"run_id": run_id},
    )
    gold = PythonOperator(
        task_id="build_gold",
        python_callable=tasks.run_gold,
        op_kwargs={"run_id": run_id},
    )
    quality = PythonOperator(
        task_id="quality_gate",
        python_callable=tasks.quality_gate,
        op_kwargs={"run_id": run_id},
    )
    report = PythonOperator(
        task_id="publish_report",
        python_callable=tasks.publish_execution_report,
        op_kwargs={"run_id": run_id},
    )

    validate >> bronze >> silver >> delta >> gold >> quality >> report
