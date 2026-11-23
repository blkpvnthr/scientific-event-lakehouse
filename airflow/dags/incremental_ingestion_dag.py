"""Incremental ingestion DAG.

Reads the committed watermark, extracts new and revised events over an
overlapping interval, then runs the medallion pipeline. The watermark is
advanced only by the Delta merge step, and only when the incoming value is
greater — so a failed run never advances it.

USGS query semantics: events are filtered by origin time, but records are revised
after the fact. We therefore re-query an overlap window (default 7 days) back
from the watermark and rely on idempotent, fingerprint-keyed revision handling
downstream.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator

from airflow import DAG
from src.orchestration import tasks

default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(hours=1),
}


def _determine_interval(**context) -> dict:
    """Read the watermark and compute the extraction interval."""
    watermark = tasks.read_current_watermark()
    now = context["ts"]  # logical execution timestamp (ISO)
    start, end = tasks.compute_query_interval(watermark, now)
    return {"start": start, "end": end}


def _extract(**context) -> str:
    interval = context["ti"].xcom_pull(task_ids="determine_interval")
    return tasks.run_incremental_extract(interval["start"], interval["end"])


with DAG(
    dag_id="incremental_ingestion",
    description="Extract new/revised events and process them incrementally",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,  # serialize to protect the watermark
    default_args=default_args,
    tags=["lakehouse", "incremental"],
) as dag:
    pulled_run_id = "{{ ti.xcom_pull(task_ids='extract') }}"

    determine_interval = PythonOperator(
        task_id="determine_interval", python_callable=_determine_interval
    )
    extract = PythonOperator(task_id="extract", python_callable=_extract)
    validate = PythonOperator(
        task_id="validate_run",
        python_callable=tasks.verify_run_ready,
        op_kwargs={"run_id": pulled_run_id},
    )
    bronze = PythonOperator(
        task_id="process_bronze",
        python_callable=tasks.run_bronze,
        op_kwargs={"run_id": pulled_run_id},
    )
    silver = PythonOperator(
        task_id="normalize_silver",
        python_callable=tasks.run_silver,
        op_kwargs={"run_id": pulled_run_id},
    )
    quality = PythonOperator(
        task_id="quality_gate",
        python_callable=tasks.quality_gate,
        op_kwargs={"run_id": pulled_run_id},
    )
    delta = PythonOperator(
        # Merge only — does NOT advance the watermark.
        task_id="merge_delta",
        python_callable=tasks.run_delta,
        op_kwargs={"run_id": pulled_run_id},
    )
    gold = PythonOperator(
        task_id="refresh_gold",
        python_callable=tasks.run_gold,
        op_kwargs={"run_id": pulled_run_id},
    )
    reconcile = PythonOperator(
        task_id="reconcile",
        python_callable=tasks.reconcile_run,
        op_kwargs={"run_id": pulled_run_id},
    )
    commit = PythonOperator(
        # The watermark is committed LAST, only after Gold + reconciliation.
        task_id="commit_watermark",
        python_callable=tasks.commit_watermark,
        op_kwargs={"run_id": pulled_run_id},
    )

    # Quality gate precedes the merge; the watermark is committed only after Gold
    # and reconciliation succeed, so any downstream failure leaves it unchanged.
    (
        determine_interval
        >> extract
        >> validate
        >> bronze
        >> silver
        >> quality
        >> delta
        >> gold
        >> reconcile
        >> commit
    )
