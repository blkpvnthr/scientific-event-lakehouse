"""Reusable, Airflow-agnostic orchestration task callables.

Heavy imports (PySpark, Delta) are deferred into the functions so that importing
this module — and therefore parsing the DAGs — is cheap and does not require a
Spark runtime. Each callable is idempotent and safe to retry.
"""

from src.orchestration.tasks import (
    compute_query_interval,
    quality_gate,
    run_bronze,
    run_delta,
    run_gold,
    run_silver,
    verify_run_ready,
)

__all__ = [
    "compute_query_interval",
    "quality_gate",
    "run_bronze",
    "run_delta",
    "run_gold",
    "run_silver",
    "verify_run_ready",
]
