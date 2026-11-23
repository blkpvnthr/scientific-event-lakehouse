"""Orchestration task callables shared by all DAGs.

Each function is a thin, idempotent wrapper around the pipeline builders. Spark
is created and stopped inside each task so tasks are independent and retry-safe.
No large datasets are returned (Airflow XCom must stay small) — only small
summary dicts.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path

from src.config import get_settings

logger = logging.getLogger(__name__)

# Default historical start if no watermark exists yet.
DEFAULT_HISTORICAL_START = "2015-01-01"
# Overlap window (days) re-queried on each incremental run to capture revised
# older events, since USGS updates records after the fact.
INCREMENTAL_OVERLAP_DAYS = 7


def verify_run_ready(run_id: str) -> dict:
    """Validate that an ingestion run completed successfully (no Spark needed)."""
    settings = get_settings()
    completion = settings.run_manifest_dir(run_id) / "_SUCCESS.json"
    if not completion.is_file():
        raise FileNotFoundError(f"Run not complete (missing _SUCCESS): {run_id}")
    data = json.loads(completion.read_bytes())
    if data.get("status") != "SUCCESS" or data.get("run_id") != run_id:
        raise ValueError(f"Run marker invalid for {run_id}")
    return {
        "run_id": run_id,
        "total_records": data["total_records"],
        "total_windows": data["total_windows"],
    }


def run_bronze(run_id: str) -> dict:
    """Build structured Bronze Parquet for a run."""
    from src.bronze import build_bronze
    from src.spark import build_spark_session, stop_spark_session

    spark = build_spark_session("airflow-bronze")
    try:
        result = build_bronze(spark, run_id)
        if result.status != "PASS":
            raise RuntimeError(f"Bronze failed for {run_id}: {result.status}")
        return {"records": result.output_records, "files": result.distinct_source_files}
    finally:
        stop_spark_session(spark)


def run_silver(run_id: str) -> dict:
    """Build the Silver layer for a run."""
    from src.silver import build_silver
    from src.spark import build_spark_session, stop_spark_session

    spark = build_spark_session("airflow-silver")
    try:
        result = build_silver(spark, run_id)
        if result.status != "PASS":
            raise RuntimeError(f"Silver failed for {run_id}")
        return {
            "valid": result.valid_records,
            "quarantined": result.quarantined_records,
        }
    finally:
        stop_spark_session(spark)


def run_delta(run_id: str) -> dict:
    """Merge Silver outputs into the Delta tables."""
    from src.delta_lake.build_delta import build_delta
    from src.spark import build_spark_session, stop_spark_session

    spark = build_spark_session("airflow-delta", with_delta=True)
    try:
        result = build_delta(spark, run_id)
        return {"current": result.current_rows, "revisions": result.revisions_rows}
    finally:
        stop_spark_session(spark)


def run_gold(run_id: str | None = None) -> dict:
    """Build the Gold layer."""
    from src.gold import build_gold
    from src.spark import build_spark_session, stop_spark_session

    spark = build_spark_session("airflow-gold", with_delta=True)
    try:
        result = build_gold(spark)
        if result.status != "PASS":
            raise RuntimeError("Gold reconciliation failed")
        return {"fact_rows": result.fact_rows, "datasets": len(result.datasets)}
    finally:
        stop_spark_session(spark)


def quality_gate(run_id: str) -> dict:
    """Fail the pipeline if any FATAL data-quality rule failed.

    Reads the Silver **Parquet** ``data_quality_results`` written by the Silver
    stage. This lets the gate run *before* the Delta merge, so a fatal failure
    halts the pipeline before the watermark is advanced. Warnings do not fail.
    """
    from src.spark import build_spark_session, stop_spark_session

    settings = get_settings()
    spark = build_spark_session("airflow-quality")
    try:
        path = settings.silver_path / "data_quality_results"
        df = spark.read.parquet(str(path)).filter(f"run_id = '{run_id}'")
        rows = df.collect()
        fatal = sum(
            r["observed_failure_count"] for r in rows if r["severity"] == "fatal"
        )
        if fatal > 0:
            raise RuntimeError(f"Quality gate failed: {fatal} fatal failures")
        return {"fatal_failures": fatal, "rules_evaluated": len(rows)}
    finally:
        stop_spark_session(spark)


def reconcile_run(run_id: str) -> dict:
    """Final reconciliation: Gold fact rows must equal Delta current-state rows.

    Runs after Gold and before the watermark is committed. A mismatch raises, so
    the watermark is not advanced for an inconsistent run.
    """
    from src.spark import build_spark_session, stop_spark_session

    settings = get_settings()
    spark = build_spark_session("airflow-reconcile", with_delta=True)
    try:
        current = (
            spark.read.format("delta")
            .load(str(settings.delta_table_path("silver", "earthquake_events_current")))
            .count()
        )
        fact = spark.read.parquet(
            str(settings.gold_path / "fact_earthquake_event")
        ).count()
        if fact != current:
            raise RuntimeError(
                f"Reconciliation failed: gold fact={fact} != delta current={current}"
            )
        return {"delta_current": current, "gold_fact": fact}
    finally:
        stop_spark_session(spark)


def commit_watermark(run_id: str) -> dict:
    """Commit the processing watermark — the FINAL task of a successful run.

    Advances the watermark only after the merge, Gold, and reconciliation have all
    succeeded. Advance-only, so a replayed/older run never regresses it.
    """
    from src.delta_lake.build_delta import commit_watermark as _commit
    from src.spark import build_spark_session, stop_spark_session

    spark = build_spark_session("airflow-commit-watermark", with_delta=True)
    try:
        watermark = _commit(spark, run_id, get_settings())
        return {"watermark": watermark}
    finally:
        stop_spark_session(spark)


def publish_execution_report(run_id: str, stage_summaries: dict | None = None) -> str:
    """Write a small JSON execution report and return its path."""
    settings = get_settings()
    report = {
        "run_id": run_id,
        "generated_at": datetime.now(UTC).isoformat(),
        "stages": stage_summaries or {},
    }
    path = (
        settings.warehouse_path
        / "_reports"
        / "orchestration"
        / f"run_id={run_id}"
        / "execution_report.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True))
    return str(path)


def compute_query_interval(
    watermark: str | None, now: str, overlap_days: int = INCREMENTAL_OVERLAP_DAYS
) -> tuple[str, str]:
    """Compute the (start, end) query interval for an incremental run.

    The start is backed off by ``overlap_days`` from the watermark so that
    older events revised after their first observation are re-queried. If no
    watermark exists, start at the configured historical start. Pure function.
    """
    end_dt = datetime.fromisoformat(now)
    if watermark:
        wm_dt = datetime.fromisoformat(watermark)
        start_dt = wm_dt - timedelta(days=overlap_days)
        start = start_dt.date().isoformat()
    else:
        start = DEFAULT_HISTORICAL_START
    return start, end_dt.date().isoformat()


def run_incremental_extract(start: str, end: str, min_magnitude: float = 2.5) -> str:
    """Extract new/revised events for an interval; return the new run id.

    Requires network access to the USGS API. Uses the existing, reconciled
    backfill path so overlapping re-queries are idempotent downstream.
    """
    from src.ingestion.backfill import backfill

    run_id = backfill(start_date=start, end_date=end, min_magnitude=min_magnitude)
    logger.info("Incremental extract produced run_id=%s", run_id)
    return run_id


def read_current_watermark(name: str = "silver_max_updated_time") -> str | None:
    """Read the committed processing watermark (None if unset)."""
    from src.delta_lake.metadata import read_watermark
    from src.spark import build_spark_session, stop_spark_session

    settings = get_settings()
    path = settings.delta_table_path("metadata", "processing_watermarks")
    if not Path(path).exists():
        return None
    spark = build_spark_session("airflow-watermark", with_delta=True)
    try:
        return read_watermark(spark, path, name)
    finally:
        stop_spark_session(spark)
