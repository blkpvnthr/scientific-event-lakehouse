"""Tests for orchestration task callables (Airflow-agnostic)."""

from __future__ import annotations

import pytest
from conftest import write_synthetic_run

from src.orchestration import tasks
from src.orchestration.tasks import compute_query_interval, verify_run_ready


def test_compute_interval_without_watermark():
    start, end = compute_query_interval(None, "2025-06-15T00:00:00+00:00")
    assert start == tasks.DEFAULT_HISTORICAL_START
    assert end == "2025-06-15"


def test_compute_interval_with_watermark_overlap():
    # Start backs off by the overlap window to re-capture revised events.
    start, end = compute_query_interval(
        "2025-06-10T12:00:00", "2025-06-15T00:00:00", overlap_days=7
    )
    assert start == "2025-06-03"
    assert end == "2025-06-15"


def test_verify_run_ready_ok(tmp_settings, monkeypatch):
    run_id = write_synthetic_run(tmp_settings)
    monkeypatch.setattr(tasks, "get_settings", lambda: tmp_settings)
    summary = verify_run_ready(run_id)
    assert summary["run_id"] == run_id
    assert summary["total_records"] == 3


def test_verify_run_ready_missing(tmp_settings, monkeypatch):
    monkeypatch.setattr(tasks, "get_settings", lambda: tmp_settings)
    with pytest.raises(FileNotFoundError):
        verify_run_ready("f" * 32)


def _write_quality_parquet(spark, settings, rows):
    spark.createDataFrame(rows).write.mode("overwrite").parquet(
        str(settings.silver_path / "data_quality_results")
    )


def test_quality_gate_reads_silver_parquet_passes(spark, tmp_settings, monkeypatch):
    # Reads the Silver Parquet (not Delta), so it can run before the merge.
    monkeypatch.setattr(tasks, "get_settings", lambda: tmp_settings)
    # Keep the shared session alive (the task stops its own session in prod).
    monkeypatch.setattr("src.spark.stop_spark_session", lambda s: None)
    _write_quality_parquet(
        spark,
        tmp_settings,
        [
            {
                "run_id": "r1",
                "rule_id": "id_present",
                "severity": "fatal",
                "observed_failure_count": 0,
            },
            {
                "run_id": "r1",
                "rule_id": "magnitude_type_present",
                "severity": "warning",
                "observed_failure_count": 1,
            },
        ],
    )
    result = tasks.quality_gate("r1")
    assert result["fatal_failures"] == 0
    assert result["rules_evaluated"] == 2


def test_quality_gate_fails_on_fatal(spark, tmp_settings, monkeypatch):
    monkeypatch.setattr(tasks, "get_settings", lambda: tmp_settings)
    monkeypatch.setattr("src.spark.stop_spark_session", lambda s: None)
    _write_quality_parquet(
        spark,
        tmp_settings,
        [
            {
                "run_id": "r1",
                "rule_id": "coordinates_in_range",
                "severity": "fatal",
                "observed_failure_count": 3,
            },
        ],
    )
    with pytest.raises(RuntimeError, match="Quality gate failed"):
        tasks.quality_gate("r1")
