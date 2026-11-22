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
