"""Tests for recovery / replay helpers."""

from __future__ import annotations

import json

from conftest import write_synthetic_run

from src.recovery import (
    detect_incomplete_run,
    find_replayable_runs,
    watermark_would_regress,
)


def test_detect_incomplete_missing_marker(tmp_settings):
    assert detect_incomplete_run("z" * 32, tmp_settings) is True


def test_detect_complete_run(tmp_settings):
    run_id = write_synthetic_run(tmp_settings)
    assert detect_incomplete_run(run_id, tmp_settings) is False


def test_detect_incomplete_bad_status(tmp_settings):
    run_id = write_synthetic_run(tmp_settings)
    marker = tmp_settings.run_manifest_dir(run_id) / "_SUCCESS.json"
    data = json.loads(marker.read_bytes())
    data["status"] = "FAILED"
    marker.write_text(json.dumps(data))
    assert detect_incomplete_run(run_id, tmp_settings) is True


def test_find_replayable_runs(tmp_settings):
    run_id = write_synthetic_run(tmp_settings)
    runs = find_replayable_runs(tmp_settings)
    assert any(r.run_id == run_id and r.has_success_marker for r in runs)


def test_watermark_protection():
    # Older incoming would regress -> blocked.
    assert watermark_would_regress("2024-06-01", "2024-05-01") is True
    # Newer incoming -> allowed.
    assert watermark_would_regress("2024-06-01", "2024-07-01") is False
    # First-ever watermark -> allowed.
    assert watermark_would_regress(None, "2024-01-01") is False
    # No incoming -> never advances.
    assert watermark_would_regress("2024-06-01", None) is False
