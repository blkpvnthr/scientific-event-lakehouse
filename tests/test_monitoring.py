"""Tests for structured logging, metrics, and alerting."""

from __future__ import annotations

import json
import logging

from src.monitoring import (
    CollectingAlerter,
    JsonLogFormatter,
    StageTimer,
    compute_pipeline_metrics,
    log_stage_event,
    redact_text,
)
from src.monitoring.alerts import alert_on_failure


def test_redact_aws_key():
    text = "failed with key AKIAABCDEFGHIJKLMNOP in request"
    assert "AKIA" not in redact_text(text)
    assert "REDACTED" in redact_text(text)


def test_redact_secret_assignment():
    assert "hunter2" not in redact_text("password=hunter2")
    assert "tok_abc" not in redact_text("api_key: tok_abc")


def test_redact_none():
    assert redact_text(None) is None


def test_json_formatter_includes_fields():
    record = logging.LogRecord("t", logging.INFO, __file__, 1, "msg", None, None)
    record.fields = {"stage": "bronze", "output_records": 10}
    out = json.loads(JsonLogFormatter().format(record))
    assert out["message"] == "msg"
    assert out["stage"] == "bronze"
    assert out["output_records"] == 10
    assert out["level"] == "INFO"


def test_log_stage_event_redacts_error():
    logger = logging.getLogger("test.stage")
    fields = log_stage_event(
        logger,
        stage="silver",
        pipeline_run_id="p1",
        ingestion_run_id="i1",
        output_records=5,
        error="boom password=secret123",
    )
    assert fields["stage"] == "silver"
    assert "secret123" not in fields["error"]
    # None-valued fields are dropped.
    assert "duplicate_records" not in fields


def test_stage_timer_measures():
    with StageTimer("x") as t:
        sum(range(1000))
    assert t.duration_seconds >= 0.0


def test_compute_pipeline_metrics():
    stages = {
        "bronze": {"records": 281421, "duration_seconds": 2.5},
        "silver": {"valid": 281421, "quarantined": 0, "duration_seconds": 12.0},
    }
    m = compute_pipeline_metrics(stages)
    assert m["stages"]["bronze"]["throughput_records_per_second"] == round(
        281421 / 2.5, 2
    )
    assert m["totals"]["quarantine_rate_pct"] == 0.0
    assert m["totals"]["total_duration_seconds"] == 14.5


def test_collecting_alerter():
    alerter = CollectingAlerter()
    alert_on_failure(
        alerter, stage="delta", run_id="r1", error="AKIAABCDEFGHIJKLMNOP leaked"
    )
    assert len(alerter.alerts) == 1
    assert alerter.alerts[0].severity == "critical"
    assert "AKIA" not in alerter.alerts[0].message
