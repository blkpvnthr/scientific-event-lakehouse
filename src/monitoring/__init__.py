"""Observability: structured logging, metrics, and alerting hooks."""

from src.monitoring.alerts import (
    Alerter,
    CollectingAlerter,
    LoggingAlerter,
)
from src.monitoring.logging import (
    JsonLogFormatter,
    get_structured_logger,
    log_stage_event,
    redact_text,
)
from src.monitoring.metrics import StageTimer, compute_pipeline_metrics

__all__ = [
    "Alerter",
    "CollectingAlerter",
    "JsonLogFormatter",
    "LoggingAlerter",
    "StageTimer",
    "compute_pipeline_metrics",
    "get_structured_logger",
    "log_stage_event",
    "redact_text",
]
