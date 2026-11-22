"""Structured JSON logging with secret redaction.

Emits one JSON object per log record so pipeline events can be parsed by log
aggregators. ``log_stage_event`` captures the standard operational fields for a
pipeline stage. Secrets are redacted from any free-text error before logging.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import UTC, datetime

# Patterns for values that must never reach logs.
_REDACTION_PATTERNS = [
    re.compile(r"AKIA[0-9A-Z]{16}"),  # AWS access key id
    re.compile(
        r"(?i)(secret|password|passwd|token|api[_-]?key|aws_secret_access_key)"
        r"(\s*[=:]\s*)(\S+)"
    ),
]

_REDACTED = "***REDACTED***"


def redact_text(text: str | None) -> str | None:
    """Mask secret-like substrings in free text."""
    if not text:
        return text
    result = _REDACTION_PATTERNS[0].sub(_REDACTED, text)
    result = _REDACTION_PATTERNS[1].sub(rf"\1\2{_REDACTED}", result)
    return result


class JsonLogFormatter(logging.Formatter):
    """Format log records as single-line JSON."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        fields = getattr(record, "fields", None)
        if isinstance(fields, dict):
            payload.update(fields)
        return json.dumps(payload, sort_keys=True, default=str)


def get_structured_logger(name: str = "lakehouse") -> logging.Logger:
    """Return a logger that emits structured JSON to stderr (idempotent setup)."""
    logger = logging.getLogger(name)
    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        handler = logging.StreamHandler()
        handler.setFormatter(JsonLogFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


def log_stage_event(
    logger: logging.Logger,
    *,
    stage: str,
    pipeline_run_id: str,
    ingestion_run_id: str | None = None,
    duration_seconds: float | None = None,
    input_records: int | None = None,
    output_records: int | None = None,
    quarantined_records: int | None = None,
    duplicate_records: int | None = None,
    retry_count: int | None = None,
    failure_category: str | None = None,
    error: str | None = None,
    level: int = logging.INFO,
) -> dict:
    """Emit a structured stage event; returns the fields logged (for testing)."""
    fields = {
        "stage": stage,
        "pipeline_run_id": pipeline_run_id,
        "ingestion_run_id": ingestion_run_id,
        "duration_seconds": duration_seconds,
        "input_records": input_records,
        "output_records": output_records,
        "quarantined_records": quarantined_records,
        "duplicate_records": duplicate_records,
        "retry_count": retry_count,
        "failure_category": failure_category,
        "error": redact_text(error),
    }
    fields = {k: v for k, v in fields.items() if v is not None}
    logger.log(level, "stage_event", extra={"fields": fields})
    return fields
