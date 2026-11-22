"""Alerting hooks with local, testable implementations.

A minimal alerter interface decouples pipeline code from any specific alerting
backend (PagerDuty, SNS, Slack). ``LoggingAlerter`` is the safe local default;
``CollectingAlerter`` captures alerts in memory for tests. Production backends can
implement ``Alerter`` without changing calling code.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from src.monitoring.logging import redact_text


@dataclass(frozen=True)
class Alert:
    severity: str  # info | warning | critical
    title: str
    message: str
    context: dict


class Alerter(ABC):
    """Abstract alert sink."""

    @abstractmethod
    def send(self, alert: Alert) -> None: ...


class LoggingAlerter(Alerter):
    """Default alerter that logs alerts (secrets redacted)."""

    def __init__(self, logger: logging.Logger | None = None):
        self._logger = logger or logging.getLogger("lakehouse.alerts")

    def send(self, alert: Alert) -> None:
        self._logger.warning(
            "ALERT [%s] %s: %s",
            alert.severity,
            alert.title,
            redact_text(alert.message),
        )


@dataclass
class CollectingAlerter(Alerter):
    """Alerter that records alerts in memory (for tests / dry runs)."""

    alerts: list[Alert] = field(default_factory=list)

    def send(self, alert: Alert) -> None:
        self.alerts.append(alert)


def alert_on_failure(
    alerter: Alerter,
    *,
    stage: str,
    run_id: str,
    error: str,
    severity: str = "critical",
) -> None:
    """Convenience helper to raise a standardized failure alert."""
    alerter.send(
        Alert(
            severity=severity,
            title=f"Pipeline stage failed: {stage}",
            message=redact_text(error) or "",
            context={"stage": stage, "run_id": run_id},
        )
    )
