"""Pipeline metrics collection.

Derives operational metrics from stage results and times stages. Metrics are
plain dicts so they can be logged, persisted as JSON, or shipped to a metrics
backend without coupling to any one system.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Self


@dataclass
class StageTimer:
    """Context manager measuring a stage's wall-clock duration.

    Uses a monotonic clock (no dependency on Date.now-style wall time), so it is
    safe and deterministic to use anywhere.
    """

    name: str
    duration_seconds: float = field(default=0.0, init=False)
    _start: float = field(default=0.0, init=False)

    def __enter__(self) -> Self:
        self._start = time.monotonic()
        return self

    def __exit__(self, *exc) -> None:
        self.duration_seconds = round(time.monotonic() - self._start, 3)


def _throughput(records: int | None, seconds: float | None) -> float | None:
    if not records or not seconds:
        return None
    return round(records / seconds, 2)


def compute_pipeline_metrics(stages: dict[str, dict]) -> dict:
    """Compute summary metrics from per-stage result dicts.

    Each stage dict may include ``records`` / ``output_records`` /
    ``valid`` / ``quarantined`` and ``duration_seconds``.
    """
    metrics: dict = {"stages": {}, "totals": {}}
    total_duration = 0.0
    total_output = 0
    total_quarantined = 0

    for name, result in stages.items():
        duration = result.get("duration_seconds")
        output = (
            result.get("output_records")
            or result.get("records")
            or result.get("valid")
            or result.get("current")
        )
        quarantined = result.get("quarantined") or result.get("quarantined_records")
        metrics["stages"][name] = {
            "duration_seconds": duration,
            "output_records": output,
            "throughput_records_per_second": _throughput(output, duration),
            "quarantined_records": quarantined,
        }
        total_duration += duration or 0.0
        total_output = max(total_output, output or 0)
        total_quarantined += quarantined or 0

    quarantine_rate = (
        round(total_quarantined / total_output * 100, 4) if total_output else 0.0
    )
    metrics["totals"] = {
        "total_duration_seconds": round(total_duration, 3),
        "max_stage_output_records": total_output,
        "total_quarantined_records": total_quarantined,
        "quarantine_rate_pct": quarantine_rate,
    }
    return metrics
