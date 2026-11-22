"""Recovery and replay helpers.

Replay is safe because every pipeline stage is idempotent: Bronze overwrites by
partition after checksum verification, and Delta merges are keyed on
``(event_id, content_fingerprint)``. These helpers detect which runs are
replayable and guard the watermark against regression. They are deliberately
light (no Spark) so they can run in monitoring/alerting contexts.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from src.config import Settings, get_settings


@dataclass(frozen=True)
class RunStatus:
    run_id: str
    has_success_marker: bool
    window_count: int
    total_records: int


def detect_incomplete_run(run_id: str, settings: Settings | None = None) -> bool:
    """Return True if a run has no valid ``_SUCCESS`` completion marker."""
    settings = settings or get_settings()
    marker = settings.run_manifest_dir(run_id) / "_SUCCESS.json"
    if not marker.is_file():
        return True
    try:
        data = json.loads(marker.read_bytes())
    except (json.JSONDecodeError, OSError):
        return True
    return data.get("status") != "SUCCESS" or data.get("run_id") != run_id


def find_replayable_runs(settings: Settings | None = None) -> list[RunStatus]:
    """List archived runs that completed and can be safely replayed."""
    settings = settings or get_settings()
    manifest_root = settings.manifest_path
    runs: list[RunStatus] = []
    if not manifest_root.exists():
        return runs
    for run_dir in sorted(manifest_root.glob("run_id=*")):
        run_id = run_dir.name.split("=", 1)[1]
        marker = run_dir / "_SUCCESS.json"
        if not marker.is_file():
            runs.append(RunStatus(run_id, False, 0, 0))
            continue
        data = json.loads(marker.read_bytes())
        runs.append(
            RunStatus(
                run_id=run_id,
                has_success_marker=data.get("status") == "SUCCESS",
                window_count=data.get("total_windows", 0),
                total_records=data.get("total_records", 0),
            )
        )
    return runs


def is_bronze_output_present(run_id: str, settings: Settings | None = None) -> bool:
    """Return True if structured Bronze output exists for a run.

    A completed manifest with no Bronze output indicates an interrupted pipeline
    that should be replayed.
    """
    settings = settings or get_settings()
    bronze = settings.bronze_path
    if not bronze.exists():
        return False
    # Any parquet partition file indicates output was written.
    return any(bronze.rglob("*.parquet"))


def watermark_would_regress(current: str | None, incoming: str | None) -> bool:
    """Return True if applying ``incoming`` would move the watermark backwards.

    Used to protect the committed watermark during replay / out-of-order runs.
    A None incoming never advances; a None current is always safe to set.
    """
    if incoming is None:
        return False
    if current is None:
        return False
    return incoming < current
