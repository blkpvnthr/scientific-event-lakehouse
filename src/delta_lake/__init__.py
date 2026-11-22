"""Transactional Delta Lake tables for the Silver layer and pipeline metadata."""

from src.delta_lake.merge import (
    insert_revisions,
    latest_per_event,
    table_version,
    upsert_current,
    upsert_quarantine,
)
from src.delta_lake.metadata import record_pipeline_run, update_watermark

__all__ = [
    "insert_revisions",
    "latest_per_event",
    "record_pipeline_run",
    "table_version",
    "update_watermark",
    "upsert_current",
    "upsert_quarantine",
]
