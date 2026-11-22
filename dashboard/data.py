"""Data-access layer for the analytics dashboard.

Reads the **actual** Gold Parquet datasets (and the Delta current table for
filtered exploration) with pandas/pyarrow — no hardcoded numbers, and no Spark
dependency for the UI. Kept separate from the Streamlit app so it is unit-testable.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.config import Settings, get_settings


def gold_path(name: str, settings: Settings | None = None) -> Path:
    settings = settings or get_settings()
    return settings.gold_path / name


def available_datasets(settings: Settings | None = None) -> list[str]:
    """List Gold datasets present on disk."""
    settings = settings or get_settings()
    root = settings.gold_path
    if not root.exists():
        return []
    return sorted(p.name for p in root.iterdir() if p.is_dir())


def load_dataset(name: str, settings: Settings | None = None) -> pd.DataFrame:
    """Load a Gold dataset as a pandas DataFrame (empty if absent)."""
    path = gold_path(name, settings)
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def load_fact(settings: Settings | None = None) -> pd.DataFrame:
    """Load the fact table for filtered exploration."""
    return load_dataset("fact_earthquake_event", settings)


def filter_fact(
    fact: pd.DataFrame,
    *,
    magnitude_range: tuple[float, float] | None = None,
    depth_range: tuple[float, float] | None = None,
    event_types: list[str] | None = None,
    networks: list[str] | None = None,
    date_range: tuple[str, str] | None = None,
) -> pd.DataFrame:
    """Apply dashboard filters to the fact table. Pure, side-effect-free."""
    df = fact
    if magnitude_range and "magnitude" in df:
        lo, hi = magnitude_range
        df = df[(df["magnitude"] >= lo) & (df["magnitude"] <= hi)]
    if depth_range and "depth_km" in df:
        lo, hi = depth_range
        df = df[(df["depth_km"] >= lo) & (df["depth_km"] <= hi)]
    if event_types and "event_type_key" in df:
        df = df[df["event_type_key"].isin(event_types)]
    if networks and "network_key" in df:
        df = df[df["network_key"].isin(networks)]
    if date_range and "event_time" in df:
        start, end = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
        ts = pd.to_datetime(df["event_time"])
        df = df[(ts >= start) & (ts <= end)]
    return df


def dataset_summary(settings: Settings | None = None) -> dict:
    """Small provenance/scale summary for the dashboard header."""
    fact = load_fact(settings)
    return {
        "total_events": len(fact),
        "datasets_available": available_datasets(settings),
        "min_magnitude_filter": 2.5,
        "note": (
            "Summaries of a magnitude >= 2.5 observational catalog; reflect "
            "catalog coverage and reporting, not earthquake prediction."
        ),
    }
