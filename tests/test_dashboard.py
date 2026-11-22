"""Tests for the dashboard data-access layer.

Verifies the dashboard consumes actual Gold datasets (not hardcoded values) and
that filters behave correctly. Builds a small Gold layer from a synthetic current
Delta table so the test is self-contained.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd

from dashboard import data
from src.gold import build_gold


def _seed_gold(spark, settings):
    t = datetime(2015, 6, 1, tzinfo=UTC)
    rows = [
        {
            "event_id": f"e{i}",
            "event_time": t,
            "updated_time": t,
            "magnitude": 3.0 + i,
            "magnitude_type": "mb",
            "magnitude_type_normalized": "mb",
            "place": "r",
            "place_normalized": "r",
            "longitude": -118.0 + i,
            "latitude": 34.0 + i,
            "depth_km": 10.0 * i - 5,
            "event_type": "earthquake",
            "event_type_normalized": "earthquake",
            "network": "ci" if i % 2 else "us",
            "significance": 100 * i,
            "felt": 0,
            "cdi": 0.0,
            "mmi": 0.0,
            "tsunami": 0,
            "station_count": 10,
            "azimuthal_gap": 90.0,
            "min_station_distance_deg": 0.5,
            "rms": 0.8,
            "event_year": 2015,
        }
        for i in range(1, 5)
    ]
    df = spark.createDataFrame(rows)
    df.write.format("delta").save(
        str(settings.delta_table_path("silver", "earthquake_events_current"))
    )
    dq = spark.createDataFrame(
        [
            {
                "run_id": "r1",
                "rule_id": "id_present",
                "severity": "fatal",
                "observed_failure_count": 0,
                "executed_at": "2026-01-01T00:00:00",
            }
        ]
    )
    dq.write.format("delta").save(
        str(settings.delta_table_path("silver", "data_quality_results"))
    )
    build_gold(spark, settings)


def test_dashboard_loads_real_gold(spark, tmp_settings):
    _seed_gold(spark, tmp_settings)
    fact = data.load_fact(tmp_settings)
    assert len(fact) == 4
    annual = data.load_dataset("annual_seismic_activity", tmp_settings)
    assert not annual.empty
    assert int(annual["event_count"].sum()) == 4
    summary = data.dataset_summary(tmp_settings)
    assert summary["total_events"] == 4
    assert "fact_earthquake_event" in summary["datasets_available"]


def test_dashboard_filters(spark, tmp_settings):
    _seed_gold(spark, tmp_settings)
    fact = data.load_fact(tmp_settings)
    # Magnitude filter.
    hi = data.filter_fact(fact, magnitude_range=(6.0, 10.0))
    assert (hi["magnitude"] >= 6.0).all()
    # Network filter.
    us = data.filter_fact(fact, networks=["us"])
    assert set(us["network_key"].unique()) <= {"us"}
    # Empty frame handled.
    assert data.filter_fact(pd.DataFrame(), networks=["us"]).empty


def test_missing_gold_returns_empty(tmp_settings):
    assert data.load_dataset("nonexistent", tmp_settings).empty
    assert data.dataset_summary(tmp_settings)["total_events"] == 0
