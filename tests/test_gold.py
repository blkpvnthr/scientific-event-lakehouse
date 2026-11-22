"""Tests for the Gold dimensional model, datasets, and analytics SQL."""

from __future__ import annotations

from datetime import UTC, datetime

from src.gold import GOLD_DATASETS, build_gold
from src.gold.queries import list_queries, register_views, run_query_file


def _current_row(event_id, t, mag, lat, lon, depth, net, sig, year):
    return {
        "event_id": event_id,
        "event_time": t,
        "updated_time": t,
        "magnitude": mag,
        "magnitude_type": "mb",
        "magnitude_type_normalized": "mb",
        "place": "region",
        "place_normalized": "region",
        "longitude": lon,
        "latitude": lat,
        "depth_km": depth,
        "event_type": "earthquake",
        "event_type_normalized": "earthquake",
        "network": net,
        "significance": sig,
        "felt": 0,
        "cdi": 0.0,
        "mmi": 0.0,
        "tsunami": 0,
        "station_count": 10,
        "azimuthal_gap": 90.0,
        "min_station_distance_deg": 0.5,
        "rms": 0.8,
        "event_year": year,
    }


def _setup_delta(spark, settings):
    t2015 = datetime(2015, 6, 1, tzinfo=UTC)
    t2016 = datetime(2016, 6, 1, tzinfo=UTC)
    current = spark.createDataFrame(
        [
            _current_row("e1", t2015, 3.0, 10.0, 20.0, 5.0, "us", 300, 2015),
            _current_row("e2", t2015, 6.5, 11.0, 21.0, -3.0, "us", 700, 2015),
            _current_row("e3", t2016, 4.0, 35.0, -118.0, 400.0, "ci", 250, 2016),
        ]
    )
    current.write.format("delta").save(
        str(settings.delta_table_path("silver", "earthquake_events_current"))
    )
    current.write.format("delta").save(
        str(settings.delta_table_path("silver", "earthquake_event_revisions"))
    )
    dq = spark.createDataFrame(
        [
            {
                "run_id": "r1",
                "rule_id": "id_present",
                "severity": "fatal",
                "observed_failure_count": 0,
                "executed_at": "2026-01-01T00:00:00",
            },
            {
                "run_id": "r1",
                "rule_id": "magnitude_type_present",
                "severity": "warning",
                "observed_failure_count": 1,
                "executed_at": "2026-01-01T00:00:00",
            },
        ]
    )
    dq.write.format("delta").save(
        str(settings.delta_table_path("silver", "data_quality_results"))
    )


def test_build_gold_reconciles(spark, tmp_settings):
    _setup_delta(spark, tmp_settings)
    result = build_gold(spark, tmp_settings)
    assert result.status == "PASS"
    assert result.reconciled is True
    assert result.current_events == 3
    assert result.fact_rows == 3
    # All declared datasets were produced.
    assert set(result.datasets) == set(GOLD_DATASETS)
    assert result.datasets["annual_seismic_activity"] == 2
    assert result.datasets["significant_events"] == 1  # only e2 (M6.5 / sig700)


def test_gold_depth_and_magnitude_datasets(spark, tmp_settings):
    _setup_delta(spark, tmp_settings)
    build_gold(spark, tmp_settings)
    depth = spark.read.parquet(str(tmp_settings.gold_path / "depth_distribution"))
    cats = {r["depth_category"] for r in depth.collect()}
    assert "above_datum" in cats  # negative depth bucket present
    mag = spark.read.parquet(str(tmp_settings.gold_path / "magnitude_distribution"))
    assert mag.count() >= 1


def test_analytics_sql_all_run(spark, tmp_settings):
    _setup_delta(spark, tmp_settings)
    register_views(spark, tmp_settings)
    queries = list_queries()
    assert len(queries) == 10
    for q in queries:
        df = run_query_file(spark, q)
        # Every query must execute and return a (possibly single-row) result.
        assert df.count() >= 0
        assert len(df.columns) > 0
