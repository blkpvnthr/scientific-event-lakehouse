"""Integration tests for the Silver builder."""

from __future__ import annotations

from datetime import UTC, datetime

from src.silver import build_silver, normalize_bronze

_RUN = "c" * 32


def _bronze_rows():
    """Synthetic Bronze rows covering revisions, quarantine, and valid edges."""
    base = {
        "magnitude_type": "MB",
        "place": "test region",
        "event_type": "earthquake",
        "status": "REVIEWED",
        "source_file": "data/bronze/x/offset_00000001.json",
        "source_checksum": "deadbeef",
        "ingestion_run_id": _RUN,
        "extraction_window": "2015-01-01",
    }
    t0 = datetime(2015, 1, 1, tzinfo=UTC)
    t1 = datetime(2015, 1, 2, tzinfo=UTC)
    t2 = datetime(2015, 1, 3, tzinfo=UTC)
    return [
        # single valid event
        {
            **base,
            "event_id": "ev_a",
            "event_time": t0,
            "updated_time": t0,
            "magnitude": 3.0,
            "latitude": 10.0,
            "longitude": 20.0,
            "depth_km": 5.0,
            "event_year": 2015,
        },
        # two revisions of the same event (t1 then t2 -> t2 is current)
        {
            **base,
            "event_id": "ev_b",
            "event_time": t0,
            "updated_time": t1,
            "magnitude": 4.0,
            "latitude": 11.0,
            "longitude": 21.0,
            "depth_km": 6.0,
            "event_year": 2015,
        },
        {
            **base,
            "event_id": "ev_b",
            "event_time": t0,
            "updated_time": t2,
            "magnitude": 4.5,
            "latitude": 11.0,
            "longitude": 21.0,
            "depth_km": 6.0,
            "event_year": 2015,
        },
        # valid negative-depth event (must NOT be quarantined)
        {
            **base,
            "event_id": "ev_neg",
            "event_time": t0,
            "updated_time": t0,
            "magnitude": 2.8,
            "latitude": 12.0,
            "longitude": 22.0,
            "depth_km": -3.5,
            "event_year": 2015,
        },
        # invalid coordinates -> quarantined
        {
            **base,
            "event_id": "ev_bad",
            "event_time": t0,
            "updated_time": t0,
            "magnitude": 5.0,
            "latitude": 999.0,
            "longitude": 22.0,
            "depth_km": 7.0,
            "event_year": 2015,
        },
    ]


def _write_bronze(spark, settings):
    df = spark.createDataFrame(_bronze_rows())
    (
        df.write.mode("overwrite")
        .partitionBy("event_year")
        .parquet(str(settings.bronze_path))
    )


def test_silver_reconciles_and_quarantines(spark, tmp_settings):
    _write_bronze(spark, tmp_settings)
    result = build_silver(spark, _RUN, tmp_settings)

    assert result.status == "PASS"
    assert result.reconciled is True
    assert result.input_records == 5
    assert result.valid_records == 4
    assert result.quarantined_records == 1
    assert result.current_records == 3  # ev_a, ev_b, ev_neg
    assert result.superseded_records == 1  # older ev_b


def test_silver_revision_selection_picks_latest(spark, tmp_settings):
    _write_bronze(spark, tmp_settings)
    build_silver(spark, _RUN, tmp_settings, write_report=False)
    current = spark.read.parquet(
        str(tmp_settings.silver_path / "earthquake_events_current")
    )
    ev_b = current.filter(current.event_id == "ev_b").collect()
    assert len(ev_b) == 1
    # The current revision is the one with the greatest updated_time (mag 4.5).
    assert ev_b[0]["magnitude"] == 4.5
    assert ev_b[0]["is_current"] is True


def test_silver_quarantine_contents(spark, tmp_settings):
    _write_bronze(spark, tmp_settings)
    build_silver(spark, _RUN, tmp_settings, write_report=False)
    q = spark.read.parquet(
        str(tmp_settings.silver_path / "quarantined_events")
    ).collect()
    assert len(q) == 1
    row = q[0]
    assert row["event_id"] == "ev_bad"
    assert "coordinates_in_range" in row["failed_rules"]
    assert row["silver_run_id"] == _RUN


def test_silver_normalization_collapses_status(spark, tmp_settings):
    _write_bronze(spark, tmp_settings)
    normalized = normalize_bronze(spark.read.parquet(str(tmp_settings.bronze_path)))
    statuses = {
        r["status_normalized"]
        for r in normalized.select("status_normalized").distinct().collect()
    }
    assert statuses == {"reviewed"}  # all "REVIEWED" folded to lowercase
    # Fingerprint is deterministic and present.
    assert normalized.filter("content_fingerprint IS NULL").count() == 0


def test_silver_idempotent(spark, tmp_settings):
    _write_bronze(spark, tmp_settings)
    first = build_silver(spark, _RUN, tmp_settings, write_report=False)
    second = build_silver(spark, _RUN, tmp_settings, write_report=False)
    assert first.current_records == second.current_records == 3
    current = spark.read.parquet(
        str(tmp_settings.silver_path / "earthquake_events_current")
    )
    assert current.count() == 3
