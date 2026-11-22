"""Tests for the distributed Bronze builder."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from conftest import write_synthetic_run

from src.bronze import (
    build_bronze,
    discover_run_files,
    verify_checksums,
)
from src.bronze.build_bronze import BRONZE_COLUMNS


def test_discover_run_files(synthetic_run):
    settings, run_id = synthetic_run
    files = discover_run_files(run_id, settings)
    assert len(files) == 2  # two windows, one page each
    assert {f.window_start for f in files} == {"2015-01-01", "2016-06-01"}
    assert all(f.sha256 for f in files)
    assert all(f.ingestion_date == "2026-11-22" for f in files)


def test_discover_rejects_incomplete_run(tmp_settings):
    with pytest.raises(ValueError, match="incomplete"):
        discover_run_files("b" * 32, tmp_settings)


def test_verify_checksums_passes(synthetic_run):
    settings, run_id = synthetic_run
    files = discover_run_files(run_id, settings)
    verify_checksums(files)  # should not raise


def test_verify_checksums_detects_corruption(synthetic_run):
    settings, run_id = synthetic_run
    files = discover_run_files(run_id, settings)
    # Corrupt one archive file in place.
    corrupt = Path(files[0].source_file)
    corrupt.unlink()
    corrupt.write_bytes(b'{"type":"FeatureCollection","features":[]}')
    with pytest.raises(ValueError, match="Checksum mismatch|File-size mismatch"):
        verify_checksums(files)


def test_verify_checksums_detects_missing(synthetic_run):
    settings, run_id = synthetic_run
    files = discover_run_files(run_id, settings)
    Path(files[0].source_file).unlink()
    with pytest.raises(ValueError, match="Archive missing"):
        verify_checksums(files)


def test_build_bronze_end_to_end(spark, synthetic_run):
    settings, run_id = synthetic_run
    result = build_bronze(spark, run_id, settings)
    assert result.status == "PASS"
    assert result.output_records == 3
    assert result.unique_events == 3
    assert result.duplicate_events == 0
    assert result.distinct_source_files == 2
    # Reconciliation report was written.
    assert result.report_path is not None
    assert Path(result.report_path).is_file()


def test_bronze_columns_and_values(spark, synthetic_run):
    settings, run_id = synthetic_run
    build_bronze(spark, run_id, settings, write_report=False)
    df = spark.read.parquet(str(settings.bronze_path))

    assert set(df.columns) == set(BRONZE_COLUMNS)

    rows = {r["event_id"]: r for r in df.collect()}
    assert set(rows) == {"ev1", "ev2", "ev3"}

    ev1 = rows["ev1"]
    # Coordinates [lon, lat, depth] extracted correctly.
    assert ev1["longitude"] == pytest.approx(-66.4)
    assert ev1["latitude"] == pytest.approx(18.4)
    assert ev1["depth_km"] == pytest.approx(105.1)
    assert ev1["magnitude"] == pytest.approx(2.5)
    assert ev1["network"] == "us"
    assert ev1["event_type"] == "earthquake"
    # Epoch-millis -> UTC timestamp (2015-01-01). event_year is computed in
    # Spark with session tz=UTC; assert on it rather than the driver-local
    # Python datetime produced by collect().
    assert ev1["event_year"] == 2015
    spark_year = (
        df.filter(df.event_id == "ev1")
        .selectExpr("year(event_time) AS y")
        .collect()[0]["y"]
    )
    assert spark_year == 2015
    assert ev1["event_time"] is not None
    # Provenance preserved.
    assert ev1["ingestion_run_id"] == run_id
    assert ev1["source_checksum"]
    assert ev1["extraction_window"] == "2015-01-01"
    # Unavailable source fields are null, not fabricated.
    assert ev1["location_source"] is None
    assert ev1["magnitude_source"] is None


def test_bronze_partitioned_by_year(spark, synthetic_run):
    settings, run_id = synthetic_run
    build_bronze(spark, run_id, settings, write_report=False)
    partitions = {
        p.name
        for p in settings.bronze_path.iterdir()
        if p.is_dir() and p.name.startswith("event_year=")
    }
    assert partitions == {"event_year=2015", "event_year=2016"}


def test_bronze_idempotent_reprocessing(spark, synthetic_run):
    settings, run_id = synthetic_run
    first = build_bronze(spark, run_id, settings, write_report=False)
    second = build_bronze(spark, run_id, settings, write_report=False)
    assert first.output_records == second.output_records == 3
    assert second.duplicate_events == 0
    # No record multiplication across re-runs.
    df = spark.read.parquet(str(settings.bronze_path))
    assert df.count() == 3
    assert df.select("event_id").distinct().count() == 3


def test_bronze_reconciliation_catches_record_drift(spark, tmp_settings):
    """A manifest record_count that overstates the file must fail loudly."""
    run_id = write_synthetic_run(tmp_settings)
    # Tamper with a manifest so expected > actual (without touching the file).
    run_dir = tmp_settings.manifest_path / f"run_id={run_id}"
    window_file = run_dir / "2015-01-01.json"
    manifest = json.loads(window_file.read_bytes())
    manifest["pages"][0]["record_count"] = 999
    window_file.write_bytes(json.dumps(manifest).encode("utf-8"))

    with pytest.raises(ValueError, match="Record count mismatch"):
        build_bronze(spark, run_id, tmp_settings, write_report=False)
