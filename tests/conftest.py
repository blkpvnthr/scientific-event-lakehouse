"""Shared pytest fixtures for Spark-based tests.

A single session-scoped SparkSession keeps the suite fast (one JVM startup).
The ``synthetic_run`` helper builds a tiny but structurally faithful raw
ingestion run (archive files + manifests) so Bronze processing can be tested
end-to-end without the network or the full historical dataset.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from src.config import Settings
from src.ingestion.archive import atomic_write, calculate_checksum


@pytest.fixture(scope="session")
def spark():
    """Session-scoped local SparkSession for tests."""
    from src.spark import build_spark_session, stop_spark_session

    # Delta-enabled so Delta-table tests work; a superset for non-Delta tests.
    session = build_spark_session(
        "lakehouse-tests", with_delta=True, shuffle_partitions=2
    )
    yield session
    stop_spark_session(session)


@pytest.fixture
def tmp_settings(tmp_path: Path) -> Settings:
    """A Settings instance rooted at a temporary directory."""
    data = tmp_path / "data"
    warehouse = data / "warehouse"
    base = Settings(
        raw_bronze_path=data / "bronze",
        manifest_path=data / "manifests",
        profile_path=data / "profiles",
        warehouse_path=warehouse,
        bronze_path=warehouse / "bronze",
        silver_path=warehouse / "silver",
        gold_path=warehouse / "gold",
        metadata_path=warehouse / "metadata",
        delta_path=warehouse / "delta",
        java_home="/opt/homebrew/opt/openjdk@17",
        spark_master="local[*]",
        spark_shuffle_partitions=2,
        spark_local_ip="127.0.0.1",
    )
    return base


def _make_feature(event_id: str, time_ms: int, mag: float, coords: list) -> dict:
    """Build one GeoJSON earthquake feature."""
    return {
        "type": "Feature",
        "id": event_id,
        "properties": {
            "mag": mag,
            "place": f"test place {event_id}",
            "time": time_ms,
            "updated": time_ms + 1000,
            "tz": None,
            "url": f"https://example.test/{event_id}",
            "detail": "https://example.test/detail",
            "felt": None,
            "cdi": None,
            "mmi": None,
            "alert": None,
            "status": "reviewed",
            "tsunami": 0,
            "sig": int(mag * 100),
            "net": "us",
            "code": event_id,
            "ids": f",{event_id},",
            "sources": ",us,",
            "types": ",origin,",
            "nst": 12,
            "dmin": 0.5,
            "rms": 0.8,
            "gap": 90.0,
            "magType": "mb",
            "type": "earthquake",
            "title": f"M {mag} - test {event_id}",
        },
        "geometry": {"type": "Point", "coordinates": coords},
    }


def write_synthetic_run(
    settings: Settings,
    run_id: str = "a" * 32,
    windows: dict | None = None,
) -> str:
    """Write a faithful raw run (archive files + manifests) under settings.

    ``windows`` maps window_start (YYYY-MM-DD) -> list of (event_id, time_ms,
    mag, [lon, lat, depth]) tuples. Defaults to two windows across two years.
    """
    if windows is None:
        windows = {
            "2015-01-01": [
                ("ev1", 1420070400000, 2.5, [-66.4, 18.4, 105.1]),
                ("ev2", 1420156800000, 4.2, [140.0, 35.0, 10.0]),
            ],
            "2016-06-01": [
                ("ev3", 1464739200000, 5.1, [-120.0, 38.0, 8.5]),
            ],
        }

    ingestion_date = "2026-11-22"
    ingestion_time = datetime.now(UTC).isoformat()
    run_manifest_dir = settings.manifest_path / f"run_id={run_id}"
    window_files: list[str] = []
    total_records = 0
    total_pages = 0

    for window_start, events in windows.items():
        features = [_make_feature(*e) for e in events]
        payload = json.dumps(
            {
                "type": "FeatureCollection",
                "metadata": {
                    "generated": 0,
                    "url": "https://example.test/query",
                    "title": "test",
                    "status": 200,
                    "count": len(features),
                    "limit": 1000,
                    "offset": 1,
                },
                "features": features,
            }
        ).encode("utf-8")

        archive_path = (
            settings.raw_bronze_path
            / f"ingestion_date={ingestion_date}"
            / f"run_id={run_id}"
            / f"window_start={window_start}"
            / "offset_00000001.json"
        )
        atomic_write(archive_path, payload)

        page = {
            "source_url": "https://example.test/query",
            "source_file": str(archive_path),
            "window_start": f"{window_start}T00:00:00.000+00:00",
            "window_end": f"{window_start}T23:59:59.999+00:00",
            "ingestion_time": ingestion_time,
            "min_magnitude": 2.5,
            "page_limit": 1000,
            "offset": 1,
            "record_count": len(features),
            "file_size_bytes": len(payload),
            "sha256": calculate_checksum(payload),
        }
        manifest = {
            "run_id": run_id,
            "window_start": f"{window_start}T00:00:00.000+00:00",
            "window_end": f"{window_start}T23:59:59.999+00:00",
            "expected_count_before": len(features),
            "expected_count_after": len(features),
            "actual_count": len(features),
            "unique_event_count": len(features),
            "status": "SUCCESS",
            "pages": [page],
        }
        window_file = f"{window_start}.json"
        atomic_write(
            run_manifest_dir / window_file,
            json.dumps(manifest, indent=2).encode("utf-8"),
        )
        window_files.append(window_file)
        total_records += len(features)
        total_pages += 1

    completion = {
        "run_id": run_id,
        "status": "SUCCESS",
        "start_date": "2015-01-01",
        "end_date_exclusive": "2017-01-01",
        "window_days": 30,
        "min_magnitude": 2.5,
        "page_size": 1000,
        "started_at": ingestion_time,
        "finished_at": ingestion_time,
        "duration_seconds": 1.0,
        "total_windows": len(window_files),
        "total_records": total_records,
        "total_pages": total_pages,
        "window_manifests": window_files,
    }
    atomic_write(
        run_manifest_dir / "_SUCCESS.json",
        json.dumps(completion, indent=2).encode("utf-8"),
    )
    return run_id


@pytest.fixture
def synthetic_run(tmp_settings: Settings):
    """Create a synthetic run and return (settings, run_id)."""
    run_id = write_synthetic_run(tmp_settings)
    return tmp_settings, run_id


__all__ = ["spark", "synthetic_run", "tmp_settings", "write_synthetic_run"]
