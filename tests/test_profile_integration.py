import json
from pathlib import Path
from unittest.mock import patch

import pytest

from src.ingestion.backfill import backfill
from src.profiling.profile_bronze import (
    ProfileError,
    profile_run,
    write_profile,
)


def make_feature(
    event_id="event1",
    magnitude=3.5,
    timestamp=1704067200000,
):
    return {
        "type": "Feature",
        "id": event_id,
        "properties": {
            "time": timestamp,
            "mag": magnitude,
            "place": "Test location",
            "updated": timestamp + 1000,
            "magType": "ml",
            "type": "earthquake",
        },
        "geometry": {
            "type": "Point",
            "coordinates": [-120.5, 35.5, 10.0],
        },
    }


def make_payload(features):
    return json.dumps(
        {
            "type": "FeatureCollection",
            "metadata": {
                "status": 200,
                "url": ("https://earthquake.usgs.gov/fdsnws/event/1/query"),
            },
            "features": features,
        }
    ).encode("utf-8")


@pytest.fixture
def storage(tmp_path, monkeypatch):
    bronze = tmp_path / "bronze"
    manifests = tmp_path / "manifests"
    profiles = tmp_path / "profiles"

    monkeypatch.setattr(
        "src.ingestion.backfill.BRONZE_PATH",
        bronze,
    )

    monkeypatch.setattr(
        "src.ingestion.backfill.MANIFEST_PATH",
        manifests,
    )

    monkeypatch.setattr(
        "src.profiling.profile_bronze.MANIFEST_PATH",
        manifests,
    )

    monkeypatch.setattr(
        "src.profiling.profile_bronze.PROFILE_PATH",
        profiles,
    )

    return {
        "bronze": bronze,
        "manifests": manifests,
        "profiles": profiles,
    }


def create_run(features):
    count = len(features)

    with (
        patch(
            "src.ingestion.backfill.count_earthquakes",
            side_effect=[count, count],
        ),
        patch(
            "src.ingestion.backfill.fetch_earthquake_page",
            return_value=make_payload(features),
        ),
    ):
        return backfill(
            start_date="2024-01-01",
            end_date="2024-01-02",
            window_days=1,
            page_size=100,
        )


@pytest.fixture
def completed_run(storage):
    features = [
        make_feature("event1", 3.0),
        make_feature("event2", 4.0),
        make_feature("event3", 5.0),
    ]

    run_id = create_run(features)

    directory = storage["manifests"] / f"run_id={run_id}"

    manifest_path = directory / "2024-01-01.json"

    manifest = json.loads(manifest_path.read_bytes())

    archive_path = Path(manifest["pages"][0]["source_file"])

    return {
        "run_id": run_id,
        "directory": directory,
        "manifest_path": manifest_path,
        "archive_path": archive_path,
    }


# ============================================================
# Successful Execution Tests
# ============================================================


def test_completed_run(completed_run):
    profile = profile_run(completed_run["run_id"])

    assert profile["integrity_status"] == "PASS"

    volume = profile["volume"]

    assert volume["windows"] == 1
    assert volume["files"] == 1
    assert volume["records"] == 3
    assert volume["unique_events"] == 3
    assert volume["duplicate_records"] == 0

    magnitude = profile["statistics"]["magnitude"]

    assert magnitude["count"] == 3
    assert magnitude["min"] == 3.0
    assert magnitude["max"] == 5.0
    assert magnitude["mean"] == 4.0
    assert magnitude["median"] == 4.0

    assert profile["distributions"]["event_types"] == {"earthquake": 3}

    assert profile["distributions"]["magnitude_types"] == {"ml": 3}


def test_repeated_profiling_preserves_reports(
    completed_run,
):
    profile = profile_run(completed_run["run_id"])

    first_path = write_profile(profile)
    second_path = write_profile(profile)

    assert first_path != second_path

    assert first_path.exists()
    assert second_path.exists()

    assert first_path.read_bytes() == second_path.read_bytes()


# ============================================================
# Failure-Handling Tests
# ============================================================


def test_missing_completion_marker(completed_run):
    completion_path = completed_run["directory"] / "_SUCCESS.json"

    completion_path.unlink()

    with pytest.raises(
        ProfileError,
        match="incomplete",
    ):
        profile_run(completed_run["run_id"])


def test_missing_archive(completed_run):
    completed_run["archive_path"].unlink()

    with pytest.raises(
        ProfileError,
        match="Archive missing",
    ):
        profile_run(completed_run["run_id"])


def test_corrupted_archive(completed_run):
    path = completed_run["archive_path"]

    payload = bytearray(path.read_bytes())

    payload[20] ^= 1

    path.write_bytes(payload)

    with pytest.raises(
        ProfileError,
        match="Checksum mismatch",
    ):
        profile_run(completed_run["run_id"])


def test_file_size_mismatch(completed_run):
    path = completed_run["archive_path"]

    path.write_bytes(path.read_bytes() + b"corrupted")

    with pytest.raises(
        ProfileError,
        match="File-size mismatch",
    ):
        profile_run(completed_run["run_id"])


def test_invalid_page_offset(completed_run):
    path = completed_run["manifest_path"]

    manifest = json.loads(path.read_bytes())

    manifest["pages"][0]["offset"] = 10

    path.write_text(json.dumps(manifest))

    with pytest.raises(
        ProfileError,
        match="Pagination mismatch",
    ):
        profile_run(completed_run["run_id"])


def test_invalid_record_count(completed_run):
    path = completed_run["manifest_path"]

    manifest = json.loads(path.read_bytes())

    manifest["pages"][0]["record_count"] = 999

    path.write_text(json.dumps(manifest))

    with pytest.raises(
        ProfileError,
        match="Page record-count mismatch",
    ):
        profile_run(completed_run["run_id"])


def test_duplicate_archive_reference(completed_run):
    path = completed_run["manifest_path"]

    manifest = json.loads(path.read_bytes())

    manifest["pages"].append(manifest["pages"][0].copy())

    path.write_text(json.dumps(manifest))

    with pytest.raises(
        ProfileError,
        match="Repeated archive reference",
    ):
        profile_run(completed_run["run_id"])


# ============================================================
# Data Quality Edge Cases
# ============================================================


def test_empty_dataset(storage):
    run_id = create_run([])

    profile = profile_run(run_id)

    assert profile["integrity_status"] == "PASS"
    assert profile["volume"]["records"] == 0
    assert profile["volume"]["files"] == 0
    assert profile["volume"]["unique_events"] == 0

    assert profile["coverage"]["earliest_event"] is None

    assert profile["statistics"]["magnitude"]["mean"] is None

    assert all(
        value is None for value in (profile["quality"]["missing_percentages"].values())
    )


def test_missing_optional_fields(storage):
    feature = make_feature()

    feature["properties"]["mag"] = None
    feature["properties"]["place"] = None
    feature["properties"]["updated"] = None
    feature["properties"]["magType"] = None

    run_id = create_run([feature])

    profile = profile_run(run_id)

    missing = profile["quality"]["missing_fields"]

    assert missing["mag"] == 1
    assert missing["place"] == 1
    assert missing["updated"] == 1
    assert missing["magType"] == 1

    assert profile["quality"]["missing_percentages"]["mag"] == 100.0

    assert profile["statistics"]["magnitude"]["count"] == 0


def test_invalid_coordinates(storage):
    feature = make_feature()

    feature["geometry"]["coordinates"] = [
        200.0,
        95.0,
        10.0,
    ]

    run_id = create_run([feature])

    profile = profile_run(run_id)

    assert profile["quality"]["missing_fields"]["coordinates"] == 1

    assert profile["statistics"]["longitude"]["count"] == 0

    assert profile["statistics"]["latitude"]["count"] == 0


def test_cross_window_duplicate_ids(storage):
    first_feature = make_feature(
        event_id="duplicate-event",
        timestamp=1704067200000,
    )

    second_feature = make_feature(
        event_id="duplicate-event",
        timestamp=1704153600000,
    )

    with (
        patch(
            "src.ingestion.backfill.count_earthquakes",
            side_effect=[1, 1, 1, 1],
        ),
        patch(
            "src.ingestion.backfill.fetch_earthquake_page",
            side_effect=[
                make_payload([first_feature]),
                make_payload([second_feature]),
            ],
        ),
    ):
        run_id = backfill(
            start_date="2024-01-01",
            end_date="2024-01-03",
            window_days=1,
            page_size=100,
        )

    profile = profile_run(run_id)

    assert profile["integrity_status"] == "FAIL"
    assert profile["volume"]["records"] == 2
    assert profile["volume"]["unique_events"] == 1
    assert profile["volume"]["duplicate_records"] == 1
