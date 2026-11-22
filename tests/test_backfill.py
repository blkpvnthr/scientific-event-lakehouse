import json
from unittest.mock import patch

import pytest

from src.ingestion.backfill import backfill, ingest_window


@pytest.fixture
def storage(tmp_path, monkeypatch):
    bronze = tmp_path / "bronze"
    manifests = tmp_path / "manifests"

    monkeypatch.setattr(
        "src.ingestion.backfill.BRONZE_PATH",
        bronze,
    )

    monkeypatch.setattr(
        "src.ingestion.backfill.MANIFEST_PATH",
        manifests,
    )

    return bronze, manifests


def make_payload(event_ids):
    return json.dumps(
        {
            "type": "FeatureCollection",
            "metadata": {
                "status": 200,
                "url": ("https://earthquake.usgs.gov/fdsnws/event/1/query"),
            },
            "features": [
                {
                    "type": "Feature",
                    "id": event_id,
                    "properties": {
                        "time": 1704067200000,
                        "mag": 3.5,
                    },
                }
                for event_id in event_ids
            ],
        }
    ).encode("utf-8")


@patch("src.ingestion.backfill.fetch_earthquake_page")
@patch("src.ingestion.backfill.count_earthquakes")
def test_successful_window(
    mock_count,
    mock_fetch,
    storage,
):
    mock_count.side_effect = [3, 3]

    mock_fetch.side_effect = [
        make_payload(["event1", "event2"]),
        make_payload(["event3"]),
    ]

    manifest = ingest_window(
        run_id="test-run",
        start="2024-01-01",
        end="2024-01-02",
        min_magnitude=2.5,
        page_size=2,
    )

    assert manifest.status == "SUCCESS"
    assert manifest.actual_count == 3
    assert manifest.unique_event_count == 3
    assert len(manifest.pages) == 2

    assert manifest.pages[0].offset == 1
    assert manifest.pages[1].offset == 3

    bronze, manifests = storage

    assert len(list(bronze.rglob("*.json"))) == 2

    assert (manifests / "run_id=test-run" / "2024-01-01.json").exists()


@patch("src.ingestion.backfill.fetch_earthquake_page")
@patch("src.ingestion.backfill.count_earthquakes")
def test_duplicate_records_fail(
    mock_count,
    mock_fetch,
    storage,
):
    mock_count.side_effect = [2, 2]

    mock_fetch.return_value = make_payload(["event1", "event1"])

    with pytest.raises(RuntimeError, match="reconciliation"):
        ingest_window(
            run_id="duplicate-run",
            start="2024-01-01",
            end="2024-01-02",
            min_magnitude=2.5,
            page_size=2,
        )

    _, manifests = storage

    manifest_path = manifests / "run_id=duplicate-run" / "2024-01-01.json"

    data = json.loads(manifest_path.read_bytes())

    assert data["status"] == "FAILED"
    assert data["actual_count"] == 2
    assert data["unique_event_count"] == 1


@patch("src.ingestion.backfill.fetch_earthquake_page")
@patch("src.ingestion.backfill.count_earthquakes")
def test_empty_window(
    mock_count,
    mock_fetch,
    storage,
):
    mock_count.side_effect = [0, 0]

    manifest = ingest_window(
        run_id="empty-run",
        start="2024-01-01",
        end="2024-01-02",
        min_magnitude=2.5,
        page_size=100,
    )

    assert manifest.status == "SUCCESS"
    assert manifest.actual_count == 0
    assert manifest.pages == []

    mock_fetch.assert_not_called()


@patch("src.ingestion.backfill.fetch_earthquake_page")
@patch("src.ingestion.backfill.count_earthquakes")
def test_source_count_changed(
    mock_count,
    mock_fetch,
    storage,
):
    mock_count.side_effect = [2, 3]

    mock_fetch.return_value = make_payload(["event1", "event2"])

    with pytest.raises(RuntimeError, match="reconciliation"):
        ingest_window(
            run_id="changed-run",
            start="2024-01-01",
            end="2024-01-02",
            min_magnitude=2.5,
            page_size=100,
        )


@patch("src.ingestion.backfill.fetch_earthquake_page")
@patch("src.ingestion.backfill.count_earthquakes")
def test_missing_records(
    mock_count,
    mock_fetch,
    storage,
):
    mock_count.side_effect = [3, 3]

    mock_fetch.side_effect = [
        make_payload(["event1", "event2"]),
        make_payload([]),
    ]

    with pytest.raises(RuntimeError, match="reconciliation"):
        ingest_window(
            run_id="missing-run",
            start="2024-01-01",
            end="2024-01-02",
            min_magnitude=2.5,
            page_size=2,
        )


@patch("src.ingestion.backfill.fetch_earthquake_page")
@patch("src.ingestion.backfill.count_earthquakes")
def test_successful_backfill(
    mock_count,
    mock_fetch,
    storage,
):
    mock_count.side_effect = [1, 1, 1, 1]

    mock_fetch.side_effect = [
        make_payload(["event1"]),
        make_payload(["event2"]),
    ]

    run_id = backfill(
        start_date="2024-01-01",
        end_date="2024-01-03",
        window_days=1,
        page_size=10,
    )

    _, manifests = storage

    completion_path = manifests / f"run_id={run_id}" / "_SUCCESS.json"

    completion = json.loads(completion_path.read_bytes())

    assert completion["status"] == "SUCCESS"
    assert completion["total_windows"] == 2
    assert completion["total_records"] == 2
    assert completion["total_pages"] == 2


@patch("src.ingestion.backfill.count_earthquakes")
def test_failed_run_has_no_success_marker(
    mock_count,
    storage,
):
    mock_count.side_effect = RuntimeError("Source unavailable")

    with pytest.raises(RuntimeError, match="Source unavailable"):
        backfill(
            start_date="2024-01-01",
            end_date="2024-01-02",
        )

    _, manifests = storage

    assert not list(manifests.rglob("_SUCCESS.json"))


@pytest.mark.parametrize("page_size", [0, -1, 20001])
def test_invalid_page_size(page_size):
    with pytest.raises(ValueError):
        backfill(
            start_date="2024-01-01",
            end_date="2024-01-02",
            page_size=page_size,
        )
