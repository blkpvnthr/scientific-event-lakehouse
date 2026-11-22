import json
from dataclasses import FrozenInstanceError

import pytest

from src.ingestion.archive import calculate_checksum
from src.ingestion.manifest import (
    PageManifest,
    WindowManifest,
    write_manifest,
)


@pytest.fixture
def page_manifest():
    payload = b'{"features":[]}'

    return PageManifest(
        source_url=("https://earthquake.usgs.gov/fdsnws/event/1/query"),
        source_file="data/bronze/earthquakes.json",
        window_start="2024-01-01T00:00:00.000Z",
        window_end="2024-01-07T23:59:59.999Z",
        ingestion_time="2024-02-01T12:00:00Z",
        min_magnitude=2.5,
        page_limit=1000,
        offset=1,
        record_count=0,
        file_size_bytes=len(payload),
        sha256=calculate_checksum(payload),
    )


@pytest.fixture
def window_manifest(page_manifest):
    return WindowManifest(
        run_id="test-run-001",
        window_start=page_manifest.window_start,
        window_end=page_manifest.window_end,
        expected_count_before=0,
        expected_count_after=0,
        actual_count=0,
        unique_event_count=0,
        status="SUCCESS",
        pages=[page_manifest],
    )


def test_page_manifest_creation(page_manifest):
    assert page_manifest.offset == 1
    assert page_manifest.page_limit == 1000
    assert page_manifest.record_count == 0
    assert len(page_manifest.sha256) == 64


def test_page_manifest_is_frozen(page_manifest):
    with pytest.raises(FrozenInstanceError):
        page_manifest.record_count = 500


def test_write_page_manifest(tmp_path, page_manifest):
    destination = tmp_path / "page.json"

    result = write_manifest(
        destination,
        page_manifest,
    )

    assert result == destination
    assert destination.exists()


def test_page_manifest_json_contents(
    tmp_path,
    page_manifest,
):
    destination = tmp_path / "page.json"

    write_manifest(destination, page_manifest)

    data = json.loads(destination.read_bytes())

    assert data["source_url"] == page_manifest.source_url
    assert data["sha256"] == page_manifest.sha256
    assert data["record_count"] == 0
    assert data["offset"] == 1


def test_window_manifest_creation(window_manifest):
    assert window_manifest.status == "SUCCESS"
    assert window_manifest.actual_count == 0
    assert len(window_manifest.pages) == 1


def test_write_window_manifest(
    tmp_path,
    window_manifest,
):
    destination = tmp_path / "window.json"

    write_manifest(destination, window_manifest)

    assert destination.exists()


def test_nested_page_serialization(
    tmp_path,
    window_manifest,
):
    destination = tmp_path / "window.json"

    write_manifest(destination, window_manifest)

    data = json.loads(destination.read_bytes())

    assert data["run_id"] == "test-run-001"
    assert data["status"] == "SUCCESS"
    assert isinstance(data["pages"], list)
    assert len(data["pages"]) == 1
    assert data["pages"][0]["offset"] == 1


def test_manifest_prevents_overwrite(
    tmp_path,
    page_manifest,
):
    destination = tmp_path / "page.json"

    write_manifest(destination, page_manifest)

    original = destination.read_bytes()

    with pytest.raises(FileExistsError):
        write_manifest(destination, page_manifest)

    assert destination.read_bytes() == original


def test_manifest_creates_parent_directories(
    tmp_path,
    page_manifest,
):
    destination = tmp_path / "manifests" / "run_id=test-run-001" / "page.json"

    write_manifest(destination, page_manifest)

    assert destination.exists()


def test_empty_window_manifest(tmp_path):
    manifest = WindowManifest(
        run_id="empty-run",
        window_start="2024-01-01",
        window_end="2024-01-02",
        expected_count_before=0,
        expected_count_after=0,
        actual_count=0,
        unique_event_count=0,
        status="SUCCESS",
        pages=[],
    )

    destination = tmp_path / "empty.json"

    write_manifest(destination, manifest)

    data = json.loads(destination.read_bytes())

    assert data["actual_count"] == 0
    assert data["pages"] == []
