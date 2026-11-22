import json

import pytest

from src.ingestion.validation import (
    PayloadValidationError,
    validate_payload,
)


def make_event(event_id="event001"):
    return {
        "type": "Feature",
        "id": event_id,
        "properties": {
            "time": 1704067200000,
            "mag": 3.22,
        },
    }


def make_payload(features, metadata=None):

    if metadata is None:
        metadata = {"count": len(features)}

    return json.dumps(
        {
            "type": "FeatureCollection",
            "metadata": metadata,
            "features": features,
        }
    ).encode("utf-8")


def test_valid_payload():
    payload = make_payload([make_event()])

    result = validate_payload(payload)

    assert len(result["features"]) == 1


def test_valid_empty_collection():
    result = validate_payload(make_payload([]))

    assert result["features"] == []


def test_valid_paginated_response_without_count():
    """Regression test for the observed USGS API response."""

    metadata = {
        "generated": 1795359564000,
        "title": "USGS Earthquakes",
        "status": 200,
        "api": "2.8.4",
        "limit": 10,
        "offset": 1,
    }

    payload = make_payload(
        [make_event(f"event{i:03d}") for i in range(10)],
        metadata,
    )

    result = validate_payload(payload)

    assert len(result["features"]) == 10
    assert "count" not in result["metadata"]


def test_valid_partial_page():
    payload = make_payload(
        [make_event()],
        {"limit": 10, "offset": 11},
    )

    result = validate_payload(payload)

    assert len(result["features"]) == 1


def test_invalid_json():
    with pytest.raises(
        PayloadValidationError,
        match="invalid JSON",
    ):
        validate_payload(b"invalid json")


def test_invalid_response_type():
    with pytest.raises(
        PayloadValidationError,
        match="Unexpected response type",
    ):
        validate_payload(b'{"type": "Invalid"}')


def test_metadata_count_mismatch():
    payload = make_payload(
        [make_event()],
        {"count": 5},
    )

    with pytest.raises(
        PayloadValidationError,
        match="Metadata count does not match",
    ):
        validate_payload(payload)


@pytest.mark.parametrize("count", [-1, True, "10", None])
def test_invalid_metadata_count(count):
    payload = make_payload([], {"count": count})

    with pytest.raises(
        PayloadValidationError,
        match="Invalid metadata count",
    ):
        validate_payload(payload)


def test_missing_event_id():
    event = make_event()
    del event["id"]

    with pytest.raises(
        PayloadValidationError,
        match="Missing or invalid event ID",
    ):
        validate_payload(make_payload([event]))


@pytest.mark.parametrize("timestamp", ["2024-01-01", True, None])
def test_invalid_timestamp(timestamp):
    event = make_event()
    event["properties"]["time"] = timestamp

    with pytest.raises(
        PayloadValidationError,
        match="Invalid event timestamp",
    ):
        validate_payload(make_payload([event]))


def test_missing_properties():
    event = make_event()
    del event["properties"]

    with pytest.raises(
        PayloadValidationError,
        match="Invalid properties",
    ):
        validate_payload(make_payload([event]))


@pytest.mark.parametrize("limit", [0, -1, True, "10"])
def test_invalid_pagination_limit(limit):
    payload = make_payload([], {"limit": limit})

    with pytest.raises(
        PayloadValidationError,
        match="Invalid pagination limit",
    ):
        validate_payload(payload)


@pytest.mark.parametrize("offset", [0, -1, True, "1"])
def test_invalid_pagination_offset(offset):
    payload = make_payload([], {"offset": offset})

    with pytest.raises(
        PayloadValidationError,
        match="Invalid pagination offset",
    ):
        validate_payload(payload)


def test_record_count_exceeds_limit():
    payload = make_payload(
        [make_event("event001"), make_event("event002")],
        {"limit": 1, "offset": 1},
    )

    with pytest.raises(
        PayloadValidationError,
        match="Record count exceeds pagination limit",
    ):
        validate_payload(payload)


def test_invalid_source_status():
    payload = make_payload([], {"status": 500})

    with pytest.raises(
        PayloadValidationError,
        match="Unexpected source response status",
    ):
        validate_payload(payload)


def test_unknown_metadata_fields():
    payload = make_payload(
        [make_event()],
        {"future_api_field": "new value"},
    )

    result = validate_payload(payload)

    assert result["metadata"]["future_api_field"] == "new value"


def test_null_magnitude():
    event = make_event()
    event["properties"]["mag"] = None

    result = validate_payload(make_payload([event]))

    assert result["features"][0]["properties"]["mag"] is None
