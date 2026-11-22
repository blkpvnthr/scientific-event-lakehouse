import json


class PayloadValidationError(ValueError):
    """Raised when a source payload violates the data contract."""


def validate_payload(payload: bytes) -> dict:
    """Validate a USGS GeoJSON response without modifying it."""

    try:
        data = json.loads(payload)

    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise PayloadValidationError("Response contains invalid JSON") from exc

    if not isinstance(data, dict):
        raise PayloadValidationError("Expected a JSON object")

    if data.get("type") != "FeatureCollection":
        raise PayloadValidationError("Unexpected response type")

    metadata = data.get("metadata")

    if not isinstance(metadata, dict):
        raise PayloadValidationError("Missing or invalid metadata")

    features = data.get("features")

    if not isinstance(features, list):
        raise PayloadValidationError("Missing or invalid features array")

    # Derive the actual record count from the payload.
    actual_count = len(features)

    # Some USGS responses provide a count; others do not.
    if "count" in metadata:
        count = metadata["count"]

        if type(count) is not int or count < 0:
            raise PayloadValidationError("Invalid metadata count")

        if count != actual_count:
            raise PayloadValidationError("Metadata count does not match features")

    # Validate source response status when available.
    if "status" in metadata:
        status = metadata["status"]

        if type(status) is not int or status != 200:
            raise PayloadValidationError("Unexpected source response status")

    # Validate pagination metadata when available.
    if "limit" in metadata:
        limit = metadata["limit"]

        if type(limit) is not int or limit < 1:
            raise PayloadValidationError("Invalid pagination limit")

        if actual_count > limit:
            raise PayloadValidationError("Record count exceeds pagination limit")

    if "offset" in metadata:
        offset = metadata["offset"]

        if type(offset) is not int or offset < 1:
            raise PayloadValidationError("Invalid pagination offset")

    # Validate individual earthquake records.
    for index, feature in enumerate(features):
        if not isinstance(feature, dict):
            raise PayloadValidationError(f"Invalid feature at index {index}")

        event_id = feature.get("id")

        if not isinstance(event_id, str) or not event_id.strip():
            raise PayloadValidationError(
                f"Missing or invalid event ID at index {index}"
            )

        properties = feature.get("properties")

        if not isinstance(properties, dict):
            raise PayloadValidationError(f"Invalid properties for event {event_id}")

        if type(properties.get("time")) is not int:
            raise PayloadValidationError(f"Invalid event timestamp for {event_id}")

    return data
