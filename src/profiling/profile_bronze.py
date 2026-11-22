import argparse
import json
import math
import statistics
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from src.ingestion.archive import atomic_write, calculate_checksum
from src.ingestion.validation import validate_payload

MANIFEST_PATH = Path("data/manifests")
PROFILE_PATH = Path("data/profiles")


class ProfileError(RuntimeError):
    """Raised when historical data fails integrity verification."""


def require(condition: bool, message: str) -> None:
    """Enforce a data integrity requirement."""
    if not condition:
        raise ProfileError(message)


def read_json(path: Path) -> dict:
    """Read a JSON file."""
    return json.loads(path.read_bytes())


def timestamp_to_iso(timestamp: int) -> str:
    """Convert epoch milliseconds to a UTC timestamp."""
    return datetime.fromtimestamp(
        timestamp / 1000,
        tz=UTC,
    ).isoformat()


def is_numeric(value) -> bool:
    """Identify finite numeric values."""
    return type(value) in (int, float) and math.isfinite(value)


def summarize_numeric(values: list[float]) -> dict:
    """Calculate descriptive statistics."""
    if not values:
        return {
            "count": 0,
            "min": None,
            "max": None,
            "mean": None,
            "median": None,
        }

    return {
        "count": len(values),
        "min": min(values),
        "max": max(values),
        "mean": round(statistics.fmean(values), 4),
        "median": statistics.median(values),
    }


def magnitude_bucket(magnitude: float) -> str:
    """Assign an earthquake to a magnitude interval."""
    if magnitude < 2.5:
        return "below_2.5"
    if magnitude < 3.0:
        return "2.5_to_3.0"
    if magnitude < 4.0:
        return "3.0_to_4.0"
    if magnitude < 5.0:
        return "4.0_to_5.0"
    if magnitude < 6.0:
        return "5.0_to_6.0"
    if magnitude < 7.0:
        return "6.0_to_7.0"

    return "7.0_and_above"


def profile_run(run_id: str) -> dict:
    """Verify and profile a completed historical ingestion run."""

    require(
        bool(run_id)
        and all(c in "0123456789abcdef" for c in run_id)
        and len(run_id) == 32,
        "Invalid run ID",
    )

    run_directory = MANIFEST_PATH / f"run_id={run_id}"

    completion_path = run_directory / "_SUCCESS.json"

    require(
        completion_path.is_file(),
        f"Run is incomplete: {run_id}",
    )

    completion = read_json(completion_path)

    require(
        completion["status"] == "SUCCESS",
        "Run status is not SUCCESS",
    )

    require(
        completion["run_id"] == run_id,
        "Run ID mismatch",
    )

    total_records = 0
    total_files = 0
    total_bytes = 0

    event_ids: set[str] = set()
    archived_paths: set[Path] = set()

    magnitudes: list[float] = []
    timestamps: list[int] = []
    longitudes: list[float] = []
    latitudes: list[float] = []
    depths: list[float] = []

    missing_fields: Counter = Counter(
        {
            "mag": 0,
            "place": 0,
            "updated": 0,
            "magType": 0,
            "type": 0,
            "coordinates": 0,
        }
    )

    magnitude_distribution: Counter = Counter()
    event_types: Counter = Counter()
    magnitude_types: Counter = Counter()

    window_manifests = completion["window_manifests"]

    require(
        len(window_manifests)
        == len(set(window_manifests))
        == completion["total_windows"],
        "Window count mismatch",
    )

    for filename in window_manifests:
        manifest = read_json(run_directory / filename)

        require(
            manifest["status"] == "SUCCESS",
            f"Failed window: {filename}",
        )

        require(
            manifest["run_id"] == run_id,
            f"Window run ID mismatch: {filename}",
        )

        expected_offset = 1
        window_records = 0
        window_ids: set[str] = set()

        for page in manifest["pages"]:
            path = Path(page["source_file"])
            resolved_path = path.resolve()

            require(
                resolved_path not in archived_paths,
                f"Repeated archive reference: {path}",
            )

            archived_paths.add(resolved_path)

            require(
                path.is_file(),
                f"Archive missing: {path}",
            )

            payload = path.read_bytes()

            require(
                page["offset"] == expected_offset,
                f"Pagination mismatch: {path}",
            )

            require(
                len(payload) == page["file_size_bytes"],
                f"File-size mismatch: {path}",
            )

            require(
                calculate_checksum(payload) == page["sha256"],
                f"Checksum mismatch: {path}",
            )

            data = validate_payload(payload)
            features = data["features"]

            require(
                len(features) == page["record_count"],
                f"Page record-count mismatch: {path}",
            )

            total_files += 1
            total_bytes += len(payload)
            total_records += len(features)
            window_records += len(features)

            expected_offset += len(features)

            for feature in features:
                event_id = feature["id"]
                properties = feature["properties"]

                event_ids.add(event_id)
                window_ids.add(event_id)

                timestamps.append(properties["time"])

                for field in (
                    "mag",
                    "place",
                    "updated",
                    "magType",
                    "type",
                ):
                    value = properties.get(field)

                    if value is None or value == "":
                        missing_fields[field] += 1

                magnitude = properties.get("mag")

                if is_numeric(magnitude):
                    magnitudes.append(magnitude)

                    magnitude_distribution[magnitude_bucket(magnitude)] += 1

                event_type = properties.get("type")
                magnitude_type = properties.get("magType")

                if event_type is not None:
                    event_types[str(event_type)] += 1

                if magnitude_type is not None:
                    magnitude_types[str(magnitude_type)] += 1

                geometry = feature.get("geometry")

                coordinates = (
                    geometry.get("coordinates") if isinstance(geometry, dict) else None
                )

                if (
                    isinstance(coordinates, list)
                    and len(coordinates) >= 3
                    and all(is_numeric(value) for value in coordinates[:3])
                    and -180 <= coordinates[0] <= 180
                    and -90 <= coordinates[1] <= 90
                ):
                    longitudes.append(coordinates[0])
                    latitudes.append(coordinates[1])
                    depths.append(coordinates[2])
                else:
                    missing_fields["coordinates"] += 1

        require(
            manifest["expected_count_before"]
            == manifest["expected_count_after"]
            == manifest["actual_count"]
            == manifest["unique_event_count"]
            == window_records
            == len(window_ids),
            f"Window reconciliation failed: {filename}",
        )

    require(
        total_records == completion["total_records"],
        "Run record-count mismatch",
    )

    require(
        total_files == completion["total_pages"],
        "Run page-count mismatch",
    )

    duplicate_records = total_records - len(event_ids)

    duration = completion["duration_seconds"]

    return {
        "run_id": run_id,
        "profiled_at": datetime.now(UTC).isoformat(),
        "integrity_status": ("PASS" if duplicate_records == 0 else "FAIL"),
        "coverage": {
            "start_date": completion["start_date"],
            "end_date_exclusive": (completion["end_date_exclusive"]),
            "min_magnitude": completion["min_magnitude"],
            "earliest_event": (
                timestamp_to_iso(min(timestamps)) if timestamps else None
            ),
            "latest_event": (timestamp_to_iso(max(timestamps)) if timestamps else None),
        },
        "volume": {
            "windows": completion["total_windows"],
            "files": total_files,
            "records": total_records,
            "unique_events": len(event_ids),
            "duplicate_records": duplicate_records,
            "bytes": total_bytes,
            "mib": round(total_bytes / 1024**2, 6),
        },
        "quality": {
            "missing_fields": dict(missing_fields),
            "missing_percentages": {
                field: round(
                    count / total_records * 100,
                    4,
                )
                if total_records
                else None
                for field, count in missing_fields.items()
            },
        },
        "statistics": {
            "magnitude": summarize_numeric(magnitudes),
            "longitude": summarize_numeric(longitudes),
            "latitude": summarize_numeric(latitudes),
            "depth_km": summarize_numeric(depths),
        },
        "distributions": {
            "magnitude_buckets": dict(magnitude_distribution),
            "event_types": dict(event_types),
            "magnitude_types": dict(magnitude_types),
        },
        "performance": {
            "duration_seconds": duration,
            "records_per_second": (
                round(total_records / duration, 4) if duration > 0 else None
            ),
        },
    }


def write_profile(profile: dict) -> Path:
    """Publish an immutable profiling report."""

    profile_id = uuid4().hex

    path = PROFILE_PATH / f"run_id={profile['run_id']}" / f"profile_{profile_id}.json"

    return atomic_write(
        path,
        json.dumps(
            profile,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        ).encode("utf-8"),
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Profile a completed Bronze ingestion run"
    )

    parser.add_argument(
        "--run-id",
        required=True,
    )

    args = parser.parse_args()

    profile = profile_run(args.run_id)
    output_path = write_profile(profile)

    print(json.dumps(profile, indent=2))
    print(f"\nProfile saved: {output_path}")

    if profile["integrity_status"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
