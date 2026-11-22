import argparse
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from src.ingestion.archive import (
    atomic_write,
    calculate_checksum,
)
from src.ingestion.date_windows import generate_date_windows
from src.ingestion.manifest import (
    PageManifest,
    WindowManifest,
    write_manifest,
)
from src.ingestion.usgs_client import (
    count_earthquakes,
    fetch_earthquake_page,
)
from src.ingestion.validation import validate_payload

BRONZE_PATH = Path("data/bronze")
MANIFEST_PATH = Path("data/manifests")

logger = logging.getLogger(__name__)


def ingest_window(
    run_id: str,
    start: str,
    end: str,
    min_magnitude: float,
    page_size: int,
) -> WindowManifest:
    """Extract, archive and reconcile one historical window."""

    started_at = datetime.now(UTC)
    window_key = start[:10]

    expected_before = count_earthquakes(
        start,
        end,
        min_magnitude,
    )

    logger.info(
        "Starting window %s to %s; expected=%s",
        start,
        end,
        expected_before,
    )

    offset = 1
    actual_count = 0

    seen_ids: set[str] = set()
    pages: list[PageManifest] = []

    archive_directory = (
        BRONZE_PATH
        / f"ingestion_date={started_at.date()}"
        / f"run_id={run_id}"
        / f"window_start={window_key}"
    )

    while actual_count < expected_before:
        payload = fetch_earthquake_page(
            start_date=start,
            end_date=end,
            min_magnitude=min_magnitude,
            limit=page_size,
            offset=offset,
        )

        ingestion_time = datetime.now(UTC)
        data = validate_payload(payload)

        features = data["features"]

        if not features:
            logger.warning(
                "Empty page received at offset=%s",
                offset,
            )
            break

        record_count = len(features)

        if record_count > page_size:
            raise ValueError("Source returned more records than requested")

        if data["metadata"].get("offset", offset) != offset:
            raise ValueError("Unexpected source pagination offset")

        checksum = calculate_checksum(payload)

        archive_path = archive_directory / f"offset_{offset:08d}.json"

        atomic_write(archive_path, payload)

        page_manifest = PageManifest(
            source_url=data["metadata"]["url"],
            source_file=str(archive_path),
            window_start=start,
            window_end=end,
            ingestion_time=ingestion_time.isoformat(),
            min_magnitude=min_magnitude,
            page_limit=page_size,
            offset=offset,
            record_count=record_count,
            file_size_bytes=len(payload),
            sha256=checksum,
        )

        pages.append(page_manifest)

        seen_ids.update(feature["id"] for feature in features)

        actual_count += record_count
        offset += record_count

        logger.info(
            "Archived page: records=%s total=%s expected=%s",
            record_count,
            actual_count,
            expected_before,
        )

    expected_after = count_earthquakes(
        start,
        end,
        min_magnitude,
    )

    unique_count = len(seen_ids)

    reconciled = expected_before == expected_after == actual_count == unique_count

    status = "SUCCESS" if reconciled else "FAILED"

    manifest = WindowManifest(
        run_id=run_id,
        window_start=start,
        window_end=end,
        expected_count_before=expected_before,
        expected_count_after=expected_after,
        actual_count=actual_count,
        unique_event_count=unique_count,
        status=status,
        pages=pages,
    )

    manifest_path = MANIFEST_PATH / f"run_id={run_id}" / f"{window_key}.json"

    write_manifest(manifest_path, manifest)

    logger.info(
        "Window completed: status=%s expected=%s actual=%s unique=%s",
        status,
        expected_before,
        actual_count,
        unique_count,
    )

    if not reconciled:
        raise RuntimeError(
            f"Record reconciliation failed for {window_key}: "
            f"before={expected_before}, "
            f"after={expected_after}, "
            f"actual={actual_count}, "
            f"unique={unique_count}"
        )

    return manifest


def backfill(
    start_date: str,
    end_date: str,
    window_days: int = 7,
    min_magnitude: float = 2.5,
    page_size: int = 1000,
) -> str:
    """Execute a historical ingestion run."""

    if not 1 <= page_size <= 20_000:
        raise ValueError("page_size must be between 1 and 20000")

    windows = list(
        generate_date_windows(
            start_date,
            end_date,
            window_days,
        )
    )

    run_id = uuid4().hex
    started_at = datetime.now(UTC)

    total_records = 0
    total_pages = 0
    completed_windows: list[str] = []

    logger.info(
        "Starting historical backfill: run_id=%s windows=%s",
        run_id,
        len(windows),
    )

    for index, (start, end) in enumerate(
        windows,
        start=1,
    ):
        logger.info(
            "Processing window %s of %s",
            index,
            len(windows),
        )

        manifest = ingest_window(
            run_id=run_id,
            start=start,
            end=end,
            min_magnitude=min_magnitude,
            page_size=page_size,
        )

        total_records += manifest.actual_count
        total_pages += len(manifest.pages)

        completed_windows.append(f"{start[:10]}.json")

    finished_at = datetime.now(UTC)

    completion = {
        "run_id": run_id,
        "status": "SUCCESS",
        "start_date": start_date,
        "end_date_exclusive": end_date,
        "window_days": window_days,
        "min_magnitude": min_magnitude,
        "page_size": page_size,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "duration_seconds": (finished_at - started_at).total_seconds(),
        "total_windows": len(completed_windows),
        "total_records": total_records,
        "total_pages": total_pages,
        "window_manifests": completed_windows,
    }

    completion_path = MANIFEST_PATH / f"run_id={run_id}" / "_SUCCESS.json"

    atomic_write(
        completion_path,
        json.dumps(
            completion,
            indent=2,
            allow_nan=False,
        ).encode("utf-8"),
    )

    logger.info(
        "Backfill completed: run_id=%s records=%s pages=%s",
        run_id,
        total_records,
        total_pages,
    )

    return run_id


def main() -> None:
    parser = argparse.ArgumentParser(
        description=("Historical USGS earthquake ingestion pipeline")
    )

    parser.add_argument(
        "--start-date",
        required=True,
        help="Inclusive start date (YYYY-MM-DD)",
    )

    parser.add_argument(
        "--end-date",
        required=True,
        help="Exclusive end date (YYYY-MM-DD)",
    )

    parser.add_argument(
        "--window-days",
        type=int,
        default=7,
    )

    parser.add_argument(
        "--min-magnitude",
        type=float,
        default=2.5,
    )

    parser.add_argument(
        "--page-size",
        type=int,
        default=1000,
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format=("%(asctime)s | %(levelname)s | %(message)s"),
    )

    run_id = backfill(
        start_date=args.start_date,
        end_date=args.end_date,
        window_days=args.window_days,
        min_magnitude=args.min_magnitude,
        page_size=args.page_size,
    )

    print(f"Completed run: {run_id}")


if __name__ == "__main__":
    main()
