import json
from dataclasses import asdict, dataclass
from pathlib import Path

from src.ingestion.archive import atomic_write


@dataclass(frozen=True)
class PageManifest:
    """Metadata describing an archived API response."""

    source_url: str
    source_file: str

    window_start: str
    window_end: str

    ingestion_time: str

    min_magnitude: float

    page_limit: int
    offset: int

    record_count: int
    file_size_bytes: int

    sha256: str


@dataclass(frozen=True)
class WindowManifest:
    """Metadata and reconciliation results for a date window."""

    run_id: str

    window_start: str
    window_end: str

    expected_count_before: int
    expected_count_after: int

    actual_count: int
    unique_event_count: int

    status: str

    pages: list[PageManifest]


def write_manifest(
    path: Path,
    manifest: PageManifest | WindowManifest,
) -> Path:
    """Serialize and atomically publish an ingestion manifest."""

    payload = json.dumps(
        asdict(manifest),
        indent=2,
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")

    return atomic_write(path, payload)
