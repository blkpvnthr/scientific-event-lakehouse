"""Distributed Bronze builder.

Transforms the immutable raw GeoJSON landing zone (``data/bronze``) into a
structured, columnar Parquet Bronze dataset (``data/warehouse/bronze``) using
Apache Spark.

Correctness design:

* **Manifest-driven discovery** — only files recorded in a completed run's
  ``_SUCCESS.json`` + window manifests are processed. Stray files are ignored.
* **Checksum gate before processing** — every source file's SHA-256 and byte
  size are verified on the driver against the manifest *before* any Spark job
  runs. A single mismatch aborts the run. This also proves the raw archive is
  byte-for-byte unchanged.
* **Distributed parse/explode/extract** — Spark reads all pages in parallel
  with an explicit schema, explodes nested features into event rows, and
  extracts scientific attributes.
* **Provenance join by ``(window_start, page_offset)``** — a stable key encoded
  in each archive path, avoiding fragile filesystem-URI string matching.
* **Reconciliation** — output record / unique-event / source-file counts are
  reconciled against the manifest totals; any drift raises.
* **Idempotent overwrite** — partitions are overwritten (dynamic mode), so
  reprocessing the same run never duplicates records.

The raw archive is only ever read, never written.
"""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.config import Settings, get_settings
from src.ingestion.archive import atomic_write, calculate_checksum
from src.schemas import FEATURE_COLLECTION_SCHEMA
from src.spark import build_spark_session, stop_spark_session

logger = logging.getLogger(__name__)

# Ordered Bronze output columns (event attributes + provenance).
BRONZE_COLUMNS = [
    # --- scientific event attributes ---
    "event_id",
    "event_time",
    "updated_time",
    "magnitude",
    "magnitude_type",
    "place",
    "longitude",
    "latitude",
    "depth_km",
    "event_type",
    "network",
    "event_code",
    "status",
    "tsunami",
    "significance",
    "felt",
    "cdi",
    "mmi",
    "alert",
    "location_source",
    "magnitude_source",
    "station_count",
    "azimuthal_gap",
    "min_station_distance_deg",
    "rms",
    "event_url",
    # --- retained raw source attributes ---
    "ids",
    "sources",
    "types",
    "title",
    # --- provenance ---
    "source_file",
    "source_checksum",
    "source_api_url",
    "ingestion_timestamp",
    "ingestion_run_id",
    "extraction_window",
    "ingestion_date",
    "page_offset",
    "processing_timestamp",
    # --- partition column (kept last) ---
    "event_year",
]


@dataclass(frozen=True)
class FileProvenance:
    """Per-file provenance derived from the ingestion manifests."""

    source_file: str
    sha256: str
    file_size_bytes: int
    record_count: int
    page_offset: int
    window_start: str
    source_api_url: str
    ingestion_time: str
    ingestion_date: str


@dataclass(frozen=True)
class BronzeResult:
    """Outcome of a Bronze build, suitable for reporting and assertions."""

    run_id: str
    expected_files: int
    expected_records: int
    output_records: int
    unique_events: int
    distinct_source_files: int
    duplicate_events: int
    status: str
    output_path: str
    duration_seconds: float
    output_bytes: int
    report_path: str | None = None


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def discover_run_files(
    run_id: str, settings: Settings | None = None
) -> list[FileProvenance]:
    """Read a completed run's manifests and return per-file provenance.

    Validates the run completion marker and that each window succeeded. The
    returned list contains exactly one entry per archived page file.
    """
    settings = settings or get_settings()
    run_dir = settings.run_manifest_dir(run_id)
    completion_path = run_dir / "_SUCCESS.json"

    _require(completion_path.is_file(), f"Run is incomplete: {run_id}")
    completion = json.loads(completion_path.read_bytes())
    _require(completion["status"] == "SUCCESS", "Run status is not SUCCESS")
    _require(completion["run_id"] == run_id, "Run ID mismatch")

    files: list[FileProvenance] = []
    for window_file in completion["window_manifests"]:
        manifest = json.loads((run_dir / window_file).read_bytes())
        _require(manifest["status"] == "SUCCESS", f"Failed window: {window_file}")
        _require(
            manifest["run_id"] == run_id,
            f"Window run ID mismatch: {window_file}",
        )
        for page in manifest["pages"]:
            source_path = Path(page["source_file"])
            # ingestion_date is encoded in the archive path layout.
            ingestion_date = ""
            for part in source_path.parts:
                if part.startswith("ingestion_date="):
                    ingestion_date = part.split("=", 1)[1]
                    break
            files.append(
                FileProvenance(
                    source_file=str(source_path),
                    sha256=page["sha256"],
                    file_size_bytes=page["file_size_bytes"],
                    record_count=page["record_count"],
                    page_offset=page["offset"],
                    window_start=manifest["window_start"][:10],
                    source_api_url=page["source_url"],
                    ingestion_time=page["ingestion_time"],
                    ingestion_date=ingestion_date,
                )
            )
    return files


def verify_checksums(files: list[FileProvenance]) -> None:
    """Verify each source file's size and SHA-256 against the manifest.

    Runs on the driver before any Spark job. Guarantees the raw archive is
    intact and unmodified prior to distributed processing.
    """
    for provenance in files:
        path = Path(provenance.source_file)
        _require(path.is_file(), f"Archive missing: {path}")
        payload = path.read_bytes()
        _require(
            len(payload) == provenance.file_size_bytes,
            f"File-size mismatch: {path}",
        )
        _require(
            calculate_checksum(payload) == provenance.sha256,
            f"Checksum mismatch: {path}",
        )
    logger.info("Verified %d source files (size + SHA-256)", len(files))


def _provenance_dataframe(
    spark: SparkSession, files: list[FileProvenance], run_id: str
) -> DataFrame:
    """Build a Spark DataFrame of per-file provenance for joining."""
    rows = [
        {
            "window_start_key": f.window_start,
            "page_offset": f.page_offset,
            "source_file": f.source_file,
            "source_checksum": f.sha256,
            "source_api_url": f.source_api_url,
            "ingestion_timestamp": f.ingestion_time,
            "ingestion_date": f.ingestion_date,
            "ingestion_run_id": run_id,
        }
        for f in files
    ]
    return spark.createDataFrame(rows)


def build_bronze_dataframe(
    spark: SparkSession,
    files: list[FileProvenance],
    run_id: str,
    processing_timestamp: str,
) -> DataFrame:
    """Parse raw pages and produce the structured Bronze DataFrame."""
    paths = [f.source_file for f in files]

    raw = (
        spark.read.schema(FEATURE_COLLECTION_SCHEMA)
        .option("multiLine", "true")
        .json(paths)
        .withColumn("_input_file", F.input_file_name())
    )

    exploded = raw.select(
        "_input_file",
        F.explode("features").alias("feature"),
    )

    # Stable provenance key extracted from the archive path layout.
    exploded = exploded.withColumn(
        "window_start_key",
        F.regexp_extract("_input_file", r"window_start=([^/]+)", 1),
    ).withColumn(
        "page_offset",
        F.regexp_extract("_input_file", r"offset_(\d+)\.json", 1).cast("int"),
    )

    props = F.col("feature.properties")
    coords = F.col("feature.geometry.coordinates")

    events = exploded.select(
        F.col("feature.id").alias("event_id"),
        F.expr("timestamp_millis(feature.properties.time)").alias("event_time"),
        F.expr("timestamp_millis(feature.properties.updated)").alias("updated_time"),
        props.getField("mag").alias("magnitude"),
        props.getField("magType").alias("magnitude_type"),
        props.getField("place").alias("place"),
        coords.getItem(0).alias("longitude"),
        coords.getItem(1).alias("latitude"),
        coords.getItem(2).alias("depth_km"),
        props.getField("type").alias("event_type"),
        props.getField("net").alias("network"),
        props.getField("code").alias("event_code"),
        props.getField("status").alias("status"),
        props.getField("tsunami").cast("int").alias("tsunami"),
        props.getField("sig").cast("int").alias("significance"),
        props.getField("felt").cast("int").alias("felt"),
        props.getField("cdi").alias("cdi"),
        props.getField("mmi").alias("mmi"),
        props.getField("alert").alias("alert"),
        # Not provided by the FDSN summary feed; never fabricated.
        F.lit(None).cast("string").alias("location_source"),
        F.lit(None).cast("string").alias("magnitude_source"),
        props.getField("nst").cast("int").alias("station_count"),
        props.getField("gap").alias("azimuthal_gap"),
        props.getField("dmin").alias("min_station_distance_deg"),
        props.getField("rms").alias("rms"),
        props.getField("url").alias("event_url"),
        props.getField("ids").alias("ids"),
        props.getField("sources").alias("sources"),
        props.getField("types").alias("types"),
        props.getField("title").alias("title"),
        "window_start_key",
        "page_offset",
    )

    provenance = _provenance_dataframe(spark, files, run_id)

    joined = events.join(provenance, on=["window_start_key", "page_offset"], how="left")

    enriched = (
        joined.withColumn(
            "processing_timestamp", F.to_timestamp(F.lit(processing_timestamp))
        )
        .withColumn("extraction_window", F.col("window_start_key"))
        .withColumn("event_year", F.year("event_time"))
    )

    return enriched.select(*BRONZE_COLUMNS)


def _dir_size_bytes(path: Path) -> int:
    """Total size in bytes of all files under a directory."""
    if not path.exists():
        return 0
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


def build_bronze(
    spark: SparkSession,
    run_id: str,
    settings: Settings | None = None,
    *,
    write_report: bool = True,
) -> BronzeResult:
    """Run the full Bronze build for an ingestion run and reconcile it."""
    settings = settings or get_settings()
    started = datetime.now(UTC)

    files = discover_run_files(run_id, settings)
    verify_checksums(files)

    expected_records = sum(f.record_count for f in files)
    expected_files = len(files)

    processing_timestamp = started.isoformat(timespec="microseconds")
    bronze = build_bronze_dataframe(spark, files, run_id, processing_timestamp)

    output_path = settings.bronze_path
    (
        bronze.repartition("event_year")
        .write.mode("overwrite")
        .partitionBy("event_year")
        .parquet(str(output_path))
    )

    # Reconcile from the persisted output (second, independent verification).
    result_df = spark.read.parquet(str(output_path))
    output_records = result_df.count()
    unique_events = result_df.select("event_id").distinct().count()
    distinct_source_files = result_df.select("source_file").distinct().count()
    duplicate_events = output_records - unique_events

    _require(
        output_records == expected_records,
        f"Record count mismatch: output={output_records} expected={expected_records}",
    )
    _require(
        distinct_source_files == expected_files,
        f"Source-file count mismatch: output={distinct_source_files} "
        f"expected={expected_files}",
    )

    finished = datetime.now(UTC)
    duration = (finished - started).total_seconds()
    output_bytes = _dir_size_bytes(output_path)

    result = BronzeResult(
        run_id=run_id,
        expected_files=expected_files,
        expected_records=expected_records,
        output_records=output_records,
        unique_events=unique_events,
        distinct_source_files=distinct_source_files,
        duplicate_events=duplicate_events,
        status="PASS" if duplicate_events == 0 else "FAIL",
        output_path=str(output_path),
        duration_seconds=round(duration, 3),
        output_bytes=output_bytes,
    )

    report_path = None
    if write_report:
        report_path = _write_report(result, settings)

    logger.info(
        "Bronze build complete: records=%s unique=%s files=%s status=%s duration=%.1fs",
        output_records,
        unique_events,
        distinct_source_files,
        result.status,
        duration,
    )
    return BronzeResult(**{**result.__dict__, "report_path": report_path})


def _write_report(result: BronzeResult, settings: Settings) -> str:
    """Persist a Bronze reconciliation report as immutable JSON."""
    report = {
        "run_id": result.run_id,
        "generated_at": datetime.now(UTC).isoformat(),
        "expected_files": result.expected_files,
        "distinct_source_files": result.distinct_source_files,
        "expected_records": result.expected_records,
        "output_records": result.output_records,
        "unique_events": result.unique_events,
        "duplicate_events": result.duplicate_events,
        "status": result.status,
        "output_path": result.output_path,
        "output_bytes": result.output_bytes,
        "duration_seconds": result.duration_seconds,
    }
    report_dir = settings.bronze_report_dir() / f"run_id={result.run_id}"
    # Unique filename per generation (immutable report convention; never
    # overwrites a prior report, consistent with the profiler).
    path = report_dir / f"reconciliation_{uuid4().hex}.json"
    atomic_write(
        path,
        json.dumps(report, indent=2, sort_keys=True, allow_nan=False).encode("utf-8"),
    )
    return str(path)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build structured Bronze Parquet from a raw ingestion run"
    )
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
    )

    spark = build_spark_session("bronze-build")
    try:
        result = build_bronze(spark, args.run_id)
        print(json.dumps(result.__dict__, indent=2))
        if result.status != "PASS":
            raise SystemExit(1)
    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    main()
