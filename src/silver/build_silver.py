"""Silver builder: canonical scientific event model + data quality.

Reads the structured Bronze Parquet dataset and produces:

* ``earthquake_events_current``   — one validated row per event (latest revision)
* ``earthquake_event_revisions``  — every valid observation (observed history)
* ``quarantined_events``          — records failing a FATAL quality rule
* ``data_quality_results``        — per-rule aggregate results for the run

Design decisions:

* **Normalization, not loss** — categorical values are standardized (e.g. status
  case-folded) while originals are retained. Scientifically valid values such as
  negative depths are preserved.
* **Deterministic revision selection** — the current record for an event is the
  one with the greatest ``updated_time``; ties break on a content fingerprint so
  the choice is stable and reproducible across runs.
* **Reconciliation** — valid + quarantined == Bronze input, and
  current + superseded == valid. Any drift raises.
* **Idempotent overwrite** — reprocessing replaces outputs without duplication.
"""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import asdict, dataclass
from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.config import Settings, get_settings
from src.ingestion.archive import atomic_write
from src.quality import annotate_quality, evaluate_rules
from src.quality.engine import FAILED_FATAL_COL, FAILED_WARNING_COL
from src.spark import build_spark_session, stop_spark_session

logger = logging.getLogger(__name__)

_TS_FMT = "yyyy-MM-dd'T'HH:mm:ss.SSS"

# Fields that define a logical revision's content fingerprint.
_FINGERPRINT_FIELDS = [
    "event_id",
    f'date_format(event_time, "{_TS_FMT}")',
    "coalesce(cast(magnitude as string), '')",
    "coalesce(magnitude_type_normalized, '')",
    "coalesce(cast(latitude as string), '')",
    "coalesce(cast(longitude as string), '')",
    "coalesce(cast(depth_km as string), '')",
    "coalesce(place_normalized, '')",
    "coalesce(status_normalized, '')",
    f"coalesce(date_format(updated_time, \"{_TS_FMT}\"), '')",
]


@dataclass(frozen=True)
class SilverResult:
    run_id: str
    input_records: int
    valid_records: int
    quarantined_records: int
    current_records: int
    superseded_records: int
    reconciled: bool
    status: str
    quality_failures_fatal: int
    quality_failures_warning: int
    duration_seconds: float
    report_path: str | None = None


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def normalize_bronze(bronze: DataFrame) -> DataFrame:
    """Add canonical normalized columns and a content fingerprint."""
    normalized = (
        bronze.withColumn("event_id", F.trim(F.col("event_id")))
        .withColumn("status_normalized", F.lower(F.trim(F.col("status"))))
        .withColumn("event_type_normalized", F.lower(F.trim(F.col("event_type"))))
        .withColumn(
            "magnitude_type_normalized",
            F.lower(F.trim(F.col("magnitude_type"))),
        )
        .withColumn("place_normalized", F.trim(F.col("place")))
    )
    fingerprint = F.sha2(
        F.concat_ws("|", *[F.expr(field) for field in _FINGERPRINT_FIELDS]), 256
    )
    return normalized.withColumn("content_fingerprint", fingerprint)


def _select_current_and_revisions(
    valid: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Deterministically rank revisions; return (current, revisions)."""
    window = Window.partitionBy("event_id").orderBy(
        F.col("updated_time").desc_nulls_last(),
        F.col("content_fingerprint").desc(),
    )
    ranked = valid.withColumn("revision_rank", F.row_number().over(window))
    revisions = ranked.withColumn("is_current", F.col("revision_rank") == 1)
    current = revisions.filter(F.col("is_current"))
    return current, revisions


def build_silver(
    spark: SparkSession,
    run_id: str,
    settings: Settings | None = None,
    *,
    write_report: bool = True,
) -> SilverResult:
    """Run the full Silver build and reconcile against Bronze input."""
    settings = settings or get_settings()
    started = datetime.now(UTC)
    silver_ts = started.isoformat(timespec="microseconds")

    bronze = spark.read.parquet(str(settings.bronze_path))
    bronze = bronze.filter(F.col("ingestion_run_id") == run_id)
    input_records = bronze.count()
    _require(input_records > 0, f"No Bronze records for run {run_id}")

    normalized = normalize_bronze(bronze).withColumn(
        "silver_processing_timestamp", F.to_timestamp(F.lit(silver_ts))
    )

    # Quality evaluation over the full input (correct denominator).
    quality_results = evaluate_rules(normalized, run_id)
    annotated = annotate_quality(normalized)

    is_quarantined = F.size(F.col(FAILED_FATAL_COL)) > 0
    quarantined = annotated.filter(is_quarantined)
    valid = annotated.filter(~is_quarantined).drop(FAILED_FATAL_COL, FAILED_WARNING_COL)

    valid_records = valid.count()
    quarantined_records = quarantined.count()

    current, revisions = _select_current_and_revisions(valid)
    current_records = current.count()
    superseded_records = revisions.filter(~F.col("is_current")).count()

    # --- Reconciliation ---
    reconciled = (
        valid_records + quarantined_records == input_records
        and current_records + superseded_records == valid_records
    )
    _require(
        valid_records + quarantined_records == input_records,
        f"Reconciliation failed: valid={valid_records} + "
        f"quarantined={quarantined_records} != input={input_records}",
    )
    _require(
        current_records + superseded_records == valid_records,
        f"Revision reconciliation failed: current={current_records} + "
        f"superseded={superseded_records} != valid={valid_records}",
    )

    # --- Persist outputs (idempotent overwrite) ---
    _write(current, settings.silver_path / "earthquake_events_current")
    _write(revisions, settings.silver_path / "earthquake_event_revisions")
    _write_quarantine(quarantined, settings.silver_path / "quarantined_events", run_id)
    _write_quality_results(
        spark, quality_results, settings.silver_path / "data_quality_results"
    )

    fatal_failures = sum(
        r.observed_failure_count for r in quality_results if r.severity == "fatal"
    )
    warning_failures = sum(
        r.observed_failure_count for r in quality_results if r.severity == "warning"
    )

    duration = (datetime.now(UTC) - started).total_seconds()
    result = SilverResult(
        run_id=run_id,
        input_records=input_records,
        valid_records=valid_records,
        quarantined_records=quarantined_records,
        current_records=current_records,
        superseded_records=superseded_records,
        reconciled=reconciled,
        status="PASS" if reconciled else "FAIL",
        quality_failures_fatal=fatal_failures,
        quality_failures_warning=warning_failures,
        duration_seconds=round(duration, 3),
    )

    report_path = None
    if write_report:
        report_path = _write_report(result, quality_results, settings)

    logger.info(
        "Silver build complete: input=%s valid=%s quarantined=%s current=%s "
        "superseded=%s status=%s",
        input_records,
        valid_records,
        quarantined_records,
        current_records,
        superseded_records,
        result.status,
    )
    return SilverResult(**{**asdict(result), "report_path": report_path})


def _write(df: DataFrame, path) -> None:
    (
        df.repartition("event_year")
        .write.mode("overwrite")
        .partitionBy("event_year")
        .parquet(str(path))
    )


def _write_quarantine(df: DataFrame, path, run_id: str) -> None:
    quarantine = df.select(
        "event_id",
        F.col(FAILED_FATAL_COL).alias("failed_rules"),
        F.col(FAILED_WARNING_COL).alias("warning_rules"),
        "source_file",
        "source_checksum",
        "ingestion_run_id",
        "extraction_window",
        "event_time",
        "magnitude",
        "latitude",
        "longitude",
        "depth_km",
        "place",
        "status",
        F.lit(run_id).alias("silver_run_id"),
        F.col("silver_processing_timestamp").alias("processing_timestamp"),
    )
    quarantine.write.mode("overwrite").parquet(str(path))


def _write_quality_results(spark, results, path) -> None:
    rows = [asdict(r) for r in results]
    spark.createDataFrame(rows).write.mode("overwrite").parquet(str(path))


def _write_report(result: SilverResult, quality_results, settings) -> str:
    from uuid import uuid4

    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "summary": asdict(result),
        "quality_results": [asdict(r) for r in quality_results],
    }
    path = (
        settings.warehouse_path
        / "_reports"
        / "silver"
        / f"run_id={result.run_id}"
        / f"silver_{uuid4().hex}.json"
    )
    atomic_write(
        path,
        json.dumps(report, indent=2, sort_keys=True, allow_nan=False).encode("utf-8"),
    )
    return str(path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the Silver layer")
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
    )
    spark = build_spark_session("silver-build")
    try:
        result = build_silver(spark, args.run_id)
        print(json.dumps(asdict(result), indent=2))
        if result.status != "PASS":
            raise SystemExit(1)
    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    main()
