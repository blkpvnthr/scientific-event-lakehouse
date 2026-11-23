"""Merge Silver Parquet outputs into transactional Delta tables.

Reads the Silver layer produced by Milestone 4 and performs idempotent,
ACID MERGE operations into the Delta tables, then records pipeline-run metadata
and advances the processing watermark. Safe to replay: identical input produces
no new revisions and never regresses current state or watermarks.
"""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import asdict, dataclass

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.config import Settings, get_settings
from src.delta_lake.merge import (
    insert_revisions,
    table_version,
    upsert_current,
    upsert_quarantine,
)
from src.delta_lake.metadata import record_pipeline_run, update_watermark
from src.spark import build_spark_session, stop_spark_session

logger = logging.getLogger(__name__)

_TS_FMT = "yyyy-MM-dd'T'HH:mm:ss.SSS"
WATERMARK_NAME = "silver_max_updated_time"


@dataclass(frozen=True)
class DeltaResult:
    run_id: str
    current_rows: int
    revisions_rows: int
    quarantine_rows: int
    current_version: int
    revisions_version: int
    watermark: str | None
    status: str


def _merge_quality(spark: SparkSession, path, incoming) -> None:
    """Upsert data-quality results keyed on (run_id, rule_id)."""
    if not DeltaTable.isDeltaTable(spark, str(path)):
        incoming.write.format("delta").mode("overwrite").save(str(path))
        return
    target = DeltaTable.forPath(spark, str(path))
    (
        target.alias("t")
        .merge(
            incoming.alias("s"),
            "t.run_id = s.run_id AND t.rule_id = s.rule_id",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def _watermark_candidate(spark: SparkSession, silver) -> str | None:
    """Compute the advance-only watermark value (max observed source revision)."""
    revisions = spark.read.parquet(str(silver / "earthquake_event_revisions"))
    return revisions.agg(
        F.date_format(F.max("updated_time"), _TS_FMT).alias("m")
    ).collect()[0]["m"]


def commit_watermark(
    spark: SparkSession, run_id: str, settings: Settings | None = None
) -> str | None:
    """Commit the processing watermark — the FINAL step of a successful run.

    Kept separate from the Delta MERGE so the watermark advances only after the
    merge, Gold, and reconciliation have all succeeded. Advance-only: a smaller
    value (older/replayed run) never regresses the stored watermark.
    """
    settings = settings or get_settings()
    watermark = _watermark_candidate(spark, settings.silver_path)
    if watermark:
        update_watermark(
            spark,
            settings.delta_table_path("metadata", "processing_watermarks"),
            name=WATERMARK_NAME,
            value=watermark,
            run_id=run_id,
        )
    logger.info("Watermark committed: %s (run=%s)", watermark, run_id)
    return watermark


def build_delta(
    spark: SparkSession,
    run_id: str,
    settings: Settings | None = None,
    *,
    advance_watermark: bool = False,
) -> DeltaResult:
    """Merge Silver outputs into Delta and record run metadata.

    Does NOT advance the processing watermark by default: the watermark is
    committed by ``commit_watermark`` only after Gold and reconciliation succeed
    (see the DAGs). The watermark *candidate* is still computed and returned.
    ``advance_watermark=True`` is available for standalone callers that
    intentionally couple the two.
    """
    settings = settings or get_settings()
    silver = settings.silver_path

    incoming_all = spark.read.parquet(str(silver / "earthquake_event_revisions"))
    incoming_quarantine = spark.read.parquet(str(silver / "quarantined_events"))
    incoming_quality = spark.read.parquet(str(silver / "data_quality_results"))

    current_path = settings.delta_table_path("silver", "earthquake_events_current")
    revisions_path = settings.delta_table_path("silver", "earthquake_event_revisions")
    quarantine_path = settings.delta_table_path("silver", "quarantined_events")
    quality_path = settings.delta_table_path("silver", "data_quality_results")
    runs_path = settings.delta_table_path("metadata", "pipeline_runs")
    watermark_path = settings.delta_table_path("metadata", "processing_watermarks")

    upsert_current(spark, current_path, incoming_all)
    insert_revisions(spark, revisions_path, incoming_all)
    quarantine_rows = incoming_quarantine.count()
    if quarantine_rows:
        upsert_quarantine(spark, quarantine_path, incoming_quarantine)
    _merge_quality(spark, quality_path, incoming_quality)

    current_rows = spark.read.format("delta").load(str(current_path)).count()
    revisions_rows = spark.read.format("delta").load(str(revisions_path)).count()

    # Compute the advance-only watermark candidate (max observed source revision).
    # It is NOT committed here by default — commit_watermark() does that only after
    # Gold + reconciliation succeed. advance_watermark=True couples them for
    # standalone callers.
    watermark = incoming_all.agg(
        F.date_format(F.max("updated_time"), _TS_FMT).alias("m")
    ).collect()[0]["m"]
    if advance_watermark and watermark:
        update_watermark(
            spark,
            watermark_path,
            name=WATERMARK_NAME,
            value=watermark,
            run_id=run_id,
        )

    record_pipeline_run(
        spark,
        runs_path,
        run_id=run_id,
        stage="delta_merge",
        status="SUCCESS",
        input_records=incoming_all.count(),
        output_records=current_rows,
        quarantined_records=quarantine_rows,
        extra={"revisions_rows": revisions_rows},
    )

    result = DeltaResult(
        run_id=run_id,
        current_rows=current_rows,
        revisions_rows=revisions_rows,
        quarantine_rows=quarantine_rows,
        current_version=table_version(spark, current_path),
        revisions_version=table_version(spark, revisions_path),
        watermark=watermark,
        status="SUCCESS",
    )
    logger.info(
        "Delta merge complete: current=%s revisions=%s quarantine=%s watermark=%s",
        current_rows,
        revisions_rows,
        quarantine_rows,
        watermark,
    )
    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Merge Silver outputs into Delta tables"
    )
    parser.add_argument("--run-id", required=True)
    parser.add_argument(
        "--commit-watermark",
        action="store_true",
        help="Commit the watermark only (no merge). Run AFTER Gold + reconciliation.",
    )
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
    )
    spark = build_spark_session("delta-merge", with_delta=True)
    try:
        if args.commit_watermark:
            wm = commit_watermark(spark, args.run_id)
            print(json.dumps({"committed_watermark": wm}, indent=2))
        else:
            # Merge only; the watermark is committed in a later step.
            result = build_delta(spark, args.run_id)
            print(json.dumps(asdict(result), indent=2))
    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    main()
