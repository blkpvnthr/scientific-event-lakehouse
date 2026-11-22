"""Pipeline metadata Delta tables: runs and processing watermarks.

* ``pipeline_runs`` — one row per (run_id, stage); upserted so replays update in
  place rather than appending duplicates.
* ``processing_watermarks`` — advance-only: a watermark is updated only when the
  incoming value is strictly greater, so a replay or an out-of-order run can
  never move a watermark backwards.
"""

from __future__ import annotations

from datetime import UTC, datetime

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def record_pipeline_run(
    spark: SparkSession,
    path,
    *,
    run_id: str,
    stage: str,
    status: str,
    input_records: int,
    output_records: int,
    quarantined_records: int,
    extra: dict | None = None,
) -> None:
    """Upsert a pipeline-run metadata row keyed on (run_id, stage)."""
    row = {
        "run_id": run_id,
        "stage": stage,
        "status": status,
        "input_records": int(input_records),
        "output_records": int(output_records),
        "quarantined_records": int(quarantined_records),
        "recorded_at": datetime.now(UTC).isoformat(),
        "details": "" if not extra else str(extra),
    }
    incoming = spark.createDataFrame([row])
    if not DeltaTable.isDeltaTable(spark, str(path)):
        incoming.write.format("delta").mode("overwrite").save(str(path))
        return
    target = DeltaTable.forPath(spark, str(path))
    (
        target.alias("t")
        .merge(
            incoming.alias("s"),
            "t.run_id = s.run_id AND t.stage = s.stage",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def update_watermark(
    spark: SparkSession,
    path,
    *,
    name: str,
    value: str,
    run_id: str,
) -> None:
    """Advance a named watermark only if the new value is strictly greater."""
    row = {
        "watermark_name": name,
        "watermark_value": value,
        "run_id": run_id,
        "updated_at": datetime.now(UTC).isoformat(),
    }
    incoming = spark.createDataFrame([row])
    if not DeltaTable.isDeltaTable(spark, str(path)):
        incoming.write.format("delta").mode("overwrite").save(str(path))
        return
    target = DeltaTable.forPath(spark, str(path))
    (
        target.alias("t")
        .merge(incoming.alias("s"), "t.watermark_name = s.watermark_name")
        .whenMatchedUpdate(
            condition="s.watermark_value > t.watermark_value",
            set={
                "watermark_value": "s.watermark_value",
                "run_id": "s.run_id",
                "updated_at": "s.updated_at",
            },
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def read_watermark(spark: SparkSession, path, name: str) -> str | None:
    """Return a watermark value, or None if unset."""
    if not DeltaTable.isDeltaTable(spark, str(path)):
        return None
    rows = (
        spark.read.format("delta")
        .load(str(path))
        .filter(F.col("watermark_name") == name)
        .collect()
    )
    return rows[0]["watermark_value"] if rows else None
