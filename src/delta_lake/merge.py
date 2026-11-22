"""Delta Lake MERGE operations for Silver tables.

Transaction semantics (the heart of Milestone 5):

* **Revisions table** — insert-only, keyed on ``(event_id, content_fingerprint)``.
  Replaying identical data inserts nothing new, so revision history never
  duplicates. New distinct revisions accumulate.
* **Current table** — upsert keyed on ``event_id``. A matched row is updated
  **only when** the incoming ``updated_time`` is strictly greater than the stored
  one, so a newer source revision wins and a late-arriving *older* revision never
  overwrites a newer current record.
* **Quarantine table** — upsert keyed on ``event_id`` (insert when absent),
  idempotent under replay.

Content fingerprints and source revision times break conflicts deterministically.
Additive schema evolution is enabled via Delta auto-merge on the session.
"""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def table_version(spark: SparkSession, path) -> int:
    """Return the latest committed Delta version for a table (-1 if absent)."""
    if not DeltaTable.isDeltaTable(spark, str(path)):
        return -1
    return (
        DeltaTable.forPath(spark, str(path))
        .history(1)
        .select("version")
        .collect()[0]["version"]
    )


def latest_per_event(df: DataFrame) -> DataFrame:
    """Reduce multiple revisions to one current row per event (deterministic)."""
    window = Window.partitionBy("event_id").orderBy(
        F.col("updated_time").desc_nulls_last(),
        F.col("content_fingerprint").desc(),
    )
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def _create_partitioned(df: DataFrame, path, partition: str | None) -> None:
    writer = df.write.format("delta").mode("overwrite")
    if partition:
        writer = writer.partitionBy(partition)
    writer.save(str(path))


def upsert_current(
    spark: SparkSession, path, incoming: DataFrame, partition: str = "event_year"
) -> None:
    """Upsert current-state events; newer revisions win, older never regress."""
    latest = latest_per_event(incoming)
    if not DeltaTable.isDeltaTable(spark, str(path)):
        _create_partitioned(latest, path, partition)
        return
    target = DeltaTable.forPath(spark, str(path))
    (
        target.alias("t")
        .merge(latest.alias("s"), "t.event_id = s.event_id")
        .whenMatchedUpdateAll(condition="s.updated_time > t.updated_time")
        .whenNotMatchedInsertAll()
        .execute()
    )


def insert_revisions(
    spark: SparkSession, path, incoming: DataFrame, partition: str = "event_year"
) -> None:
    """Insert-only merge of observed revisions (idempotent on replay)."""
    if not DeltaTable.isDeltaTable(spark, str(path)):
        _create_partitioned(incoming, path, partition)
        return
    target = DeltaTable.forPath(spark, str(path))
    (
        target.alias("t")
        .merge(
            incoming.alias("s"),
            "t.event_id = s.event_id AND t.content_fingerprint = s.content_fingerprint",
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def upsert_quarantine(spark: SparkSession, path, incoming: DataFrame) -> None:
    """Upsert quarantined events keyed on event_id (idempotent)."""
    if not DeltaTable.isDeltaTable(spark, str(path)):
        incoming.write.format("delta").mode("overwrite").save(str(path))
        return
    target = DeltaTable.forPath(spark, str(path))
    (
        target.alias("t")
        .merge(incoming.alias("s"), "t.event_id = s.event_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
