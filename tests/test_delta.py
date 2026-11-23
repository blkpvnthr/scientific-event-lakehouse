"""Tests for Delta Lake transaction semantics (Milestone 5)."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from delta.tables import DeltaTable
from pyspark.sql import functions as F

from src.delta_lake import (
    insert_revisions,
    record_pipeline_run,
    table_version,
    update_watermark,
    upsert_current,
)
from src.delta_lake.metadata import read_watermark

T0 = datetime(2024, 1, 1, tzinfo=UTC)
T1 = datetime(2024, 1, 2, tzinfo=UTC)
T2 = datetime(2024, 1, 3, tzinfo=UTC)


def _rev(spark, rows):
    """Build a revisions-like DataFrame."""
    return spark.createDataFrame(rows)


def _row(event_id, fp, updated, mag, year=2024):
    return {
        "event_id": event_id,
        "content_fingerprint": fp,
        "updated_time": updated,
        "magnitude": mag,
        "event_year": year,
    }


def test_revisions_insert_only_idempotent(spark, tmp_settings):
    path = tmp_settings.delta_table_path("silver", "rev_idem")
    df = _rev(spark, [_row("e1", "fp1", T1, 4.0), _row("e2", "fp1", T1, 3.0)])
    insert_revisions(spark, path, df)
    insert_revisions(spark, path, df)  # replay identical data
    result = spark.read.format("delta").load(str(path))
    assert result.count() == 2  # no duplicates on replay


def test_revisions_accumulate_distinct(spark, tmp_settings):
    path = tmp_settings.delta_table_path("silver", "rev_accum")
    insert_revisions(spark, path, _rev(spark, [_row("e1", "fp1", T1, 4.0)]))
    # Same event, new fingerprint (a genuinely new revision).
    insert_revisions(spark, path, _rev(spark, [_row("e1", "fp2", T2, 4.5)]))
    result = spark.read.format("delta").load(str(path))
    assert result.filter("event_id = 'e1'").count() == 2


def test_current_newer_revision_wins(spark, tmp_settings):
    path = tmp_settings.delta_table_path("silver", "cur_newer")
    upsert_current(spark, path, _rev(spark, [_row("e1", "fp1", T1, 4.0)]))
    upsert_current(spark, path, _rev(spark, [_row("e1", "fp2", T2, 4.5)]))
    row = spark.read.format("delta").load(str(path)).collect()[0]
    assert row["magnitude"] == 4.5


def test_current_late_older_revision_ignored(spark, tmp_settings):
    path = tmp_settings.delta_table_path("silver", "cur_late")
    upsert_current(spark, path, _rev(spark, [_row("e1", "fp2", T2, 4.5)]))
    # Late-arriving OLDER revision must not overwrite the newer current.
    upsert_current(spark, path, _rev(spark, [_row("e1", "fp0", T0, 9.9)]))
    row = spark.read.format("delta").load(str(path)).collect()[0]
    assert row["magnitude"] == 4.5


def test_current_idempotent_replay(spark, tmp_settings):
    path = tmp_settings.delta_table_path("silver", "cur_idem")
    df = _rev(spark, [_row("e1", "fp1", T1, 4.0)])
    upsert_current(spark, path, df)
    upsert_current(spark, path, df)
    assert spark.read.format("delta").load(str(path)).count() == 1


def test_time_travel(spark, tmp_settings):
    path = tmp_settings.delta_table_path("silver", "tt")
    upsert_current(spark, path, _rev(spark, [_row("e1", "fp1", T1, 4.0)]))
    upsert_current(spark, path, _rev(spark, [_row("e1", "fp2", T2, 4.5)]))
    latest = table_version(spark, path)
    assert latest >= 1
    v0 = (
        spark.read.format("delta").option("versionAsOf", 0).load(str(path)).collect()[0]
    )
    assert v0["magnitude"] == 4.0  # historical version preserved


def test_schema_evolution_additive(spark, tmp_settings):
    path = tmp_settings.delta_table_path("silver", "evo")
    upsert_current(spark, path, _rev(spark, [_row("e1", "fp1", T1, 4.0)]))
    # New revision carries an additional column; auto-merge adds it.
    extended = _rev(spark, [_row("e2", "fp1", T1, 3.0)]).withColumn(
        "new_attr", F.lit("x")
    )
    upsert_current(spark, path, extended)
    cols = set(spark.read.format("delta").load(str(path)).columns)
    assert "new_attr" in cols


def test_transactional_atomicity_on_failure(spark, tmp_settings):
    """A failing MERGE must leave the table unchanged (atomic)."""
    path = tmp_settings.delta_table_path("silver", "atomic")
    upsert_current(spark, path, _rev(spark, [_row("e1", "fp1", T1, 4.0)]))
    version_before = table_version(spark, path)
    count_before = spark.read.format("delta").load(str(path)).count()

    # Two source rows match the same target key -> MERGE must raise.
    bad = _rev(spark, [_row("e1", "fpA", T2, 5.0), _row("e1", "fpB", T2, 6.0)])
    target = DeltaTable.forPath(spark, str(path))
    # Delta raises on multiple source matches; exact type varies by version.
    with pytest.raises(Exception):  # noqa: B017
        (
            target.alias("t")
            .merge(bad.alias("s"), "t.event_id = s.event_id")
            .whenMatchedUpdateAll()
            .execute()
        )
    # Table is intact: version and data unchanged.
    assert table_version(spark, path) == version_before
    assert spark.read.format("delta").load(str(path)).count() == count_before


def test_pipeline_run_upsert(spark, tmp_settings):
    path = tmp_settings.delta_table_path("metadata", "runs")
    for status in ("RUNNING", "SUCCESS"):
        record_pipeline_run(
            spark,
            path,
            run_id="r1",
            stage="delta_merge",
            status=status,
            input_records=10,
            output_records=10,
            quarantined_records=0,
        )
    rows = spark.read.format("delta").load(str(path)).collect()
    assert len(rows) == 1  # upsert, not append
    assert rows[0]["status"] == "SUCCESS"


def test_watermark_advance_only(spark, tmp_settings):
    path = tmp_settings.delta_table_path("metadata", "wm")
    update_watermark(spark, path, name="w", value="2024-01-02", run_id="r1")
    # Older value must not regress the watermark.
    update_watermark(spark, path, name="w", value="2024-01-01", run_id="r2")
    assert read_watermark(spark, path, "w") == "2024-01-02"
    # Newer value advances it.
    update_watermark(spark, path, name="w", value="2024-01-05", run_id="r3")
    assert read_watermark(spark, path, "w") == "2024-01-05"


# --- Watermark commitment is separated from the Delta MERGE (v1.0.2 audit) ---


def _build_silver_from_synthetic(spark, settings):
    """Produce Silver Parquet outputs from a tiny synthetic run."""
    from conftest import write_synthetic_run

    from src.bronze import build_bronze
    from src.silver import build_silver

    run_id = write_synthetic_run(settings)
    build_bronze(spark, run_id, settings, write_report=False)
    build_silver(spark, run_id, settings, write_report=False)
    return run_id


def test_merge_does_not_advance_watermark(spark, tmp_settings):
    from src.delta_lake.build_delta import build_delta

    run_id = _build_silver_from_synthetic(spark, tmp_settings)
    wm_path = tmp_settings.delta_table_path("metadata", "processing_watermarks")

    # Merge only (default): the watermark table must not even be created.
    result = build_delta(spark, run_id, tmp_settings)
    assert result.current_rows == 3
    assert read_watermark(spark, wm_path, "silver_max_updated_time") is None


def test_commit_watermark_advances_after_merge(spark, tmp_settings):
    from src.delta_lake.build_delta import build_delta, commit_watermark

    run_id = _build_silver_from_synthetic(spark, tmp_settings)
    wm_path = tmp_settings.delta_table_path("metadata", "processing_watermarks")

    build_delta(spark, run_id, tmp_settings)
    assert read_watermark(spark, wm_path, "silver_max_updated_time") is None

    # Simulated: Gold + reconciliation succeeded, so commit the watermark.
    wm = commit_watermark(spark, run_id, tmp_settings)
    assert wm is not None
    assert read_watermark(spark, wm_path, "silver_max_updated_time") == wm


def test_downstream_failure_before_commit_leaves_watermark_unchanged(
    spark, tmp_settings
):
    """If a downstream stage (Gold/reconcile) fails, commit_watermark is never
    reached, so the watermark stays unset/unchanged."""
    from src.delta_lake.build_delta import build_delta

    run_id = _build_silver_from_synthetic(spark, tmp_settings)
    wm_path = tmp_settings.delta_table_path("metadata", "processing_watermarks")
    build_delta(spark, run_id, tmp_settings)  # merge succeeds
    # ... Gold raises here in a real run; commit_watermark is NOT called ...
    assert read_watermark(spark, wm_path, "silver_max_updated_time") is None


def test_commit_watermark_idempotent_and_advance_only(spark, tmp_settings):
    from src.delta_lake.build_delta import build_delta, commit_watermark

    run_id = _build_silver_from_synthetic(spark, tmp_settings)
    wm_path = tmp_settings.delta_table_path("metadata", "processing_watermarks")
    build_delta(spark, run_id, tmp_settings)

    wm1 = commit_watermark(spark, run_id, tmp_settings)
    wm2 = commit_watermark(spark, run_id, tmp_settings)  # replay, identical data
    assert wm1 == wm2
    assert read_watermark(spark, wm_path, "silver_max_updated_time") == wm1

    # An older replayed value must not regress the committed watermark.
    update_watermark(
        spark,
        wm_path,
        name="silver_max_updated_time",
        value="1970-01-01T00:00:00.000",
        run_id="old",
    )
    assert read_watermark(spark, wm_path, "silver_max_updated_time") == wm1
