"""Synthetic end-to-end pipeline test (Bronze -> Silver -> Delta -> Gold).

Runs the full medallion chain on a tiny synthetic ingestion run, with no network
and no cloud credentials. This is the fast, deterministic end-to-end check used
by CI (the full 281k dataset is not required for correctness).
"""

from __future__ import annotations

from conftest import write_synthetic_run

from src.bronze import build_bronze
from src.delta_lake.build_delta import build_delta
from src.gold import build_gold
from src.silver import build_silver


def test_full_pipeline_synthetic(spark, tmp_settings):
    run_id = write_synthetic_run(tmp_settings)

    bronze = build_bronze(spark, run_id, tmp_settings, write_report=False)
    assert bronze.status == "PASS"
    assert bronze.output_records == 3

    silver = build_silver(spark, run_id, tmp_settings, write_report=False)
    assert silver.status == "PASS"
    assert silver.valid_records == 3
    assert silver.quarantined_records == 0

    delta = build_delta(spark, run_id, tmp_settings)
    assert delta.current_rows == 3
    assert delta.revisions_rows == 3

    gold = build_gold(spark, tmp_settings)
    assert gold.status == "PASS"
    assert gold.fact_rows == 3

    # Replaying the whole chain is idempotent end-to-end.
    build_bronze(spark, run_id, tmp_settings, write_report=False)
    build_silver(spark, run_id, tmp_settings, write_report=False)
    delta2 = build_delta(spark, run_id, tmp_settings)
    assert delta2.current_rows == 3
    assert delta2.revisions_rows == 3  # no duplicate revisions on replay
