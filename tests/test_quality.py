"""Unit tests for the data-quality rule engine."""

from __future__ import annotations

from datetime import UTC, datetime

from src.quality import annotate_quality, evaluate_rules
from src.quality.engine import FAILED_FATAL_COL, FAILED_WARNING_COL

_T0 = datetime(2015, 1, 1, tzinfo=UTC)
_T1 = datetime(2015, 1, 2, tzinfo=UTC)


def _rows():
    """One good row plus rows that each violate exactly one rule."""
    base = {
        "event_id": "good",
        "event_time": _T0,
        "updated_time": _T1,
        "magnitude": 4.0,
        "magnitude_type": "mb",
        "place": "somewhere",
        "latitude": 10.0,
        "longitude": 20.0,
        "depth_km": 10.0,
        "event_type": "earthquake",
    }
    good = dict(base)
    neg_depth = {**base, "event_id": "negdepth", "depth_km": -3.5}  # valid!
    bad_coords = {**base, "event_id": "badcoord", "latitude": 999.0}
    deep = {**base, "event_id": "toodeep", "depth_km": 5000.0}
    huge_mag = {**base, "event_id": "hugemag", "magnitude": 20.0}
    no_magtype = {**base, "event_id": "nomagtype", "magnitude_type": None}
    future_rev = {**base, "event_id": "badrev", "updated_time": _T0, "event_time": _T1}
    return [good, neg_depth, bad_coords, deep, huge_mag, no_magtype, future_rev]


def test_annotate_flags_fatal_and_warning(spark):
    df = annotate_quality(spark.createDataFrame(_rows()))
    out = {r["event_id"]: r for r in df.collect()}

    assert out["good"][FAILED_FATAL_COL] == []
    assert out["negdepth"][FAILED_FATAL_COL] == []  # negative depth is valid
    assert "coordinates_in_range" in out["badcoord"][FAILED_FATAL_COL]
    assert "depth_plausible" in out["toodeep"][FAILED_FATAL_COL]
    assert "magnitude_plausible" in out["hugemag"][FAILED_FATAL_COL]
    assert "revision_not_before_event" in out["badrev"][FAILED_FATAL_COL]
    # Missing optional field is only a warning, never fatal.
    assert out["nomagtype"][FAILED_FATAL_COL] == []
    assert "magnitude_type_present" in out["nomagtype"][FAILED_WARNING_COL]


def test_evaluate_rules_counts(spark):
    results = {
        r.rule_id: r for r in evaluate_rules(spark.createDataFrame(_rows()), "run1")
    }
    assert results["coordinates_in_range"].observed_failure_count == 1
    assert results["coordinates_in_range"].status == "FAIL"
    assert results["depth_plausible"].observed_failure_count == 1
    assert results["magnitude_plausible"].observed_failure_count == 1
    assert results["revision_not_before_event"].observed_failure_count == 1
    assert results["magnitude_type_present"].observed_failure_count == 1
    assert results["magnitude_type_present"].severity == "warning"
    # A rule that nothing violates passes.
    assert results["id_present"].observed_failure_count == 0
    assert results["id_present"].status == "PASS"
    # Percentage is computed against the row count (7 rows).
    assert results["coordinates_in_range"].failure_percentage == round(1 / 7 * 100, 4)
