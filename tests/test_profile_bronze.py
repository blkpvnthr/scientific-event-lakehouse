import pytest

from src.profiling.profile_bronze import (
    magnitude_bucket,
    summarize_numeric,
    timestamp_to_iso,
)


def test_numeric_summary():
    result = summarize_numeric([2.5, 3.0, 3.5, 4.0])

    assert result["count"] == 4
    assert result["min"] == 2.5
    assert result["max"] == 4.0
    assert result["mean"] == 3.25
    assert result["median"] == 3.25


def test_empty_numeric_summary():
    result = summarize_numeric([])

    assert result["count"] == 0
    assert result["min"] is None
    assert result["max"] is None
    assert result["mean"] is None
    assert result["median"] is None


@pytest.mark.parametrize(
    ("magnitude", "expected"),
    [
        (2.0, "below_2.5"),
        (2.5, "2.5_to_3.0"),
        (2.99, "2.5_to_3.0"),
        (3.0, "3.0_to_4.0"),
        (4.0, "4.0_to_5.0"),
        (5.0, "5.0_to_6.0"),
        (6.0, "6.0_to_7.0"),
        (7.0, "7.0_and_above"),
    ],
)
def test_magnitude_bucket(magnitude, expected):
    assert magnitude_bucket(magnitude) == expected


def test_timestamp_conversion():
    assert timestamp_to_iso(1704067200000) == ("2024-01-01T00:00:00+00:00")
