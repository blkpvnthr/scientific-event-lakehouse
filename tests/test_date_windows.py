from datetime import datetime, timedelta
from itertools import pairwise

import pytest

from src.ingestion.date_windows import generate_date_windows


def test_generate_date_windows():
    windows = list(
        generate_date_windows(
            "2024-01-01",
            "2024-02-01",
            window_days=7,
        )
    )

    assert len(windows) == 5


def test_invalid_date_range():
    with pytest.raises(ValueError):
        list(
            generate_date_windows(
                "2024-02-01",
                "2024-01-01",
            )
        )


def test_invalid_window_size():
    with pytest.raises(ValueError):
        list(
            generate_date_windows(
                "2024-01-01",
                "2024-02-01",
                window_days=0,
            )
        )


def test_windows_do_not_overlap():
    windows = list(
        generate_date_windows(
            "2024-01-01",
            "2024-02-01",
            window_days=7,
        )
    )

    for previous, current in pairwise(windows):
        previous_end = datetime.fromisoformat(previous[1])
        current_start = datetime.fromisoformat(current[0])

        assert previous_end + timedelta(milliseconds=1) == current_start


def test_leap_year():
    windows = list(
        generate_date_windows(
            "2024-02-01",
            "2024-03-01",
            window_days=31,
        )
    )

    assert len(windows) == 1

    assert windows[0][1].startswith("2024-02-29T23:59:59.999")


def test_single_day_window():
    windows = list(
        generate_date_windows(
            "2024-01-01",
            "2024-01-02",
            window_days=7,
        )
    )

    assert len(windows) == 1

    assert windows[0][0].startswith("2024-01-01T00:00:00.000")

    assert windows[0][1].startswith("2024-01-01T23:59:59.999")
