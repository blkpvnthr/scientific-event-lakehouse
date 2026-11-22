from collections.abc import Iterator
from datetime import UTC, datetime, timedelta


def generate_date_windows(
    start_date: str,
    end_date: str,
    window_days: int = 7,
) -> Iterator[tuple[str, str]]:
    """Generate contiguous, non-overlapping UTC date windows.

    The overall start date is inclusive.
    The overall end date is exclusive.
    Individual query windows have inclusive endpoints.
    """

    start = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=UTC)

    end = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=UTC)

    if start >= end:
        raise ValueError("start_date must be before end_date")

    if window_days < 1:
        raise ValueError("window_days must be greater than zero")

    current = start

    while current < end:
        next_boundary = min(
            current + timedelta(days=window_days),
            end,
        )

        window_end = next_boundary - timedelta(milliseconds=1)

        yield (
            current.isoformat(timespec="milliseconds"),
            window_end.isoformat(timespec="milliseconds"),
        )

        current = next_boundary
