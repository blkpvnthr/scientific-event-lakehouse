import random
import time

import requests
from requests.exceptions import (
    ConnectionError,
    HTTPError,
    Timeout,
)

BASE_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"

COUNT_URL = "https://earthquake.usgs.gov/fdsnws/event/1/count"

RETRYABLE_STATUS_CODES = {
    429,
    500,
    502,
    503,
    504,
}


def request_with_retry(
    url: str,
    params: dict,
    max_attempts: int = 5,
) -> requests.Response:

    if max_attempts < 1:
        raise ValueError("max_attempts must be positive")

    for attempt in range(max_attempts):
        try:
            response = requests.get(
                url,
                params=params,
                timeout=(10, 60),
            )

            response.raise_for_status()

            return response

        except HTTPError as exc:
            if exc.response is None:
                raise

            status = exc.response.status_code

            if status not in RETRYABLE_STATUS_CODES:
                raise

            if attempt == max_attempts - 1:
                raise

        except (ConnectionError, Timeout):
            if attempt == max_attempts - 1:
                raise

        delay = (2**attempt) + random.random()

        time.sleep(delay)

    raise RuntimeError("Retry loop exhausted")


def build_query_params(
    start_date: str,
    end_date: str,
    min_magnitude: float = 2.5,
) -> dict:

    return {
        "starttime": start_date,
        "endtime": end_date,
        "minmagnitude": min_magnitude,
        "eventtype": "earthquake",
    }


def count_earthquakes(
    start_date: str,
    end_date: str,
    min_magnitude: float = 2.5,
) -> int:

    params = build_query_params(
        start_date,
        end_date,
        min_magnitude,
    )

    response = request_with_retry(
        COUNT_URL,
        params,
    )

    return int(response.text.strip())


def fetch_earthquake_page(
    start_date: str,
    end_date: str,
    min_magnitude: float = 2.5,
    limit: int = 1000,
    offset: int = 1,
) -> bytes:

    if not 1 <= limit <= 20000:
        raise ValueError("Invalid page size")

    if offset < 1:
        raise ValueError("Invalid offset")

    params = build_query_params(
        start_date,
        end_date,
        min_magnitude,
    )

    params.update(
        {
            "format": "geojson",
            "limit": limit,
            "offset": offset,
            "orderby": "time-asc",
        }
    )

    response = request_with_retry(
        BASE_URL,
        params,
    )

    return response.content


def fetch_earthquakes(
    start_date: str,
    end_date: str,
    min_magnitude: float = 2.5,
    limit: int = 1000,
) -> dict:

    params = {
        "format": "geojson",
        "starttime": start_date,
        "endtime": end_date,
        "minmagnitude": min_magnitude,
        "eventtype": "earthquake",
        "limit": limit,
        "orderby": "time-asc",
    }

    response = requests.get(
        BASE_URL,
        params=params,
        timeout=30,
    )

    response.raise_for_status()

    return response.json()
