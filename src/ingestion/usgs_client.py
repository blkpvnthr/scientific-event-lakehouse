import requests

BASE_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"


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
