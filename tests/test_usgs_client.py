from unittest.mock import patch

from requests.exceptions import Timeout

from src.ingestion.usgs_client import (
    fetch_earthquakes,
    request_with_retry,
)


@patch("src.ingestion.usgs_client.requests.get")
def test_fetch_earthquakes(mock_get):

    mock_response = mock_get.return_value

    mock_response.json.return_value = {
        "type": "FeatureCollection",
        "features": [],
        "metadata": {"count": 0},
    }

    result = fetch_earthquakes(
        start_date="2024-01-01",
        end_date="2024-01-07",
    )

    assert result["type"] == "FeatureCollection"
    assert isinstance(result["features"], list)

    mock_response.raise_for_status.assert_called_once()
    mock_get.assert_called_once()


@patch("src.ingestion.usgs_client.requests.get")
def test_request_parameters(mock_get):

    fetch_earthquakes(
        start_date="2024-01-01",
        end_date="2024-01-07",
        min_magnitude=4.0,
    )

    params = mock_get.call_args.kwargs["params"]

    assert params["format"] == "geojson"
    assert params["minmagnitude"] == 4.0
    assert params["starttime"] == "2024-01-01"
    assert params["eventtype"] == "earthquake"


@patch("src.ingestion.usgs_client.time.sleep")
@patch("src.ingestion.usgs_client.requests.get")
def test_retry_after_timeout(
    mock_get,
    mock_sleep,
):

    successful_response = mock_get.return_value

    mock_get.side_effect = [
        Timeout("Temporary timeout"),
        successful_response,
    ]

    result = request_with_retry(
        url="https://example.com",
        params={},
    )

    assert result is successful_response
    assert mock_get.call_count == 2

    mock_sleep.assert_called_once()
