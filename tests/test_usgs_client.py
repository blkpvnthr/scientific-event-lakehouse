from unittest.mock import patch

from src.ingestion.usgs_client import fetch_earthquakes


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
