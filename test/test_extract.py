"""
Tests for extract.extract_weather_data().

Covers:
- happy path: API returns valid JSON → function returns dict with 'current_weather'
- network error: requests raises RequestException → function returns None
"""

import sys
import importlib
from pathlib import Path
from unittest.mock import patch

import requests
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

extract_weather_data = importlib.import_module("extract").extract_weather_data


@pytest.fixture
def mock_api_response():
    return {
        "current_weather": {
            "time": "2026-06-29T08:15",
            "temperature": 30.7,
            "windspeed": 3.8,
            "winddirection": 253,
            "is_day": 1,
            "weathercode": 0,
        },
        "latitude": 45.4375,
        "longitude": 23.375,
    }


@patch(target="extract.requests.get", autospec=True)
def test_happy_path(mock_get, mock_api_response):
    mock_get.return_value.raise_for_status.return_value = None
    mock_get.return_value.json.return_value = mock_api_response

    result = extract_weather_data()

    assert result is not None
    assert "current_weather" in result


@patch("extract.time.sleep")
@patch(target="extract.requests.get", autospec=True)
def test_network_error_returns_none(mock_get, mock_sleep):
    mock_get.side_effect = requests.exceptions.RequestException("timeout")

    result = extract_weather_data()

    assert result is None
