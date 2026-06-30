"""
Tests for transform.transform_data().

Covers:
- happy path: raw API dict → transformed dict with correct keys, ingestion_timestamp, bool is_day
- missing key: empty dict → raises ValueError
"""

import sys
import importlib
from pathlib import Path
from typing import Dict, Any

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

transform_data = importlib.import_module("transform").transform_data


@pytest.fixture
def raw_api_data_response():
    return {
        "current_weather": {
            "temperature": 13,
            "windspeed": 3.8,
            "winddirection": 319,
            "weathercode": 2,
            "is_day": 0,
        },
        "latitude": 45.4375,
        "longitude": 23.375,
    }


def test_transform_happy_path(raw_api_data_response: Dict[str, Any]):
    result = transform_data(raw_api_data_response)

    assert result is not None
    assert "temperature_celsius" in result
    assert result.get("ingestion_timestamp")
    assert isinstance(result["is_day"], bool)


def test_transform_missing_key():
    with pytest.raises(ValueError):
        transform_data({})
