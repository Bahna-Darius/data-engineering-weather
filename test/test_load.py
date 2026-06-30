"""
Tests for load.load_to_csv().

Covers:
- new file: first call writes the header row before the data
- existing file: second call appends data without duplicating the header
"""

import importlib
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

load_to_csv = importlib.import_module("load").load_to_csv


@pytest.fixture
def csv_file(tmp_path):
    path = str(tmp_path / "weather.csv")
    data = {
        "ingestion_timestamp": "2026-06-29T08:15:00",
        "temperature_celsius": 30.7,
        "windspeed_kmh": 3.8,
        "wind_direction": 253,
        "weather_code": 0,
        "is_day": True,
        "location_lat": 45.4375,
        "location_lon": 23.375,
    }
    return path, data


def test_new_file_writes_header(csv_file):
    path, data = csv_file
    load_to_csv(data=data, filename=path)

    with open(path) as f:
        first_line = f.readline().strip()

    for key in data.keys():
        assert key in first_line        # check columns


def test_existing_file_no_duplicate_header(csv_file):
    path, data = csv_file
    load_to_csv(data=data, filename=path)
    load_to_csv(data=data, filename=path)

    with open(path) as f:
        lines = f.readlines()

    assert len(lines) == 3
