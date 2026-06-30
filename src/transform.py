from typing import Any, Dict
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


def transform_data(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """Transforms nested API JSON into a flat dict optimized for tabular storage."""
    if not raw_data or "current_weather" not in raw_data:
        raise ValueError("Invalid raw data: 'current_weather' key missing.")

    current = raw_data["current_weather"]

    transformed = {
        "ingestion_timestamp": datetime.now().isoformat(),
        "temperature_celsius": current.get("temperature"),
        "windspeed_kmh": current.get("windspeed"),
        "wind_direction": current.get("winddirection"),
        "weather_code": current.get("weathercode"),
        "is_day": bool(current.get("is_day")),
        "location_lat": raw_data.get("latitude"),
        "location_lon": raw_data.get("longitude")
    }

    if not (-90 <= transformed["temperature_celsius"] <= 60):
        raise ValueError(f"Temperature out of range: {transformed['temperature_celsius']}")
    if transformed["windspeed_kmh"] < 0:
        raise ValueError(f"Windspeed cannot be negative: {transformed['windspeed_kmh']}")
    if transformed["weather_code"] < 0:
        raise ValueError(f"Invalid weather code: {transformed['weather_code']}")

    logger.info("Transformation completed successfully.")
    return transformed
