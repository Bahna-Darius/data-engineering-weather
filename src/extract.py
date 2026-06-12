from config import BASE_URL, LATITUDE, LONGITUDE
from typing import Any, Dict, Optional
import requests
import logging

logger = logging.getLogger(__name__)


def extract_weather_data(lat: float = LATITUDE, lon: float = LONGITUDE) -> Optional[Dict[str, Any]]:
    """Fetches current weather metrics from the Open-Meteo API."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "current_weather": "true"
    }
    try:
        logger.info(f"Initiating extraction for Lat: {lat}, Lon: {lon}...")
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        logger.info("Extraction successful.")
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Network or API Error: {e}")
        return None
