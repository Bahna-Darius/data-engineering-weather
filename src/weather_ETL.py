from config import DATABASE_URL, LONGITUDE, LATITUDE, BASE_URL
from typing import Dict, Any, Optional
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import requests
import logging
import json
import csv
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

engine = create_engine(DATABASE_URL)


def extract_weather_data(lat: float, lon: float) -> Optional[Dict[str, Any]]:
    """Fetches current weather metrics from the Open-Meteo API."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "current_weather": "true"
    }
    try:
        logger.info(f"Initiating data extraction for Lat: {lat}, Lon: {lon}...")
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        logger.info("Extraction successful.")
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Network or API Error during extraction: {e}")
        return None


def transform_data(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """Transforms nested API JSON into a flat dict optimized for tabular storage."""
    if not raw_data or "current_weather" not in raw_data:
        raise ValueError("Invalid raw data: 'current_weather' key missing.")

    current = raw_data["current_weather"]

    return {
        "ingestion_timestamp": datetime.now().isoformat(),
        "temperature_celsius": current.get("temperature"),
        "windspeed_kmh": current.get("windspeed"),
        "wind_direction": current.get("winddirection"),
        "weather_code": current.get("weathercode"),
        "is_day": bool(current.get("is_day")),
        "location_lat": raw_data.get("latitude"),
        "location_lon": raw_data.get("longitude")
    }


def load_data_to_csv(data: Dict[str, Any], filename: str = "data/weather_data.csv"):
    """Persists processed data to a local CSV file (Bronze landing zone)."""
    file_exists = os.path.isfile(filename)
    try:
        with open(filename, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=data.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(data)
        logger.info(f"Data persisted to {filename}")
    except IOError as e:
        logger.error(f"File I/O Error: {e}")


def load_data_to_db(data: Dict[str, Any]) -> None:
    """Converts dict to DataFrame and appends it to the PostgreSQL database."""
    try:
        pd.DataFrame([data]).to_sql(
            name='weather_data',
            con=engine,
            if_exists='append',
            index=False,
        )
        logger.info("Data successfully pushed to database.")
    except Exception as e:
        logger.error(f"Database Load Error: {e}")


def main():
    logger.info("--- ETL Job Started ---")

    raw_weather = extract_weather_data(LATITUDE, LONGITUDE)

    if raw_weather:
        try:
            clean_weather = transform_data(raw_weather)
            load_data_to_csv(clean_weather)
            load_data_to_db(clean_weather)
            logger.info(f"Processed record: {json.dumps(clean_weather, indent=2)}")
        except ValueError as ve:
            logger.error(f"Data Validation Error: {ve}")
    else:
        logger.warning("No data retrieved from source.")

    logger.info("--- ETL Job Completed ---")


if __name__ == "__main__":
    main()
