from typing import Dict, Any, Optional, List
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import requests
import logging
import time
import json
import csv
import os

# --- LOGGING CONFIGURATION ---
# We use the logging module instead of print() for production-grade traceability.
# This allows us to track timestamps and severity levels (INFO, ERROR).
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)

# --- API CONFIGURATION ---
# Open-Meteo API (Free tier, no API Key required)
# Target Coordinates: Petroșani, Romania
LATITUDE = 45.41
LONGITUDE = 23.37
BASE_URL = "https://api.open-meteo.com/v1/forecast"

# --- DATABASE CONFIGURATION ---
DATABASE_URL = os.getenv('DB_URL')
engine = create_engine(DATABASE_URL)


def extract_weather_data(lat: float, lon: float) -> Optional[Dict[str, Any]]:
    """
    Fetches current weather metrics from the external API.
    Uses Type Hinting for better code maintenance and IDE support.
    """
    params = {
        "latitude": lat,
        "longitude": lon,
        "current_weather": "true"
    }

    try:
        logger.info(f"Initiating data extraction for Lat: {lat}, Lon: {lon}...")
        response = requests.get(BASE_URL, params=params, timeout=10)

        # Raise HTTPError for bad responses (4xx or 5xx)
        response.raise_for_status()

        data = response.json()
        logger.info("Extraction successful.")
        return data

    except requests.exceptions.RequestException as e:
        logger.error(f"Network or API Error during extraction: {e}")
        return None


def transform_data(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transforms complex nested JSON into a flat dictionary format
    optimized for tabular storage (SQL/CSV).
    """
    if not raw_data or "current_weather" not in raw_data:
        raise ValueError("Invalid raw data: 'current_weather' key missing.")

    current = raw_data["current_weather"]

    # Map raw API response to internal data schema
    processed_data = {
        "ingestion_timestamp": datetime.now().isoformat(),
        "temperature_celsius": current.get("temperature"),
        "windspeed_kmh": current.get("windspeed"),
        "wind_direction": current.get("winddirection"),
        "weather_code": current.get("weathercode"),
        "is_day": bool(current.get("is_day")),  # Normalize 1/0 to Boolean
        "location_lat": raw_data.get("latitude"),
        "location_lon": raw_data.get("longitude")
    }

    logger.info("Data transformation completed successfully.")
    return processed_data


def load_data_to_csv(data: Dict[str, Any], filename: str = "weather_data.csv"):
    """
    Persists processed data to a local CSV file.
    Simulates a landing zone in a Data Lake architecture.
    """
    file_exists = os.path.isfile(filename)

    try:
        with open(filename, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=data.keys())

            # Write header only on initial file creation
            if not file_exists:
                writer.writeheader()

            writer.writerow(data)
            logger.info(f"Data successfully persisted to {filename}")

    except IOError as e:
        logger.error(f"File I/O Error: {e}")


def load_data_to_db(data: Dict[str, Any]) -> None:
    """
    Converts dict to DataFrame and appends it to the target SQL database.
    """
    try:
        data_df = pd.DataFrame([data])

        data_df.to_sql(
            name='weather_data',
            con=engine,
            if_exists='append',
            index=False,
        )
        logger.info("Data successfully pushed to Database.")

    except Exception as e:
        logger.error(f"Database Load Error: {e}")


def main():
    logger.info("--- WEATHER MONITORING SERVICE STARTED ---")

    while True:  # Server loop for continuous ingestion
        logger.info("--- Scheduled Ingestion Job Triggered ---")

        # 1. EXTRACT
        raw_weather = extract_weather_data(LATITUDE, LONGITUDE)

        if raw_weather:
            try:
                # 2. TRANSFORM
                clean_weather = transform_data(raw_weather)

                # 3. LOAD
                # load_data_to_csv(clean_weather, filename="../data/weather_data.csv")
                load_data_to_db(clean_weather)

                # Optional debug print for monitoring
                print(f"Processed Record: {json.dumps(clean_weather, indent=2)}")

            except ValueError as ve:
                logger.error(f"Data Validation Error: {ve}")
        else:
            logger.warning("No data retrieved from source.")

        logger.info("--- Job Sleeping for 1 Hour (3600 seconds) ---")
        time.sleep(3600)


if __name__ == "__main__":
    main()