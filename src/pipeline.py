import json
import logging
import os
from pathlib import Path

from extract import extract_weather_data
from load import load_to_csv, load_to_db
from transform import transform_data

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


CSV_PATH = Path(__file__).resolve().parent.parent / "data" / "weather_data.csv"


def run_pipeline():
    logger.info("--- ETL Pipeline Started ---")

    # 1. EXTRACT
    raw_data = extract_weather_data()

    if not raw_data:
        logger.warning("No data retrieved from source. Aborting.")
        return

    try:
        # 2. TRANSFORM
        clean_data = transform_data(raw_data)

        # 3. LOAD
        # CSV is always written (Bronze landing zone for local analysis).
        # DB load is opt-in: set LOAD_TO_DB=true in the calling script.
        #   - scripts/take_data.sh  → does NOT set it → CSV only (hourly cron)
        #   - scripts/etl_pipeline.sh → sets LOAD_TO_DB=true → CSV + PostgreSQL
        load_to_csv(clean_data, str(CSV_PATH))

        if os.getenv("LOAD_TO_DB", "false").lower() == "true":
            load_to_db(clean_data)

        logger.info(f"Processed record: {json.dumps(clean_data, indent=2)}")

    except ValueError as ve:
        logger.error(f"Data Validation Error: {ve}")

    logger.info("--- ETL Pipeline Completed ---")


if __name__ == "__main__":
    run_pipeline()
