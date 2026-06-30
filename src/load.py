import csv
import logging
import os
from typing import Any, Dict

import pandas as pd
from sqlalchemy import create_engine

from config import DATABASE_URL

logger = logging.getLogger(__name__)


def load_to_csv(data: Dict[str, Any], filename: str = "data/weather_data.csv") -> None:
    """Persists processed data to a local CSV file (Bronze landing zone)."""
    file_exists = os.path.isfile(filename)

    if file_exists:
        with open(filename, encoding='utf-8') as f:
            if data["ingestion_timestamp"] in f.read():
                logger.warning("Duplicate record detected, skipping.")
                return

    try:
        with open(filename, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=data.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(data)
        logger.info(f"Data persisted to {filename}")
    except IOError as e:
        logger.error(f"File I/O Error: {e}")


def load_to_db(data: Dict[str, Any]) -> None:
    """Converts dict to DataFrame and appends it to the PostgreSQL database."""
    try:
        engine = create_engine(DATABASE_URL)
        pd.DataFrame([data]).to_sql(
            name='weather_data',
            con=engine,
            if_exists='append',
            index=False,
        )
        logger.info("Data successfully pushed to database.")
    except Exception as e:
        logger.error(f"Database Load Error: {e}")
