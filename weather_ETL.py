from typing import Dict, Any, Optional, List
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import requests
import logging
import json
import csv
import os


# --- CONFIGURARE LOGGING ---
# În loc de print(), folosim logging. Asta ne ajută să vedem
# timestamp-ul și severitatea (INFO, ERROR) fiecărui mesaj.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)

# --- CONFIGURARE API ---
# Folosim Open-Meteo pentru că nu necesită API Key (gratuit și simplu)
# Coordonate pentru Petroșani
LATITUDE = 45.41
LONGITUDE = 23.37
BASE_URL = "https://api.open-meteo.com/v1/forecast"

#DB conf:
DATABASE_URL = os.getenv('DB_URL')
engine = create_engine(DATABASE_URL)


def extract_weather_data(lat: float, lon: float) -> Optional[Dict[str, Any]]:
    """
    Extrage datele meteo curente dintr-un API extern.
    Folosește Type Hinting (-> Optional[Dict]) pentru claritate.
    """
    params = {
        "latitude": lat,
        "longitude": lon,
        "current_weather": "true"
    }

    try:
        logger.info(f"Încep extragerea datelor pentru Lat: {lat}, Lon: {lon}...")
        response = requests.get(BASE_URL, params=params, timeout=10)

        # Verificăm status code-ul. Dacă e 4xx sau 5xx, ridică o eroare.
        response.raise_for_status()

        data = response.json()
        logger.info("Date extrase cu succes!")
        return data

    except requests.exceptions.RequestException as e:
        # Prindem orice eroare legată de rețea sau API
        logger.error(f"Eroare la extragerea datelor: {e}")
        return None


def transform_data(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transformă JSON-ul complex într-un format simplu (flat),
    pregătit pentru salvarea în tabel/CSV.
    """
    if not raw_data or "current_weather" not in raw_data:
        raise ValueError("Datele raw sunt invalide sau lipsesc.")

    current = raw_data["current_weather"]

    # Creăm un dicționar curat
    processed_data = {
        "ingestion_timestamp": datetime.now().isoformat(),
        "temperature_celsius": current.get("temperature"),
        "windspeed_kmh": current.get("windspeed"),
        "wind_direction": current.get("winddirection"),
        "weather_code": current.get("weathercode"),
        "is_day": bool(current.get("is_day")),  # Convertim 1/0 în True/False
        "location_lat": raw_data.get("latitude"),
        "location_lon": raw_data.get("longitude")
    }

    logger.info("Transformarea datelor completă.")
    return processed_data


def load_data_to_csv(data: Dict[str, Any], filename: str = "weather_data.csv"):
    """
    Salvează datele procesate într-un fișier CSV local.
    În practică, aici am salva în Azure Blob Storage sau SQL Database.
    """
    file_exists = os.path.isfile(filename)

    try:
        with open(filename, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=data.keys())

            # Scriem header-ul doar dacă fișierul nu există
            if not file_exists:
                writer.writeheader()

            writer.writerow(data)
            logger.info(f"Date salvate cu succes în {filename}")

    except IOError as e:
        logger.error(f"Eroare la scrierea fișierului: {e}")


def load_data_to_db(data: Dict[str, Any]) -> None:
    """
    Primeste datele in format DICT -> DATAFRAME -> LOAD DB
    :param data: Dict Raw
    :return: None
    """
    try:
        data_df = pd.DataFrame(
            [data]
        )

        data_df.to_sql(
            name='weather_data',
            con=engine,
            if_exists='append',
            index=False,
        )
        logger.info("Date salvate cu succes in Baza de Date!")

    except Exception as e:
        logger.error(f"Eroare la salvarea in DB: {e}")


def main():
    """
    Funcția principală care orchestrează fluxul ETL (Extract, Transform, Load).
    """
    logger.info("--- START PIPELINE ---")

    # 1. EXTRACT
    raw_weather = extract_weather_data(LATITUDE, LONGITUDE)

    if raw_weather:
        try:
            # 2. TRANSFORM
            clean_weather = transform_data(raw_weather)

            # 3. LOAD
            load_data_to_csv(clean_weather)     # CSV SAVE
            load_data_to_db(clean_weather)      # DB SAVE

            # Bonus: Afișăm rezultatul final în consolă pentru verificare
            print("\nDate procesate:")
            print(json.dumps(clean_weather, indent=2))

        except ValueError as ve:
            logger.error(f"Eroare de validare a datelor: {ve}")
    else:
        logger.warning("Pipeline oprit din cauza lipsei datelor.")

    logger.info("--- END PIPELINE ---")


if __name__ == "__main__":
    main()