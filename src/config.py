import os

# --- API ---
BASE_URL = "https://api.open-meteo.com/v1/forecast"
LATITUDE = 45.41
LONGITUDE = 23.37

# --- DATABASE ---
DATABASE_URL = os.getenv("DB_URL")
