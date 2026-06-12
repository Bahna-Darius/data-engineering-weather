import os

# --- API ---
BASE_URL = "https://api.open-meteo.com/v1/forecast"
LATITUDE = 45.41
LONGITUDE = 23.37

# --- DATABASE ---
# In Docker, DB_URL is injected directly by docker-compose.
# Locally, it is constructed from the individual .env variables.
DATABASE_URL = os.getenv("DB_URL") or (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST', 'localhost')}:5432/{os.getenv('POSTGRES_DB')}"
)
