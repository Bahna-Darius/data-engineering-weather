#!/bin/bash
# =============================================================================
# Weather ETL - Hourly Data Extraction Job
# Description: Extracts weather data from Open-Meteo API and loads it into
#              the local PostgreSQL database.
# Scheduled:   Every hour via cron
# Author:      Bahna Darius
# =============================================================================

set -euo pipefail

# --- PATHS ---
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_FILE="$PROJECT_DIR/logs/data_extract/extract.log"
PYTHON_BIN="$(which python3)"

# --- ENVIRONMENT ---
if [ ! -f "$PROJECT_DIR/.env" ]; then
    echo "[ERROR] .env file not found at $PROJECT_DIR/.env" >&2
    exit 1
fi
source "$PROJECT_DIR/.env"

# --- RUN ---
echo "-----------------------------------------------------------" >> "$LOG_FILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting ETL job..." >> "$LOG_FILE"

"$PYTHON_BIN" "$PROJECT_DIR/src/weather_ETL.py" >> "$LOG_FILE" 2>&1

echo "[$(date '+%Y-%m-%d %H:%M:%S')] ETL job finished." >> "$LOG_FILE"
