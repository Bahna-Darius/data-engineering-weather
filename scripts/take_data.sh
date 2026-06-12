#!/bin/bash
# =============================================================================
# Weather ETL - Hourly Data Extraction Job
# Description: Extracts weather data from Open-Meteo API and saves it to CSV.
# Scheduled:   Every hour via cron
# Author:      Bahna Darius
# =============================================================================

set -euo pipefail

# --- PATHS ---
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG_FILE="$PROJECT_DIR/logs/data_extract/extract_$(date '+%Y-%m-%d').log"
PYTHON_BIN="$PROJECT_DIR/.venv/bin/python3"

# --- ENVIRONMENT ---
if [ ! -f "$PROJECT_DIR/.env" ]; then
    echo "[ERROR] .env file not found at $PROJECT_DIR/.env" >&2
    exit 1
fi
source "$PROJECT_DIR/.env"

# --- RUN ---
echo "-----------------------------------------------------------" >> "$LOG_FILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting ETL job..." >> "$LOG_FILE"

PYTHONPATH="$PROJECT_DIR/src" "$PYTHON_BIN" "$PROJECT_DIR/src/pipeline.py" >> "$LOG_FILE" 2>&1

echo "[$(date '+%Y-%m-%d %H:%M:%S')] ETL job finished." >> "$LOG_FILE"
