# End-to-End Weather Data Engineering Pipeline

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white)
![Azure](https://img.shields.io/badge/azure-%230072C6.svg?style=for-the-badge&logo=microsoftazure&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=Databricks&logoColor=white)

A Data Engineering project demonstrating a production-grade ETL/ELT pipeline with a **hybrid architecture** — a fully automated local environment for data collection, and a scalable cloud architecture on **Microsoft Azure** following the **Medallion Architecture** (Bronze → Silver → Gold).

---

## Architecture Overview

### Local Pipeline (Automated)
- **Extract:** `extract.py` fetches real-time weather data from the Open-Meteo API (Petroșani, Romania).
- **Transform:** `transform.py` flattens the nested JSON response and normalizes data types.
- **Load:** `load.py` persists data to a local CSV file (Bronze layer). DB load is opt-in via `LOAD_TO_DB`.
- **Scheduling:** A Linux cron job triggers the pipeline every hour, writing daily rotating log files.
- **Infrastructure:** Fully containerized via `docker-compose` (PostgreSQL + ETL service + dbt).

### Cloud Production (Microsoft Azure)
- **Ingestion:** CSV files land in Azure Data Lake Storage Gen2 (Bronze layer).
- **Orchestration:** Azure Data Factory automates the pipeline using GetMetadata + ForEach for dynamic file processing.
- **Transformation:** ADF Mapping Data Flows and Databricks (PySpark) produce the Silver and Gold tables.
- **Security:** All credentials are managed by Azure Key Vault, accessed via Managed Identity — no secrets in code.

---

## Data Flow

```
Open-Meteo API
      │
      ▼
 extract.py  ──→  transform.py  ──→  load.py  ──→  data/weather_data.csv  (Bronze)
                                         │
                                         └──→  PostgreSQL (opt-in: LOAD_TO_DB=true)
                                                     │
                                             dbt Silver model
                                                     │
                                             dbt Gold model  ──→  Power BI
```

---

## Technical Stack

| Layer | Technology |
|---|---|
| Language | Python 3.x, SQL, PySpark |
| ETL Modules | extract, transform, load, pipeline (Single Responsibility) |
| Transformation | dbt Core, Pandas |
| Containerization | Docker, Docker Compose |
| Scheduling | Linux Cron (hourly, daily rotating logs) |
| Cloud | Azure Data Factory, Azure SQL, ADLS Gen2, Key Vault |
| Big Data | Databricks |

---

## Repository Structure

```
weather_project/
│
├── src/                            # Python ETL source code
│   ├── config.py                   # API coordinates and DB configuration
│   ├── extract.py                  # Fetches data from Open-Meteo API
│   ├── transform.py                # Cleans and normalizes raw API response
│   ├── load.py                     # Writes to CSV and/or PostgreSQL
│   ├── pipeline.py                 # Orchestrator: Extract → Transform → Load
│   └── notebooks/
│       └── databricks_pyspark_meteo.ipynb
│
├── scripts/
│   ├── take_data.sh                # Cron job: hourly CSV extraction (no DB)
│   └── etl_pipeline.sh             # Full ETL: starts Docker, loads DB, stops Docker
│
├── weather_dbt_pipeline/           # dbt project (Silver & Gold models)
│   └── models/
│       ├── weather_silver.sql      # Cleans Bronze, adds Fahrenheit column
│       ├── weather_gold.sql        # Daily aggregations for BI
│       └── schema.yml              # Automated data quality tests
│
├── sql_queries/                    # DDL scripts for Azure SQL tables
│   ├── create_bronze.sql
│   ├── create_silver.sql
│   └── create_gold.sql
│
├── ADF/                            # Azure Data Factory JSON exports
│   ├── pipeline/                   # PL_CopyWeather, PL_TransformaSilver, PL_IncarcaGold
│   ├── dataflow/                   # DF_CleanWeather, DF_AggregateGold
│   ├── dataset/
│   └── linkedService/
│
├── docs/
│   └── images/                     # Screenshots for documentation
│
├── logs/                           # Auto-generated daily log files (gitignored)
│   └── data_extract/
│       └── extract_YYYY-MM-DD.log
│
├── data/                           # Local Bronze landing zone (gitignored)
│   └── weather_data.csv
│
├── docker-compose.yml
├── Dockerfile
├── Dockerfile.dbt
├── requirements.txt
├── .env.example                    # Environment variable template (copy to .env)
└── .gitignore
```

---

## How to Run Locally

### Prerequisites
- Docker & Docker Compose
- Python 3.9+

### Setup

1. **Clone the repository:**
   ```bash
   git clone https://github.com/Bahna-Darius/data-engineering-weather.git
   cd data-engineering-weather
   ```

2. **Create a virtual environment and install dependencies:**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

3. **Configure environment variables:**
   ```bash
   cp .env.example .env
   # Edit .env with your credentials
   ```

4. **Start the database:**
   ```bash
   docker-compose up -d db_weather
   ```

### Run Modes

The pipeline supports two execution modes controlled by the `LOAD_TO_DB` environment variable:

| Mode | Script | Behavior |
|---|---|---|
| CSV only | `take_data.sh` | Extracts from API → saves to `data/weather_data.csv` |
| Full ETL | `etl_pipeline.sh` | Starts Docker → CSV + PostgreSQL → stops Docker |

**Run once manually (CSV only):**
```bash
bash scripts/take_data.sh
```

**Run dbt transformations:**
```bash
cd weather_dbt_pipeline
dbt run
dbt test
```

### Automated Hourly Collection (Cron)

```bash
chmod +x scripts/take_data.sh
crontab -e
# Add the following line:
0 * * * * /path/to/weather_project/scripts/take_data.sh
```

Logs are written to `logs/data_extract/extract_YYYY-MM-DD.log` — one file per day.

---

## Project Showcase

### 1. dbt Lineage Graph
![dbt Lineage](docs/images/dbt_lineage.png)

### 2. ADF Pipeline — Bronze Ingestion
![ADF Pipeline](docs/images/2026-03-06_13-50.png)

### 3. Azure Key Vault — Enterprise Security
![Azure Linked Services](docs/images/2026-03-06_13-50_1.png)

### 4. Databricks — PySpark Transformation
![Databricks Transformations](docs/images/databricks_pyspark.png)

### 5. Gold Layer in DBeaver
![Gold Table in DBeaver](docs/images/2026-03-06_13-52.png)
