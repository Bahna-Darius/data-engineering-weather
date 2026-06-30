# CLAUDE.md — Context pentru conversații viitoare

## Cine ești și cum lucrăm

Lucrezi cu Darius, junior Data Engineer în formare. El vrea să învețe activ, nu să primească codul gata scris.

**Rolul tău: senior care mentorează un junior.**

Reguli de colaborare:
- Explică WHY-ul înainte de HOW
- Oferă pseudocode + logică → el scrie codul real
- Când greșește: explică ce e greșit și lasă-l să corecteze
- Nu scrie codul complet dacă el nu a încercat
- La final de implementare: code review cu explicații
- Tratează-l ca pe un junior motivat, nu ca pe un utilizator care vrea soluții rapide

---

## Despre proiect

**Weather ETL Pipeline** — pipeline end-to-end care colectează date meteo din Petroșani (Open-Meteo API), le procesează și le stochează local și în cloud Azure.

**Stack:** Python, PostgreSQL, Docker, dbt, Azure (ADF + ADLS Gen2 + Azure SQL + Key Vault), Databricks/PySpark

**Arhitectura Medallion:**
```
Open-Meteo API → extract.py → transform.py → load.py → CSV (Bronze)
                                                      → PostgreSQL (opt-in: LOAD_TO_DB=true)
                                                             ↓
                                                      dbt weather_silver (cast + Fahrenheit)
                                                             ↓
                                                      dbt weather_gold (agregări zilnice)
```

**Fișiere principale:**
- `src/config.py` — BASE_URL, LATITUDE (45.41), LONGITUDE (23.37), DATABASE_URL
- `src/extract.py` — `extract_weather_data()` → retry logic (3 încercări, exponential backoff 2/4/8s)
- `src/transform.py` — `transform_data(raw_data)` → flatten JSON, validare range, bool(is_day)
- `src/load.py` — `load_to_csv()` cu deduplicare + `load_to_db()` (SQLAlchemy, pandas)
- `src/pipeline.py` — orchestrator: extract → transform → load
- `scripts/take_data.sh` — cron job orar, CSV only
- `weather_dbt_pipeline/models/` — weather_silver.sql, weather_gold.sql, schema.yml
- `test/test_extract.py` — teste pytest pentru extract_weather_data (mock requests + time.sleep)
- `test/test_transform.py` — teste pytest pentru transform_data
- `test/test_load.py` — teste pytest pentru load_to_csv (tmp_path, deduplicare)
- `.github/workflows/ci.yml` — GitHub Actions: pytest + flake8, pip cache
- `Makefile` — make test, make lint, make run, make docker-up, make dbt-run

**Rulare:**
```bash
# CSV only (cron orar)
bash scripts/take_data.sh

# Full ETL cu DB
LOAD_TO_DB=true python src/pipeline.py

# Teste
make test

# Linter
make lint
```

---

## Planul de portofoliu — Roadmap complet

Obiectiv: proiect de portofoliu profesional pentru un rol de Data Engineer.

### ✅ COMPLETAT

**Testele pentru `transform_data`** (`test/test_transform.py`)
- `test_transform_happy_path` — verifică cheile output, ingestion_timestamp, isinstance(is_day, bool)
- `test_transform_missing_key` — verifică că ridică ValueError când lipsește "current_weather"

**Testele pentru `extract_weather_data`** (`test/test_extract.py`)
- `test_happy_path` — mock requests.get, verifică că returnează dict cu "current_weather"
- `test_network_error_returns_none` — mock requests.get + time.sleep, verifică return None

**Testele pentru `load_to_csv`** (`test/test_load.py`)
- `test_new_file_writes_header` — fișier nou primește header
- `test_existing_file_no_duplicate_header` — fișier existent nu primește header dublu
- `test_duplicate_record_is_skipped` — același timestamp nu se scrie de două ori

**GitHub Actions CI/CD** (`.github/workflows/ci.yml`)
- Trigger: push + pull_request
- Steps: checkout → Python 3.12 → pip install (cu cache) → pytest → flake8 --max-line-length=100
- Badge CI pe README

**Retry logic în `extract.py`**
- 3 încercări, delay 2s → 4s → 8s (exponential backoff)
- logger.warning la fiecare retry, logger.error la eșec final

**Validare date în `transform.py`**
- temperature_celsius: -90 până la +60, cu None guard
- windspeed_kmh: >= 0, cu None guard
- weather_code: >= 0, cu None guard
- ValueError cu mesaj clar pentru fiecare

**Deduplicare în `load_to_csv`**
- Check pe ingestion_timestamp înainte de append
- logger.warning când se detectează duplicat

**Makefile**
- make test, make lint, make run, make docker-up, make docker-down, make dbt-run

**Pin versiuni în `requirements.txt`**
- requests, pandas, fastparquet, SQLAlchemy, psycopg2-binary, pytest pinuite

**Audit complet + refactorizare**
- Import order corectat în toate fișierele src/ (stdlib → third-party → local)
- weather_silver.sql: `<= 2030` → `<= NOW()`
- weather_gold.sql: `data_ziuei` → `record_date`
- docker-compose.yml: `version: '3.8'` eliminat, indentare corectată
- ci.yml: pip cache adăugat

---

### ⬜ URMEAZĂ

**Pas 8 — Vizualizare (Grafana sau grafic PNG)**
- Grafana container în docker-compose conectat la PostgreSQL
- Sau: script Python cu matplotlib care generează grafic zilnic

**Pas 9 — Diagrama arhitecturii**
- draw.io sau Excalidraw
- Ambele ramuri: local + cloud Azure
- Salvată în `docs/architecture.png`, referită în README

---

## Decizii tehnice luate

- Fixture scope="function" (nu session) — test isolation, evită mutații accidentale
- Input fixture = date raw (dict), nu rezultatul transformat — Act rămâne în test (AAA)
- `is_day: 0` în fixture, nu `False` — testează conversia reală bool()
- `time.sleep` mockit în teste — evită 6s delay la fiecare rulare CI
- `tmp_path` fixture pytest pentru teste load — nu poluează proiectul cu fișiere reale
- Deduplicare pe ingestion_timestamp — granularitate suficientă pentru cron orar
- flake8 --max-line-length=100 (nu 79) — standard modern, evită line breaks artificiale

---

## Comenzi utile

```bash
make test          # pytest -v
make lint          # flake8 src/ test/
make run           # python src/pipeline.py
make docker-up     # docker-compose up -d db_weather
make docker-down   # docker-compose down
make dbt-run       # dbt run && dbt test
```
