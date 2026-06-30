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
- `src/extract.py` — `extract_weather_data()` → requests.get Open-Meteo, timeout=10
- `src/transform.py` — `transform_data(raw_data)` → flatten JSON, adaugă ingestion_timestamp, bool(is_day)
- `src/load.py` — `load_to_csv()` + `load_to_db()` (SQLAlchemy, pandas)
- `src/pipeline.py` — orchestrator: extract → transform → load
- `scripts/take_data.sh` — cron job orar, CSV only
- `weather_dbt_pipeline/models/` — weather_silver.sql, weather_gold.sql, schema.yml
- `test/test_transform.py` — teste pytest pentru transform_data

**Rulare:**
```bash
# CSV only (cron orar)
bash scripts/take_data.sh

# Full ETL cu DB
LOAD_TO_DB=true python src/pipeline.py

# Teste
pytest   # pytest.ini configurează testpaths=test
```

---

## Planul de portofoliu — Roadmap complet

Obiectiv: proiect de portofoliu profesional pentru un rol de Data Engineer.

### ✅ COMPLETAT

**Testele pentru `transform_data`** (`test/test_transform.py`)
- `test_transform_happy_path` — verifică cheile output, ingestion_timestamp, isinstance(is_day, bool)
- `test_transform_missing_key` — verifică că ridică ValueError când lipsește "current_weather"
- Fixture `raw_api_data_response` cu `is_day: 0` (integer) pentru a testa conversia la bool
- `pytest.ini` creat cu `testpaths=test` (rezolvă PermissionError pe pg_data/)

**Decizii tehnice luate:**
- Fixture scope="function" (nu session) — test isolation, evită mutații accidentale
- Input fixture = date raw (dict), nu rezultatul transformat — Act rămâne în test (AAA)
- Error case = funcție separată (Single Responsibility pentru teste)
- `is_day: 0` în fixture, nu `False` — testează conversia reală bool()

---

### 🔄 ÎN PROGRES — Testele pentru `extract_weather_data`

**Unde am rămas:** Darius tocmai a primit explicația despre mocking și urmează să scrie testele.

**Ce trebuie scris în `test/test_extract.py`:**

Conceptul explicat: `@patch("extract.requests.get")` înlocuiește requests.get cu un MagicMock care returnează ce controlezi tu — fără internet real.

**Scenariul 1 — succes:**
```
Arrange: mock_get.return_value = MagicMock cu .json() și .raise_for_status()
Act: result = extract_weather_data()
Assert: result is not None, "current_weather" in result
```

**Scenariul 2 — network error:**
```
Arrange: mock_get.side_effect = requests.exceptions.RequestException
Act: result = extract_weather_data()
Assert: result is None  (funcția prinde excepția și returnează None)
```

---

### ⬜ URMEAZĂ — în ordinea asta

**Pas 2 — `test/test_load.py`**
- `test_load_to_csv_creates_header` — fișier nou primește header
- `test_load_to_csv_no_duplicate_header` — fișier existent nu primește header dublu
- Folosește `tmp_path` fixture din pytest (pytest creează dir temporar, nu poluezi proiectul)

**Pas 3 — GitHub Actions CI/CD** (`.github/workflows/ci.yml`)
- Trigger: push + pull_request pe orice branch
- Steps: checkout → setup Python → pip install → pytest → flake8
- Badge `passing` pe README

**Pas 4 — Retry logic în `extract.py`**
- `tenacity` library sau implementare manuală cu exponential backoff
- 3 încercări, delay 2s → 4s → 8s
- Log la fiecare retry

**Pas 5 — Validare date (Data Quality) în `transform.py`**
- temperature: -90°C până la +60°C
- windspeed: >= 0
- weather_code: în setul WMO valid
- Ridică ValueError cu mesaj clar dacă e în afara range-ului

**Pas 6 — Deduplicare în `load_to_csv`**
- Check pe ingestion_timestamp înainte de append
- Previne duplicate dacă cron rulează de două ori

**Pas 7 — Makefile**
```makefile
make test, make lint, make run, make docker-up, make dbt-run
```

**Pas 8 — Vizualizare (Grafana sau grafic PNG)**
- Grafana container în docker-compose conectat la PostgreSQL
- Sau: script Python cu matplotlib care generează grafic zilnic

**Pas 9 — Diagrama arhitecturii**
- draw.io sau Excalidraw
- Ambele ramuri: local + cloud Azure
- Salvată în `docs/architecture.png`, referită în README

**Pas 10 — Pin versiuni în requirements.txt**
```
requests==2.31.0
pandas==2.2.0
...
```

---

## Probleme cunoscute de corectat

1. `weather_silver.sql` linia 21: `WHERE EXTRACT(YEAR FROM ...) <= 2030` — hardcodat, se va strica în 2031
2. `load_to_db` creează engine nou la fiecare apel — anti-pattern minor
3. `Dockerfile.dbt` rulează `tail -f /dev/null` — dbt nu e integrat în flow automat

---

## Comenzi utile

```bash
# Teste
pytest -v

# Rulare pipeline manual
cd src && python pipeline.py

# Docker
docker-compose up -d db_weather
docker-compose down

# dbt
cd weather_dbt_pipeline && dbt run && dbt test
```
