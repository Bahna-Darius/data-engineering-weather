FROM python:3.9-slim as builder

WORKDIR /app


RUN apt-get update && apt-get install -y --no-install-recommends gcc

COPY requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt


# --- STAGE 2: Runtime  ---
FROM python:3.9-slim as runtime

WORKDIR /app

COPY --from=builder /root/.local /root/.local
COPY weather_ETL.py .

ENV PATH=/root/.local/bin:$PATH


CMD ["python", "-u", "weather_ETL.py"]