# 1. Alegem sistemul de bază (O versiune mică de Python instalată pe Linux)
FROM python:3.9-slim

# 2. Creăm un folder numit /app în interiorul containerului și intrăm în el
WORKDIR /app

# 3. Copiem fișierele noastre de pe Ubuntu IN interiorul containerului
COPY requirements.txt .
COPY weather_ETL.py .

# 4. Instalăm librăriile necesare (pandas, requests etc.)
RUN pip install -r requirements.txt

# 5. Comanda care se execută automat când pornim containerul
CMD ["python", "weather_ETL.py"]