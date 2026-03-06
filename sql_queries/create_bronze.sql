-- Stratul Bronze: Date brute încărcate din CSV/Data Lake
CREATE TABLE dbo.weather_bronze (
    ingestion_timestamp varchar(100),
    temperature_celsius float,
    windspeed_kmh float,
    wind_direction int,
    weather_code int,
    is_day varchar(10),
    location_lat float,
    location_lon float
);