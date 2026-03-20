{{ config(materialized='table') }}

WITH bronze_data AS (
    SELECT * FROM public.weather_data
),

silver_transformation AS (
    SELECT
        CAST(ingestion_timestamp AS TIMESTAMP) AS ingestion_timestamp,
        CAST(temperature_celsius AS FLOAT) AS temperature_celsius,
        CAST(windspeed_kmh AS FLOAT) AS windspeed_kmh,
        CAST(wind_direction AS INTEGER) AS wind_direction,
        CAST(weather_code AS INTEGER) AS weather_code,
        CAST(is_day AS BOOLEAN) AS is_day,
        CAST(location_lat AS FLOAT) AS location_lat,
        CAST(location_lon AS FLOAT) AS location_lon,

        (CAST(temperature_celsius AS FLOAT) * 1.8) + 32 AS temperature_fahrenheit

    FROM bronze_data
    WHERE EXTRACT(YEAR FROM CAST(ingestion_timestamp AS TIMESTAMP)) <= 2030
)

SELECT * FROM silver_transformation