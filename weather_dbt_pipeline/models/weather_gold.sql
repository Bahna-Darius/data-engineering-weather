{{ config(materialized='table') }}

WITH silver_data AS (
    SELECT * FROM {{ ref('weather_silver') }}
),

daily_aggregates AS (
    SELECT
        DATE(ingestion_timestamp) AS record_date,
        ROUND(AVG(temperature_celsius)::numeric, 2) AS avg_temperature_celsius,
        ROUND(MAX(windspeed_kmh)::numeric, 2) AS max_windspeed_kmh,
        ROUND(AVG(temperature_fahrenheit)::numeric, 2) AS avg_temperature_fahrenheit
    FROM silver_data
    GROUP BY DATE(ingestion_timestamp)
)

SELECT * FROM daily_aggregates