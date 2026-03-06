-- Stratul Gold: Date agregate, pregătite pentru Business Intelligence (Power BI)
CREATE TABLE dbo.weather_gold (
    data_ziuei date,
    avg_temperature_celsius float,
    max_windspeed_kmh float,
    avg_temperature_fahrenheit float
);