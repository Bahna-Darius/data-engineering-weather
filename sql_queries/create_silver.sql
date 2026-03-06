-- Stratul Silver: Date curățate, filtrate și îmbogățite cu noi coloane (Fahrenheit)
CREATE TABLE dbo.weather_silver (
    ingestion_timestamp datetime2,
    temperature_celsius float,
    windspeed_kmh float,
    wind_direction int,
    weather_code int,
    is_day varchar(10),
    location_lat float,
    location_lon float,
    temperature_fahrenheit float
);