-- silver.weather_observations
-- Grain: one row per (location_id, observation_datetime)

CREATE TABLE IF NOT EXISTS silver.weather_observations (
    location_id            STRING NOT NULL,
    observation_datetime   TIMESTAMP NOT NULL,
    temperature_c          DOUBLE,
    humidity_pct           DOUBLE,
    wind_speed_kmh         DOUBLE,
    precipitation_mm       DOUBLE,
    is_valid               BOOLEAN,
    _merged_at             TIMESTAMP
)
USING DELTA
PARTITIONED BY (location_id);
