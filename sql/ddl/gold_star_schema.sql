-- Gold layer: star schema

CREATE TABLE IF NOT EXISTS gold.dim_location (
    location_id    STRING NOT NULL,
    city_name      STRING,
    country        STRING,
    latitude       DOUBLE,
    longitude      DOUBLE
)
USING DELTA;

CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_id        INT NOT NULL,
    full_date      DATE,
    year           INT,
    month          INT,
    day            INT,
    day_of_week    STRING,
    is_weekend     BOOLEAN
)
USING DELTA;

CREATE TABLE IF NOT EXISTS gold.fact_weather_daily (
    location_id                    STRING NOT NULL,
    date_id                        INT NOT NULL,
    avg_temperature_c              DOUBLE,
    avg_humidity_pct               DOUBLE,
    avg_wind_speed_kmh             DOUBLE,
    comfort_index                  DOUBLE,
    anomaly_vs_historical_avg      DOUBLE,
    streak_days_above_threshold    INT,
    pipeline_run_id                STRING,
    _computed_at                   TIMESTAMP
)
USING DELTA
PARTITIONED BY (location_id);
