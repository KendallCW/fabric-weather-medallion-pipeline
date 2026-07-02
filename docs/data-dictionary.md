# Data Dictionary

## Bronze — `bronze/weather/{location_id}/{year}/{month}/{day}/data.json`

Raw JSON response from Open-Meteo, unmodified, plus ingestion metadata added by ADF.

| Field | Type | Description |
|---|---|---|
| `ingestion_timestamp` | datetime | When ADF wrote the file |
| `source_endpoint` | string | Open-Meteo endpoint called |
| `batch_id` | string | Groups files from the same pipeline run |
| `pipeline_run_id` | string | ADF run ID, for traceability back to `etl_run_log` |
| `payload` | JSON | Raw API response, untouched |

## Silver — `silver.weather_observations`

| Column | Type | Description |
|---|---|---|
| `location_id` | string | Natural key, joins to `dim_location` |
| `observation_datetime` | timestamp (UTC) | Normalized to UTC regardless of source timezone |
| `temperature_c` | float | Normalized to Celsius |
| `humidity_pct` | float | 0–100 |
| `wind_speed_kmh` | float | Normalized to km/h |
| `precipitation_mm` | float | |
| `is_valid` | boolean | False if a range check failed (row retained, not dropped) |
| `_merged_at` | timestamp | Last time this row was touched by MERGE INTO |

**Grain:** one row per `(location_id, observation_datetime)`. Deduplicated on this key.

## Gold — star schema

### `fact_weather_daily`

| Column | Type | Description |
|---|---|---|
| `location_id` | string (FK) | |
| `date_id` | int (FK) | |
| `avg_temperature_c` | float | |
| `comfort_index` | float | Derived from temp + humidity + wind |
| `anomaly_vs_historical_avg` | float | Deviation from same day/month historical baseline |
| `streak_days_above_threshold` | int | Consecutive-day window calculation |

### `dim_location`

| Column | Type | Description |
|---|---|---|
| `location_id` | string (PK) | |
| `city_name` | string | |
| `country` | string | |
| `latitude` / `longitude` | float | |

### `dim_date`

Standard date dimension (date_id, year, month, day, day_of_week, is_weekend, etc.)

## Control table — `etl_run_log`

| Column | Type | Description |
|---|---|---|
| `pipeline_run_id` | string | |
| `layer` | string | bronze / silver / gold |
| `status` | string | success / failed |
| `error_message` | string (nullable) | |
| `rows_affected` | int | |
| `run_timestamp` | datetime | |
