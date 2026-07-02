# 02_silver_to_gold.py
# Fabric PySpark notebook (exported as .py; convert to .ipynb in Fabric if preferred)
#
# Responsibilities:
#   1. Read silver.weather_observations (incremental, watermark-based)
#   2. Aggregate to daily grain per location
#   3. Compute comfort_index, anomaly_vs_historical_avg, streak_days_above_threshold
#   4. Upsert into gold.fact_weather_daily
#   5. Write run outcome to etl_run_log

# To be implemented.
