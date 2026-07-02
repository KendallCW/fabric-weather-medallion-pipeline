# 01_bronze_to_silver.py
# Fabric PySpark notebook (exported as .py; convert to .ipynb in Fabric if preferred)
#
# Responsibilities:
#   1. Read new bronze JSON partitions since last watermark (per location_id)
#   2. Parse + type + normalize units
#   3. Deduplicate on (location_id, observation_datetime)
#   4. Validate ranges, flag (not drop) invalid rows
#   5. MERGE INTO silver.weather_observations
#   6. Update watermark table
#   7. Write run outcome to etl_run_log

# To be implemented.
