# local_silver_to_gold.py
# Local adaptation of fabric/notebooks/02_silver_to_gold.py for testing the
# transformation logic on a Windows machine with PySpark + Delta Lake,
# without a Fabric Lakehouse. Same logic, only the Spark session setup and
# table registration change (see docs/design-decisions.md: "Execution
# environment: local PySpark instead of Fabric Lakehouse").
#
# All CREATE TABLE statements below (including the read-only reference to
# silver.weather_observations) use explicit LOCATION. Local runs use Spark's
# in-memory catalog, which doesn't persist across process runs, while the
# Delta files on disk do — without LOCATION, every table's identity would
# depend on catalog state that resets every run instead of the data that
# doesn't (see docs/design-decisions.md: "Fix Delta tables to use explicit
# LOCATION").

from pyspark.sql import SparkSession, functions as F, Window
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
import uuid
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Local Spark session setup (this block replaces Fabric's built-in `spark`)
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.as_posix()
WAREHOUSE_DIR = f"{PROJECT_ROOT}/warehouse"

builder = (
    SparkSession.builder.appName("silver_to_gold_local")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

PIPELINE_RUN_ID = str(uuid.uuid4())
RUN_TIMESTAMP = datetime.now(timezone.utc)

# ---------------------------------------------------------------------------
# 0. Ensure control + target tables exist
# ---------------------------------------------------------------------------

spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# Read-only reference: registers the existing silver table (written by
# local_bronze_to_silver.py) in this session's catalog so spark.table(...)
# below resolves it.
spark.sql(f"""
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
    LOCATION '{WAREHOUSE_DIR}/silver.db/weather_observations'
    PARTITIONED BY (location_id)
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS etl_run_log (
        pipeline_run_id    STRING,
        layer              STRING,
        status             STRING,
        error_message      STRING,
        rows_affected      INT,
        run_timestamp      TIMESTAMP
    )
    USING DELTA
    LOCATION '{WAREHOUSE_DIR}/etl_run_log'
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS gold.dim_location (
        location_id    STRING NOT NULL,
        city_name      STRING,
        country        STRING,
        latitude       DOUBLE,
        longitude      DOUBLE
    )
    USING DELTA
    LOCATION '{WAREHOUSE_DIR}/gold.db/dim_location'
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS gold.dim_date (
        date_id        INT NOT NULL,
        full_date      DATE,
        year           INT,
        month          INT,
        day            INT,
        day_of_week    STRING,
        is_weekend     BOOLEAN
    )
    USING DELTA
    LOCATION '{WAREHOUSE_DIR}/gold.db/dim_date'
""")

spark.sql(f"""
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
    LOCATION '{WAREHOUSE_DIR}/gold.db/fact_weather_daily'
    PARTITIONED BY (location_id)
""")


def _log_run(status: str, rows_affected: int, error_message: str = None) -> None:
    log_row = spark.createDataFrame(
        [(PIPELINE_RUN_ID, "gold", status, error_message, rows_affected, RUN_TIMESTAMP)],
        schema="pipeline_run_id STRING, layer STRING, status STRING, "
        "error_message STRING, rows_affected INT, run_timestamp TIMESTAMP",
    )
    log_row.write.format("delta").mode("append").saveAsTable("etl_run_log")


# ---------------------------------------------------------------------------
# 1. dim_location: small, manually curated reference data (5 known cities).
#    Upserted rather than overwritten so this survives being re-run without
#    clobbering anything (there's nothing derived here yet, but the pattern
#    matters more than the current triviality of the source data).
# ---------------------------------------------------------------------------

LOCATIONS = [
    ("cincinnati", "Cincinnati", "United States", 39.1031, -84.5120),
    ("chicago", "Chicago", "United States", 41.8781, -87.6298),
    ("dubai", "Dubai", "United Arab Emirates", 25.2048, 55.2708),
    ("reykjavik", "Reykjavik", "Iceland", 64.1466, -21.9426),
    ("singapore", "Singapore", "Singapore", 1.3521, 103.8198),
]
location_updates_df = spark.createDataFrame(
    LOCATIONS,
    schema="location_id STRING, city_name STRING, country STRING, latitude DOUBLE, longitude DOUBLE",
)
dim_location_table = DeltaTable.forName(spark, "gold.dim_location")
(
    dim_location_table.alias("t")
    .merge(location_updates_df.alias("s"), "t.location_id = s.location_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

# ---------------------------------------------------------------------------
# 2. dim_date: fixed calendar window (standard date-dimension pattern — built
#    once, wide enough to cover the historical backfill plus years of future
#    daily loads). Insert-only: calendar facts for a given date never change.
# ---------------------------------------------------------------------------

date_range_df = spark.sql(
    "SELECT explode(sequence(to_date('2015-01-01'), to_date('2035-12-31'), interval 1 day)) AS full_date"
)
dim_date_updates_df = date_range_df.select(
    (F.year("full_date") * 10000 + F.month("full_date") * 100 + F.dayofmonth("full_date")).alias("date_id"),
    "full_date",
    F.year("full_date").alias("year"),
    F.month("full_date").alias("month"),
    F.dayofmonth("full_date").alias("day"),
    F.date_format("full_date", "EEEE").alias("day_of_week"),
    F.dayofweek("full_date").isin([1, 7]).alias("is_weekend"),
)
dim_date_table = DeltaTable.forName(spark, "gold.dim_date")
(
    dim_date_table.alias("t")
    .merge(dim_date_updates_df.alias("s"), "t.date_id = s.date_id")
    .whenNotMatchedInsertAll()
    .execute()
)

# ---------------------------------------------------------------------------
# 3. fact_weather_daily: full recompute from all of silver, not incremental.
#    anomaly_vs_historical_avg and streak_days_above_threshold are cross-row
#    aggregates over each location's entire history — a new day changes the
#    historical baseline for every prior year's same (month, day), and
#    streak length is inherently sequential. Per-row incremental processing
#    can't produce correct results for either metric, so every run
#    recomputes the whole fact table and MERGEs it back in. At this data
#    volume (~20K daily rows across 5 cities x 11 years) this is cheap.
# ---------------------------------------------------------------------------

daily_df = (
    spark.table("silver.weather_observations")
    .filter("is_valid = true")
    .withColumn("obs_date", F.to_date("observation_datetime"))
    .groupBy("location_id", "obs_date")
    .agg(
        F.avg("temperature_c").alias("avg_temperature_c"),
        F.avg("humidity_pct").alias("avg_humidity_pct"),
        F.avg("wind_speed_kmh").alias("avg_wind_speed_kmh"),
    )
)

# 3a. comfort_index: a documented project heuristic, not an official
# meteorological index (no single standard formula combines temp + humidity
# + wind into one number). Heat-index-like adjustment above 20C (humidity
# makes heat feel worse), wind-chill-like adjustment below 10C (wind makes
# cold feel worse), passthrough in the 10-20C comfort band. See
# docs/data-dictionary.md for the exact thresholds/coefficients.
comfort_df = daily_df.withColumn(
    "comfort_index",
    F.when(
        F.col("avg_temperature_c") > 20,
        F.col("avg_temperature_c") + (F.col("avg_humidity_pct") - 50) / 100 * 2,
    )
    .when(
        F.col("avg_temperature_c") < 10,
        F.col("avg_temperature_c") - F.col("avg_wind_speed_kmh") * 0.2,
    )
    .otherwise(F.col("avg_temperature_c")),
)

# 3b. anomaly_vs_historical_avg: mean of avg_temperature_c for the same
# (location, month, day) across years STRICTLY BEFORE the year being
# evaluated. Using an expanding window ordered by year (rowsBetween
# unboundedPreceding, -1) instead of the full partition average is
# intentional: comparing a 2016 reading against a baseline that includes
# 2017-2035 data is lookahead bias (the baseline would "know" about years
# that hadn't happened yet from that reading's point of view). The earliest
# year on record for each (location, month, day) has no prior years, so its
# anomaly is null rather than a fabricated zero-history baseline.
md_prior_window = (
    Window.partitionBy("location_id", "month", "day")
    .orderBy("year")
    .rowsBetween(Window.unboundedPreceding, -1)
)
anomaly_df = (
    comfort_df.withColumn("year", F.year("obs_date"))
    .withColumn("month", F.month("obs_date"))
    .withColumn("day", F.dayofmonth("obs_date"))
    .withColumn("md_prior_sum", F.sum("avg_temperature_c").over(md_prior_window))
    .withColumn("md_prior_count", F.count("avg_temperature_c").over(md_prior_window))
    .withColumn(
        "historical_avg",
        F.when(
            F.col("md_prior_count") > 0,
            F.col("md_prior_sum") / F.col("md_prior_count"),
        ),
    )
    .withColumn("anomaly_vs_historical_avg", F.col("avg_temperature_c") - F.col("historical_avg"))
)

# 3c. streak_days_above_threshold: threshold is per-location (that city's own
# mean + 1 stddev of avg_temperature_c), so it's meaningful across wildly
# different climates instead of one fixed number. Streak length is a
# classic gaps-and-islands run-length count: the difference between two
# row_number() sequences is constant within a consecutive run of the same
# is_above flag, giving each run a stable group id to count within.
#
# NOTE: unlike anomaly_vs_historical_avg, this threshold is intentionally
# computed over each location's ENTIRE history (not just prior years) — every
# full recompute can shift it slightly as new days arrive, which means
# streak_days_above_threshold values for past dates can change between runs.
# This is an accepted consequence of the full-recompute design (see
# docs/design-decisions.md: "Gold layer: full recompute"), not a bug: the
# threshold is meant to represent "hot for this city, given everything we
# know," and that definition necessarily moves as more data arrives.
loc_window = Window.partitionBy("location_id")
with_threshold_df = anomaly_df.withColumn(
    "location_threshold",
    F.avg("avg_temperature_c").over(loc_window) + F.stddev_pop("avg_temperature_c").over(loc_window),
).withColumn("is_above", F.col("avg_temperature_c") > F.col("location_threshold"))

date_order_window = Window.partitionBy("location_id").orderBy("obs_date")
run_window = Window.partitionBy("location_id", "is_above").orderBy("obs_date")
grouped_df = with_threshold_df.withColumn(
    "streak_group",
    F.row_number().over(date_order_window) - F.row_number().over(run_window),
)

streak_window = Window.partitionBy("location_id", "streak_group", "is_above").orderBy("obs_date")
streaked_df = grouped_df.withColumn(
    "streak_days_above_threshold",
    F.when(F.col("is_above"), F.row_number().over(streak_window)).otherwise(F.lit(0)),
)

gold_updates_df = streaked_df.withColumn(
    "date_id",
    F.year("obs_date") * 10000 + F.month("obs_date") * 100 + F.dayofmonth("obs_date"),
).select(
    "location_id",
    "date_id",
    "avg_temperature_c",
    "avg_humidity_pct",
    "avg_wind_speed_kmh",
    "comfort_index",
    "anomaly_vs_historical_avg",
    "streak_days_above_threshold",
    F.lit(PIPELINE_RUN_ID).alias("pipeline_run_id"),
    F.lit(RUN_TIMESTAMP).alias("_computed_at"),
)
gold_updates_df.cache()
rows_affected = gold_updates_df.count()

try:
    fact_table = DeltaTable.forName(spark, "gold.fact_weather_daily")
    (
        fact_table.alias("t")
        .merge(
            gold_updates_df.alias("s"),
            "t.location_id = s.location_id AND t.date_id = s.date_id",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        # A location_id + date_id no longer present in the source means silver
        # no longer has a valid observation backing that day (e.g. a row was
        # reclassified is_valid=false in a later correction). Full recompute
        # means the source always represents "everything currently valid," so
        # anything in the target but absent from source is stale and should
        # be removed rather than left behind as an orphaned fact row.
        .whenNotMatchedBySourceDelete()
        .execute()
    )
    _log_run(status="success", rows_affected=rows_affected)

except Exception as e:
    _log_run(status="failed", rows_affected=0, error_message=str(e))

finally:
    gold_updates_df.unpersist()

# ---------------------------------------------------------------------------
# Quick verification queries
# ---------------------------------------------------------------------------
print("\n--- gold.fact_weather_daily row count by location ---")
spark.sql("SELECT location_id, COUNT(*) AS filas FROM gold.fact_weather_daily GROUP BY location_id").show()

print("\n--- sample rows (Cincinnati, most recent 10 days) ---")
spark.sql("""
    SELECT f.location_id, d.full_date, f.avg_temperature_c, f.avg_humidity_pct,
           f.avg_wind_speed_kmh, f.comfort_index, f.anomaly_vs_historical_avg,
           f.streak_days_above_threshold, f.pipeline_run_id, f._computed_at
    FROM gold.fact_weather_daily f
    JOIN gold.dim_date d ON f.date_id = d.date_id
    WHERE f.location_id = 'cincinnati'
    ORDER BY d.full_date DESC
    LIMIT 10
""").show(truncate=False)

print("\n--- sample rows (Cincinnati, earliest 3 days on record — anomaly should be NULL, no prior years) ---")
spark.sql("""
    SELECT f.location_id, d.full_date, f.avg_temperature_c, f.anomaly_vs_historical_avg
    FROM gold.fact_weather_daily f
    JOIN gold.dim_date d ON f.date_id = d.date_id
    WHERE f.location_id = 'cincinnati'
    ORDER BY d.full_date ASC
    LIMIT 3
""").show(truncate=False)

print("\n--- etl_run_log (gold layer) ---")
spark.sql("SELECT * FROM etl_run_log WHERE layer = 'gold' ORDER BY run_timestamp DESC").show(truncate=False)
