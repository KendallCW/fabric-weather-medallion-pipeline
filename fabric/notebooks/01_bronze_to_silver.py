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

from pyspark.sql import functions as F
from delta.tables import DeltaTable
import uuid
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

# Bronze layout: bronze/weather/{location_id}/{year}/{month}/{day}/data.json
# (see docs/data-dictionary.md). Adjust the Files/ root if the Lakehouse
# mounts bronze under a different shortcut/path.
BRONZE_GLOB = "Files/bronze/weather/*/*/*/*/data.json"
FILE_PATH_REGEX = r"weather/([^/]+)/(\d{4})/(\d{2})/(\d{2})/data\.json"

PIPELINE_RUN_ID = str(uuid.uuid4())
RUN_TIMESTAMP = datetime.now(timezone.utc)

TEMP_C_RANGE = (-60.0, 60.0)
HUMIDITY_PCT_RANGE = (0.0, 100.0)

# ---------------------------------------------------------------------------
# 0. Ensure control + target tables exist
# ---------------------------------------------------------------------------

spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

# Watermark control table: one row per location_id, tracks the latest bronze
# ingestion date (derived from the bronze partition path) already merged
# into silver for that city. Empty/missing row => process all history.
spark.sql("""
    CREATE TABLE IF NOT EXISTS watermark_location (
        location_id                    STRING NOT NULL,
        last_processed_ingestion_date  DATE
    )
    USING DELTA
""")

# Exact DDL per docs/data-dictionary.md.
spark.sql("""
    CREATE TABLE IF NOT EXISTS etl_run_log (
        pipeline_run_id    STRING,
        layer              STRING,
        status             STRING,
        error_message      STRING,
        rows_affected      INT,
        run_timestamp      TIMESTAMP
    )
    USING DELTA
""")

# Exact DDL per sql/ddl/silver_weather_observations.sql.
spark.sql("""
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
    PARTITIONED BY (location_id)
""")


def _log_run(status: str, rows_affected: int, error_message: str = None) -> None:
    log_row = spark.createDataFrame(
        [(PIPELINE_RUN_ID, "silver", status, error_message, rows_affected, RUN_TIMESTAMP)],
        schema="pipeline_run_id STRING, layer STRING, status STRING, "
        "error_message STRING, rows_affected INT, run_timestamp TIMESTAMP",
    )
    log_row.write.format("delta").mode("append").saveAsTable("etl_run_log")


# ---------------------------------------------------------------------------
# 1. INCREMENTAL: discover only the bronze files newer than each location's
#    watermark. First run (no watermark rows yet) processes everything.
# ---------------------------------------------------------------------------

watermarks = {
    row["location_id"]: row["last_processed_ingestion_date"]
    for row in spark.table("watermark_location").collect()
}

all_files_df = (
    spark.read.format("binaryFile")
    .load(BRONZE_GLOB)
    .select("path")
    .withColumn("location_id", F.regexp_extract("path", FILE_PATH_REGEX, 1))
    .withColumn(
        "ingestion_date",
        F.to_date(
            F.concat_ws(
                "-",
                F.regexp_extract("path", FILE_PATH_REGEX, 2),
                F.regexp_extract("path", FILE_PATH_REGEX, 3),
                F.regexp_extract("path", FILE_PATH_REGEX, 4),
            )
        ),
    )
)

files_to_process = [
    row["path"]
    for row in all_files_df.collect()
    if watermarks.get(row["location_id"]) is None
    or row["ingestion_date"] > watermarks[row["location_id"]]
]

if not files_to_process:
    _log_run(status="success", rows_affected=0)
else:
    # -----------------------------------------------------------------------
    # 2. LECTURA: raw bronze JSON is the untouched Open-Meteo payload
    #    (HTTP+Binary connector — see docs/design-decisions.md). location_id
    #    and ingestion_date aren't in the payload, so pull them from the path.
    # -----------------------------------------------------------------------
    raw_df = (
        spark.read.option("multiline", "true")
        .json(files_to_process)
        .withColumn("_file_path", F.input_file_name())
        .withColumn("location_id", F.regexp_extract("_file_path", FILE_PATH_REGEX, 1))
        .withColumn(
            "ingestion_date",
            F.to_date(
                F.concat_ws(
                    "-",
                    F.regexp_extract("_file_path", FILE_PATH_REGEX, 2),
                    F.regexp_extract("_file_path", FILE_PATH_REGEX, 3),
                    F.regexp_extract("_file_path", FILE_PATH_REGEX, 4),
                )
            ),
        )
    )

    # -------------------------------------------------------------------
    # 3. TRANSFORMACIÓN: hourly.* are parallel arrays -> zip into an array
    #    of structs -> explode into one row per hour.
    # -------------------------------------------------------------------
    zipped_df = raw_df.withColumn(
        "hourly_zipped",
        F.arrays_zip(
            F.col("hourly.time").alias("time"),
            F.col("hourly.temperature_2m").alias("temperature_2m"),
            F.col("hourly.relative_humidity_2m").alias("relative_humidity_2m"),
            F.col("hourly.wind_speed_10m").alias("wind_speed_10m"),
            F.col("hourly.precipitation").alias("precipitation"),
        ),
    )

    exploded_df = zipped_df.select(
        "location_id",
        "ingestion_date",
        F.explode("hourly_zipped").alias("hourly_row"),
    )

    # -------------------------------------------------------------------
    # 4. TIPADO: ADF requests the Open-Meteo archive with timezone=UTC
    #    (see adf/datasets/ds_openmeteo_http.json), so hourly.time strings
    #    are already UTC wall-clock; cast straight to timestamp.
    # -------------------------------------------------------------------
    typed_df = exploded_df.select(
        "location_id",
        "ingestion_date",
        F.to_timestamp("hourly_row.time").alias("observation_datetime"),
        F.col("hourly_row.temperature_2m").cast("double").alias("temperature_c"),
        F.col("hourly_row.relative_humidity_2m").cast("double").alias("humidity_pct"),
        F.col("hourly_row.wind_speed_10m").cast("double").alias("wind_speed_kmh"),
        F.col("hourly_row.precipitation").cast("double").alias("precipitation_mm"),
    )

    # -------------------------------------------------------------------
    # 5. VALIDACIÓN: flag out-of-range rows, don't drop them.
    # -------------------------------------------------------------------
    validated_df = typed_df.withColumn(
        "is_valid",
        (
            F.col("temperature_c").between(*TEMP_C_RANGE)
            & F.col("humidity_pct").between(*HUMIDITY_PCT_RANGE)
            & (F.col("wind_speed_kmh") >= 0)
            & (F.col("precipitation_mm") >= 0)
        ),
    )

    # -------------------------------------------------------------------
    # 6. DEDUPLICACIÓN on the silver grain.
    # -------------------------------------------------------------------
    deduped_df = validated_df.dropDuplicates(["location_id", "observation_datetime"])

    silver_updates_df = deduped_df.withColumn("_merged_at", F.lit(RUN_TIMESTAMP)).select(
        "location_id",
        "observation_datetime",
        "temperature_c",
        "humidity_pct",
        "wind_speed_kmh",
        "precipitation_mm",
        "is_valid",
        "_merged_at",
    )
    silver_updates_df.cache()
    rows_affected = silver_updates_df.count()

    # Latest bronze ingestion_date processed per location, for the watermark.
    max_ingestion_by_location = (
        deduped_df.groupBy("location_id")
        .agg(F.max("ingestion_date").alias("max_ingestion_date"))
        .collect()
    )

    try:
        # -----------------------------------------------------------------
        # 7. MERGE INTO silver.weather_observations
        # -----------------------------------------------------------------
        silver_table = DeltaTable.forName(spark, "silver.weather_observations")
        (
            silver_table.alias("t")
            .merge(
                silver_updates_df.alias("s"),
                "t.location_id = s.location_id AND t.observation_datetime = s.observation_datetime",
            )
            .whenMatchedUpdate(
                set={
                    "temperature_c": "s.temperature_c",
                    "humidity_pct": "s.humidity_pct",
                    "wind_speed_kmh": "s.wind_speed_kmh",
                    "precipitation_mm": "s.precipitation_mm",
                    "is_valid": "s.is_valid",
                    "_merged_at": "s._merged_at",
                }
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

        # -----------------------------------------------------------------
        # 8. CIERRE: advance the watermark per location, log success.
        # -----------------------------------------------------------------
        watermark_updates_df = spark.createDataFrame(
            max_ingestion_by_location,
            schema="location_id STRING, max_ingestion_date DATE",
        )
        watermark_table = DeltaTable.forName(spark, "watermark_location")
        (
            watermark_table.alias("w")
            .merge(
                watermark_updates_df.alias("u"),
                "w.location_id = u.location_id",
            )
            .whenMatchedUpdate(
                set={"last_processed_ingestion_date": "u.max_ingestion_date"}
            )
            .whenNotMatchedInsert(
                values={
                    "location_id": "u.location_id",
                    "last_processed_ingestion_date": "u.max_ingestion_date",
                }
            )
            .execute()
        )

        _log_run(status="success", rows_affected=rows_affected)

    except Exception as e:
        # MERGE or watermark update failed: don't crash the notebook, but
        # make sure the failure (and the fact the watermark did NOT advance,
        # so the next run retries the same files) is visible in etl_run_log.
        _log_run(status="failed", rows_affected=0, error_message=str(e))

    finally:
        silver_updates_df.unpersist()
