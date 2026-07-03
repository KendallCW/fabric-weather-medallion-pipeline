# local_bronze_to_silver.py
# Local adaptation of fabric/notebooks/01_bronze_to_silver.py for testing the
# transformation logic on a Windows machine with PySpark + Delta Lake,
# without a Fabric Lakehouse. Same logic, only the Spark session setup and
# file paths change (see docs/design-decisions.md: "Execution environment:
# local PySpark instead of Fabric Lakehouse").

from pyspark.sql import SparkSession, functions as F
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
    SparkSession.builder.appName("bronze_to_silver_local")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

# Local mirror of the bronze layout: bronze/weather/{location_id}/{year}/{month}/{day}/data.json
BRONZE_GLOB = f"file:///{PROJECT_ROOT}/bronze/weather/*/*/*/*/data.json"
FILE_PATH_REGEX = r"weather/([^/]+)/(\d{4})/(\d{2})/(\d{2})/data\.json"

PIPELINE_RUN_ID = str(uuid.uuid4())
RUN_TIMESTAMP = datetime.now(timezone.utc)

TEMP_C_RANGE = (-60.0, 60.0)
HUMIDITY_PCT_RANGE = (0.0, 100.0)

# ---------------------------------------------------------------------------
# 0. Ensure control + target tables exist
# ---------------------------------------------------------------------------

spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

spark.sql("""
    CREATE TABLE IF NOT EXISTS watermark_location (
        location_id                    STRING NOT NULL,
        last_processed_ingestion_date  DATE
    )
    USING DELTA
""")

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
# 1. INCREMENTAL
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

print(f"Files to process: {files_to_process}")

if not files_to_process:
    _log_run(status="success", rows_affected=0)
    print("No new files to process (watermark up to date).")
else:
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

    typed_df = exploded_df.select(
        "location_id",
        "ingestion_date",
        F.to_timestamp("hourly_row.time").alias("observation_datetime"),
        F.col("hourly_row.temperature_2m").cast("double").alias("temperature_c"),
        F.col("hourly_row.relative_humidity_2m").cast("double").alias("humidity_pct"),
        F.col("hourly_row.wind_speed_10m").cast("double").alias("wind_speed_kmh"),
        F.col("hourly_row.precipitation").cast("double").alias("precipitation_mm"),
    )

    validated_df = typed_df.withColumn(
        "is_valid",
        (
            F.col("temperature_c").between(*TEMP_C_RANGE)
            & F.col("humidity_pct").between(*HUMIDITY_PCT_RANGE)
            & (F.col("wind_speed_kmh") >= 0)
            & (F.col("precipitation_mm") >= 0)
        ),
    )

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
    print(f"Rows to merge into silver: {rows_affected}")

    max_ingestion_by_location = (
        deduped_df.groupBy("location_id")
        .agg(F.max("ingestion_date").alias("max_ingestion_date"))
        .collect()
    )

    try:
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
        print("MERGE succeeded.")

    except Exception as e:
        _log_run(status="failed", rows_affected=0, error_message=str(e))
        print(f"MERGE failed: {e}")

    finally:
        silver_updates_df.unpersist()

# ---------------------------------------------------------------------------
# Quick verification queries
# ---------------------------------------------------------------------------
print("\n--- silver.weather_observations by location ---")
spark.sql("SELECT location_id, COUNT(*) AS filas FROM silver.weather_observations GROUP BY location_id").show()

print("\n--- etl_run_log ---")
spark.sql("SELECT * FROM etl_run_log ORDER BY run_timestamp DESC").show(truncate=False)
