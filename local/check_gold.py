from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

WH = "C:/Users/kcast/Projects/fabric-weather-medallion-pipeline/local/warehouse"
b = (
    SparkSession.builder.appName("check")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.warehouse.dir", WH)
)
spark = configure_spark_with_delta_pip(b).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

fact = spark.read.format("delta").load(f"{WH}/gold.db/fact_weather_daily")
dim_date = spark.read.format("delta").load(f"{WH}/gold.db/dim_date")
dim_location = spark.read.format("delta").load(f"{WH}/gold.db/dim_location")

print("--- dim_location ---")
dim_location.show(truncate=False)

print("--- dubai summer 2024 sample (streak check) ---")
(
    fact.join(dim_date, "date_id")
    .filter("location_id = 'dubai' AND year = 2024 AND month = 7")
    .select("location_id", "full_date", "avg_temperature_c", "streak_days_above_threshold")
    .orderBy("full_date")
    .show(31, truncate=False)
)

print("--- max streak per city ---")
fact.groupBy("location_id").agg({"streak_days_above_threshold": "max"}).show()
