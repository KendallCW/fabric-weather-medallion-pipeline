# export_gold_for_powerbi.py
# Exports the local gold Delta tables to flat, single-file Parquet for Power
# BI import — no Delta transaction log/metadata, no partition directories,
# just one .parquet file per table. Same local Spark session setup as
# local_bronze_to_silver.py / local_silver_to_gold.py (see
# docs/design-decisions.md: "Execution environment: local PySpark instead of
# Fabric Lakehouse").

import shutil
from pathlib import Path

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# ---------------------------------------------------------------------------
# Local Spark session setup (this block replaces Fabric's built-in `spark`)
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.as_posix()
WAREHOUSE_DIR = f"{PROJECT_ROOT}/warehouse"
EXPORT_DIR = Path(PROJECT_ROOT) / "powerbi_export"

builder = (
    SparkSession.builder.appName("export_gold_for_powerbi")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# Export: read each gold Delta table, write as a single flat Parquet file.
# coalesce(1) forces Spark to write exactly one part file; Spark itself has
# no "write to one exact filename" mode, so it's written to a temp directory
# first and the single part file inside is moved out and renamed to the
# requested name, then the temp directory (with its _SUCCESS/CRC files) is
# discarded.
# ---------------------------------------------------------------------------

TABLES = {
    "gold.db/fact_weather_daily": "fact_weather_daily.parquet",
    "gold.db/dim_location": "dim_location.parquet",
    "gold.db/dim_date": "dim_date.parquet",
}

EXPORT_DIR.mkdir(parents=True, exist_ok=True)

for delta_subpath, output_filename in TABLES.items():
    df = spark.read.format("delta").load(f"{WAREHOUSE_DIR}/{delta_subpath}")

    tmp_dir = EXPORT_DIR / f"_tmp_{output_filename}"
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)

    df.coalesce(1).write.mode("overwrite").parquet(tmp_dir.as_posix())

    part_file = next(tmp_dir.glob("part-*.parquet"))
    output_path = EXPORT_DIR / output_filename
    if output_path.exists():
        output_path.unlink()
    shutil.move(str(part_file), str(output_path))
    shutil.rmtree(tmp_dir)

    print(f"Wrote {output_path} ({df.count()} rows)")

# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------
print("\n--- powerbi_export contents ---")
for f in sorted(EXPORT_DIR.glob("*.parquet")):
    print(f.name, f.stat().st_size, "bytes")
