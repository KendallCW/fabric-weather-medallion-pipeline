# Local execution environment

This folder holds the local PySpark + Delta Lake adaptation of the Fabric
notebooks, used while a proper Fabric environment was blocked by Microsoft
account/licensing issues unrelated to this project's engineering (see
`docs/design-decisions.md` → "Execution environment: local PySpark instead
of Fabric Lakehouse").

## Scripts

| File | What it does |
|---|---|
| `local_bronze_to_silver.py` | Local mirror of `fabric/notebooks/01_bronze_to_silver.py` — parses bronze JSON, writes `silver.weather_observations` |
| `local_silver_to_gold.py` | Local mirror of `fabric/notebooks/02_silver_to_gold.py` — aggregates silver to `gold.fact_weather_daily`, `dim_location`, `dim_date` |
| `check_gold.py` | Ad-hoc verification script — spot-checks `dim_location`, a Dubai/summer-2024 sample (for eyeballing `streak_days_above_threshold`), and max streak per city, directly against the local Delta warehouse |
| `export_gold_for_powerbi.py` | Exports the 3 gold tables to flat, single-file Parquet in `powerbi_export/` for import into Power BI Desktop |
| `run_local_notebook.ps1` | Sets up the Windows environment (PATH, `PYSPARK_PYTHON`, `PYSPARK_DRIVER_PYTHON`) for the current session and runs `local_bronze_to_silver.py` |

All four Python scripts derive their paths from their own file location (`Path(__file__).resolve().parent`), so none of them need editing before running — clone the repo and they work as-is. `local_silver_to_gold.py`, `check_gold.py`, and `export_gold_for_powerbi.py` aren't wired into `run_local_notebook.ps1` yet; run them the same way — set the same environment variables shown in that script first, then `python <script_name>.py` — or adapt the `.ps1` to accept the script name as a parameter.

## Setup (Windows)

1. Install Java (OpenJDK 17): `winget install Microsoft.OpenJDK.17`
2. Install Python 3.11 (PySpark 4.1.1 has a known crash on Windows with Python
   3.12+ — SPARK-53759): `winget install Python.Python.3.11`
3. Create a virtual environment and install dependencies:
   ```powershell
   python3.11 -m venv .venv311
   .\.venv311\Scripts\pip install pyspark==4.1.1 delta-spark==4.3.0
   ```
4. Download `winutils.exe` and `hadoop.dll` (Hadoop 3.4.x) from
   https://github.com/kontext-tech/winutils into `C:\hadoop\bin`
5. Download bronze files from the ADLS Gen2 `bronze` container into a local
   `bronze/` folder here, mirroring the real partition layout, e.g.:
   ```
   local/bronze/weather/cincinnati/2026/07/02/data.json
   ```

## Running

```powershell
.\run_local_notebook.ps1
```

This sets `PATH`, `PYSPARK_PYTHON`, and `PYSPARK_DRIVER_PYTHON` scoped to the
current PowerShell session only (not persisted globally — see
`docs/design-decisions.md` for why) and runs `local_bronze_to_silver.py`.
Run `local_silver_to_gold.py` the same way afterward (same environment
variables, then point the script at it).

## Moving back to Fabric later

The transformation logic here is identical to the notebooks under
`fabric/notebooks/`. To move back to a real Fabric Lakehouse once available,
no logic changes are needed — only re-point `BRONZE_GLOB` (and the
equivalent silver read path in the gold notebook) from a local `file:///`
path back to `Files/bronze/...`.
