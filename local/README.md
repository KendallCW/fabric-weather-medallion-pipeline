# Local execution environment

This folder holds the local PySpark + Delta Lake adaptation of the Fabric
silver-layer notebook, used while a proper Fabric environment was blocked by
Microsoft account/licensing issues unrelated to this project's engineering
(see `docs/design-decisions.md` → "Execution environment: local PySpark
instead of Fabric Lakehouse").

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
5. Download sample bronze files from the ADLS Gen2 `bronze` container into a
   local folder mirroring the real partition layout, e.g.:
   ```
   <project_root>/bronze/weather/cincinnati/2026/07/02/data.json
   ```
6. Update `PROJECT_ROOT` in `local_bronze_to_silver.py` to match your local path.

## Running

```powershell
.\run_local_notebook.ps1
```

This sets `PATH`, `PYSPARK_PYTHON`, and `PYSPARK_DRIVER_PYTHON` scoped to the
current PowerShell session only (not persisted globally — see design-decisions.md
for why) and runs `local_bronze_to_silver.py`.

## Moving back to Fabric later

The transformation logic here is identical to `fabric/notebooks/01_bronze_to_silver.py`.
To move back to a real Fabric Lakehouse once available, no logic changes are
needed — only re-point `BRONZE_GLOB` from a local `file:///` path back to
`Files/bronze/...`.
