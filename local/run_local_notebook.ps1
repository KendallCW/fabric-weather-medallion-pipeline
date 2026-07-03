# run_local_notebook.ps1
# Sets up the environment scoped to THIS PowerShell session only (not persisted
# globally) and runs the local bronze-to-silver notebook.
#
# Why not persist these as global user env vars: PYSPARK_PYTHON is pinned to
# this project's specific venv (Python 3.11, PySpark 4.1.1). Project 2 of the
# portfolio roadmap (Databricks/PySpark) will likely need a different Python/
# PySpark version — a global PYSPARK_PYTHON would silently break that project
# instead of being an obvious, scoped setting like this script.
#
# Usage: from this folder, run:  .\run_local_notebook.ps1

$env:PATH = "C:\hadoop\bin;" + $env:PATH
$env:PYSPARK_PYTHON = "$PWD\.venv311\Scripts\python.exe"
$env:PYSPARK_DRIVER_PYTHON = "$PWD\.venv311\Scripts\python.exe"

Write-Host "Environment configured for this session. Running notebook..." -ForegroundColor Cyan
& "$PWD\.venv311\Scripts\python.exe" local_bronze_to_silver.py
