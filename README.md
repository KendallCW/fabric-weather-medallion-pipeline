# Fabric Weather Medallion Pipeline

End-to-end data engineering pipeline: public weather API → Azure Data Factory → ADLS Gen2 (bronze) → medallion transformation (silver/gold) → Power BI semantic model.

**Status:** ✅ Complete — bronze, silver, gold, and a Power BI dashboard are built, validated, and documented.

## Overview

Eleven years (2015–2025) of hourly weather data for 5 cities (Cincinnati, Chicago, Dubai, Reykjavik, Singapore) flow through a medallion architecture: raw API ingestion, incremental cleaning/validation, and business-logic aggregation (comfort index, historical anomaly, heat streaks), landing in a Power BI dashboard.

**Key numbers:**
- 482,160 raw hourly observations in silver (96,432 per city — exact, verified, zero duplicates/gaps)
- 20,090 daily aggregates in gold (4,018 days × 5 cities)
- 7 DAX measures, 3 relationships, 1 semantic model built via MCP-driven modeling on Power BI Desktop

## Architecture

```
Open-Meteo API → Azure Data Factory (HTTP + Binary connector)
              → ADLS Gen2 (bronze, raw JSON)
              → PySpark + Delta Lake (silver: parse/type/dedupe/validate, MERGE + watermark)
              → PySpark + Delta Lake (gold: star schema, comfort index, historical anomaly, heat streaks)
              → Power BI (semantic model + dashboard)
```

Credentials for the ADF↔storage connection are managed via Azure Key Vault.

**Note on execution environment:** the silver/gold notebooks were designed for Microsoft Fabric Lakehouse but currently run against local PySpark + Delta Lake, due to a chain of Microsoft account/licensing blockers unrelated to the engineering itself (shared trial capacity contention, Developer Program rejections, tenant conflicts — see `docs/design-decisions.md` for the full account). The transformation logic is identical either way, since Fabric's Lakehouse is itself built on Delta Lake/Spark; moving back to Fabric requires no logic changes.

See [`docs/design-decisions.md`](docs/design-decisions.md) for the full decision log — including things that didn't work on the first try — and [`docs/data-dictionary.md`](docs/data-dictionary.md) for the schema of every layer.

### 🧰 Stack

Tools listed here were actually used and verified in this project — no badge without evidence in the repo.

**Cloud & Data Platform**
<p>
  <img alt="Azure" src="https://img.shields.io/badge/Microsoft_Azure-0078D4?style=flat-square&logo=microsoftazure&logoColor=white" />
  <img alt="Microsoft Fabric" src="https://img.shields.io/badge/Microsoft_Fabric-0078D4?style=flat-square" />
  <img alt="Azure Data Factory" src="https://img.shields.io/badge/Azure_Data_Factory-0078D4?style=flat-square" />
  <img alt="ADLS Gen2" src="https://img.shields.io/badge/ADLS_Gen2-0078D4?style=flat-square" />
</p>

**Security**
<p>
  <img alt="Azure Key Vault" src="https://img.shields.io/badge/Azure_Key_Vault-0078D4?style=flat-square" />
</p>

**BI & Modeling**
<p>
  <img alt="Power BI" src="https://img.shields.io/badge/Power_BI-F2C811?style=flat-square&logo=powerbi&logoColor=black" />
  <img alt="DAX" src="https://img.shields.io/badge/DAX-FF6C37?style=flat-square" />
</p>

**Languages & Processing**
<p>
  <img alt="Python" src="https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white" />
  <img alt="PySpark" src="https://img.shields.io/badge/PySpark-E25A1C?style=flat-square&logo=apachespark&logoColor=white" />
  <img alt="SQL" src="https://img.shields.io/badge/SQL-CC2927?style=flat-square&logo=microsoftsqlserver&logoColor=white" />
  <img alt="Delta Lake" src="https://img.shields.io/badge/Delta_Lake-00ADD4?style=flat-square" />
</p>

**AI-Assisted Engineering**
<p>
  <img alt="Claude" src="https://img.shields.io/badge/Claude-D97757?style=flat-square&logo=anthropic&logoColor=white" />
  <img alt="Claude Code" src="https://img.shields.io/badge/Claude_Code-D97757?style=flat-square" />
  <img alt="MCP" src="https://img.shields.io/badge/MCP-D97757?style=flat-square" />
</p>

Used for architecture/design discussion, debugging ADF and PySpark issues, and — notably — building the Power BI semantic model (tables, relationships, DAX measures) programmatically via an MCP server connected directly to a local Power BI Desktop instance, rather than manual UI-only modeling.

**Tooling**
<p>
  <img alt="Git" src="https://img.shields.io/badge/Git-F05032?style=flat-square&logo=git&logoColor=white" />
  <img alt="GitHub" src="https://img.shields.io/badge/GitHub-181717?style=flat-square&logo=github&logoColor=white" />
</p>

## Repo structure

```
├── docs/                    # design decisions, data dictionary, retrospective
├── adf/                     # exported ADF pipelines, datasets, linked services
├── fabric/notebooks/        # PySpark notebooks (bronze→silver, silver→gold) — Fabric target
├── local/                   # local PySpark execution path (README explains why + how)
├── powerbi/                 # .pbix dashboard + DAX measure docs
└── sql/ddl/                 # table definitions for silver/gold layers
```

## Key engineering decisions

- **Incremental loading**, not full reload — watermark-based extraction with `MERGE INTO` upserts at the silver layer, verified with exact row counts across multiple runs
- **Error handling & logging** — retry policies in ADF, centralized `etl_run_log` control table
- **Data quality validation** — range checks and null flagging at silver, not silent drops
- **Lookahead bias caught and fixed in gold** — the historical anomaly baseline originally leaked future years into "historical" averages; caught in a self-review pass, fixed with an expanding prior-years-only window, and verified by hand against a specific row
- **Local execution over Fabric**, by necessity not preference — documented as a licensing/access constraint, not a technical limitation, with a clear path back to Fabric when access is available

## What I would do differently

See [`docs/what-i-would-do-differently.md`](docs/what-i-would-do-differently.md).

## Author

Kendall Castro — [GitHub](https://github.com/KendallCW)
