# Fabric Weather Medallion Pipeline

End-to-end data engineering pipeline: public weather API → Azure Data Factory → ADLS Gen2 (bronze) → Microsoft Fabric Lakehouse (silver/gold) → Power BI semantic model.

**Status:** 🚧 In progress

## Architecture

Open-Meteo API → Azure Data Factory (ingestion) → ADLS Gen2 (bronze) → Fabric Lakehouse (silver: cleaning/typing/dedup → gold: business logic) → semantic model → Power BI report (DAX)

Credentials managed via Azure Key Vault.

See [`docs/architecture-diagram.png`](docs/architecture-diagram.png) and [`docs/design-decisions.md`](docs/design-decisions.md) for details.

## Stack

- **Ingestion:** Azure Data Factory
- **Storage:** Azure Data Lake Storage Gen2
- **Processing:** Microsoft Fabric (Lakehouse, PySpark notebooks)
- **Secrets:** Azure Key Vault
- **BI:** Power BI, DAX
- **Source data:** [Open-Meteo API](https://open-meteo.com/)

## Repo structure

```
├── docs/                    # architecture diagram, design decisions, data dictionary
├── adf/                     # exported ADF pipelines, datasets, linked services, triggers
├── fabric/notebooks/        # PySpark notebooks (bronze→silver, silver→gold)
├── fabric/semantic_model/   # semantic model documentation
├── powerbi/                 # .pbix report + DAX measures
└── sql/ddl/                 # table definitions for silver/gold layers
```

## Key engineering decisions

- **Incremental loading**, not full reload — watermark-based extraction with `MERGE INTO` upserts at the silver layer
- **Error handling & logging** — retry policies in ADF, centralized `etl_run_log` control table
- **Data quality validation** — range checks and null flagging at silver, not silent drops

## What I would do differently

See [`docs/what-i-would-do-differently.md`](docs/what-i-would-do-differently.md) — updated as the project evolves.

## Author

Kendall Castro — [GitHub](https://github.com/KendallCW)
