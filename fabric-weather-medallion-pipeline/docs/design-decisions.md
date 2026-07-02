# Design Decisions

This document records the *why* behind architectural choices — not just what was built, but what was considered and rejected.

## Data source: Open-Meteo

**Considered:** weather (Open-Meteo), public transit (GTFS), crypto/stock prices.

**Chosen:** Open-Meteo — no API key required, generous rate limits, robust historical + forecast data. Enables meaningful business logic at the gold layer (comfort index, historical anomalies, streaks) rather than trivial pass-through aggregations. Natural fit for incremental loading (new observations arrive on a schedule).

**Rejected:**
- *Crypto/stocks:* gold-layer logic tends to be trivial (OHLC, % change); heavily saturated in DE portfolios.
- *Public transit (GTFS):* more complex/realistic schemas, but higher setup risk (regional API instability) that could shift focus from architecture to source troubleshooting.

## Orchestration: Azure Data Factory (not Fabric Data Pipelines)

*(fill in once implemented — e.g., ADF chosen for explicit linked service / Key Vault integration patterns relevant to the target role)*

## Incremental load strategy

Watermark-based extraction (`last_processed_date` per `location_id`) rather than full reload on every run. Silver layer uses Delta Lake `MERGE INTO` to handle upstream corrections/backfills without duplicating rows.

**Why this matters:** full reload is the default "tutorial" pattern. Incremental load with proper merge semantics is what production pipelines actually require, and it's explicitly called out as non-negotiable in the portfolio philosophy.

## Partitioning scheme (bronze)

`bronze/weather/{location_id}/{year}/{month}/{day}/data.json`

Rationale: partition-by-date supports efficient incremental reads at silver (only new date partitions are processed); partition-by-location keeps per-city backfills isolated.

## Error handling approach

- ADF: retry policy (3 attempts, exponential backoff) on ingestion activities
- Centralized `etl_run_log` control table capturing `status`, `error_message`, `rows_affected`, `pipeline_run_id`
- Silver-layer validation flags out-of-range values instead of silently dropping them

## Open questions / things to revisit

- [ ] Confirm final list of tracked locations
- [ ] Decide backfill window for historical anomaly baseline (gold layer)
- [ ] Evaluate whether ADF triggers should be schedule-based or event-based
