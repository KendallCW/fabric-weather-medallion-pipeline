## Data source: Open-Meteo

**Considered:** weather (Open-Meteo), public transit (GTFS), crypto/stock prices.

**Chosen:** Open-Meteo — no API key required, generous rate limits, robust historical + forecast data. Enables meaningful business logic at the gold layer (comfort index, historical anomalies, streaks) rather than trivial pass-through aggregations. Natural fit for incremental loading (new observations arrive on a schedule).

**Rejected:**
- *Crypto/stocks:* gold-layer logic tends to be trivial (OHLC, % change); heavily saturated in DE portfolios.
- *Public transit (GTFS):* more complex/realistic schemas, but higher setup risk (regional API instability) that could shift focus from architecture to source troubleshooting.

## Data connector: HTTP + Binary (not REST connector)

**Considered:** ADF's native REST connector (Copy Activity source type `RestSource`), which seemed like the obvious fit for calling a REST API.

**Rejected:** the REST connector consistently threw `RestResourceReadFailed` / `Found invalid data while decoding` when calling Open-Meteo's archive endpoint — reproducible even with a small 7-day date range, ruling out payload size as the cause. The same URL worked correctly when tested directly in a browser and is documented as a known issue affecting ADF's REST connector specifically (Copy Activity + REST fails while Web Activity calling the identical endpoint succeeds, per community reports). Root cause appears to be internal to the REST connector's response decoding, not a configuration error.

**Decision:** switched to an HTTP linked service + Binary-format datasets on both source and sink. This copies the raw response bytes without ADF attempting to parse/decode them — which is actually a better fit for bronze anyway, since bronze is meant to store the untouched raw payload. JSON parsing happens explicitly and intentionally at the silver-layer notebook instead.

**Lesson:** don't assume the "obviously named" connector (REST, for a REST API) is the right one — test early with a small payload before building out the full parameterization on top of it.

## Historical backfill strategy

**Considered:** chunking the 2015-2025 backfill by year (nested ForEach over years) to avoid a hypothesized response-size limitation in the ingestion connector.

**Revisited:** once the connector switched from REST to HTTP+Binary (see above), the size concern no longer applied — Binary format copies raw bytes without ADF attempting to parse/decode the response, removing the failure mode chunking was meant to avoid.

**Outcome:** the full 2015-2025 range succeeded in a single call per city with the HTTP+Binary approach — no chunking needed. Confirms that the earlier size concern was specific to the REST connector's decoding step, not an inherent limit on response size for this API/date range combination.

## Orchestration: Azure Data Factory (not Fabric Data Pipelines)

**Considered:** mid-build, evaluated switching from standalone ADF to Fabric Data Factory (pipelines built inside the Fabric workspace), prompted by Microsoft's own migration messaging recommending Fabric-native pipelines going forward.

**Investigated:** Azure Key Vault integration for Fabric Data Factory connections is currently in preview and has documented limitations — it doesn't yet support the same native Key Vault reference pattern ADF has for Web/REST activities specifically, based on active community threads reporting friction with this exact use case (Web activity + Key Vault secret retrieval).

**Decision:** kept standalone ADF for ingestion. Reasons:
1. Key Vault integration for REST/Web activities is mature and well-documented in ADF; the equivalent in Fabric is still preview-stage with open gaps for this exact pattern.
2. Using a separate ADF resource (rather than everything inside one Fabric workspace) better demonstrates cross-service Azure orchestration — a common real-world enterprise pattern, and more defensible portfolio evidence than a single-workspace solution.
3. ADF remains the dominant tool referenced in current Azure Data Engineer job postings, even as Microsoft pushes Fabric Data Factory as the forward path.

**Revisit when:** Key Vault support for Fabric Data Factory connections reaches general availability — worth demonstrating in a future project as evidence of staying current with the platform's direction.

## Incremental load strategy

Watermark-based extraction (`last_processed_date` per `location_id`) rather than full reload on every run. Silver layer uses Delta Lake `MERGE INTO` to handle upstream corrections/backfills without duplicating rows.

**Why this matters:** full reload is the default "tutorial" pattern. Incremental load with proper merge semantics is what production pipelines actually require, and it's explicitly called out as non-negotiable in the portfolio philosophy.

## Partitioning scheme (bronze)

`bronze/weather/{location_id}/{year}/{month}/{day}/data.json`

Rationale: partition-by-date supports efficient incremental reads at silver (only new date partitions are processed); partition-by-location keeps per-city backfills isolated.

## Error handling approach

- ADF: retry policy (3 attempts, 30s interval) on the Copy Data activity ingesting from Open-Meteo
- **`etl_run_log` control table lives in the Fabric Lakehouse (Delta), not in ADF.** Considered adding a dedicated Azure SQL Database purely to host this table so ADF could write to it via Stored Procedure activity, but that adds a whole extra resource for a single logging table. Bronze-layer operational visibility is covered by ADF's native Monitor tab (run duration, rows copied, per-iteration success/failure, retries) instead. The actual `etl_run_log` Delta table gets populated starting at the silver-layer notebook, where writing to Delta is a native, one-line operation.
- Silver-layer validation flags out-of-range values instead of silently dropping them

## Open questions / things to revisit

- [ ] Confirm final list of tracked locations
- [ ] Decide backfill window for historical anomaly baseline (gold layer)
- [ ] Evaluate whether ADF triggers should be schedule-based or event-based
