# Design Decisions

This document records the *why* behind architectural choices — not just what was built, but what was considered and rejected.

## Data source: Open-Meteo

**Considered:** weather (Open-Meteo), public transit (GTFS), crypto/stock prices.

**Chosen:** Open-Meteo — no API key required, generous rate limits, robust historical + forecast data. Enables meaningful business logic at the gold layer (comfort index, historical anomalies, streaks) rather than trivial pass-through aggregations. Natural fit for incremental loading (new observations arrive on a schedule).

**Rejected:**
- *Crypto/stocks:* gold-layer logic tends to be trivial (OHLC, % change); heavily saturated in DE portfolios.
- *Public transit (GTFS):* more complex/realistic schemas, but higher setup risk (regional API instability) that could shift focus from architecture to source troubleshooting.

## Silver layer backfill: complete and validated

The full 2015-2025 historical backfill has been processed end-to-end through the local PySpark execution path: **482,160 rows total, exactly 96,432 rows per city across all 5 locations (Cincinnati, Chicago, Dubai, Reykjavik, Singapore), with zero duplicates and zero gaps.**

96,432 = 4,018 days (11 years, including leap years 2016/2020/2024) × 24 hourly observations — the exact expected count if the watermark, deduplication (`dropDuplicates` on `location_id` + `observation_datetime`), and `MERGE INTO` logic all worked correctly across multiple incremental runs (initial test batches for Cincinnati/Chicago, followed by the full 5-city backfill). Matching counts across every city, with no city over or under the expected total, is strong evidence the incremental design handles reprocessing and multi-city merges correctly rather than silently duplicating or dropping rows.

## Data connector: HTTP + Binary (not REST connector)

**Considered:** ADF's native REST connector (Copy Activity source type `RestSource`), which seemed like the obvious fit for calling a REST API.

**Rejected:** the REST connector consistently threw `RestResourceReadFailed` / `Found invalid data while decoding` when calling Open-Meteo's archive endpoint — reproducible even with a small 7-day date range, ruling out payload size as the cause. The same URL worked correctly when tested directly in a browser and is documented as a known issue affecting ADF's REST connector specifically (Copy Activity + REST fails while Web Activity calling the identical endpoint succeeds, per community reports). Root cause appears to be internal to the REST connector's response decoding, not a configuration error.

**Decision:** switched to an HTTP linked service + Binary-format datasets on both source and sink. This copies the raw response bytes without ADF attempting to parse/decode them — which is actually a better fit for bronze anyway, since bronze is meant to store the untouched raw payload. JSON parsing happens explicitly and intentionally at the silver-layer notebook instead.

**Lesson:** don't assume the "obviously named" connector (REST, for a REST API) is the right one — test early with a small payload before building out the full parameterization on top of it.

## Historical backfill strategy

**Considered:** chunking the 2015-2025 backfill by year (nested ForEach over years) to avoid a hypothesized response-size limitation in the ingestion connector.

**Revisited:** once the connector switched from REST to HTTP+Binary (see above), the size concern no longer applied — Binary format copies raw bytes without ADF attempting to parse/decode the response, removing the failure mode chunking was meant to avoid.

**Outcome:** the full 2015-2025 range succeeded in a single call per city with the HTTP+Binary approach — no chunking needed. Confirms that the earlier size concern was specific to the REST connector's decoding step, not an inherent limit on response size for this API/date range combination.

## Local PySpark on Windows: environment issues resolved

Getting PySpark + Delta Lake running locally on Windows (see "Execution environment" decision above) surfaced four distinct, non-obvious issues, each worth recording since they're easy to misdiagnose as code bugs:

1. **Missing `winutils.exe`/`hadoop.dll` on PATH** — Spark's Hadoop dependency needs Windows-native binaries that Windows doesn't ship. Downloading them wasn't enough; `C:\hadoop\bin` also had to be explicitly added to PATH so the JVM could load `hadoop.dll` via `System.loadLibrary`.
2. **Corrupted local warehouse from failed prior runs** — the local run uses Spark's in-memory catalog (not persisted), so leftover Delta table directories from earlier failed attempts conflicted with fresh runs. Fix: delete the local `warehouse/` directory before re-running after a failure. Later hardened by explicitly setting `LOCATION` on each `CREATE TABLE` statement, removing ambiguity about where each table's data lives across runs.
3. **PySpark 4.1.1 crashes on Windows with Python 3.12+** (a known PySpark/Windows compatibility issue, tracked as SPARK-53759) — the fix was installing Python 3.11 specifically and creating a dedicated virtual environment for this project rather than using the system-wide Python 3.14.
4. **`PYSPARK_PYTHON` not automatically inferred** — even with the 3.11 venv active, Spark's worker processes kept resolving `python` from the global PATH (3.14) unless `PYSPARK_PYTHON`/`PYSPARK_DRIVER_PYTHON` were explicitly set to the venv's interpreter.

**Decision:** rather than persisting `PYSPARK_PYTHON` as a global Windows user environment variable, use a project-scoped activation script (`run_local_notebook.ps1`) that sets these variables only for the current PowerShell session. A global pin would risk silently breaking Project 2 of the portfolio roadmap (Databricks/PySpark), which will likely need a different Python/PySpark version.

## Execution environment: local PySpark instead of Fabric Lakehouse (for now)

**Context:** the silver-layer notebook (`01_bronze_to_silver.py`) was designed and implemented for Microsoft Fabric Lakehouse. Getting it running in an actual Fabric environment turned into a multi-hour licensing/access obstacle course, unrelated to the code or the architecture itself:

- The university's shared Fabric Trial capacity hit persistent `TooManyRequestsForCapacity` (HTTP 430) errors, reproducible even on a trivial job (`print("hello")`), confirming capacity-level contention shared across many students rather than anything related to this project's workload.
- Attempts to get a dedicated, personal Fabric environment ran into a chain of Microsoft account/licensing walls: the Microsoft 365 Developer Program rejected sign-up twice (once due to an old B2B guest association with a former employer's tenant, once due to a phone number already tied to another developer account); creating a personal Azure subscription led to a "Default Directory" conflict with that same legacy tenant; and creating a paid Fabric (F2) capacity was blocked with "Unsupported account — you cannot create a Microsoft capacity using a personal account," even from a freshly created tenant.

**Decision:** run the bronze→silver (and later silver→gold) transformation logic locally with PySpark + Delta Lake, against files downloaded from the real ADLS Gen2 bronze container, rather than continuing to spend portfolio-building time on Microsoft account/licensing troubleshooting unrelated to data engineering skill.

**Why this is still valid portfolio evidence:** the transformation logic, incremental watermark design, MERGE semantics, and validation rules are identical to what would run in Fabric — Delta Lake tables behave the same locally as in a Fabric Lakehouse, since Fabric's Lakehouse is itself built on Delta Lake/Spark. The only difference is the execution host (local machine vs. Fabric's managed Spark runtime), not the engineering. The full historical backfill (482,160 rows, verified exact) is proof the pipeline works correctly end-to-end regardless of host.

**Revisit when:** a properly isolated, non-contended Fabric environment becomes available (e.g., a work environment with organizational Fabric access) — at that point, the same unmodified notebook code can be moved back to a real Fabric Lakehouse with no logic changes, only re-pointing `BRONZE_GLOB` from a local path back to `Files/bronze/...`.

## Silver-layer backfill: chunked by location (Fabric Free Trial capacity)

**Problem:** running the bronze→silver notebook against the full backfill (11 years × 5 cities in one job) repeatedly hit `TooManyRequestsForCapacity` (HTTP 430) on the Fabric Free Trial capacity, even after clearing queued/running jobs via Monitoring hub — suggesting capacity-level throttling from cumulative usage, not just a single stuck job.

**Decision:** temporarily parameterize `BRONZE_GLOB` to target one `location_id` at a time (e.g. `Files/bronze/weather/cincinnati/*/*/*/data.json`) and run the notebook once per city, with a short pause between runs. This required no logic changes — the watermark table already tracks progress per `location_id`, so partial, city-by-city backfill runs are a natural fit rather than a workaround bolted on.

**Note:** this chunking is a Free Trial capacity accommodation, not a permanent pattern. Once the historical backfill is complete, `BRONZE_GLOB` reverts to the full wildcard (`*/*/*/*`) for ongoing daily incremental runs, which handle a small enough volume per run that chunking is unnecessary.

## Known limitation: cross-region shortcut (not fixed, documented instead)

The ADLS Gen2 storage account (`stweathermedallion`) lives in **East US** (chosen manually when creating the resource group), while the Fabric workspace/capacity is in **West US** — inherited automatically from the Free Trial's tenant-assigned capacity, which isn't a region you select manually the way you do for an Azure resource group.

**Impact:** the OneLake shortcut from `Files/bronze` to the storage account works correctly (Fabric supports cross-region shortcuts), but every read crosses regions, adding latency and, outside of trial capacity, would incur cross-region data transfer costs.

**Decision:** left as-is rather than recreating the storage account in West US to match, because fixing it would mean rebuilding the entire ADF pipeline (new storage account, new linked service, new shortcut) for a cost/latency issue that doesn't block functionality on a trial capacity. In a production scenario, this is exactly the kind of mismatch a data architect would flag and fix before go-live — co-locating compute and storage in the same region is a real best practice being consciously deviated from here for portfolio-timeline reasons, not overlooked.

## Operational constraint: Fabric Free Trial capacity

This project runs on the **Fabric Free Trial** capacity, which allows effectively one Spark job at a time — no queuing, no parallel execution. This surfaced as repeated `TooManyRequestsForCapacity` (HTTP 430) errors, usually caused by leftover sessions from a prior run or multiple browser tabs holding separate Spark sessions open simultaneously.

**Practical implications for this project:**
- Notebooks must be run one at a time, from a single browser tab, with prior sessions confirmed closed via Monitoring hub before starting a new run.
- Pipeline scheduling design (bronze → silver → gold) must be strictly sequential, never parallel, as long as this capacity tier is in use.
- If this becomes a recurring blocker, the fix is upgrading to a paid capacity SKU — not a code or configuration change.

## Fabric shortcut authentication: organizational account (not account key)

**Considered:** account key (matching the pattern used for ADF's ADLS linked service) vs. organizational account (Entra ID delegated identity) for the OneLake shortcut connecting `Files/bronze` to the ADLS Gen2 container.

**Decision:** organizational account. Reasons:
1. No static secret to manage, rotate, or leak — Fabric delegates to the caller's Azure AD identity instead.
2. This is Microsoft's recommended default pattern for OneLake shortcuts specifically.
3. Key Vault-backed authentication for Fabric connections is still preview-stage with documented gaps (consistent with what was found when evaluating Fabric Data Factory earlier in this project).
4. This project has a single developer running and validating each step — organizational account is the appropriate pattern here. A service principal (not account key) would be the right upgrade if this shortcut were consumed by an unattended production process instead.

**Note:** consistency with the ADF pattern (Key Vault) wasn't the goal — using the right authentication pattern per technology and context is, even when that means two different patterns in the same project.

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
