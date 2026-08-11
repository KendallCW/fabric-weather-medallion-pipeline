# ADF exports

JSON definitions for this project's Data Factory objects.

**Note on `linked_services/`:** these 3 files were manually reconstructed to match the deployed portal configuration (documented step-by-step in `docs/design-decisions.md`), not pulled via an automatic export from ADF Studio — no live export was taken at build time. `pipelines/` and `datasets/` *were* exported/edited directly as JSON through ADF Studio's code view, so those are exact.

- `ls_akv_weathermedallion` — Azure Key Vault, referenced by the storage linked service below for its account key
- `ls_http_openmeteo` — HTTP linked service pointed at the Open-Meteo archive API, anonymous auth (see `docs/design-decisions.md` for why HTTP+Binary was chosen over the native REST connector)
- `ls_adlsgen2_bronze` — ADLS Gen2 linked service for the bronze container, authenticating via a Key Vault-backed account key rather than a key pasted directly into the linked service
