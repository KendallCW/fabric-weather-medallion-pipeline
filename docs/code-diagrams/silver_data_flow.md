# Data flow: bronze → silver transformation

Unlike a call graph (see [`silver_gold_call_graph.md`](silver_gold_call_graph.md), which came back empty because this script is procedural, not function-based), this diagram traces how the **DataFrame itself** changes shape at each step — variable name by variable name, matching `local/local_bronze_to_silver.py` exactly.

- **Gray** — read the raw bronze JSON
- **Teal** — the `arrays_zip` + `explode` pair that turns parallel hourly arrays into one row per hour (the step that took the most work to build intuition for)
- **Coral** — cleanup: typing, validation, deduplication, and adding the merge timestamp
- **Purple** — the final `MERGE INTO` upsert into `silver.weather_observations`

```mermaid
flowchart TD
    A["raw_df<br/>Read bronze JSON"]
    B["zipped_df<br/>Zip hourly arrays together"]
    C["exploded_df<br/>One row per hour now"]
    D["typed_df<br/>Cast types, rename columns"]
    E["validated_df<br/>Flag out-of-range values"]
    F["deduped_df<br/>Drop duplicate hours"]
    G["silver_updates_df<br/>Add merge timestamp"]
    H["MERGE INTO<br/>Upsert into silver table"]

    A --> B --> C --> D --> E --> F --> G --> H

    classDef gray fill:#F1EFE8,stroke:#5F5E5A,color:#2C2C2A;
    classDef teal fill:#E1F5EE,stroke:#0F6E56,color:#04342C;
    classDef coral fill:#FAECE7,stroke:#993C1D,color:#4A1B0C;
    classDef purple fill:#EEEDFE,stroke:#534AB7,color:#26215C;

    class A gray
    class B,C teal
    class D,E,F,G coral
    class H purple
```
