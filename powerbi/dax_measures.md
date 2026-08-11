# Power BI — DAX Measures

The 7 measures behind the dashboard, all in a dedicated `Measure` table (kept separate from the data tables — standard Power BI practice). Built and validated via an MCP server connected directly to Power BI Desktop, not typed manually into the UI.

| Measure | DAX | Format | Description |
|---|---|---|---|
| **Avg Temperature** | `AVERAGE(fact_weather_daily[avg_temperature_c])` | `0.0"°C"` | Average daily temperature across the filtered context |
| **Avg Humidity** | `AVERAGE(fact_weather_daily[avg_humidity_pct])` | `0.0` | Average daily humidity percentage |
| **Avg Wind Speed** | `AVERAGE(fact_weather_daily[avg_wind_speed_kmh])` | `0.0` | Average daily wind speed in km/h |
| **Avg Comfort Index** | `AVERAGE(fact_weather_daily[comfort_index])` | `0.0` | Average comfort index (project heuristic combining temperature, humidity, and wind) |
| **Avg Anomaly** | `AVERAGE(fact_weather_daily[anomaly_vs_historical_avg])` | `0.0;-0.0` | Average deviation from each day's historical baseline (prior years only — see `docs/design-decisions.md` for the lookahead-bias fix behind this) |
| **Max Streak** | `MAX(fact_weather_daily[streak_days_above_threshold])` | `0" days"` | Longest consecutive run of days above each location's own heat threshold (mean + 1 standard deviation) |
| **Days Tracked** | `COUNTROWS(fact_weather_daily)` | `#,##0` | Count of daily rows in the current filter context — a data-coverage check as much as a metric |

## What the numbers actually show

Validated with a direct DAX query (`SUMMARIZECOLUMNS` by city) before any visuals were built:

| City | Avg Temp | Avg Comfort | Avg Anomaly | Max Streak | Days |
|---|---|---|---|---|---|
| Chicago | 10.5°C | 8.8 | +0.55 | 38 days | 4,018 |
| Cincinnati | 13.2°C | 12.5 | +0.05 | 44 days | 4,018 |
| Dubai | 28.1°C | 28.3 | -0.10 | 59 days | 4,018 |
| Reykjavik | 4.8°C | 2.1 | +0.13 | 56 days | 4,018 |
| Singapore | 26.7°C | 27.4 | +0.01 | 18 days | 4,018 |

Every city lands on exactly 4,018 days — the same count validated in silver and gold — confirming nothing was lost or duplicated on the way into the Power BI model.
