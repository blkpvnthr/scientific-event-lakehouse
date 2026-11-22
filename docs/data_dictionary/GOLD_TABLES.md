# Data Dictionary — Gold Datasets

All datasets are Parquet under `data/warehouse/gold/<name>`, built by
`src/gold/build_gold.py` from the Silver `earthquake_events_current` Delta table.

## Dimensional model

| Dataset | Grain | Key(s) | Notes |
|---|---|---|---|
| `fact_earthquake_event` | current event | `event_id` | measures + dimension FKs |
| `dim_date` | calendar day | `date_key` | year/quarter/month/day/dow/week |
| `dim_magnitude_type` | magnitude type | `magnitude_type_key` | normalized magType |
| `dim_event_type` | event type | `event_type_key` | normalized type |
| `dim_source_network` | network | `network_key` | reporting network |
| `dim_geographic_region` | 10° grid cell | `region_key` | `lat_cell`, `lon_cell` |

## Analytical datasets

| Dataset | Grain | Aggregation rules |
|---|---|---|
| `daily_seismic_activity` | day | count, avg/min/max magnitude, avg depth, max sig, tsunami count |
| `monthly_seismic_activity` | year+month | same measures |
| `annual_seismic_activity` | year | measures + `prev_year_count`, `yoy_change_pct` (lag window) |
| `geographic_seismic_activity` | region cell | measures + avg lat/lon |
| `magnitude_distribution` | magnitude bucket | event count per bucket |
| `depth_distribution` | depth category | count + avg magnitude per category |
| `significant_events` | event | filter M≥6.0 OR significance≥600 |
| `seismic_hotspots` | region cell | count + rank (ranking window) |
| `source_network_metrics` | network | count, avg magnitude, share % |
| `data_quality_trends` | run×severity | failure counts from `data_quality_results` |

### Documented thresholds

- **Magnitude buckets**: `below_2.5`, `2.5_to_3.0`, `3.0_to_4.0`, `4.0_to_5.0`,
  `5.0_to_6.0`, `6.0_to_7.0`, `7.0_and_above`.
- **Depth categories**: `above_datum` (<0 km, valid), `shallow` (<70),
  `intermediate` (70–300), `deep` (≥300) — USGS convention.
- **Significant events**: magnitude ≥ 6.0 OR significance ≥ 600 (configurable).

## Reusable analytics SQL

`sql/analytics/*.sql` contains ten portable Spark SQL queries operating on the
registered views `events_current`, `event_revisions`, and `data_quality_results`
(see `src/gold/queries.py`): annual activity, monthly counts, geographic
concentration, high-magnitude identification, rolling 30-day activity,
year-over-year change, depth/magnitude relationship, source-network comparison,
missing attributes, and observed revision frequency. They demonstrate CTEs,
window functions (lag, rank, rolling range), ranking, and conditional
aggregation.

## Verified baseline (run `3d8d9a25…`)

Built from 281,421 current events: `fact_earthquake_event`=281,421 (reconciled),
`dim_date`=3,653, `annual`=10, `monthly`=120, `significant_events`=1,861,
`geographic`/`hotspots`=435 cells, `source_network_metrics`=20.
