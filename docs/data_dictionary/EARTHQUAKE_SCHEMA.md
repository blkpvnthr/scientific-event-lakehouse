# Data Dictionary — Bronze Earthquake Events

Structured Bronze dataset produced by `src/bronze/build_bronze.py` from the raw
USGS GeoJSON landing zone. Stored as Parquet under `data/warehouse/bronze`,
partitioned by `event_year`.

Grain: **one row per archived event occurrence (feature)** within an ingestion
run. Logical key: `event_id`.

## Scientific event attributes

| Column | Type | Source | Notes |
|---|---|---|---|
| `event_id` | string | `feature.id` | USGS event identifier (logical key) |
| `event_time` | timestamp (UTC) | `properties.time` | Event origin time; epoch-ms converted |
| `updated_time` | timestamp (UTC) | `properties.updated` | Source last-update time; epoch-ms converted |
| `magnitude` | double | `properties.mag` | Reported magnitude |
| `magnitude_type` | string | `properties.magType` | e.g. `mb`, `ml`, `mww` |
| `place` | string | `properties.place` | Human-readable location description |
| `longitude` | double | `geometry.coordinates[0]` | Decimal degrees |
| `latitude` | double | `geometry.coordinates[1]` | Decimal degrees |
| `depth_km` | double | `geometry.coordinates[2]` | Kilometres; negative values are valid |
| `event_type` | string | `properties.type` | `earthquake` for this catalog |
| `network` | string | `properties.net` | Authoritative contributing network |
| `event_code` | string | `properties.code` | Network event code |
| `status` | string | `properties.status` | `reviewed` / `automatic` |
| `tsunami` | int | `properties.tsunami` | 1 if tsunami flag set |
| `significance` | int | `properties.sig` | USGS significance score |
| `felt` | int | `properties.felt` | Felt reports count (nullable) |
| `cdi` | double | `properties.cdi` | Community-reported intensity (nullable) |
| `mmi` | double | `properties.mmi` | Max instrumental intensity (nullable) |
| `alert` | string | `properties.alert` | PAGER alert level (nullable) |
| `location_source` | string | — | **Not in FDSN summary feed; always null (never fabricated)** |
| `magnitude_source` | string | — | **Not in FDSN summary feed; always null (never fabricated)** |
| `station_count` | int | `properties.nst` | Number of stations (nullable) |
| `azimuthal_gap` | double | `properties.gap` | Largest azimuthal gap, degrees (nullable) |
| `min_station_distance_deg` | double | `properties.dmin` | Nearest station distance, degrees (nullable) |
| `rms` | double | `properties.rms` | Travel-time residual RMS (nullable) |
| `event_url` | string | `properties.url` | USGS event page |

## Retained raw source attributes

| Column | Type | Source | Notes |
|---|---|---|---|
| `ids` | string | `properties.ids` | Pipe-delimited contributing event ids |
| `sources` | string | `properties.sources` | Pipe-delimited contributing sources |
| `types` | string | `properties.types` | Pipe-delimited product types |
| `title` | string | `properties.title` | Summary title |

## Provenance

| Column | Type | Source | Notes |
|---|---|---|---|
| `source_file` | string | manifest | Archive path of the originating page |
| `source_checksum` | string | manifest | SHA-256 of the raw page (verified pre-processing) |
| `source_api_url` | string | manifest | USGS query URL for the page |
| `ingestion_timestamp` | string | manifest | When the page was archived |
| `ingestion_run_id` | string | manifest | Ingestion run identifier |
| `extraction_window` | string | path | Window start date (YYYY-MM-DD) |
| `ingestion_date` | string | path | Ingestion date partition value |
| `page_offset` | int | path/manifest | Pagination offset of the source page |
| `processing_timestamp` | timestamp | Bronze build | When this Bronze record was produced |
| `event_year` | int (partition) | `year(event_time)` | UTC occurrence year; partition column |

## Time semantics

Four distinct times are preserved and must not be conflated:

- **Event occurrence** — `event_time` (when the earthquake happened).
- **Source revision** — `updated_time` (when USGS last revised the record).
- **Ingestion** — `ingestion_timestamp` (when we archived the raw page).
- **Processing** — `processing_timestamp` (when Bronze was built).

All timestamps are UTC. `event_year` is computed in Spark with session
timezone UTC, so partitioning reflects UTC occurrence year.
