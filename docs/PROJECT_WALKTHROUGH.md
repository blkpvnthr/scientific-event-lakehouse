# Project Walkthrough

A guided tour for explaining the entire system end to end.

## 1. Problem statement

Build a reproducible, production-oriented lakehouse over USGS earthquake data that
guarantees scientific data integrity: every derived record is traceable to an
immutable raw source, reconciled by count, and reprocessable without duplication.

## 2. Dataset

- Source: USGS FDSN event API (GeoJSON).
- Scope: 2015-01-01 to 2025-01-01 (exclusive), magnitude ≥ 2.5.
- Volume: **281,421 events**, 347 archived pages, 122 windows, 192.62 MiB raw.
- Known characteristics: 1,777 valid negative depths; mixed magnitude types;
  `status` case inconsistencies; one missing `magType`.

## 3. Extraction & integrity (M1–2)

Paginated extraction with exponential-backoff retry; atomic, non-overwriting
archival; SHA-256 per page; window + run manifests; before/after count
reconciliation; independent profiling. The raw archive is immutable by convention.

## 4. Distributed Bronze (M3)

PySpark reads the archive with an **explicit** GeoJSON schema (no inference, no
fabricated fields), verifies every checksum on the driver first, explodes features
to event rows, attaches provenance, converts to UTC, and writes Parquet
partitioned by `event_year`. Reconciles to 281,421 / 347; idempotent overwrite;
7.68× compression.

## 5. Silver + data quality (M4)

Canonical model with normalized categoricals (originals retained), a content
fingerprint, and a configurable rule engine (integrity / plausibility /
completeness; fatal vs warning). Negative depths are preserved — only physically
impossible values quarantine. Valid + quarantined reconcile to Bronze input.

## 6. Delta Lake (M5)

ACID tables: `earthquake_events_current` (upsert, newer `updated_time` wins),
`earthquake_event_revisions` (insert-only on `event_id`+fingerprint → idempotent),
plus quarantine, quality, `pipeline_runs`, and advance-only `processing_watermarks`.
Time travel, schema evolution, and transactional atomicity are all verified.

## 7. Gold analytics (M6)

Star schema (`fact_earthquake_event` + 5 dims) and 10 analytical datasets, plus 10
reusable Spark SQL queries (CTEs, window functions, ranking, conditional
aggregation). All reconcile to the current-events table.

## 8. Cloud, orchestration, ops (M7–11)

Storage abstraction (local / S3, mock-tested); Terraform (secure S3 + least-priv
IAM, validated, not applied); Airflow DAGs (idempotent, watermark-safe, import-
verified); structured logging/metrics/alerts + recovery helpers + runbooks;
GitHub Actions CI (lint, tests, coverage, scans, tofu validate, DAG import, docker).

## 9. Dashboard (M12)

Streamlit app reading the **actual** Gold datasets (no hardcoded numbers), with
magnitude/depth/network/event-type filters, trend/distribution/geographic/quality
views.

## 10. What is and isn't claimed

- Verified locally: the full pipeline on the real 281,421-record dataset, 153
  tests, 85% coverage.
- Not claimed: live AWS deployment, cloud-scale/cluster performance,
  storage-enforced immutability, or multi-writer Delta — each is explicitly gated
  on authorization/configuration.

## 11. How to run

See [LOCAL_DEVELOPMENT.md](LOCAL_DEVELOPMENT.md): `make setup && make pipeline`,
then `streamlit run dashboard/app.py`.
