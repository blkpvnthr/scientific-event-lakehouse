# Changelog

All notable changes to this project. Format based on
[Keep a Changelog](https://keepachangelog.com/); this project uses semantic
versioning. Dates are UTC.

## [1.0.0] - 2026-11-22

Verified complete reference implementation. Milestones 3–12 implemented, tested
on the real 281,421-record historical dataset, and released. Local acceptance:
156 tests passing (main venv) + 2 Airflow DAG tests (Airflow venv), 85% coverage,
OpenTofu validate, Docker image builds and runs.

### Added
- **Milestone 3 — Distributed PySpark Bronze**: explicit USGS schema, checksum
  gate, distributed parse/explode, provenance, `event_year` partitioning,
  idempotent overwrite, reconciliation (7.7× Parquet compression).
- **Milestone 4 — Silver + data quality**: canonical model, configurable rule
  engine (integrity/plausibility/completeness), deterministic revision selection,
  quarantine, reconciliation. Negative depths preserved.
- **Milestone 5 — Delta Lake**: ACID tables, idempotent fingerprint-keyed MERGE,
  newer-wins current state, time travel, schema evolution, advance-only watermark.
- **Milestone 6 — Gold analytics**: star schema (fact + 5 dims) + 10 datasets +
  10 reusable Spark SQL queries.
- **Milestone 7 — AWS S3**: storage abstraction (local + S3), AWS-native
  credentials, deterministic keys, mocked-S3 tests.
- **Milestone 8 — Terraform**: secure S3 + least-privilege IAM modules, validated
  with OpenTofu (not applied).
- **Milestone 9 — Airflow**: historical, incremental, and recovery/replay DAGs;
  idempotent tasks; watermark-safe; Docker image + compose.
- **Milestone 10 — Observability & recovery**: structured logging with redaction,
  metrics, alerting hooks, recovery helpers, runbooks.
- **Milestone 11 — CI/CD**: GitHub Actions (lint, tests, coverage, dependency +
  secret scans, OpenTofu validate, DAG import, Docker build); synthetic E2E test.
- **Milestone 12 — Dashboard & docs**: Streamlit dashboard over real Gold data,
  measured benchmarks, full architecture/portfolio documentation.

### Fixed (release audit)
- Dockerfiles: architecture-independent `JAVA_HOME` (was hardcoded arm64).
- Root image base pinned to `python:3.11-slim-bookworm` (trixie dropped openjdk-17).

### Notes
- Local-mode Spark (not a distributed cluster); single-writer Delta; raw archive
  immutable by convention (not object-lock); no live AWS deployment.

## [0.2.0] - prior

Historical USGS ingestion, atomic archival, manifests/provenance, and Bronze
profiling (Milestones 1–2).
