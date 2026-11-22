# Changelog

All notable changes to this project. Format based on
[Keep a Changelog](https://keepachangelog.com/); this project uses semantic
versioning. Dates are UTC.

## [1.0.1] - 2026-11-22

Corrective patch found during the final release-verification pass.

### Fixed
- **Watermark protection (correctness):** both the historical and incremental
  Airflow DAGs ran the Delta merge (which advances the processing watermark)
  *before* the data-quality gate, so a fatal quality failure could not prevent
  watermark advancement. Reordered both DAGs to `… → Silver → quality_gate →
  merge_delta → Gold`, and `quality_gate` now reads the Silver Parquet
  `data_quality_results` so it can run before the merge. Added regression tests
  (gate pass/fail; DAG asserts `quality_gate` precedes `merge_delta`).

### Changed
- **Documentation:** README modernized for v1.0 (badges, executive summary,
  expanded ToC, complete-architecture diagram, implementation-aware enterprise
  and medallion diagrams with Gold shown as Parquet, corrected technology stack
  and repository tree, completed roadmap, replaced the obsolete "Upcoming
  Milestone 3" section with the completed end-to-end flow, added Dashboard,
  Releases, and Technical Documentation sections). All 24 Mermaid diagrams
  preserved and validated with mermaid-cli v11.

### Verified
- 158 tests passing / 1 skipped (main venv) + 3 Airflow DAG tests, 86% coverage.

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
