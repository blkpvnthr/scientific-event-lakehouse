# v1.0.0 — Scientific Event Data Lakehouse

A production-oriented **reference implementation** of a distributed scientific
data lakehouse over the USGS earthquake catalog, built on PySpark and Delta Lake
with end-to-end data-integrity guarantees.

## Architecture overview

Raw GeoJSON (immutable, checksummed) → **Bronze** (distributed Parquet) →
**Silver** (canonical model + data quality + quarantine) → **Delta** (ACID
current + observed revisions + metadata) → **Gold** (star schema + 10 analytical
datasets) → **Streamlit dashboard**. Orchestrated by Airflow; storage abstracted
over local/S3; infrastructure as Terraform; observability + recovery + CI/CD.

## Verified capabilities

- Distributed Bronze with checksum-gated, reconciled, idempotent processing.
- Configurable data-quality framework (integrity/plausibility/completeness) with
  quarantine and full reconciliation; scientifically valid edge cases preserved.
- Idempotent, fingerprint-keyed Delta MERGE with deterministic revision
  resolution, time travel, schema evolution, and advance-only watermarks.
- Gold dimensional model + 10 reusable Spark SQL analytics.
- Local/S3 storage abstraction (AWS-native credentials), validated Terraform,
  import-verified Airflow DAGs, structured observability, and GitHub Actions CI.

## Historical dataset (verified)

| Metric | Value |
|---|---:|
| Coverage | 2015-01-01 → 2025-01-01 (exclusive), magnitude ≥ 2.5 |
| Archived files / windows | 347 / 122 |
| Records / unique events / duplicates | 281,421 / 281,421 / 0 |
| Raw payload | 201,979,748 bytes (192.62 MiB) |
| Run ID | `3d8d9a251f33489e807356890c692789` |

## Test results

- Main venv: **156 passed, 1 skipped**, 85% line coverage of `src/`.
- Airflow venv: **2 DAG tests passed** (the main-venv-skipped test).
- OpenTofu `fmt`/`init`/`validate`: pass. Docker image builds and runs.

## Measured performance (local-mode Spark; not cluster performance)

| Stage | Duration | Output |
|---|---:|---|
| Bronze | ~2.6 s (warm) | 281,421 records, ~7.7× Parquet compression |
| Silver | 11.15 s | 281,421 valid / 0 quarantined |
| Delta MERGE | 11.37 s | 281,421 current / 281,421 revisions |
| Gold | 6.13 s | fact 281,421 / 16 datasets |

## Deployment / quickstart

```bash
make setup && make pipeline     # bronze→silver→delta→gold on the historical run
make dashboard                  # Streamlit over real Gold data
```

See `docs/LOCAL_DEVELOPMENT.md`, `docs/DEPLOYMENT.md`, and `docs/runbooks/`.

## Known limitations

- Local-mode Spark; figures are not distributed-cluster benchmarks.
- Single-writer Delta on object storage; no multi-writer guarantees.
- Raw archive immutable **by convention** (append-only), not S3 object-lock.
- No live AWS deployment; Terraform is validated but **not applied**.
- Analytics are descriptive summaries of a magnitude-filtered catalog, **not**
  earthquake prediction.

## Cloud deployment status

Authored and validated, **awaiting authorization**: `terraform apply`, S3 bucket
provisioning, and running the pipeline against S3. No paid resources created.
