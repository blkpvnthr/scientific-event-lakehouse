# Testing

## How to run

```bash
make test                      # full suite (main .venv): unit + Spark + Delta
pytest --cov=src --cov-report=term-missing
PYTHONPATH=. .venv-airflow/bin/python -m pytest tests/test_dags.py   # DAG imports
```

The main suite needs Java 17 (Spark). Apache Airflow conflicts with the Spark
pinset, so DAG-import tests run in a separate `.venv-airflow` (skipped in the main
venv). CI runs both in separate jobs (`.github/workflows/ci.yml`).

## Test files

| File | Area |
|---|---|
| `test_usgs_client.py` | API client, retry/backoff, pagination |
| `test_validation.py` | GeoJSON contract validation |
| `test_date_windows.py` | window generation |
| `test_archive.py` | atomic, non-overwriting archival + checksums |
| `test_manifest.py` | manifest serialization |
| `test_backfill.py` | ingestion + reconciliation |
| `test_profile_bronze.py`, `test_profile_integration.py` | profiling + integrity |
| `test_spark_bronze_support.py` | Spark session + USGS schema |
| `test_bronze.py` | distributed Bronze build |
| `test_quality.py` | data-quality rule engine |
| `test_silver.py` | Silver normalization, quarantine, revisions |
| `test_delta.py` | Delta MERGE semantics, time travel, schema evolution |
| `test_gold.py` | Gold datasets + analytics SQL |
| `test_storage.py` | local + mocked S3 storage |
| `test_orchestration.py` | orchestration callables |
| `test_dags.py` | Airflow DAG imports (airflow venv) |
| `test_monitoring.py` | logging/redaction, metrics, alerts |
| `test_recovery.py` | failed-run detection, watermark protection |
| `test_end_to_end.py` | synthetic Bronze→Silver→Delta→Gold |

## Required-scenario coverage

| Scenario | Covered by |
|---|---|
| Empty source response | `test_backfill.py`, `test_validation.py` |
| API timeout / retryable HTTP failures | `test_usgs_client.py` |
| Pagination inconsistency | `test_usgs_client.py`, `test_validation.py` |
| Checksum corruption | `test_bronze.py::test_verify_checksums_detects_corruption` |
| Missing archive | `test_bronze.py::test_verify_checksums_detects_missing` |
| Missing manifest | `test_bronze.py::test_discover_rejects_incomplete_run` |
| Duplicate / cross-window duplicate event IDs | `test_profile_integration.py`, `test_silver.py` |
| Missing optional scientific fields | `test_quality.py`, `test_silver.py` |
| Invalid coordinates | `test_quality.py`, `test_silver.py` |
| Late-arriving / out-of-order revisions | `test_delta.py` |
| Schema evolution | `test_delta.py::test_schema_evolution_additive` |
| Reprocessing an existing run | `test_bronze.py`, `test_silver.py`, `test_end_to_end.py` |
| Transactional recovery | `test_delta.py::test_transactional_atomicity_on_failure` |
| Watermark protection | `test_recovery.py`, `test_delta.py::test_watermark_advance_only` |
| Local-to-cloud storage compatibility | `test_storage.py` (local + mocked S3) |

## Coverage

Coverage is reported in CI (`--cov=src`). No arbitrary numeric gate is enforced:
the goal is meaningful behavioral coverage of each stage's correctness and
reconciliation, not a vanity percentage. Current measured result: **153 tests
passing, 1 skipped** (the Airflow DAG test, which runs in its own CI job), with
**85% line coverage** of `src/`.
