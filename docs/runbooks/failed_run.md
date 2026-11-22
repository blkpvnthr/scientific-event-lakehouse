# Runbook: Failed or Interrupted Run

## Symptoms
- An Airflow DAG task failed; downstream tables may be partially updated.
- A pipeline process was killed mid-run.

## Diagnose
```python
from src.recovery import detect_incomplete_run, is_bronze_output_present

detect_incomplete_run("<run_id>")  # True -> ingestion never completed
is_bronze_output_present("<run_id>")  # False -> Bronze did not finish
```
- Check structured logs for the failing `stage` and `failure_category`.
- Inspect the Delta transaction history: `DeltaTable.forPath(...).history()`.

## Recover
1. If **ingestion** is incomplete (no valid `_SUCCESS.json`): re-run the backfill
   for the interval. Archival is append-only and reconciled, so partial pages are
   safe to re-fetch.
2. If ingestion completed but **processing** failed: replay the run.
   ```bash
   # Airflow:
   airflow dags trigger recovery_replay --conf '{"run_id": "<run_id>"}'
   # or locally:
   make bronze silver delta gold RUN_ID=<run_id>
   ```
   Each stage is idempotent — replay reconciles to the same record counts and
   creates no duplicate revisions (verified: MERGE updates/inserts 0 on replay).

## Verify
- Bronze reconciliation report shows `status = PASS`.
- `earthquake_events_current` row count equals the expected unique events.
- Delta history shows the merge committed; no partial writes (writes are atomic).
