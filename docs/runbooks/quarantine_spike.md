# Runbook: Quarantine-Rate Spike

## Symptoms
- `quarantine_rate_pct` (from `compute_pipeline_metrics`) rises above baseline
  (baseline is 0% for the verified historical dataset).
- `data_quality_results` shows a fatal rule with a non-zero failure count.

## Diagnose
1. Identify the failing rule(s):
   ```sql
   SELECT rule_id, severity, observed_failure_count, failure_percentage
   FROM data_quality_results
   WHERE run_id = '<run_id>' AND status = 'FAIL'
   ORDER BY observed_failure_count DESC;
   ```
2. Inspect quarantined records and their `failed_rules`:
   ```sql
   SELECT event_id, failed_rules, latitude, longitude, depth_km, magnitude
   FROM quarantined_events WHERE silver_run_id = '<run_id>' LIMIT 50;
   ```

## Decide
- **Genuine upstream data change** (e.g. USGS schema/field change): update the
  Bronze schema / extraction, do **not** weaken rules to force a pass.
- **A rule is too strict / scientifically wrong**: adjust the rule in
  `src/quality/rules.py` with a documented scientific justification (e.g. the
  negative-depth allowance) and add a regression test. Never silently drop valid
  science.
- **Transient corruption**: the Bronze checksum gate should have caught it; if a
  checksum mismatch occurred, treat as a failed run (see failed_run.md).

## Verify
- Re-run Silver; `quarantine_rate_pct` returns to the expected level.
- `valid + quarantined == bronze_input` still holds (reconciliation PASS).
