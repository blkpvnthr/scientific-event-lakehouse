# Operational Runbooks

Common failure scenarios and recovery procedures for the lakehouse. All recovery
paths rely on the pipeline's idempotency: raw archives are immutable by
convention, Bronze overwrites by partition after checksum verification, and Delta
merges are keyed on `(event_id, content_fingerprint)`, so reprocessing never
duplicates records.

## Observability signals

- **Structured logs** (`src/monitoring/logging.py`): one JSON object per event
  with run ids, stage, duration, record counts, retry count, failure category,
  and redacted error. Secrets are redacted before logging.
- **Metrics** (`src/monitoring/metrics.py`): per-stage throughput, durations,
  quarantine rate, totals.
- **Alerts** (`src/monitoring/alerts.py`): `LoggingAlerter` by default;
  swap in an SNS/Slack `Alerter` in production.

## Runbooks

- [Failed or interrupted run](failed_run.md)
- [Watermark did not advance / regressed](watermark.md)
- [Quarantine-rate spike](quarantine_spike.md)
