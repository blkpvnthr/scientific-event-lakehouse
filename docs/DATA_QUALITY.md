# Data Quality Framework

The Silver layer (`src/silver`, `src/quality`) enforces a configurable,
independently-testable set of data-quality rules and separates valid from
quarantined records without discarding scientifically legitimate values.

## Rule model

Each rule (`src/quality/rules.py`) is a *pass condition* expressed as a Spark SQL
boolean expression, classified by:

- **Category** — `integrity`, `plausibility`, or `completeness`.
- **Severity** — `fatal` (failing records are quarantined) or `warning`
  (observed only; the record remains valid).

This separation is deliberate: mandatory integrity and scientific plausibility
are enforced, while optional-field completeness is merely observed so that valid
records are never dropped for missing optional attributes.

## Default rules

| Rule | Category | Severity | Pass condition (summary) |
|---|---|---|---|
| `id_present` | integrity | fatal | `event_id` non-empty |
| `event_time_present` | integrity | fatal | `event_time` not null |
| `coordinates_present` | integrity | fatal | lat & lon not null |
| `coordinates_in_range` | integrity | fatal | lat∈[-90,90], lon∈[-180,180] |
| `magnitude_present` | integrity | fatal | `magnitude` not null |
| `event_type_present` | integrity | fatal | `event_type` non-empty |
| `magnitude_plausible` | plausibility | fatal | magnitude ∈ [-1, 12] |
| `depth_plausible` | plausibility | fatal | depth ∈ [-10, 1000] km (null ok) |
| `revision_not_before_event` | plausibility | fatal | `updated_time` ≥ `event_time` |
| `magnitude_type_present` | completeness | warning | `magnitude_type` non-empty |
| `place_present` | completeness | warning | `place` non-empty |

### Scientific justification for thresholds

- **Negative depths are valid.** Earthquakes above the reference datum report
  negative depth; the baseline contains 1,777 such records. The `depth_plausible`
  lower bound (-10 km) only rejects physically impossible values, never the
  genuine negatives.
- **Magnitude bounds** allow a generous margin around the observed catalog
  (observed 0.7–8.3) to catch encoding errors without rejecting real events.

## Quarantine

Records failing any **fatal** rule are written to `quarantined_events` with the
original event id, source provenance (file + checksum + run), the key scientific
fields for reconstruction, the list of `failed_rules`, the pipeline run id, and a
processing timestamp. No record is silently dropped.

## Reconciliation

Every Silver build asserts:

```
valid_records + quarantined_records == bronze_input_records
current_records + superseded_records == valid_records
```

## Reproducibility

`data_quality_results` persists, per rule: category, severity, description,
scope, `observed_failure_count`, `failure_percentage`, `status` (PASS/FAIL),
`run_id`, and `executed_at`. Re-running over identical input yields identical
counts.

## Verified baseline result (run `3d8d9a25…`)

| Metric | Value |
|---|---:|
| Input (Bronze) records | 281,421 |
| Valid records | 281,421 |
| Quarantined records | 0 |
| Current events | 281,421 |
| Superseded revisions | 0 |
| Fatal rule failures | 0 |
| Completeness warnings | 1 (one missing `magType`) |
| Status normalization | `REVIEWED`+`reviewed` → `reviewed` (279,381) |
| Reconciliation | PASS |
