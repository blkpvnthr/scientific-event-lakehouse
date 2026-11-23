# Runbook: Watermark Did Not Advance / Regressed

## Background
The watermark (`metadata.processing_watermarks`, name `silver_max_updated_time`)
advances **only** in a dedicated final `commit_watermark` step — which runs after
the data-quality gate, the Delta merge, Gold, and reconciliation all succeed — and
only when the incoming value is strictly greater. The Delta merge itself does not
advance the watermark. This is by design: a fatal quality failure, a Gold failure,
a reconciliation mismatch, or any upstream failure must not advance it, and an
out-of-order/replay run must not move it backwards.

## Symptoms
- Watermark unchanged after a run.
- Concern that a replay moved the watermark backwards.

## Diagnose
```python
from src.recovery import watermark_would_regress

watermark_would_regress(current, incoming)  # True -> correctly blocked
```
- Check whether the Delta merge stage actually succeeded (logs + Delta history).
- Read the current watermark:
  `src.delta_lake.metadata.read_watermark(spark, path, "silver_max_updated_time")`.

## Resolve
- **Did not advance after a failed run**: expected. Fix the failing stage and
  re-run; the watermark advances once the merge succeeds.
- **Did not advance after a successful run**: the incoming `max(updated_time)` was
  not greater than the stored value (e.g. a replay of already-processed data).
  This is expected and safe.
- **Appears to have regressed**: not possible via the pipeline — the advance-only
  MERGE guards it. Investigate manual writes to the metadata table.

## Verify
- `watermark_would_regress(current, incoming)` returns False for the next run's
  value before committing.
