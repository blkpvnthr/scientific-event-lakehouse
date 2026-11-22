# Interview Preparation

Technical Q&A grounded in the actual implementation, plus evidence-backed resume
bullets. Answers reflect what was built and measured — not aspirations.

## Technical questions & answers

**Q: Why an explicit PySpark schema instead of inference?**
Inference needs an extra data pass, can change column types between runs depending
on sampled values, and silently coerces malformed data. An explicit schema is a
contract: it documents exactly which fields the USGS summary feed provides and
makes unexpected payloads visible. Fields absent from the feed (location/magnitude
source) are carried as documented nulls, never fabricated.

**Q: How do you guarantee no record loss or duplication?**
Count reconciliation at every layer: raw (before==after==unique), Bronze (output ==
manifest totals, 281,421 / 347), Silver (valid+quarantined==input), Delta (replay
adds 0 revisions). Any drift raises. Verified on the real dataset.

**Q: How does revision handling work, and how is it deterministic?**
Each observation gets a SHA-256 content fingerprint. The revisions table is
insert-only on `(event_id, fingerprint)` — replaying identical data inserts
nothing. The current table upserts on `event_id`, updating only when the incoming
`updated_time` is greater, so newer revisions win and a late-arriving older
revision never regresses current state. Ties break on the fingerprint.

**Q: Why partition Bronze by `event_year`?**
Low cardinality (10 values for 2015–2024), aligns with common time-range queries,
and avoids the tiny-file / high-cardinality problems of partitioning by event id.

**Q: How do you keep the watermark correct?**
It advances only after a successful Delta merge and only if the new
`max(updated_time)` exceeds the stored value (advance-only MERGE). Failed runs and
replays can't move it backwards; `max_active_runs=1` serializes incremental runs.

**Q: Negative depths — why keep them?**
They're scientifically valid (events above the reference datum); the baseline has
1,777. The plausibility rule only rejects physically impossible depths, with a
documented bound.

**Q: Idempotency — concretely?**
Bronze overwrites partitions after a checksum gate; Delta merges are
fingerprint-keyed. The end-to-end test replays the whole chain and asserts
unchanged counts and zero new revisions.

**Q: What are the honest limitations?**
No live AWS deployment; local-mode Spark performance isn't cluster performance;
single-writer Delta only; immutability is by convention until object-lock is
configured. All stated explicitly in the docs.

## Evidence-backed resume bullets

- Built a reproducible PySpark + Delta Lake medallion lakehouse over 281,421 USGS
  earthquake events with count reconciliation at every layer and 0 duplicates.
- Implemented idempotent, fingerprint-keyed Delta MERGE with deterministic
  revision conflict resolution, time travel, schema evolution, and advance-only
  watermarks.
- Achieved 7.68× Parquet compression and ~110k records/s local Bronze throughput;
  measured every stage (Silver 11s, Delta 11s, Gold 6s on the full dataset).
- Authored a configurable data-quality rule engine (integrity/plausibility/
  completeness) with quarantine + reconciliation, preserving scientifically valid
  edge cases (negative depths).
- Delivered Airflow DAGs, Terraform (secure S3 + least-privilege IAM, validated),
  a local/S3 storage abstraction, observability + recovery runbooks, GitHub
  Actions CI (153 tests, 85% coverage), and a Streamlit dashboard over real Gold
  data.

> This is a personal/portfolio reference implementation, not professional
> employment or enterprise production experience.
