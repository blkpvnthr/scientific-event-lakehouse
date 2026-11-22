# Benchmarks

All figures are **measured**, not estimated. Local Spark runs in `local[*]`
mode on a single machine; this is **not** representative of distributed cluster
performance and must not be presented as cloud-scale throughput.

## Environment

| Property | Value |
|---|---|
| Hardware | Apple Silicon arm64, 18 cores, 48 GiB RAM |
| OS | Darwin 27.0.0 (macOS) |
| Python | 3.11.8 |
| Java | OpenJDK 17.0.21.1 |
| Spark | 3.5.3 (`local[*]`) |
| delta-spark | 3.2.1 |
| Dataset | run `3d8d9a251f33489e807356890c692789`, 281,421 records, 347 files |

## Milestone 2 — Historical ingestion & profiling (baseline, from profile)

| Stage | Duration | Notes |
|---|---:|---|
| Historical extraction (10 yrs) | 226.76 s | Network-bound, 122 windows |
| Local Bronze profiling | 2.087 s | Pure-Python single process |

## Milestone 3 — Distributed Bronze (Parquet)

Command: `make benchmark` (`scripts/benchmark_bronze.py`, 3 repetitions,
warm JVM; the first repetition includes JIT warmup).

| Metric | Value |
|---|---:|
| Repetition durations | 8.38 s, 3.44 s, 2.56 s |
| Min duration (warm) | 2.56 s |
| Mean duration | 4.79 s |
| Records processed | 281,421 |
| Output size (Parquet, snappy) | ~24.9–25.1 MiB |
| Raw logical payload | 192.62 MiB (201,979,748 bytes) |
| Compression ratio vs raw | ~7.7x (measured 7.68–7.74 across runs) |
| Throughput (min duration) | ~110,000 records/s |
| Duplicate events | 0 |
| Integrity status | PASS |

The compression ratio varies slightly run-to-run because `repartition` produces
marginally different per-file Parquet layouts; independently re-verified from
actual file sizes: 201,979,748 / 26,081,745 bytes = 7.74x.

## Milestones 4–6 — Silver / Delta / Gold (full dataset)

Measured on the full 281,421-record dataset (warm JVM, single measurement;
includes read + compute + write + reconciliation):

| Stage | Duration | Output |
|---|---:|---|
| Silver normalization + quality | 11.15 s | 281,421 valid / 0 quarantined |
| Delta MERGE (into existing tables) | 11.37 s | 281,421 current / 281,421 revisions |
| Gold (fact + 5 dims + 10 datasets) | 6.13 s | fact 281,421; 16 datasets |

Delta MERGE time reflects merging 281k rows against already-populated tables
(the most expensive case); a first-time create is faster. These are single warm
measurements intended as order-of-magnitude references, not statistical means.

## Synthetic end-to-end (CI)

`tests/test_end_to_end.py` runs Bronze→Silver→Delta→Gold plus an idempotent
replay on a 3-record synthetic run in ~20–25 s wall clock (dominated by repeated
SparkSession/Delta operations), with no network or credentials.

### Measurement notes & limitations

- `duration_seconds` is measured inside `build_bronze` and covers checksum
  verification, distributed parse/explode/extract, Parquet write, and the
  post-write reconciliation read. It excludes SparkSession startup (~3–4 s).
- The first repetition is slower due to JVM/JIT warmup; the warm minimum is the
  more representative steady-state figure.
- Output size depends on Parquet/snappy encoding; it is deterministic for this
  dataset but will vary with schema or codec changes.
- Local-mode throughput reflects a single machine. Real cluster performance
  depends on executor count, shuffle, and object-store latency and is **not**
  claimed here.
