# Implementation Plan — Scientific Event Data Lakehouse

> Phase 0 audit output and the engineering plan for Milestones 3–12.
> Authored after a real repository + environment audit on 2026-11-22.
> This document is descriptive of **verified** state; forward-looking items are
> labelled **planned** and are not claimed as done.

## 1. Verified Baseline (Phase 0 audit)

All figures below were re-verified locally, not copied from the brief.

| Check | Command | Result |
|---|---|---|
| Git tree | `git status` | clean, branch `main` |
| Release tag | `git tag` | `v0.2.0` present |
| Python | `python --version` | 3.11.8 |
| Homebrew | `brew --version` | 7.0.14 |
| Docker | `docker --version` | 29.6.2 |
| **Java** | `java -version` | **absent — installing OpenJDK 17** |
| Hardware | `sysctl` | Apple Silicon arm64, 18 cores, 48 GiB RAM |
| Lint | `ruff check .` | All checks passed |
| Format | `ruff format --check .` | 20 files already formatted |
| Tests | `pytest -q` | **91 passed** in 0.34s |
| Raw archive | `find data -name '*.json'` | 347 files under run `3d8d9a2…` |
| Baseline profile | `profile_94c7423e…json` | 281,421 records, 0 dupes, 122 windows, 192.62 MiB, PASS |

## 2. Existing Capabilities (preserve, do not rebuild)

`src/ingestion/`
- `usgs_client.py` — FDSN query/count clients, exponential-backoff retry on
  `{429,500,502,503,504}`, `time-asc` paginated `fetch_earthquake_page`.
- `date_windows.py` — contiguous, non-overlapping UTC windows (inclusive start,
  exclusive end; per-window inclusive endpoints at ms resolution).
- `validation.py` — `validate_payload()` enforces the GeoJSON FeatureCollection
  contract without mutating the payload.
- `archive.py` — `calculate_checksum()` (SHA-256) and `atomic_write()` that
  publishes via temp-file + `os.link` so it **never overwrites** existing files.
- `manifest.py` — frozen `PageManifest` / `WindowManifest` dataclasses + writer.
- `backfill.py` / `ingest.py` — window extraction, reconciliation, `_SUCCESS.json`.

`src/profiling/profile_bronze.py`
- End-to-end integrity verifier: checksum + file-size + pagination + per-window
  and per-run reconciliation, statistical/geographic/quality profiling.

## 3. Repository Architecture (current)

```
src/ingestion/      raw extraction, archival, manifests, reconciliation
src/profiling/      integrity verification + statistical profiling
data/bronze/        RAW immutable GeoJSON landing zone (347 offset_*.json)
data/manifests/     window manifests + _SUCCESS.json completion markers
data/profiles/      immutable profiling reports
tests/              91 unit/integration tests
docs/architecture/  existing architecture notes
```

### Naming decision (important)
The existing `data/bronze/` directory holds **raw GeoJSON**, i.e. it is the
medallion *landing/raw* zone despite the name. Non-negotiable rule #2 forbids
modifying it. Therefore all new structured outputs use a **separate tree** and
the raw zone keeps its path for backward compatibility:

```
data/bronze/              (unchanged) raw GeoJSON landing zone
data/warehouse/bronze/    NEW structured Parquet (Milestone 3)
data/warehouse/silver/    NEW normalized Delta tables (Milestones 4–5)
data/warehouse/gold/      NEW analytical datasets (Milestone 6)
data/warehouse/metadata/  NEW pipeline_runs / watermarks (Milestone 5)
```

## 4. Dependency Matrix (selected)

| Component | Version | Rationale |
|---|---|---|
| Java | OpenJDK 17 (Homebrew `openjdk@17`) | Spark 3.5 supports 8/11/17; 17 is the modern LTS on arm64 |
| PySpark | 3.5.3 | Stable, arm64-friendly, Java 17 compatible |
| delta-spark | 3.2.1 | Officially paired with Spark 3.5.x |
| pytest / ruff | existing | unchanged |

Heavy Spark/Delta deps live in **`requirements-spark.txt`** so the core
ingestion install stays lightweight (rule #17). `requirements.txt` is unchanged.

## 5. Proposed Module Structure (additive)

```
src/config/        centralized settings (paths, env, storage backend)
src/spark/         SparkSession factory (+ Delta config)
src/schemas/       explicit PySpark schemas for USGS GeoJSON
src/bronze/        distributed raw→Parquet builder + reconciliation
src/silver/        canonical model, normalization, revision selection
src/quality/       configurable data-quality rule engine
src/metadata/      pipeline_runs, watermarks
src/gold/          dimensional model + aggregate builders
src/storage/       local/S3 storage abstraction (Milestone 7)
src/monitoring/    structured logging + metrics (Milestone 10)
src/recovery/      replay / failed-run detection (Milestone 10)
```
Existing modules are **not relocated**; imports stay stable.

## 6. Technical Risks

| Risk | Mitigation |
|---|---|
| Java/Spark env drift on arm64 | Pin JDK 17 + Spark 3.5.3; centralize JAVA_HOME in Makefile + Spark factory |
| Raw archive accidental mutation | All writers use append-only `atomic_write`; Bronze reads raw read-only; tests assert raw checksums unchanged |
| Reconciliation loss in Spark explode | Count-based reconciliation vs 281,421 baseline in every Bronze run |
| Delta single-writer on S3 | Document single-writer only; no multi-writer claims (Milestone 7) |
| Scope: 10 milestones | Milestone-by-milestone feature branches, each independently verified + committed |
| Non-determinism | Content fingerprints + deterministic revision ordering; no `Date.now`-style nondeterminism in outputs |

## 7. Milestone Acceptance Criteria (summary)

- **M3 Bronze**: 347 files + 281,421 records reconciled to Parquet; raw unchanged;
  idempotent re-run; Spark-SQL queryable; timestamps/coords preserved.
- **M4 Silver**: canonical model; valid + quarantine + superseded reconcile to
  Bronze input; configurable, independently-testable quality rules.
- **M5 Delta**: idempotent MERGE; current + revision history tables; schema
  evolution; time travel; out-of-order/late-arrival correctness.
- **M6 Gold**: 10 analytical datasets + dimensional model; reusable Spark SQL.
- **M7 S3**: storage abstraction; local works without creds; deterministic paths.
- **M8 Terraform**: `fmt/validate` pass; no `apply` without authorization.
- **M9 Airflow**: DAGs import; idempotent tasks; watermark-safe.
- **M10 Observability**: structured logs, metrics, replay, runbooks.
- **M11 CI/CD**: GitHub Actions; small deterministic datasets; scenario tests.
- **M12 Dashboard + Benchmarks**: Streamlit over real Gold; measured benchmarks.

## 8. Implementation Sequence

Phase 0 (this doc) → M3 → M4 → M5 → M6 → M7 → M8 → M9 → M10 → M11 → M12.
Each milestone: branch → implement → test → acceptance checks → full regression
→ docs/diagrams → review diff → commit → report. `docs/IMPLEMENTATION_STATUS.md`
is updated after every milestone for safe resume.

## 9. Existing Technical Debt / Observations

- `fetch_earthquakes()` in `usgs_client.py` is an unused convenience path
  (no retry); keep for compatibility, do not extend.
- `data/` is fully gitignored — generated datasets never enter git (correct).
- Profiling re-derives counts from manifests; Spark Bronze will reconcile
  independently from the raw files as a second, stronger check.
- No central config module yet — introduced in M3 (`src/config`).
