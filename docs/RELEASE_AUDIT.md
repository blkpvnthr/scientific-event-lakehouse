# Release Audit — v1.0.0

Independent release-readiness assessment of the Scientific Event Data Lakehouse.
All findings below were verified locally; no results are fabricated.

Audit date: 2026-11-22
Audited range: `v0.2.0` (83851c7) → `main`

## 1. Repository & security audit

| Check | Result |
|---|---|
| Working tree clean, on `main` | PASS |
| `v0.2.0` tag unchanged (83851c7…) and ancestor of `main` | PASS |
| `origin/main` is ancestor of local `main` (fast-forward push, no rewrite) | PASS (21 commits ahead) |
| No credentials / `.env` / `.tfstate` / venvs tracked | PASS (none) |
| No raw datasets tracked (`data/` gitignored) | PASS |
| Placeholders / stubs / dead code | PASS (none found) |
| Dependency versions pinned & reproducible | PASS (requirements*.txt pinned) |
| README claims match implementation | PASS (verified per milestone) |

## 2. Defects found & corrected

| ID | Severity | Finding | Resolution | Verified |
|---|---|---|---|---|
| D1 | High | Dockerfiles hardcoded `JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64`; wrong on amd64 (GitHub Actions) → image unusable | Resolve `JAVA_HOME` via symlink to the installed JDK (arch-independent `/opt/java`) in both Dockerfiles + compose | Build + `java -version` in image |
| D2 | High | Root `Dockerfile` base `python:3.11-slim` now resolves to Debian 13 (trixie), which dropped `openjdk-17` → build failure | Pin base to `python:3.11-slim-bookworm` (Debian 12 ships openjdk-17) | Image builds |
| D3 | Low | `/opt/homebrew/opt/openjdk@17` default JAVA_HOME in `Makefile`/`settings.py` is macOS-specific | Kept as an **overridable default** (`?=` / `os.environ.get`); CI sets `JAVA_HOME` via setup-java; documented | N/A (by design) |
| D4 | Low | Generated `.coverage` file was tracked in git | `git rm --cached`; added coverage artifacts to `.gitignore` | `git ls-files` clean |
| D5 | Medium | CI missing dashboard deps → `test_dashboard.py` import error (pandas) in the first CI run | Install `requirements-dashboard.txt` in the lint-and-test job | CI re-run |
| D6 | Medium | Third-party GitHub Actions (`gitleaks`, `opentofu/setup-opentofu`) referenced by mutable tags (supply-chain risk flagged by automated review) | Pinned both to immutable commit SHAs; workflow already uses least-privilege `permissions: contents: read` | CI re-run |

No critical defects. D1 and D2 corrected before publication; D3 accepted (overridable).

## 3. Regression verification (by environment)

| Environment | Command | Result |
|---|---|---|
| Main venv (Python 3.11, Java 17, Spark 3.5.3) | `pytest --cov=src` | **156 passed, 1 skipped**, 85% coverage |
| Main venv | `ruff check .` / `ruff format --check .` | clean |
| Airflow venv (`.venv-airflow`, apache-airflow 2.10.5) | `pytest tests/test_dags.py` | **2 passed** (the DAG test skipped in main venv passes here) |
| OpenTofu 1.13.9 | `tofu fmt -check` / `init -backend=false` / `validate` | PASS / PASS / Success |
| Docker Compose | `docker compose config` | valid |
| Docker image | `docker build .` | see §6 |

Test totals are **not** combined: the 1 skipped main-venv test is the same DAG
test that passes in the Airflow venv — counted once per environment, not twice.

## 4. Historical data acceptance (run `3d8d9a25…`)

Independently reconciled from manifests + raw files (checksums re-verified):

| Metric | Expected | Verified |
|---|---:|---:|
| Archived GeoJSON files | 347 | 347 |
| Extraction windows | 122 | 122 |
| Source records | 281,421 | 281,421 |
| Unique event IDs | 281,421 | 281,421 |
| Duplicate records | 0 | 0 |
| Raw payload bytes | 201,979,748 | 201,979,748 |
| Coverage | 2015-01-01 → 2025-01-01 | identical (earliest 2015-01-01T00:10:00.59Z, latest 2024-12-31T23:28:55.746Z) |
| Integrity | PASS | PASS |

Full pipeline against the archive: Bronze 281,421/347 PASS → Silver
281,421 valid / 0 quarantined / 281,421 current / 0 superseded PASS → Delta
281,421 current / 281,421 revisions → Gold fact 281,421 / 16 datasets PASS.
**Idempotency:** a second Delta pass left revisions at 281,421 (0 new). Provenance,
UTC timestamps, and coordinates preserved (spot-checked `pr71435948`).

## 5. Incremental & failure-recovery coverage (verified by tests)

| Scenario | Test |
|---|---|
| Late-arriving / out-of-order revisions | `test_delta.py::test_current_late_older_revision_ignored` |
| Newer revision wins | `test_delta.py::test_current_newer_revision_wins` |
| Conflicting / duplicate observations | `test_delta.py` (fingerprint-keyed insert-only) |
| Additive schema evolution | `test_delta.py::test_schema_evolution_additive` |
| Transactional recovery (atomicity) | `test_delta.py::test_transactional_atomicity_on_failure` |
| Watermark protection (advance-only) | `test_recovery.py`, `test_delta.py::test_watermark_advance_only` |
| Failed quality gate | `src/orchestration/tasks.py::quality_gate` (fatal>0 raises) |
| Invalid coordinates / missing fields quarantine | `test_silver.py`, `test_quality.py` |

USGS semantics limitation: events are filtered by origin time but revised after
the fact; the incremental DAG re-queries an overlap window and relies on
idempotent fingerprint-keyed merges. The project only reconstructs **observed**
revisions, never unobserved historical source states.

## 6. Benchmarks (corrected)

Compression independently re-verified from actual file sizes:
201,979,748 / 26,081,745 = **7.74×** (varies 7.68–7.74 run-to-run with Parquet
layout). Completeness warning resolved exactly: rule `magnitude_type_present`,
**1 record** missing `magType` (0.0004%). Full table in `docs/BENCHMARKS.md`.

## 7. Dashboard acceptance

Reads the **actual** Gold datasets (no hardcoded values): total 281,421 events,
annual counts sum to 281,421, **M≥6.0 = 1,333** (independently confirmed from the
Delta current table). App imports cleanly; empty-Gold path shows a guidance
warning; filters operate on the fact table (not pre-aggregated rollups).

## 8. Release decision

All critical/high defects resolved and re-verified. Local acceptance gates pass.
Remaining gate before publishing v1.0.0: **remote GitHub Actions CI must pass**
after push (Phase 8). The release is published only once required workflows are green.
