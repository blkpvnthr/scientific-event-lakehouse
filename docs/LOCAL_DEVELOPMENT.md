# Local Development

## Prerequisites

- Python 3.11, Java 17 (OpenJDK), Homebrew, Docker (for Airflow).
- Create the virtualenv and install deps:

```bash
make setup        # core + Spark deps into .venv
```

## Quickstart (no AWS credentials)

Run the full medallion pipeline on the verified historical run:

```bash
make pipeline     # bronze -> silver -> delta -> gold for the default RUN_ID
```

Individual stages:

```bash
make bronze RUN_ID=<run_id>
make silver RUN_ID=<run_id>
make delta  RUN_ID=<run_id>
make gold
```

## Tests, lint, benchmarks

```bash
make test             # full suite (unit + Spark + Delta integration)
make lint             # ruff check
make validate         # ruff check + format check
make benchmark        # Bronze performance (3 reps)
make terraform-validate
```

## Airflow (Docker)

The DAGs live in `airflow/dags/`. A Docker image with Java 17 + the Spark/Delta
runtime is provided so tasks can execute.

```bash
docker compose up        # Airflow at http://localhost:8080 (standalone)
docker compose down
```

Trigger the historical pipeline:

```bash
# inside the Airflow UI, or:
airflow dags trigger historical_processing \
    --conf '{"run_id": "3d8d9a251f33489e807356890c692789"}'
```

### Validating DAG imports without Docker

Apache Airflow conflicts with the Spark pinset, so DAG-import tests run in a
separate virtualenv:

```bash
python3.11 -m venv .venv-airflow
.venv-airflow/bin/pip install "apache-airflow==2.10.5" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt"
PYTHONPATH=. .venv-airflow/bin/python -m pytest tests/test_dags.py -q
```

In the main `.venv`, `tests/test_dags.py` is skipped automatically.

## Storage backend

Default is local (no credentials). To target S3, set `STORAGE_BACKEND=s3` and the
`S3_*` variables (see `.env.example` and `docs/DEPLOYMENT.md`).
