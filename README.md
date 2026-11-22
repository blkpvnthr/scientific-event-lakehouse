# Data-Lakehouse

# Scientific Event Data Lakehouse

A scientific data engineering platform for
ingesting, processing, and analyzing seismic
events using the USGS Earthquake Catalog.

## Current Architecture

```mermaid
flowchart TD
    A[USGS Earthquake API]
    B[Python API Client]
    C[Ingestion Pipeline]
    D[Bronze Raw JSON]
    E[Pytest Validation]

    A --> B
    B --> C
    C --> D
    E -.-> B

    style D fill:#CD7F32,color:#fff
```

## Current Status

Milestone 1: Initial API ingestion

## Planned Technology Stack

- Python
- PySpark
- AWS S3
- Delta Lake
- Apache Airflow
- Docker
- Terraform
- GitHub Actions