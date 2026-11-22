# Cloud Cost Estimates

Order-of-magnitude estimates for running the lakehouse on AWS. These are
**planning estimates** using public on-demand us-east-1 pricing (subject to
change), not measured bills. No paid resources have been provisioned.

## Dataset size reference (measured)

| Layer | Size |
|---|---|
| Raw GeoJSON (logical) | 192.62 MiB |
| Bronze Parquet | ~25 MiB |
| Silver + Gold Parquet/Delta | ~50 MiB combined (local observation) |

Total retained footprint is well under 1 GiB for the 10-year baseline.

## S3 storage (standard)

- ~$0.023 / GB-month. At <1 GiB the baseline is **< $0.03/month**.
- Versioning retains noncurrent versions until the 90-day lifecycle expiry;
  revision churn is the main growth driver.
- Requests: PUT ~$0.005/1k, GET ~$0.0004/1k — negligible at this volume.

## Compute

Local Spark incurs **no cloud cost**. On AWS, batch processing options:

| Option | Rough cost driver |
|---|---|
| EMR / EMR Serverless | per-vCPU-hour + per-GB-hour while running |
| Glue (Spark) | ~$0.44 / DPU-hour |
| EC2 (self-managed Spark) | instance-hours |

For a dataset this small, a single short EMR Serverless run would cost a few
cents to low single-digit dollars per run.

## Orchestration

- MWAA (managed Airflow) has an hourly environment charge (tens of $/month) that
  **dominates** total cost for a dataset this small — self-managed Airflow on a
  small instance or local Docker is far cheaper for a reference implementation.

## Guidance

For this reference project the storage cost is negligible; the real levers are
whether managed compute (EMR/Glue) and managed orchestration (MWAA) run
continuously. Prefer on-demand/serverless runs and local orchestration for
demos. Always run `terraform plan` and review before `apply`.
