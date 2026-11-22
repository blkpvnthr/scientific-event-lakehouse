# Security

## Credentials

- **No hard-coded secrets.** The S3 backend uses the AWS-native credential
  provider chain (environment variables, shared config/profile, or instance/role
  credentials). Spark S3 access uses `DefaultAWSCredentialsProviderChain`.
- `.env` is gitignored; only `.env.example` (no secrets) is committed.
- No credentials, environment files, generated datasets, or Terraform state are
  committed (see `.gitignore`).

## Data integrity

- Raw archives are append-only: `atomic_write` publishes via a temp file + hard
  link and never overwrites an existing file.
- Storage backends refuse silent overwrites (`ObjectExistsError`) unless
  `overwrite=True` is explicitly passed.
- Every Bronze build re-verifies raw-file SHA-256 + size before processing.

## Storage (planned cloud posture)

The Terraform milestone configures S3 with public-access block, encryption at
rest, TLS-only bucket policies, and least-privilege IAM. Until that is applied
and verified, **no storage-level immutability is claimed** — current guarantees
are by convention (append-only writers), not enforced by object-lock.

## Concurrency

Delta on S3 is used in a **single-writer** configuration. Multi-writer ACID
safety would require an external commit coordinator (e.g. a DynamoDB log store)
and is not claimed.

## Secrets in logs

Structured logging (Milestone 10) redacts secrets from error details. Avoid
logging full request URLs or credentials.
