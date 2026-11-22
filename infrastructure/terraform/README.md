# Terraform Infrastructure

Infrastructure as Code for the AWS deployment of the Scientific Event Data
Lakehouse. Provisions a secure S3 bucket and a least-privilege IAM role.

## Layout

```
infrastructure/terraform/
├── modules/
│   ├── s3_bucket/   # private, encrypted, versioned, TLS-only bucket + lifecycle
│   └── iam/         # least-privilege role + policy scoped to the bucket
└── environments/
    └── dev/         # composes the modules for the dev environment
```

## Validation (no AWS account required)

```bash
cd infrastructure/terraform/environments/dev
terraform fmt -check -recursive ..   # or: tofu fmt -check -recursive ..
terraform init -backend=false
terraform validate
```

> Validated locally with OpenTofu 1.13.9 (`tofu`), which is HCL-compatible with
> Terraform. Use `terraform` or `tofu` interchangeably for the commands above.

## Deploy (requires authorization + credentials)

```bash
cp terraform.tfvars.example terraform.tfvars   # set a unique bucket_name
terraform init
terraform plan
terraform apply       # creates real, billable AWS resources
```

**`terraform apply` is never run automatically.** It creates billable resources
and requires explicit authorization and valid AWS credentials.

## Security posture

- Public access fully blocked.
- Encryption at rest (SSE-S3; switch to `aws:kms` for a customer-managed key).
- TLS-only bucket policy (denies `aws:SecureTransport = false`).
- Object versioning enabled; noncurrent versions expire after 90 days.
- IAM policy scoped to a single bucket with read/write/delete on objects only.

## Cost considerations

Primary costs are S3 storage (~$0.023/GB-month standard), request charges, and
data transfer. The verified dataset is ~25 MiB (Bronze Parquet) / ~193 MiB raw,
so storage cost is negligible; costs scale with retained versions and request
volume. See `docs/CLOUD_COST_ESTIMATES.md`.

## Safe teardown

```bash
terraform destroy
```

For non-dev environments `force_destroy` is false, so empty the bucket before
destroying. Review the plan before confirming.
