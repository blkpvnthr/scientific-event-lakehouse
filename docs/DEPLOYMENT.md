# Deployment

## Storage backends

The pipeline targets two storage backends through one abstraction
(`src/storage`), selected by `STORAGE_BACKEND`:

| Backend | Use | Credentials |
|---|---|---|
| `local` (default) | development, CI, local demo | none |
| `s3` | cloud deployment | AWS-native provider chain |

Local development and CI require **no AWS credentials**. Switching to S3 is a
configuration change, not a code change.

### Local

```bash
export STORAGE_BACKEND=local
export STORAGE_LOCAL_DIR=data/object_store
```

### AWS S3

```bash
export STORAGE_BACKEND=s3
export S3_BUCKET=my-scientific-lakehouse
export S3_PREFIX=lakehouse
export AWS_REGION=us-east-1
# Credentials via env, profile, or instance role — never hard-coded.
```

Object layout is deterministic: `s3://<bucket>/<prefix>/<raw|bronze|silver|gold|metadata>/...`.
Raw, processed, and metadata areas are kept separate by top-level prefix, and
writes never silently overwrite existing objects.

## Spark / Delta on S3

`src/storage/spark_s3.py` provides the `s3a://` Hadoop configs. Delta on S3 is
single-writer; see [SECURITY.md](SECURITY.md). Reading/writing Delta on real S3
requires the `hadoop-aws` + `aws-java-sdk-bundle` JARs and is a cloud-deployment
step, not part of local CI.

## Prerequisites

- Python 3.11, Java 17, and the dependencies in `requirements*.txt`.
- For S3: an AWS account, a bucket (provisioned via Terraform in Milestone 8),
  and credentials available to the AWS provider chain.

> Creating AWS resources (including `terraform apply`) requires explicit
> authorization and is **not** performed automatically.
