"""Spark configuration for reading/writing the lakehouse on S3 (``s3a://``).

These configs are provided for cloud deployment. They are **documented and
unit-tested for correctness of the mapping**, but reading/writing Delta on real
S3 requires the ``hadoop-aws`` + ``aws-java-sdk-bundle`` JARs and live/mock S3,
and is not exercised in local CI.

Concurrency note: a single-writer configuration is assumed. Delta on S3 provides
ACID for a single writer; multi-writer safety requires an external commit
coordinator (e.g. a DynamoDB log store) and is **not** claimed here.
"""

from __future__ import annotations

# Maven coordinates matching Spark 3.5 (Hadoop 3.3.x).
S3A_PACKAGES = (
    "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
)


def s3a_spark_configs(region: str = "us-east-1") -> dict[str, str]:
    """Return Spark/Hadoop configs for S3 access via the default credential chain.

    Uses AWS-native credential discovery (no secrets in config).
    """
    return {
        "spark.jars.packages": S3A_PACKAGES,
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        # AWS-native credential provider chain (env, profile, instance role).
        "spark.hadoop.fs.s3a.aws.credentials.provider": (
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
        ),
        "spark.hadoop.fs.s3a.endpoint.region": region,
        "spark.hadoop.fs.s3a.connection.maximum": "64",
        "spark.hadoop.fs.s3a.fast.upload": "true",
        # Single-writer Delta on S3.
        "spark.databricks.delta.multiClusterWrites.enabled": "false",
    }


def s3a_path(bucket: str, key: str) -> str:
    """Return an ``s3a://`` path for Spark I/O."""
    return f"s3a://{bucket}/{key.strip('/')}"
