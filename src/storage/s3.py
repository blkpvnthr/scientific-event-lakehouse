"""AWS S3 storage backend.

Uses boto3 with AWS-native credential discovery (environment, shared config,
instance/role profiles) — never hard-coded secrets. Deterministic keys map to
``s3://<bucket>/<prefix>/<key>``. Writes refuse to overwrite existing objects
unless explicitly allowed, matching the raw-archive immutability convention.
"""

from __future__ import annotations

import logging

from src.storage.base import ObjectExistsError, StorageBackend, normalize_key

logger = logging.getLogger(__name__)


class S3Storage(StorageBackend):
    """Store objects in an S3 bucket under a fixed prefix."""

    def __init__(
        self,
        bucket: str,
        prefix: str = "",
        *,
        client=None,
        max_attempts: int = 5,
    ):
        import boto3
        from botocore.config import Config

        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self._client = client or boto3.client(
            "s3",
            config=Config(retries={"max_attempts": max_attempts, "mode": "standard"}),
        )

    def _object_key(self, key: str) -> str:
        key = normalize_key(key)
        return f"{self.prefix}/{key}" if self.prefix else key

    def write_bytes(self, key: str, data: bytes, *, overwrite: bool = False) -> str:
        object_key = self._object_key(key)
        if not overwrite and self._object_exists(object_key):
            raise ObjectExistsError(
                f"Object already exists: s3://{self.bucket}/{object_key}"
            )
        self._client.put_object(Bucket=self.bucket, Key=object_key, Body=data)
        return self.uri(key)

    def read_bytes(self, key: str) -> bytes:
        response = self._client.get_object(
            Bucket=self.bucket, Key=self._object_key(key)
        )
        return response["Body"].read()

    def _object_exists(self, object_key: str) -> bool:
        from botocore.exceptions import ClientError

        try:
            self._client.head_object(Bucket=self.bucket, Key=object_key)
            return True
        except ClientError as exc:
            if exc.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
                return False
            raise

    def exists(self, key: str) -> bool:
        return self._object_exists(self._object_key(key))

    def list_keys(self, prefix: str = "") -> list[str]:
        search = self._object_key(prefix) if prefix else self.prefix
        paginator = self._client.get_paginator("list_objects_v2")
        keys: list[str] = []
        strip = f"{self.prefix}/" if self.prefix else ""
        for page in paginator.paginate(Bucket=self.bucket, Prefix=search):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                keys.append(
                    key[len(strip) :] if strip and key.startswith(strip) else key
                )
        return sorted(keys)

    def uri(self, key: str) -> str:
        return f"s3://{self.bucket}/{self._object_key(key)}"
