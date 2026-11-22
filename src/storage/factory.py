"""Select a storage backend from environment configuration.

Environment variables:
    STORAGE_BACKEND   "local" (default) or "s3"
    STORAGE_LOCAL_DIR base directory for local storage (default: data/object_store)
    S3_BUCKET         bucket name (required for s3)
    S3_PREFIX         key prefix within the bucket (optional)

Local is the default so development and CI work with no AWS credentials.
"""

from __future__ import annotations

import os

from src.config import get_settings
from src.storage.base import StorageBackend
from src.storage.local import LocalStorage


def get_storage(backend: str | None = None) -> StorageBackend:
    """Construct the configured storage backend."""
    backend = (backend or os.environ.get("STORAGE_BACKEND", "local")).lower()

    if backend == "local":
        default_dir = get_settings().warehouse_path.parent / "object_store"
        base_dir = os.environ.get("STORAGE_LOCAL_DIR", str(default_dir))
        return LocalStorage(base_dir)

    if backend == "s3":
        from src.storage.s3 import S3Storage

        bucket = os.environ.get("S3_BUCKET")
        if not bucket:
            raise ValueError("S3_BUCKET must be set for the s3 storage backend")
        return S3Storage(bucket, os.environ.get("S3_PREFIX", ""))

    raise ValueError(f"Unknown storage backend: {backend}")
