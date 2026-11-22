"""Storage abstraction supporting local filesystem and AWS S3."""

from src.storage.base import ObjectExistsError, StorageBackend
from src.storage.factory import get_storage
from src.storage.local import LocalStorage

__all__ = [
    "LocalStorage",
    "ObjectExistsError",
    "StorageBackend",
    "get_storage",
]
