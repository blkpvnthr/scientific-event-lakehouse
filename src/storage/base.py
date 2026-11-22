"""Storage backend abstraction.

A single interface lets the pipeline target local filesystem for development and
AWS S3 for cloud deployment without changing calling code. Logical *keys* are
backend-agnostic relative paths (e.g. ``raw/run_id=abc/file.json``); each backend
maps them to a concrete location and returns a canonical URI.

Writes are append-only by default (``overwrite=False``): attempting to write an
existing object raises ``ObjectExistsError``, mirroring the raw-archive guarantee
that published data is never silently overwritten.
"""

from __future__ import annotations

import re
from abc import ABC, abstractmethod

# Logical top-level prefixes; keep raw, processed, and metadata separated.
RAW_PREFIX = "raw"
BRONZE_PREFIX = "bronze"
SILVER_PREFIX = "silver"
GOLD_PREFIX = "gold"
METADATA_PREFIX = "metadata"

_KEY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._=\-/]*$")


class ObjectExistsError(RuntimeError):
    """Raised when a write would overwrite an existing object."""


def normalize_key(key: str) -> str:
    """Validate and normalize a logical storage key (deterministic path)."""
    key = key.strip().lstrip("/")
    if not key or ".." in key.split("/"):
        raise ValueError(f"Invalid storage key: {key!r}")
    if not _KEY_RE.match(key):
        raise ValueError(f"Invalid storage key characters: {key!r}")
    return key


class StorageBackend(ABC):
    """Abstract object-storage backend."""

    @abstractmethod
    def write_bytes(self, key: str, data: bytes, *, overwrite: bool = False) -> str:
        """Write bytes to ``key``; return the canonical URI. Never overwrites
        unless ``overwrite`` is True."""

    @abstractmethod
    def read_bytes(self, key: str) -> bytes:
        """Read and return the bytes stored at ``key``."""

    @abstractmethod
    def exists(self, key: str) -> bool:
        """Return True if an object exists at ``key``."""

    @abstractmethod
    def list_keys(self, prefix: str = "") -> list[str]:
        """Return logical keys under ``prefix`` (sorted)."""

    @abstractmethod
    def uri(self, key: str) -> str:
        """Return the canonical URI for ``key`` without accessing it."""
