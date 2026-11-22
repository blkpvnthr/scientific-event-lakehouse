"""Local filesystem storage backend (default; requires no credentials)."""

from __future__ import annotations

from pathlib import Path

from src.ingestion.archive import atomic_write
from src.storage.base import ObjectExistsError, StorageBackend, normalize_key


class LocalStorage(StorageBackend):
    """Store objects under a local base directory."""

    def __init__(self, base_dir: str | Path):
        self.base_dir = Path(base_dir)

    def _path(self, key: str) -> Path:
        return self.base_dir / normalize_key(key)

    def write_bytes(self, key: str, data: bytes, *, overwrite: bool = False) -> str:
        path = self._path(key)
        if path.exists():
            if not overwrite:
                raise ObjectExistsError(f"Object already exists: {key}")
            path.unlink()
        # atomic_write publishes via temp-file + link (never clobbers).
        atomic_write(path, data)
        return self.uri(key)

    def read_bytes(self, key: str) -> bytes:
        return self._path(key).read_bytes()

    def exists(self, key: str) -> bool:
        return self._path(key).is_file()

    def list_keys(self, prefix: str = "") -> list[str]:
        root = self.base_dir / prefix if prefix else self.base_dir
        if not root.exists():
            return []
        keys = [
            str(p.relative_to(self.base_dir)) for p in root.rglob("*") if p.is_file()
        ]
        return sorted(keys)

    def uri(self, key: str) -> str:
        return self._path(key).resolve().as_uri()
