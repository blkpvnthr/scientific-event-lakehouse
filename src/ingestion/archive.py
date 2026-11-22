import hashlib
import os
import tempfile
from pathlib import Path


def calculate_checksum(payload: bytes) -> str:
    """Calculate the SHA-256 checksum of a payload."""

    return hashlib.sha256(payload).hexdigest()


def atomic_write(path: Path, payload: bytes) -> Path:
    """Publish a complete file without overwriting existing data."""

    path = Path(path)

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temp_path = None

    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=path.parent,
            prefix=".tmp-",
            delete=False,
        ) as temporary_file:
            temp_path = Path(temporary_file.name)

            temporary_file.write(payload)

            temporary_file.flush()

            os.fsync(temporary_file.fileno())

        # Publish atomically without replacing an existing file.
        os.link(temp_path, path)

    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)

    return path
