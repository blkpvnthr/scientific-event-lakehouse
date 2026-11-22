from unittest.mock import patch

import pytest

from src.ingestion.archive import (
    atomic_write,
    calculate_checksum,
)


def test_known_sha256_checksum():

    expected = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"

    assert calculate_checksum(b"hello") == expected


def test_checksum_is_deterministic():

    payload = b"earthquake data"

    assert calculate_checksum(payload) == calculate_checksum(payload)


def test_checksum_changes_when_payload_changes():

    assert calculate_checksum(b"original") != calculate_checksum(b"modified")


def test_atomic_write(tmp_path):

    destination = tmp_path / "earthquakes.json"

    payload = b'{"features":[]}'

    result = atomic_write(destination, payload)

    assert result == destination
    assert destination.exists()
    assert destination.read_bytes() == payload


def test_atomic_write_creates_parent_directories(tmp_path):

    destination = tmp_path / "bronze" / "ingestion_date=2024-01-01" / "earthquakes.json"

    atomic_write(destination, b"test payload")

    assert destination.exists()


def test_atomic_write_prevents_overwrite(tmp_path):

    destination = tmp_path / "earthquakes.json"

    atomic_write(destination, b"original payload")

    with pytest.raises(FileExistsError):
        atomic_write(destination, b"modified payload")

    assert destination.read_bytes() == b"original payload"


def test_archived_checksum_matches_original(tmp_path):

    destination = tmp_path / "earthquakes.json"

    payload = b'{"features":[{"id":"event001"}]}'

    expected_checksum = calculate_checksum(payload)

    atomic_write(destination, payload)

    archived_checksum = calculate_checksum(destination.read_bytes())

    assert archived_checksum == expected_checksum


def test_temporary_file_cleanup(tmp_path):

    destination = tmp_path / "earthquakes.json"

    atomic_write(destination, b"earthquake data")

    assert list(tmp_path.iterdir()) == [destination]


def test_cleanup_after_publication_failure(tmp_path):

    destination = tmp_path / "earthquakes.json"

    with (
        patch(
            "src.ingestion.archive.os.link",
            side_effect=OSError("Simulated publication failure"),
        ),
        pytest.raises(OSError),
    ):
        atomic_write(destination, b"earthquake data")

    assert not destination.exists()
    assert list(tmp_path.iterdir()) == []


def test_cleanup_after_sync_failure(tmp_path):

    destination = tmp_path / "earthquakes.json"

    with (
        patch(
            "src.ingestion.archive.os.fsync",
            side_effect=OSError("Simulated sync failure"),
        ),
        pytest.raises(OSError),
    ):
        atomic_write(destination, b"earthquake data")

    assert not destination.exists()
    assert list(tmp_path.iterdir()) == []
