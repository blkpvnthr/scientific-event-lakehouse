"""Tests for the storage abstraction (local + mocked S3)."""

from __future__ import annotations

import pytest

from src.storage import LocalStorage, ObjectExistsError, get_storage
from src.storage.base import normalize_key
from src.storage.spark_s3 import s3a_path, s3a_spark_configs

# --- Local backend ---


def test_local_write_read_roundtrip(tmp_path):
    store = LocalStorage(tmp_path)
    uri = store.write_bytes("raw/a/f.json", b"hello")
    assert uri.startswith("file://")
    assert store.read_bytes("raw/a/f.json") == b"hello"
    assert store.exists("raw/a/f.json")


def test_local_no_silent_overwrite(tmp_path):
    store = LocalStorage(tmp_path)
    store.write_bytes("raw/a/f.json", b"one")
    with pytest.raises(ObjectExistsError):
        store.write_bytes("raw/a/f.json", b"two")
    # explicit overwrite is allowed
    store.write_bytes("raw/a/f.json", b"three", overwrite=True)
    assert store.read_bytes("raw/a/f.json") == b"three"


def test_local_list_keys(tmp_path):
    store = LocalStorage(tmp_path)
    store.write_bytes("raw/a.json", b"1")
    store.write_bytes("silver/b.json", b"2")
    assert store.list_keys("raw") == ["raw/a.json"]
    assert set(store.list_keys()) == {"raw/a.json", "silver/b.json"}


def test_key_validation():
    assert normalize_key("/raw/a.json") == "raw/a.json"
    with pytest.raises(ValueError):
        normalize_key("../escape")
    with pytest.raises(ValueError):
        normalize_key("bad space")


def test_factory_defaults_to_local(monkeypatch, tmp_path):
    monkeypatch.delenv("STORAGE_BACKEND", raising=False)
    monkeypatch.setenv("STORAGE_LOCAL_DIR", str(tmp_path))
    store = get_storage()
    assert isinstance(store, LocalStorage)


def test_factory_s3_requires_bucket(monkeypatch):
    monkeypatch.setenv("STORAGE_BACKEND", "s3")
    monkeypatch.delenv("S3_BUCKET", raising=False)
    with pytest.raises(ValueError, match="S3_BUCKET"):
        get_storage()


# --- S3 backend (mocked with moto) ---


@pytest.fixture
def s3_storage():
    boto3 = pytest.importorskip("boto3")
    moto = pytest.importorskip("moto")
    with moto.mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="test-lake")
        from src.storage.s3 import S3Storage

        yield S3Storage("test-lake", prefix="lakehouse")


def test_s3_roundtrip_and_uri(s3_storage):
    uri = s3_storage.write_bytes("raw/a/f.json", b"data")
    assert uri == "s3://test-lake/lakehouse/raw/a/f.json"
    assert s3_storage.read_bytes("raw/a/f.json") == b"data"
    assert s3_storage.exists("raw/a/f.json")
    assert not s3_storage.exists("raw/missing.json")


def test_s3_no_silent_overwrite(s3_storage):
    s3_storage.write_bytes("raw/a.json", b"one")
    with pytest.raises(ObjectExistsError):
        s3_storage.write_bytes("raw/a.json", b"two")
    s3_storage.write_bytes("raw/a.json", b"three", overwrite=True)
    assert s3_storage.read_bytes("raw/a.json") == b"three"


def test_s3_list_keys(s3_storage):
    s3_storage.write_bytes("raw/a.json", b"1")
    s3_storage.write_bytes("silver/b.json", b"2")
    assert s3_storage.list_keys("raw") == ["raw/a.json"]
    assert set(s3_storage.list_keys()) == {"raw/a.json", "silver/b.json"}


def test_s3_deterministic_paths(s3_storage):
    assert s3_storage.uri("bronze/x=1/f.parquet") == (
        "s3://test-lake/lakehouse/bronze/x=1/f.parquet"
    )


# --- Spark S3 config mapping ---


def test_spark_s3_configs():
    cfg = s3a_spark_configs("us-west-2")
    assert cfg["spark.hadoop.fs.s3a.endpoint.region"] == "us-west-2"
    assert "hadoop-aws" in cfg["spark.jars.packages"]
    assert cfg["spark.databricks.delta.multiClusterWrites.enabled"] == "false"
    assert s3a_path("b", "/k/v") == "s3a://b/k/v"
