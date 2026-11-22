"""Centralized, environment-overridable configuration.

All lakehouse paths and Spark/JVM defaults are resolved here so that every
module (Bronze, Silver, Gold, orchestration, tests) shares one source of truth.
Values may be overridden via environment variables, which keeps local
development, CI, and future cloud execution consistent.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


def _env_path(name: str, default: Path) -> Path:
    """Resolve a path setting from the environment with a default."""
    value = os.environ.get(name)
    return Path(value) if value else default


@dataclass(frozen=True)
class Settings:
    """Immutable project configuration.

    The existing ``data/bronze`` tree is the *raw GeoJSON landing zone* produced
    by Milestones 1-2 and must never be mutated. Structured medallion outputs
    are written to a separate ``data/warehouse`` tree to preserve that archive.
    """

    # --- Raw (landing) layer, produced by ingestion (do not mutate) ---
    raw_bronze_path: Path
    manifest_path: Path
    profile_path: Path

    # --- Structured medallion layers (new) ---
    warehouse_path: Path
    bronze_path: Path
    silver_path: Path
    gold_path: Path
    metadata_path: Path

    # --- Spark / JVM ---
    java_home: str
    spark_master: str
    spark_shuffle_partitions: int
    spark_local_ip: str

    def run_manifest_dir(self, run_id: str) -> Path:
        """Return the manifest directory for an ingestion run."""
        return self.manifest_path / f"run_id={run_id}"

    def bronze_report_dir(self) -> Path:
        """Return the directory for Bronze reconciliation reports."""
        return self.bronze_path.parent / "_reports" / "bronze"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide settings singleton."""
    project_root = _env_path("LAKEHOUSE_ROOT", Path(__file__).resolve().parents[2])
    data_root = _env_path("LAKEHOUSE_DATA_ROOT", project_root / "data")
    warehouse = _env_path("LAKEHOUSE_WAREHOUSE", data_root / "warehouse")

    default_java_home = "/opt/homebrew/opt/openjdk@17"

    return Settings(
        raw_bronze_path=_env_path("LAKEHOUSE_RAW_BRONZE", data_root / "bronze"),
        manifest_path=_env_path("LAKEHOUSE_MANIFESTS", data_root / "manifests"),
        profile_path=_env_path("LAKEHOUSE_PROFILES", data_root / "profiles"),
        warehouse_path=warehouse,
        bronze_path=_env_path("LAKEHOUSE_BRONZE", warehouse / "bronze"),
        silver_path=_env_path("LAKEHOUSE_SILVER", warehouse / "silver"),
        gold_path=_env_path("LAKEHOUSE_GOLD", warehouse / "gold"),
        metadata_path=_env_path("LAKEHOUSE_METADATA", warehouse / "metadata"),
        java_home=os.environ.get("JAVA_HOME", default_java_home),
        spark_master=os.environ.get("SPARK_MASTER", "local[*]"),
        spark_shuffle_partitions=int(os.environ.get("SPARK_SHUFFLE_PARTITIONS", "32")),
        spark_local_ip=os.environ.get("SPARK_LOCAL_IP", "127.0.0.1"),
    )
