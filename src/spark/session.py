"""Centralized SparkSession construction.

A single factory guarantees every pipeline stage runs with identical,
reproducible Spark configuration: UTC timezone, a bounded local shuffle
partition count, and (optionally) Delta Lake support. Keeping this in one place
avoids subtle correctness bugs from divergent session settings.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

from src.config import get_settings


def build_spark_session(
    app_name: str = "lakehouse",
    *,
    with_delta: bool = False,
    shuffle_partitions: int | None = None,
) -> SparkSession:
    """Build (or fetch) a configured SparkSession.

    Args:
        app_name: Spark application name shown in logs/UI.
        with_delta: If True, register the Delta Lake SQL extensions and catalog.
        shuffle_partitions: Override the default shuffle partition count.

    Returns:
        A ready SparkSession configured for deterministic UTC processing.
    """
    settings = get_settings()

    # Ensure the JVM is discoverable even when the caller did not export it.
    os.environ.setdefault("JAVA_HOME", settings.java_home)
    os.environ.setdefault("SPARK_LOCAL_IP", settings.spark_local_ip)

    partitions = (
        shuffle_partitions
        if shuffle_partitions is not None
        else settings.spark_shuffle_partitions
    )

    builder = (
        SparkSession.builder.appName(app_name)
        .master(settings.spark_master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(partitions))
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        # Deterministic, portable Parquet timestamps.
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        # Allow safe dynamic partition overwrite for idempotent reprocessing.
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )

    if with_delta:
        from delta import configure_spark_with_delta_pip

        builder = (
            builder.config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            # Controlled additive schema evolution on MERGE.
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        )
        # Resolve and attach the matching Delta Lake JARs (cached after first run).
        builder = configure_spark_with_delta_pip(builder)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def stop_spark_session(spark: SparkSession) -> None:
    """Stop a SparkSession if it is active."""
    if spark is not None:
        spark.stop()
