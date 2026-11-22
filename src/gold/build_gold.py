"""Gold layer builder: dimensional model + analytical data products.

Reads the transactional Silver ``earthquake_events_current`` Delta table (one row
per current event) and produces a documented star-schema plus ten analytical
datasets. All aggregates are derived from the same validated current-state table,
so counts reconcile to it.

Scientific caveat: these are summaries of a magnitude>=2.5 observational catalog.
They reflect catalog coverage and reporting practices, not earthquake prediction.

Grain and keys are documented in docs/data_dictionary/GOLD_TABLES.md.
"""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import asdict, dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.config import Settings, get_settings
from src.spark import build_spark_session, stop_spark_session

logger = logging.getLogger(__name__)

# Dimensional model + analytical datasets written under data/warehouse/gold.
GOLD_DATASETS = [
    "dim_date",
    "dim_magnitude_type",
    "dim_event_type",
    "dim_source_network",
    "dim_geographic_region",
    "fact_earthquake_event",
    "daily_seismic_activity",
    "monthly_seismic_activity",
    "annual_seismic_activity",
    "geographic_seismic_activity",
    "magnitude_distribution",
    "depth_distribution",
    "significant_events",
    "seismic_hotspots",
    "source_network_metrics",
    "data_quality_trends",
]

# Spatial grid cell size in degrees for geographic aggregation / hotspots.
_GRID_DEG = 10
# Significant-event criteria (configurable, documented).
_SIG_MIN_MAGNITUDE = 6.0
_SIG_MIN_SIGNIFICANCE = 600


@dataclass(frozen=True)
class GoldResult:
    current_events: int
    fact_rows: int
    datasets: dict
    reconciled: bool
    status: str


def _region_key(lat, lon):
    """Deterministic 10-degree grid cell key, e.g. 'lat10_lon-70'."""
    lat_cell = (F.floor(lat / _GRID_DEG) * _GRID_DEG).cast("int")
    lon_cell = (F.floor(lon / _GRID_DEG) * _GRID_DEG).cast("int")
    return F.concat_ws(
        "_",
        F.concat(F.lit("lat"), lat_cell),
        F.concat(F.lit("lon"), lon_cell),
    )


def _magnitude_bucket(mag):
    return (
        F.when(mag < 2.5, "below_2.5")
        .when(mag < 3.0, "2.5_to_3.0")
        .when(mag < 4.0, "3.0_to_4.0")
        .when(mag < 5.0, "4.0_to_5.0")
        .when(mag < 6.0, "5.0_to_6.0")
        .when(mag < 7.0, "6.0_to_7.0")
        .otherwise("7.0_and_above")
    )


def _depth_category(depth):
    """Documented depth categories (USGS convention; negatives = above datum)."""
    return (
        F.when(depth < 0, "above_datum")
        .when(depth < 70, "shallow")
        .when(depth < 300, "intermediate")
        .otherwise("deep")
    )


def _build_fact(current: DataFrame) -> DataFrame:
    """One row per current event with dimension keys and measures."""
    return current.select(
        "event_id",
        F.date_format("event_time", "yyyyMMdd").cast("int").alias("date_key"),
        F.to_date("event_time").alias("event_date"),
        F.coalesce(F.col("magnitude_type_normalized"), F.lit("unknown")).alias(
            "magnitude_type_key"
        ),
        F.coalesce(F.col("event_type_normalized"), F.lit("unknown")).alias(
            "event_type_key"
        ),
        F.coalesce(F.col("network"), F.lit("unknown")).alias("network_key"),
        _region_key(F.col("latitude"), F.col("longitude")).alias("region_key"),
        "event_time",
        "magnitude",
        "depth_km",
        "latitude",
        "longitude",
        "significance",
        "felt",
        "cdi",
        "mmi",
        "tsunami",
        "station_count",
        "azimuthal_gap",
        "min_station_distance_deg",
        "rms",
        "event_year",
    )


def _dim_date(fact: DataFrame) -> DataFrame:
    return (
        fact.select("date_key", "event_date")
        .distinct()
        .withColumn("year", F.year("event_date"))
        .withColumn("quarter", F.quarter("event_date"))
        .withColumn("month", F.month("event_date"))
        .withColumn("day", F.dayofmonth("event_date"))
        .withColumn("day_of_week", F.dayofweek("event_date"))
        .withColumn("week_of_year", F.weekofyear("event_date"))
    )


def _magnitude_measures(prefix_count: str = "event_count"):
    return [
        F.count("*").alias(prefix_count),
        F.round(F.avg("magnitude"), 3).alias("avg_magnitude"),
        F.min("magnitude").alias("min_magnitude"),
        F.max("magnitude").alias("max_magnitude"),
        F.round(F.avg("depth_km"), 3).alias("avg_depth_km"),
        F.max("significance").alias("max_significance"),
        F.sum("tsunami").alias("tsunami_count"),
    ]


def _build_datasets(
    spark: SparkSession, fact: DataFrame, quality_path
) -> dict[str, DataFrame]:
    datasets: dict[str, DataFrame] = {}

    datasets["daily_seismic_activity"] = fact.groupBy("event_date").agg(
        *_magnitude_measures()
    )

    monthly = fact.groupBy("event_year", F.month("event_date").alias("month")).agg(
        *_magnitude_measures()
    )
    datasets["monthly_seismic_activity"] = monthly

    # Annual with year-over-year change (window function).
    annual = fact.groupBy("event_year").agg(*_magnitude_measures())
    yoy_window = Window.orderBy("event_year")
    datasets["annual_seismic_activity"] = annual.withColumn(
        "prev_year_count", F.lag("event_count").over(yoy_window)
    ).withColumn(
        "yoy_change_pct",
        F.round(
            (F.col("event_count") - F.col("prev_year_count"))
            / F.col("prev_year_count")
            * 100,
            2,
        ),
    )

    datasets["geographic_seismic_activity"] = fact.groupBy("region_key").agg(
        *_magnitude_measures(),
        F.round(F.avg("latitude"), 3).alias("avg_latitude"),
        F.round(F.avg("longitude"), 3).alias("avg_longitude"),
    )

    datasets["magnitude_distribution"] = (
        fact.withColumn("magnitude_bucket", _magnitude_bucket(F.col("magnitude")))
        .groupBy("magnitude_bucket")
        .agg(F.count("*").alias("event_count"))
    )

    datasets["depth_distribution"] = (
        fact.withColumn("depth_category", _depth_category(F.col("depth_km")))
        .groupBy("depth_category")
        .agg(
            F.count("*").alias("event_count"),
            F.round(F.avg("magnitude"), 3).alias("avg_magnitude"),
        )
    )

    datasets["significant_events"] = fact.filter(
        (F.col("magnitude") >= _SIG_MIN_MAGNITUDE)
        | (F.col("significance") >= _SIG_MIN_SIGNIFICANCE)
    ).select(
        "event_id",
        "event_time",
        "magnitude",
        "magnitude_type_key",
        "depth_km",
        "latitude",
        "longitude",
        "significance",
        "tsunami",
        "region_key",
    )

    # Hotspots: rank grid cells by event count (ranking window).
    hotspot_base = fact.groupBy("region_key").agg(
        F.count("*").alias("event_count"),
        F.round(F.avg("magnitude"), 3).alias("avg_magnitude"),
        F.max("magnitude").alias("max_magnitude"),
    )
    datasets["seismic_hotspots"] = hotspot_base.withColumn(
        "rank", F.rank().over(Window.orderBy(F.col("event_count").desc()))
    )

    datasets["source_network_metrics"] = fact.groupBy("network_key").agg(
        F.count("*").alias("event_count"),
        F.round(F.avg("magnitude"), 3).alias("avg_magnitude"),
        F.round(F.count("*") / fact.count() * 100, 3).alias("share_pct"),
    )

    # Data quality trends from the Silver data_quality_results Delta table.
    quality_trend = (
        spark.read.format("delta")
        .load(str(quality_path))
        .groupBy("run_id", "executed_at", "severity")
        .agg(
            F.sum("observed_failure_count").alias("total_failures"),
            F.count("*").alias("rule_count"),
        )
    )
    datasets["data_quality_trends"] = quality_trend

    return datasets


def build_gold(spark: SparkSession, settings: Settings | None = None) -> GoldResult:
    """Build the Gold dimensional model and analytical datasets."""
    settings = settings or get_settings()
    current_path = settings.delta_table_path("silver", "earthquake_events_current")
    quality_path = settings.delta_table_path("silver", "data_quality_results")

    current = spark.read.format("delta").load(str(current_path))
    current_events = current.count()

    fact = _build_fact(current)
    fact.cache()
    fact_rows = fact.count()

    tables: dict[str, DataFrame] = {
        "fact_earthquake_event": fact,
        "dim_date": _dim_date(fact),
        "dim_magnitude_type": fact.select(F.col("magnitude_type_key")).distinct(),
        "dim_event_type": fact.select(F.col("event_type_key")).distinct(),
        "dim_source_network": fact.select(F.col("network_key")).distinct(),
        "dim_geographic_region": fact.select(
            "region_key",
            (F.floor(F.col("latitude") / _GRID_DEG) * _GRID_DEG).alias("lat_cell"),
            (F.floor(F.col("longitude") / _GRID_DEG) * _GRID_DEG).alias("lon_cell"),
        ).distinct(),
    }
    tables.update(_build_datasets(spark, fact, quality_path))

    counts: dict[str, int] = {}
    for name, df in tables.items():
        out = settings.gold_path / name
        df.write.mode("overwrite").parquet(str(out))
        counts[name] = df.count()

    reconciled = counts["fact_earthquake_event"] == current_events
    fact.unpersist()

    result = GoldResult(
        current_events=current_events,
        fact_rows=fact_rows,
        datasets=counts,
        reconciled=reconciled,
        status="PASS" if reconciled else "FAIL",
    )
    logger.info(
        "Gold build complete: fact=%s datasets=%s status=%s",
        fact_rows,
        len(counts),
        result.status,
    )
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the Gold layer")
    parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
    )
    spark = build_spark_session("gold-build", with_delta=True)
    try:
        result = build_gold(spark)
        print(json.dumps(asdict(result), indent=2))
        if result.status != "PASS":
            raise SystemExit(1)
    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    main()
