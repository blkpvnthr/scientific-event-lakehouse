"""Explicit PySpark schema for the USGS FDSN GeoJSON response.

Declaring the schema explicitly (rather than letting Spark infer it) is a
deliberate correctness decision:

* Inference would require an extra full pass over the data and could silently
  change column types between runs depending on sampled values.
* An explicit contract makes malformed or unexpected payloads visible instead
  of coercing them away.
* It documents exactly which scientific attributes the USGS summary feed
  provides. Fields absent from the summary feed (e.g. per-event location/
  magnitude source) are intentionally *not* fabricated here.

Field reference: https://earthquake.usgs.gov/data/comcat/data-eventterms.php
"""

from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# Per-event scientific + descriptive attributes from the summary feed.
PROPERTIES_SCHEMA = StructType(
    [
        StructField("mag", DoubleType(), True),
        StructField("place", StringType(), True),
        StructField("time", LongType(), True),  # epoch milliseconds (UTC)
        StructField("updated", LongType(), True),  # epoch milliseconds (UTC)
        StructField("tz", LongType(), True),
        StructField("url", StringType(), True),
        StructField("detail", StringType(), True),
        StructField("felt", LongType(), True),
        StructField("cdi", DoubleType(), True),  # community intensity
        StructField("mmi", DoubleType(), True),  # max instrumental intensity
        StructField("alert", StringType(), True),
        StructField("status", StringType(), True),
        StructField("tsunami", LongType(), True),
        StructField("sig", LongType(), True),  # significance
        StructField("net", StringType(), True),  # network
        StructField("code", StringType(), True),  # event code
        StructField("ids", StringType(), True),
        StructField("sources", StringType(), True),
        StructField("types", StringType(), True),
        StructField("nst", LongType(), True),  # station count
        StructField("dmin", DoubleType(), True),  # min station distance (deg)
        StructField("rms", DoubleType(), True),  # travel-time residual
        StructField("gap", DoubleType(), True),  # azimuthal gap (deg)
        StructField("magType", StringType(), True),
        StructField("type", StringType(), True),  # event type
        StructField("title", StringType(), True),
    ]
)

# GeoJSON Point geometry: coordinates are [longitude, latitude, depth_km].
GEOMETRY_SCHEMA = StructType(
    [
        StructField("type", StringType(), True),
        StructField("coordinates", ArrayType(DoubleType()), True),
    ]
)

FEATURE_SCHEMA = StructType(
    [
        StructField("type", StringType(), True),
        StructField("id", StringType(), True),
        StructField("properties", PROPERTIES_SCHEMA, True),
        StructField("geometry", GEOMETRY_SCHEMA, True),
    ]
)

METADATA_SCHEMA = StructType(
    [
        StructField("generated", LongType(), True),
        StructField("url", StringType(), True),
        StructField("title", StringType(), True),
        StructField("status", LongType(), True),
        StructField("count", LongType(), True),
        StructField("limit", LongType(), True),
        StructField("offset", LongType(), True),
    ]
)

# Top-level FeatureCollection as stored in each archived page file.
FEATURE_COLLECTION_SCHEMA = StructType(
    [
        StructField("type", StringType(), True),
        StructField("metadata", METADATA_SCHEMA, True),
        StructField("features", ArrayType(FEATURE_SCHEMA), True),
    ]
)
