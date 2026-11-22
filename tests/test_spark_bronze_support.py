"""Tests for the Spark session factory and explicit USGS schema."""

from __future__ import annotations

from src.schemas import FEATURE_COLLECTION_SCHEMA, PROPERTIES_SCHEMA


def test_session_is_utc_and_reusable(spark):
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"
    # Factory returns the same active session (getOrCreate).
    from src.spark import build_spark_session

    again = build_spark_session("lakehouse-tests")
    assert again is spark


def test_properties_schema_covers_summary_feed():
    names = {f.name for f in PROPERTIES_SCHEMA.fields}
    # Core scientific attributes must be present in the contract.
    for required in (
        "mag",
        "magType",
        "time",
        "updated",
        "place",
        "net",
        "sig",
        "nst",
        "gap",
        "dmin",
        "rms",
        "tsunami",
    ):
        assert required in names


def test_schema_parses_feature_collection(spark):
    payload = (
        '{"type":"FeatureCollection",'
        '"metadata":{"count":1,"status":200,"offset":1,"limit":1000},'
        '"features":[{"type":"Feature","id":"x1",'
        '"properties":{"mag":3.1,"time":1420070400000,"magType":"mb",'
        '"type":"earthquake","net":"us"},'
        '"geometry":{"type":"Point","coordinates":[1.0,2.0,3.0]}}]}'
    )
    rdd = spark.sparkContext.parallelize([payload])
    df = spark.read.schema(FEATURE_COLLECTION_SCHEMA).json(rdd)
    row = df.collect()[0]
    assert row["type"] == "FeatureCollection"
    assert row["metadata"]["count"] == 1
    feature = row["features"][0]
    assert feature["id"] == "x1"
    assert feature["properties"]["mag"] == 3.1
    assert feature["geometry"]["coordinates"] == [1.0, 2.0, 3.0]
