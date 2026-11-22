"""Explicit PySpark schemas for source and lakehouse datasets."""

from src.schemas.usgs import (
    FEATURE_COLLECTION_SCHEMA,
    GEOMETRY_SCHEMA,
    PROPERTIES_SCHEMA,
)

__all__ = [
    "FEATURE_COLLECTION_SCHEMA",
    "GEOMETRY_SCHEMA",
    "PROPERTIES_SCHEMA",
]
