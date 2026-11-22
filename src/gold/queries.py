"""Register Silver Delta tables as SQL views and run reusable analytics queries.

The ``sql/analytics`` directory holds portable Spark SQL queries that operate on
three registered views: ``events_current``, ``event_revisions``, and
``data_quality_results``. This keeps analytical logic in version-controlled SQL
rather than embedded strings.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from src.config import Settings, get_settings

_SQL_DIR = Path(__file__).resolve().parents[2] / "sql" / "analytics"

_VIEW_TABLES = {
    "events_current": ("silver", "earthquake_events_current"),
    "event_revisions": ("silver", "earthquake_event_revisions"),
    "data_quality_results": ("silver", "data_quality_results"),
}


def register_views(spark: SparkSession, settings: Settings | None = None) -> None:
    """Register the Silver Delta tables as temporary SQL views."""
    settings = settings or get_settings()
    for view, (namespace, table) in _VIEW_TABLES.items():
        path = settings.delta_table_path(namespace, table)
        spark.read.format("delta").load(str(path)).createOrReplaceTempView(view)


def list_queries() -> list[Path]:
    """Return the available analytics SQL files."""
    return sorted(_SQL_DIR.glob("*.sql"))


def run_query_file(spark: SparkSession, path: Path) -> DataFrame:
    """Execute a single-statement SQL file and return the result DataFrame."""
    sql = path.read_text().strip().rstrip(";")
    return spark.sql(sql)
