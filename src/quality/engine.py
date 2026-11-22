"""Evaluate data-quality rules over a Spark DataFrame.

Two outputs:

* ``annotate_quality`` adds per-row arrays of the fatal and warning rules each
  record failed, so callers can split valid from quarantined records.
* ``evaluate_rules`` computes the aggregate per-rule result set (failure counts,
  percentages, pass/fail) for reproducible data-quality reports.

Rule evaluation is expressed entirely as Spark SQL so it runs distributed and is
independently testable per rule.
"""

from __future__ import annotations

from datetime import UTC, datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.quality.rules import (
    DEFAULT_RULES,
    QualityResult,
    QualityRule,
    Severity,
)

FAILED_FATAL_COL = "failed_fatal_rules"
FAILED_WARNING_COL = "failed_warning_rules"


def _failed_array_expr(rules: tuple[QualityRule, ...], severity: Severity) -> str:
    """Build a SQL expression yielding an array of failed rule ids."""
    selected = [r for r in rules if r.severity == severity]
    if not selected:
        return "array()"
    cases = [
        f"CASE WHEN NOT ({r.pass_condition}) THEN '{r.rule_id}' END" for r in selected
    ]
    array_expr = "array(" + ", ".join(cases) + ")"
    # Drop the NULLs produced by rules that passed.
    return f"filter({array_expr}, x -> x IS NOT NULL)"


def annotate_quality(
    df: DataFrame, rules: tuple[QualityRule, ...] = DEFAULT_RULES
) -> DataFrame:
    """Add ``failed_fatal_rules`` and ``failed_warning_rules`` columns."""
    return df.withColumn(
        FAILED_FATAL_COL, F.expr(_failed_array_expr(rules, Severity.FATAL))
    ).withColumn(
        FAILED_WARNING_COL,
        F.expr(_failed_array_expr(rules, Severity.WARNING)),
    )


def evaluate_rules(
    df: DataFrame,
    run_id: str,
    rules: tuple[QualityRule, ...] = DEFAULT_RULES,
    executed_at: str | None = None,
) -> list[QualityResult]:
    """Compute aggregate per-rule results over ``df``.

    A single aggregation pass counts failures for every rule.
    """
    executed_at = executed_at or datetime.now(UTC).isoformat()
    total = df.count()

    # One aggregate column per rule counting failing records.
    agg_cols = [
        F.sum(F.when(~F.expr(r.pass_condition), 1).otherwise(0)).alias(r.rule_id)
        for r in rules
    ]
    counts = df.agg(*agg_cols).collect()[0].asDict() if total else {}

    results: list[QualityResult] = []
    for rule in rules:
        failures = int(counts.get(rule.rule_id, 0) or 0)
        pct = round(failures / total * 100, 4) if total else 0.0
        results.append(
            QualityResult(
                rule_id=rule.rule_id,
                category=rule.category.value,
                severity=rule.severity.value,
                description=rule.description,
                scope=rule.scope,
                observed_failure_count=failures,
                failure_percentage=pct,
                status="PASS" if failures == 0 else "FAIL",
                run_id=run_id,
                executed_at=executed_at,
            )
        )
    return results
