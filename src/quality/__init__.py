"""Configurable data-quality rule engine."""

from src.quality.engine import annotate_quality, evaluate_rules
from src.quality.rules import (
    DEFAULT_RULES,
    Category,
    QualityResult,
    QualityRule,
    Severity,
)

__all__ = [
    "DEFAULT_RULES",
    "Category",
    "QualityResult",
    "QualityRule",
    "Severity",
    "annotate_quality",
    "evaluate_rules",
]
