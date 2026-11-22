"""Declarative data-quality rules.

A rule is a *pass* condition expressed as a Spark SQL boolean expression over the
Silver canonical columns. A record satisfying the condition passes the rule.

Rules are classified two ways:

* **Category** — what kind of check it is (integrity, plausibility, completeness).
* **Severity** — the consequence of failure:
    * ``FATAL``   → the record is quarantined.
    * ``WARNING`` → the record remains valid; the failure is only observed.

This separation lets us enforce mandatory integrity and scientific plausibility
while merely *observing* optional-field completeness, which must never cause
legitimate scientific records to be dropped.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Category(str, Enum):
    INTEGRITY = "integrity"
    PLAUSIBILITY = "plausibility"
    COMPLETENESS = "completeness"


class Severity(str, Enum):
    FATAL = "fatal"
    WARNING = "warning"


@dataclass(frozen=True)
class QualityRule:
    """A single data-quality rule.

    Attributes:
        rule_id: Stable identifier.
        category: Kind of check.
        severity: FATAL (quarantine) or WARNING (observe only).
        description: Human-readable intent.
        scope: Field or record scope the rule applies to.
        pass_condition: Spark SQL expression; True == record passes.
    """

    rule_id: str
    category: Category
    severity: Severity
    description: str
    scope: str
    pass_condition: str


@dataclass(frozen=True)
class QualityResult:
    """Aggregate outcome of evaluating one rule over a dataset."""

    rule_id: str
    category: str
    severity: str
    description: str
    scope: str
    observed_failure_count: int
    failure_percentage: float
    status: str  # PASS | FAIL
    run_id: str
    executed_at: str


# Default rule set for the earthquake canonical model. Thresholds reflect
# documented seismological plausibility, not arbitrary filters:
#   * Negative depths are VALID (events above the reference datum); the lower
#     bound only rejects physically impossible values.
#   * Magnitude bounds allow a generous margin around the observed catalog.
DEFAULT_RULES: tuple[QualityRule, ...] = (
    # --- Integrity (mandatory) ---
    QualityRule(
        "id_present",
        Category.INTEGRITY,
        Severity.FATAL,
        "Event identifier must be present and non-empty",
        "event_id",
        "event_id IS NOT NULL AND length(trim(event_id)) > 0",
    ),
    QualityRule(
        "event_time_present",
        Category.INTEGRITY,
        Severity.FATAL,
        "Event occurrence time must be present",
        "event_time",
        "event_time IS NOT NULL",
    ),
    QualityRule(
        "coordinates_present",
        Category.INTEGRITY,
        Severity.FATAL,
        "Latitude and longitude must be present",
        "latitude,longitude",
        "latitude IS NOT NULL AND longitude IS NOT NULL",
    ),
    QualityRule(
        "coordinates_in_range",
        Category.INTEGRITY,
        Severity.FATAL,
        "Coordinates must lie within valid geographic bounds",
        "latitude,longitude",
        "latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180",
    ),
    QualityRule(
        "magnitude_present",
        Category.INTEGRITY,
        Severity.FATAL,
        "Magnitude must be present",
        "magnitude",
        "magnitude IS NOT NULL",
    ),
    QualityRule(
        "event_type_present",
        Category.INTEGRITY,
        Severity.FATAL,
        "Event type must be present",
        "event_type",
        "event_type IS NOT NULL AND length(trim(event_type)) > 0",
    ),
    # --- Plausibility (scientific) ---
    QualityRule(
        "magnitude_plausible",
        Category.PLAUSIBILITY,
        Severity.FATAL,
        "Magnitude must be within a physically plausible range [-1, 12]",
        "magnitude",
        "magnitude BETWEEN -1.0 AND 12.0",
    ),
    QualityRule(
        "depth_plausible",
        Category.PLAUSIBILITY,
        Severity.FATAL,
        "Depth must be within a plausible range [-10, 1000] km "
        "(negative depths above the datum are valid)",
        "depth_km",
        "depth_km IS NULL OR depth_km BETWEEN -10.0 AND 1000.0",
    ),
    QualityRule(
        "revision_not_before_event",
        Category.PLAUSIBILITY,
        Severity.FATAL,
        "Source revision time must not precede the event time",
        "updated_time,event_time",
        "updated_time IS NULL OR updated_time >= event_time",
    ),
    # --- Completeness (optional-field observations; non-fatal) ---
    QualityRule(
        "magnitude_type_present",
        Category.COMPLETENESS,
        Severity.WARNING,
        "Magnitude type is expected but optional",
        "magnitude_type",
        "magnitude_type IS NOT NULL AND length(trim(magnitude_type)) > 0",
    ),
    QualityRule(
        "place_present",
        Category.COMPLETENESS,
        Severity.WARNING,
        "Place description is expected but optional",
        "place",
        "place IS NOT NULL AND length(trim(place)) > 0",
    ),
)
