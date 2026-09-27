"""
Modern Data Architecture POC - Domain Entities & Value Objects.
Pure Python implementations with no external infrastructure dependencies.
Follows GRASP Information Expert and SOLID principles.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class DailySalesMetrics:
    """Curated business aggregation for the Gold tier."""
    order_date: str
    status: str
    gross_revenue: float
    total_orders: int
    average_order_value: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "order_date": self.order_date,
            "status": self.status,
            "gross_revenue": round(self.gross_revenue, 2),
            "total_orders": self.total_orders,
            "average_order_value": round(self.average_order_value, 2),
        }


@dataclass(frozen=True)
class QualityAssertionResult:
    """Result of evaluating a single data quality expectation."""
    rule_name: str
    column: str | None
    success: bool
    observed_value: Any
    expected_value: Any
    details: str = ""


@dataclass(frozen=True)
class QualityGateEvaluation:
    """Aggregated outcome of a quality gate execution."""
    suite_name: str
    table_name: str
    total_assertions: int
    passed_assertions: int
    pass_rate: float
    threshold: float
    passed: bool
    failures: list[QualityAssertionResult] = field(default_factory=list)
    # Pass ratio per quality dimension present in the suite (e.g. completeness).
    dimension_rates: dict[str, float] = field(default_factory=dict)
