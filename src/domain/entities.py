"""
Modern Data Architecture POC - Domain Entities & Value Objects.
Pure Python implementations with no external infrastructure dependencies.
Follows GRASP Information Expert and SOLID principles.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class OrderStatus(str, Enum):
    CONFIRMED = "confirmed"
    PENDING = "pending"
    SHIPPED = "shipped"
    CANCELLED = "cancelled"


class CustomerSegment(str, Enum):
    PREMIUM = "premium"
    STANDARD = "standard"
    BASIC = "basic"


class DataClassification(str, Enum):
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    PII = "pii"
    RESTRICTED = "restricted"


@dataclass(frozen=True)
class Customer:
    """Customer entity within the Sales domain."""
    id: int
    name: str
    email: str
    segment: str
    registration_date: str
    created_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "email": self.email,
            "segment": self.segment,
            "registration_date": self.registration_date,
            "created_at": self.created_at,
        }


@dataclass(frozen=True)
class Order:
    """Sales order entity."""
    id: int
    customer_id: int
    total_amount: float
    status: str
    order_date: str
    created_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "customer_id": self.customer_id,
            "total_amount": self.total_amount,
            "status": self.status,
            "order_date": self.order_date,
            "created_at": self.created_at,
        }


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
