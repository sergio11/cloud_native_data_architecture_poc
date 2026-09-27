"""
Modern Data Architecture POC - Domain Package.
Contains pure domain entities, privacy policies, aggregations and contract logic.
"""

from src.domain.aggregations import BusinessAggregator
from src.domain.contracts import ContractCompatibilityEvaluator
from src.domain.entities import (
    Customer,
    DailySalesMetrics,
    QualityAssertionResult,
    QualityGateEvaluation,
)
from src.domain.privacy import PrivacyPolicy

__all__ = [
    "Customer",
    "DailySalesMetrics",
    "QualityAssertionResult",
    "QualityGateEvaluation",
    "PrivacyPolicy",
    "ContractCompatibilityEvaluator",
    "BusinessAggregator",
]
