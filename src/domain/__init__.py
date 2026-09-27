"""
Modern Data Architecture POC - Domain Package.
Contains pure domain entities, privacy policies, aggregations and contract logic.
"""

from src.domain.aggregations import BusinessAggregator
from src.domain.contracts import ContractCompatibilityEvaluator
from src.domain.entities import (
    DailySalesMetrics,
    QualityAssertionResult,
    QualityGateEvaluation,
)
from src.domain.privacy import PrivacyPolicy

__all__ = [
    "DailySalesMetrics",
    "QualityAssertionResult",
    "QualityGateEvaluation",
    "PrivacyPolicy",
    "ContractCompatibilityEvaluator",
    "BusinessAggregator",
]
