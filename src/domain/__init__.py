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
from src.domain.exceptions import QualityGateBreachError, SuiteNotFoundError
from src.domain.naming import (
    GOLD_PRODUCTS,
    GoldProduct,
    bronze_table,
    gold_table,
    physical_sql_name,
    silver_table,
    stream_table,
)
from src.domain.privacy import PrivacyPolicy
from src.domain.quality_gate import DEFAULT_QUALITY_THRESHOLD, GateVerdict, assess_gate

__all__ = [
    "DailySalesMetrics",
    "QualityAssertionResult",
    "QualityGateEvaluation",
    "QualityGateBreachError",
    "SuiteNotFoundError",
    "PrivacyPolicy",
    "ContractCompatibilityEvaluator",
    "BusinessAggregator",
    "GateVerdict",
    "assess_gate",
    "DEFAULT_QUALITY_THRESHOLD",
    "GoldProduct",
    "GOLD_PRODUCTS",
    "bronze_table",
    "silver_table",
    "gold_table",
    "stream_table",
    "physical_sql_name",
]
