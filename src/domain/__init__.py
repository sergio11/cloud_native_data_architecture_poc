"""
Modern Data Architecture POC - Domain Package.
Contains pure domain entities, privacy policies, aggregations and contract logic.
"""

from src.domain.aggregations import BusinessAggregator
from src.domain.contracts import ContractCompatibilityEvaluator
from src.domain.entities import (
    DailySalesMetrics,
    ExtractionResult,
    QualityAssertionResult,
    QualityGateEvaluation,
)
from src.domain.exceptions import QualityGateBreachError, SuiteNotFoundError
from src.domain.naming import (
    GOLD_PRODUCTS,
    GoldProduct,
    bronze_table,
    contract_entity_for_table,
    gold_table,
    physical_sql_name,
    silver_table,
    stream_table,
)
from src.domain.privacy import PrivacyPolicy
from src.domain.quality_gate import (
    DEFAULT_QUALITY_THRESHOLD,
    FAIL_CLOSED_RULE,
    GateVerdict,
    assess_gate,
)

__all__ = [
    "DailySalesMetrics",
    "ExtractionResult",
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
    "FAIL_CLOSED_RULE",
    "GoldProduct",
    "GOLD_PRODUCTS",
    "bronze_table",
    "silver_table",
    "gold_table",
    "stream_table",
    "contract_entity_for_table",
    "physical_sql_name",
]
