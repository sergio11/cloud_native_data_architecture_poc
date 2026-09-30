"""
Modern Data Architecture POC - Ports Package.
Contains Driving (Inbound) and Driven (Outbound) interface definitions.
"""

from src.ports.inbound import (
    AggregateGoldPort,
    AggregateGoldResult,
    CleanseSilverPort,
    CleanseSilverResult,
    IngestBronzePort,
    IngestBronzeResult,
    ProcessStreamPort,
    ProcessStreamResult,
    QueryServingPort,
    RunQualityGatePort,
    UnifiedRevenueRow,
    ValidateContractPort,
    ValidateContractResult,
)
from src.ports.outbound import (
    FAIL_CLOSED_RULE,
    ContractCatalogPort,
    DomainCatalogPort,
    EventBrokerPort,
    LakehouseStoragePort,
    LakehouseTableInspector,
    LakehouseTableReader,
    LakehouseTableWriter,
    LineagePublisherPort,
    OltpSourcePort,
    QualityEnginePort,
    QueryExecutorPort,
)

__all__ = [
    "IngestBronzePort",
    "IngestBronzeResult",
    "CleanseSilverPort",
    "CleanseSilverResult",
    "AggregateGoldPort",
    "AggregateGoldResult",
    "ProcessStreamPort",
    "ProcessStreamResult",
    "RunQualityGatePort",
    "ValidateContractPort",
    "ValidateContractResult",
    "QueryServingPort",
    "UnifiedRevenueRow",
    "OltpSourcePort",
    "LakehouseStoragePort",
    "LakehouseTableReader",
    "LakehouseTableWriter",
    "LakehouseTableInspector",
    "EventBrokerPort",
    "FAIL_CLOSED_RULE",
    "LineagePublisherPort",
    "QualityEnginePort",
    "QueryExecutorPort",
    "DomainCatalogPort",
    "ContractCatalogPort",
]
