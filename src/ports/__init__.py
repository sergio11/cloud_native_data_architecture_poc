"""
Modern Data Architecture POC - Ports Package.
Contains Driving (Inbound) and Driven (Outbound) interface definitions.
"""

from src.ports.inbound import (
    AggregateGoldPort,
    CleanseSilverPort,
    IngestBronzePort,
    ProcessStreamPort,
    QueryServingPort,
    RunQualityGatePort,
    ValidateContractPort,
)
from src.ports.outbound import (
    DomainCatalogPort,
    EventBrokerPort,
    LakehouseStoragePort,
    LineagePublisherPort,
    OltpSourcePort,
    QualityEnginePort,
    QueryExecutorPort,
)

__all__ = [
    "IngestBronzePort",
    "CleanseSilverPort",
    "AggregateGoldPort",
    "ProcessStreamPort",
    "RunQualityGatePort",
    "ValidateContractPort",
    "QueryServingPort",
    "OltpSourcePort",
    "LakehouseStoragePort",
    "EventBrokerPort",
    "LineagePublisherPort",
    "QualityEnginePort",
    "QueryExecutorPort",
    "DomainCatalogPort",
]
