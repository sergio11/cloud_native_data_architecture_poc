"""
Modern Data Architecture POC - Composition Root & Dependency Injection Container.
Wires domain use cases with infrastructure adapters according to Dependency Inversion Principle (DIP).
"""

from __future__ import annotations

import functools
import os

from src.adapters.catalog.fs_domain_catalog import FsDomainCatalogAdapter
from src.adapters.lineage.marquez_lineage import MarquezLineageAdapter
from src.adapters.quality.expectation_engine import ExpectationEngineAdapter
from src.adapters.serving.sql_serving import SqlServingAdapter
from src.adapters.source.oltp_source import OltpSourceAdapter
from src.adapters.storage.iceberg_catalog import _iceberg_catalog
from src.adapters.storage.iceberg_storage import IcebergLakehouseStorageAdapter
from src.adapters.storage.lakehouse_storage import LakehouseStorageAdapter
from src.adapters.streaming.event_stream import EventStreamAdapter
from src.domain.privacy import DEFAULT_PII_HASH_SALT
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
from src.use_cases.aggregate_gold import AggregateGoldUseCase
from src.use_cases.cleanse_silver import CleanseSilverUseCase
from src.use_cases.ingest_bronze import IngestBronzeUseCase
from src.use_cases.process_stream import ProcessStreamUseCase
from src.use_cases.query_serving import QueryServingUseCase
from src.use_cases.run_quality_gate import RunQualityGateUseCase
from src.use_cases.validate_contract import ValidateContractUseCase


class Container:
    """Dependency injection container assembling hexagonal components.

    Construction is side-effect free; adapters and use cases are built lazily
    on first attribute access.
    """

    def __init__(
        self,
        db_path: str | None = None,
        marquez_url: str | None = None,
        repo_root: str = ".",
        event_db_path: str | None = None,
        lineage_audit_path: str | None = None,
        lakehouse_backend: str | None = None,
    ) -> None:
        effective_db_path = db_path if db_path is not None else os.getenv("LAKEHOUSE_DB_PATH", "data/lakehouse.db")
        self._db_path = effective_db_path
        self._marquez_url = marquez_url or os.getenv("MARQUEZ_URL")
        self._repo_root = repo_root
        # Lakehouse implementation: "sqlite" (default, local/unit tests) or
        # "iceberg" (cluster), selected by environment.
        self._lakehouse_backend = (
            (lakehouse_backend if lakehouse_backend is not None else os.getenv("LAKEHOUSE_BACKEND", "sqlite"))
            .strip()
            .lower()
        )
        # Hermetic defaults: sibling files of the lakehouse DB.
        base_dir = os.path.dirname(effective_db_path) or "."
        self._event_db_path = event_db_path or os.getenv("EVENT_DB_PATH") or os.path.join(base_dir, "event_log.db")
        self._lineage_audit_path = (
            lineage_audit_path or os.getenv("LINEAGE_AUDIT_PATH") or os.path.join(base_dir, "lineage_audit.jsonl")
        )

    # Outbound adapters (lazy). Drivers depend on port abstractions.
    @functools.cached_property
    def storage(self) -> LakehouseStoragePort:
        if self._lakehouse_backend == "iceberg":
            return IcebergLakehouseStorageAdapter(catalog=_iceberg_catalog())
        if self._lakehouse_backend != "sqlite":
            raise ValueError(f"Unknown LAKEHOUSE_BACKEND '{self._lakehouse_backend}'. Expected 'sqlite' or 'iceberg'.")
        return LakehouseStorageAdapter(db_path=self._db_path)

    @functools.cached_property
    def source(self) -> OltpSourcePort:
        return OltpSourceAdapter()

    @functools.cached_property
    def stream_broker(self) -> EventBrokerPort:
        return EventStreamAdapter(db_path=self._event_db_path)

    @functools.cached_property
    def lineage(self) -> LineagePublisherPort:
        return MarquezLineageAdapter(
            marquez_url=self._marquez_url,
            audit_file_path=self._lineage_audit_path,
        )

    @functools.cached_property
    def quality_engine(self) -> QualityEnginePort:
        return ExpectationEngineAdapter()

    @functools.cached_property
    def serving_sql(self) -> QueryExecutorPort:
        return SqlServingAdapter(db_path=self._db_path)

    @functools.cached_property
    def domain_catalog(self) -> DomainCatalogPort:
        return FsDomainCatalogAdapter(repo_root=self._repo_root)

    # Inbound Use Cases (lazy)
    @functools.cached_property
    def ingest_bronze(self) -> IngestBronzePort:
        return IngestBronzeUseCase(
            source=self.source,
            storage=self.storage,
            lineage=self.lineage,
        )

    @functools.cached_property
    def cleanse_silver(self) -> CleanseSilverPort:
        return CleanseSilverUseCase(
            storage_reader=self.storage,
            storage_writer=self.storage,
            quality_engine=self.quality_engine,
            lineage=self.lineage,
            catalog=self.domain_catalog,
        )

    @functools.cached_property
    def aggregate_gold(self) -> AggregateGoldPort:
        return AggregateGoldUseCase(
            storage_reader=self.storage,
            storage_writer=self.storage,
            lineage=self.lineage,
            quality_engine=self.quality_engine,
            catalog=self.domain_catalog,
            privacy_salt=os.environ.get("PII_HASH_SALT", DEFAULT_PII_HASH_SALT),
        )

    @functools.cached_property
    def process_stream(self) -> ProcessStreamPort:
        return ProcessStreamUseCase(
            broker=self.stream_broker,
            storage=self.storage,
            lineage=self.lineage,
        )

    @functools.cached_property
    def query_serving(self) -> QueryServingPort:
        return QueryServingUseCase(
            query_executor=self.serving_sql,
        )

    @functools.cached_property
    def validate_contract(self) -> ValidateContractPort:
        return ValidateContractUseCase(catalog=self.domain_catalog)

    @functools.cached_property
    def run_quality_gate(self) -> RunQualityGatePort:
        return RunQualityGateUseCase(
            storage=self.storage,
            quality_engine=self.quality_engine,
            catalog=self.domain_catalog,
        )


# Global default container instance (lazy: no side effects on import)
container = Container()
