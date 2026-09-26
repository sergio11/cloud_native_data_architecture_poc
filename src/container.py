"""
Modern Data Architecture POC - Composition Root & Dependency Injection Container.
Wires domain use cases with infrastructure adapters according to Dependency Inversion Principle (DIP).
"""

from __future__ import annotations

from src.adapters.catalog.fs_domain_catalog import FsDomainCatalogAdapter
from src.adapters.lineage.marquez_lineage import MarquezLineageAdapter
from src.adapters.quality.expectation_engine import ExpectationEngineAdapter
from src.adapters.serving.sql_serving import SqlServingAdapter
from src.adapters.source.oltp_source import OltpSourceAdapter
from src.adapters.storage.lakehouse_storage import LakehouseStorageAdapter
from src.adapters.streaming.event_stream import EventStreamAdapter
from src.use_cases.aggregate_gold import AggregateGoldUseCase
from src.use_cases.cleanse_silver import CleanseSilverUseCase
from src.use_cases.ingest_bronze import IngestBronzeUseCase
from src.use_cases.process_stream import ProcessStreamUseCase
from src.use_cases.query_serving import QueryServingUseCase
from src.use_cases.run_quality_gate import RunQualityGateUseCase
from src.use_cases.validate_contract import ValidateContractUseCase


class Container:
    """Dependency injection container assembling hexagonal components."""

    def __init__(
        self,
        db_path: str = "data/lakehouse.db",
        marquez_url: str | None = None,
        repo_root: str = ".",
    ) -> None:
        # Outbound Adapters
        self.storage = LakehouseStorageAdapter(db_path=db_path)
        self.source = OltpSourceAdapter()
        self.stream_broker = EventStreamAdapter()
        self.lineage = MarquezLineageAdapter(marquez_url=marquez_url)
        self.quality_engine = ExpectationEngineAdapter()
        self.serving_sql = SqlServingAdapter(db_path=db_path)
        self.domain_catalog = FsDomainCatalogAdapter(repo_root=repo_root)

        # Inbound Use Cases
        self.ingest_bronze = IngestBronzeUseCase(
            source=self.source,
            storage=self.storage,
            lineage=self.lineage,
        )
        self.cleanse_silver = CleanseSilverUseCase(
            storage=self.storage,
            quality_engine=self.quality_engine,
            lineage=self.lineage,
        )
        self.aggregate_gold = AggregateGoldUseCase(
            storage=self.storage,
            lineage=self.lineage,
        )
        self.process_stream = ProcessStreamUseCase(
            broker=self.stream_broker,
            storage=self.storage,
            lineage=self.lineage,
        )
        self.query_serving = QueryServingUseCase(
            query_executor=self.serving_sql,
        )
        self.validate_contract = ValidateContractUseCase(catalog=self.domain_catalog)
        self.run_quality_gate = RunQualityGateUseCase(
            storage=self.storage,
            quality_engine=self.quality_engine,
        )


# Global default container instance
container = Container()
