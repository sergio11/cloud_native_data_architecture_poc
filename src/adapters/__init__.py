"""Modern Data Architecture POC - Adapters package.

Storage, source, streaming, lineage and serving adapters.
"""

from src.adapters.lineage.marquez_lineage import MarquezLineageAdapter
from src.adapters.quality.expectation_engine import ExpectationEngineAdapter
from src.adapters.serving.sql_serving import SqlServingAdapter
from src.adapters.source.oltp_source import OltpSourceAdapter
from src.adapters.storage.lakehouse_storage import LakehouseStorageAdapter
from src.adapters.streaming.event_stream import EventStreamAdapter

__all__ = [
    "LakehouseStorageAdapter",
    "OltpSourceAdapter",
    "EventStreamAdapter",
    "MarquezLineageAdapter",
    "ExpectationEngineAdapter",
    "SqlServingAdapter",
]
