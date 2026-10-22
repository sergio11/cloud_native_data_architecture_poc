"""Modern Data Architecture POC - Streaming speed layer (Kappa) use case.

Consumes event streams, computes tumbling windows and updates the speed table.
"""

from __future__ import annotations

import logging
from typing import Literal

from src.domain.aggregations import BusinessAggregator
from src.domain.naming import stream_table
from src.ports.inbound import ProcessStreamPort, ProcessStreamResult
from src.ports.outbound import EventBrokerPort, LakehouseTableWriter, LineagePublisherPort

logger = logging.getLogger("use_case.process_stream")

CONSUMER_GROUP = "speed_layer"

#: Tumbling-window size in seconds (5-minute windows).
WINDOW_SIZE_SECONDS = 300

#: Incremental cursor batch cap per run (observability only).
CURSOR_OBSERVABILITY_BATCH = 500


class ProcessStreamUseCase(ProcessStreamPort):
    """Orchestrates stream window processing and Kappa replay logic.

    Incremental runs rebuild the whole speed table from the full log; the
    cursor is advanced only after a durable write.
    """

    def __init__(
        self,
        broker: EventBrokerPort,
        storage: LakehouseTableWriter,
        lineage: LineagePublisherPort,
    ) -> None:
        self._broker = broker
        self._storage = storage
        self._lineage = lineage

    def execute(
        self,
        topic: str = "sales.orders.events",
        target_table: str = stream_table(),
        replay: bool = False,
    ) -> ProcessStreamResult:
        """Consumes stream events and rebuilds/updates the speed table.

        Both modes recompute every tumbling window from the full log and
        overwrite the table; the cursor advances only after a successful write.
        """
        mode: Literal["replay_full", "append"] = "replay_full" if replay else "append"
        logger.info("Processing streaming speed layer on topic '%s' (replay=%s)...", topic, replay)

        # 1. Read the full log first without touching cursors.
        events = self._broker.consume_events(topic=topic, max_messages=None, replay=True)
        events_consumed = len(events)

        # 2. Compute 5-minute tumbling windows from event time
        windows = BusinessAggregator.aggregate_tumbling_window(events, window_size_seconds=WINDOW_SIZE_SECONDS)
        logger.info("Calculated %d window aggregations", len(windows))

        # 3. Write the speed table (full recompute on both modes).
        written = self._storage.write_table(target_table, windows, mode="overwrite")

        # 4. Emit lineage for the durable recompute.
        self._lineage.emit_job_event(
            job_name="streaming_orders_windowing",
            inputs=[f"kafka.{topic}"],
            outputs=[target_table],
            status="COMPLETE",
        )

        # 5. Advance the observability cursor only after the durable write.
        if not replay:
            incremental = self._broker.consume_events(
                topic=topic,
                max_messages=CURSOR_OBSERVABILITY_BATCH,
                replay=False,
                consumer_group=CONSUMER_GROUP,
            )
            logger.info("Consumed %d stream events from topic '%s'", len(incremental), topic)
            if not incremental:
                return {
                    "topic": topic,
                    "target_table": target_table,
                    "events_consumed": 0,
                    "windows_generated": len(windows),
                    "windows_written": written,
                    "mode": mode,
                    "status": "NO_EVENTS",
                }
            events_consumed = len(incremental)

        return {
            "topic": topic,
            "target_table": target_table,
            "events_consumed": events_consumed,
            "windows_generated": len(windows),
            "windows_written": written,
            "mode": mode,
            "status": "SUCCESS",
        }
