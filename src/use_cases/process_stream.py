"""
Modern Data Architecture POC - Real-time Streaming Speed Layer (Kappa) Use Case.
Consumes continuous event streams, calculates tumbling windows,
and updates the Speed layer table with support for log replay (RF-10).
"""

from __future__ import annotations

import logging
from typing import Any

from src.domain.aggregations import BusinessAggregator
from src.ports.outbound import EventBrokerPort, LakehouseStoragePort, LineagePublisherPort

logger = logging.getLogger("use_case.process_stream")

CONSUMER_GROUP = "speed_layer"


class ProcessStreamUseCase:
    """Orchestrates stream window processing and Kappa replay logic."""

    def __init__(
        self,
        broker: EventBrokerPort,
        storage: LakehouseStoragePort,
        lineage: LineagePublisherPort,
    ) -> None:
        self._broker = broker
        self._storage = storage
        self._lineage = lineage

    def execute(
        self,
        topic: str = "sales.orders.events",
        target_table: str = "stream.sales.orders_windowed",
        replay: bool = False,
    ) -> dict[str, Any]:
        """Consumes stream events and rebuilds/updates the speed buffer table.

        replay=False: consumes only not-yet-consumed events for the
        ``speed_layer`` consumer group and appends their windows; when there is
        nothing new it returns ``NO_EVENTS`` without writing anything.

        replay=True: FULL RECOMPUTE (RF-10) - rewinds to offset 0, consumes the
        entire topic log, recomputes every tumbling window from scratch and
        overwrites the whole speed table deterministically (idempotent,
        no row growth across repeated replays).
        """
        mode = "replay_full" if replay else "append"
        logger.info("Processing streaming speed layer on topic '%s' (replay=%s)...", topic, replay)

        # 1. Consume events from the persistent log (replay rewinds to earliest offset RF-10)
        if replay:
            events = self._broker.consume_events(topic=topic, max_messages=None, replay=True)
        else:
            events = self._broker.consume_events(
                topic=topic, max_messages=500, replay=False, consumer_group=CONSUMER_GROUP,
            )
        logger.info("Consumed %d stream events from topic '%s'", len(events), topic)

        if not events:
            return {
                "topic": topic,
                "target_table": target_table,
                "events_consumed": 0,
                "windows_generated": 0,
                "windows_written": 0,
                "mode": mode,
                "status": "NO_EVENTS",
            }

        # 2. Compute 5-minute tumbling windows from event time (RF-09)
        windows = BusinessAggregator.aggregate_tumbling_window(events, window_size_seconds=300)
        logger.info("Calculated %d window aggregations", len(windows))

        # 3. Write/Update Speed layer table (append incremental, overwrite on full replay)
        write_mode = "overwrite" if replay else "append"
        written = self._storage.write_table(target_table, windows, mode=write_mode)

        # 4. Emit OpenLineage metadata
        self._lineage.emit_job_event(
            job_name="streaming_orders_windowing",
            inputs=[f"kafka.{topic}"],
            outputs=[target_table],
            status="COMPLETE",
        )

        return {
            "topic": topic,
            "target_table": target_table,
            "events_consumed": len(events),
            "windows_generated": len(windows),
            "windows_written": written,
            "mode": mode,
            "status": "SUCCESS",
        }
