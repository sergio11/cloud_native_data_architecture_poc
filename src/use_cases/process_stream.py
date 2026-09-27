"""
Modern Data Architecture POC - Real-time Streaming Speed Layer (Kappa) Use Case.
Consumes continuous event streams, calculates tumbling windows,
and updates the Speed layer table with support for log replay (RF-10).
"""

from __future__ import annotations

import logging
from typing import Any

from src.domain.aggregations import BusinessAggregator
from src.domain.naming import stream_table
from src.ports.inbound import ProcessStreamPort
from src.ports.outbound import EventBrokerPort, LakehouseStoragePort, LineagePublisherPort

logger = logging.getLogger("use_case.process_stream")

CONSUMER_GROUP = "speed_layer"


class ProcessStreamUseCase(ProcessStreamPort):
    """Orchestrates stream window processing and Kappa replay logic.

    Incremental runs rebuild the whole speed table from the full event log
    (overwrite) so that tumbling windows shared across batches never fragment
    into partial rows. The ``speed_layer`` cursor is advanced for
    observability ONLY after the speed table has been durably written: the
    full log is first re-consumed with ``replay=True`` (which never touches
    cursors), windows are recomputed and written, and only then is the log
    consumed incrementally to count newly arrived events
    (``events_consumed``). A write failure therefore leaves the cursor
    untouched and the next run still materializes the pending windows.
    """

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
        target_table: str = stream_table(),
        replay: bool = False,
    ) -> dict[str, Any]:
        """Consumes stream events and rebuilds/updates the speed buffer table.

        replay=False (incremental): the FULL speed table is rebuilt
        deterministically from the entire topic log re-consumed with
        ``replay=True`` (which never touches cursors) via
        ``BusinessAggregator.aggregate_tumbling_window`` and written with
        ``mode='overwrite'``. Only AFTER the write succeeds is the log
        consumed incrementally (``max_messages=500``) to advance the
        ``speed_layer`` cursor for observability and count newly arrived
        events (``events_consumed``); when there is nothing new it returns
        ``NO_EVENTS`` (the idempotent rewrite already applied changes
        nothing). ``windows_generated`` / ``windows_written`` therefore
        report full-table recompute counts, not just the delta of the
        latest batch.

        Rationale: the event log is tiny at POC scale, so a full recompute per
        incremental run is cheap, idempotent and eliminates partial-window
        fragmentation (events of the same window arriving in different runs
        would otherwise produce multiple partial rows for the same
        ``(window_start, status)`` key). Advancing the cursor strictly after
        a successful write guarantees that a failed run is safely retried by
        the next execution instead of being skipped as ``NO_EVENTS``.

        replay=True: FULL RECOMPUTE (RF-10) - rewinds to offset 0, consumes the
        entire topic log, recomputes every tumbling window from scratch and
        overwrites the whole speed table deterministically (idempotent,
        no row growth across repeated replays).
        """
        mode = "replay_full" if replay else "append"
        logger.info("Processing streaming speed layer on topic '%s' (replay=%s)...", topic, replay)

        # 1. Consume events from the persistent log (replay rewinds to earliest offset RF-10).
        # Incremental runs read the full log first WITHOUT touching cursors so a
        # downstream write failure never leaves the cursor falsely advanced.
        events = self._broker.consume_events(topic=topic, max_messages=None, replay=True)
        if not replay and not events:
            logger.info("No events on topic '%s'; nothing to materialize", topic)
            return {
                "topic": topic,
                "target_table": target_table,
                "events_consumed": 0,
                "windows_generated": 0,
                "windows_written": 0,
                "mode": mode,
                "status": "NO_EVENTS",
            }
        events_consumed = len(events)

        # 2. Compute 5-minute tumbling windows from event time (RF-09)
        windows = BusinessAggregator.aggregate_tumbling_window(events, window_size_seconds=300)
        logger.info("Calculated %d window aggregations", len(windows))

        # 3. Write Speed layer table (overwrite on both replay and incremental
        # full recompute; incremental overwrites to avoid partial-window fragmentation)
        written = self._storage.write_table(target_table, windows, mode="overwrite")

        # 4. Advance the observability cursor ONLY after the durable write. When
        # there is nothing new the idempotent rewrite above changed nothing, so
        # report NO_EVENTS with zeroed counters.
        if not replay:
            incremental = self._broker.consume_events(
                topic=topic, max_messages=500, replay=False, consumer_group=CONSUMER_GROUP,
            )
            logger.info("Consumed %d stream events from topic '%s'", len(incremental), topic)
            if not incremental:
                return {
                    "topic": topic,
                    "target_table": target_table,
                    "events_consumed": 0,
                    "windows_generated": 0,
                    "windows_written": 0,
                    "mode": mode,
                    "status": "NO_EVENTS",
                }
            events_consumed = len(incremental)

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
            "events_consumed": events_consumed,
            "windows_generated": len(windows),
            "windows_written": written,
            "mode": mode,
            "status": "SUCCESS",
        }
