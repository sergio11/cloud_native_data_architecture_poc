"""
Modern Data Architecture POC - Event Stream Adapter.
Implements EventBrokerPort with support for Kafka and an embedded persistent event log.
Fulfills Kappa Architecture requirement RF-10 (replay from earliest offset,
persistent per-consumer-group cursors, never fabricating events).
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.ports.outbound import EventBrokerPort

logger = logging.getLogger("adapter.streaming.event_stream")


class EventStreamAdapter(EventBrokerPort):
    """
    Event broker adapter supporting Kafka and an embedded local WAL event log.
    Ensures persistent replayability (RF-10) and durable consumer cursors so
    incremental consumption never returns already-processed events twice.
    """

    def __init__(self, db_path: str = "data/event_log.db") -> None:
        self._db_path = db_path
        Path(os.path.dirname(db_path) or ".").mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self) -> None:
        with sqlite3.connect(self._db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS event_log (
                    offset INTEGER PRIMARY KEY AUTOINCREMENT,
                    topic TEXT NOT NULL,
                    key TEXT,
                    payload TEXT NOT NULL,
                    published_at TEXT NOT NULL
                );
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS consumer_offsets (
                    topic TEXT NOT NULL,
                    consumer_group TEXT NOT NULL,
                    last_offset INTEGER NOT NULL,
                    PRIMARY KEY (topic, consumer_group)
                );
            """)
            conn.commit()

    def publish_event(self, topic: str, key: str, payload: dict[str, Any]) -> bool:
        # 1. Try Kafka if broker is active (never silent on failure - RF-10)
        try:
            from confluent_kafka import Producer
            producer = Producer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")})
            producer.produce(topic, key=str(key), value=json.dumps(payload))
            producer.poll(0)
            producer.flush(1)
        except Exception as exc:
            logger.warning("Kafka publish skipped (%s); persisting to local event log", exc)

        # 2. Persist to local immutable event log (guarantees Kappa replay RF-10)
        with sqlite3.connect(self._db_path) as conn:
            conn.execute(
                "INSERT INTO event_log (topic, key, payload, published_at) VALUES (?, ?, ?, ?);",
                (topic, str(key), json.dumps(payload), datetime.now(UTC).isoformat())
            )
            conn.commit()
        return True

    def consume_events(
        self,
        topic: str,
        max_messages: int | None = None,
        replay: bool = False,
        consumer_group: str = "default",
    ) -> list[dict[str, Any]]:
        """Consumes events per EventBrokerPort contract (RF-10).

        replay=False: reads events with offset > the committed cursor of
        ``consumer_group`` (ascending) and atomically advances that cursor to
        the highest offset returned, in the same transaction.
        replay=True: reads the full topic log from offset 0 (ascending) and
        never touches consumer cursors. Never fabricates events when empty.
        """
        with sqlite3.connect(self._db_path) as conn:
            if replay:
                query = "SELECT offset, payload FROM event_log WHERE topic = ? ORDER BY offset ASC"
                params: list[Any] = [topic]
                if max_messages is not None:
                    query += " LIMIT ?"
                    params.append(int(max_messages))
                rows = conn.execute(query, params).fetchall()
            else:
                cursor_row = conn.execute(
                    "SELECT last_offset FROM consumer_offsets WHERE topic = ? AND consumer_group = ?;",
                    (topic, consumer_group),
                ).fetchone()
                committed = int(cursor_row[0]) if cursor_row else 0

                query = (
                    "SELECT offset, payload FROM event_log "
                    "WHERE topic = ? AND offset > ? ORDER BY offset ASC"
                )
                params = [topic, committed]
                if max_messages is not None:
                    query += " LIMIT ?"
                    params.append(int(max_messages))
                rows = conn.execute(query, params).fetchall()

                if rows:
                    last_offset = max(int(r[0]) for r in rows)
                    conn.execute(
                        """
                        INSERT INTO consumer_offsets (topic, consumer_group, last_offset)
                        VALUES (?, ?, ?)
                        ON CONFLICT(topic, consumer_group)
                        DO UPDATE SET last_offset = excluded.last_offset;
                        """,
                        (topic, consumer_group, last_offset),
                    )
                    conn.commit()

        events: list[dict[str, Any]] = []
        for offset, payload in rows:
            try:
                events.append(json.loads(payload))
            except (TypeError, json.JSONDecodeError) as exc:
                logger.warning(
                    "Skipping corrupt event payload at offset %s on topic '%s': %s",
                    offset, topic, exc,
                )
        return events
