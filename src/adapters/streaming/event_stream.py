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
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from src.adapters._files import ensure_parent_dir
from src.adapters._sqlite import sqlite_connection
from src.ports.outbound import EventBrokerPort

logger = logging.getLogger("adapter.streaming.event_stream")

KAFKA_ENV_VAR = "KAFKA_BOOTSTRAP_SERVERS"
_POLL_TIMEOUT_SECONDS = 1.0
_MAX_EMPTY_POLLS = 2
_POLL_BUDGET_SECONDS = 30.0


class EventStreamAdapter(EventBrokerPort):
    """
    Event broker adapter supporting Kafka and an embedded local WAL event log.
    Ensures persistent replayability (RF-10) and durable consumer cursors so
    incremental consumption never returns already-processed events twice.

    Backend selection is driven by the ``KAFKA_BOOTSTRAP_SERVERS`` environment
    variable: when it is set and non-empty the adapter talks ONLY to Kafka,
    otherwise it uses ONLY the embedded SQLite log (no double writes and no
    silent fallbacks). The variable is read ONCE at construction and the
    backend is then fixed for the adapter's lifetime, so every publish and
    consume call hits the same backend (the SQLite schema is initialised only
    when that backend is selected). ``confluent_kafka`` is imported lazily
    inside the Kafka code paths so the SQLite profile never requires the
    package. An optional ``consumer_factory`` seam (a callable receiving the
    consumer config dict and returning a ``confluent_kafka.Consumer``-like
    object) allows tests to inject a fake consumer.
    """

    def __init__(
        self,
        db_path: str = "data/event_log.db",
        consumer_factory: Callable[..., Any] | None = None,
    ) -> None:
        self._db_path = db_path
        self._consumer_factory = consumer_factory
        # Backend decided ONCE here (never re-read per call): with the Kafka
        # backend selected this adapter talks ONLY to Kafka, so it must not
        # create local files or schemas as a side effect of construction.
        self._bootstrap = self._resolve_bootstrap()
        if self._bootstrap is None:
            ensure_parent_dir(db_path)
            self._init_db()

    def _init_db(self) -> None:
        with sqlite_connection(self._db_path) as conn:
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

    @staticmethod
    def _resolve_bootstrap() -> str | None:
        """Returns the configured Kafka bootstrap servers, or None for the SQLite backend."""
        value = os.getenv(KAFKA_ENV_VAR)
        if value is None or not value.strip():
            return None
        return value.strip()

    def publish_event(self, topic: str, key: str, payload: dict[str, Any]) -> bool:
        """Publishes to the backend selected at construction (Kafka or SQLite)."""
        if self._bootstrap is not None:
            self._publish_to_kafka(self._bootstrap, topic, key, payload)
            return True
        self._publish_to_sqlite(topic, key, payload)
        return True

    def _publish_to_sqlite(self, topic: str, key: str, payload: dict[str, Any]) -> None:
        # Persist to local immutable event log (guarantees Kappa replay RF-10)
        with sqlite_connection(self._db_path) as conn:
            conn.execute(
                "INSERT INTO event_log (topic, key, payload, published_at) VALUES (?, ?, ?, ?);",
                (topic, str(key), json.dumps(payload), datetime.now(UTC).isoformat()),
            )
            conn.commit()

    def _publish_to_kafka(self, bootstrap: str, topic: str, key: str, payload: dict[str, Any]) -> None:
        """Publishes a single event to Kafka, failing fast without any SQLite fallback."""
        try:
            from confluent_kafka import Producer

            producer = Producer({"bootstrap.servers": bootstrap})
            producer.produce(topic, key=str(key), value=json.dumps(payload))
            producer.poll(0)
            pending = producer.flush(10)
            if pending:
                raise RuntimeError(f"timed out delivering 1 event to topic '{topic}'")
        except Exception as exc:
            raise RuntimeError(
                f"Kafka publish failed for topic '{topic}' (bootstrap='{bootstrap}'); "
                f"check or unset {KAFKA_ENV_VAR} to use the local event log: {exc}"
            ) from exc

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

        Backend selection is fixed at construction: Kafka when
        ``KAFKA_BOOTSTRAP_SERVERS`` was set and non-empty at that moment
        (consuming ONLY from Kafka), SQLite otherwise. An unreachable Kafka
        broker raises ``RuntimeError`` (fail-fast, never a silent fallback
        to SQLite).
        """
        if self._bootstrap is not None:
            return self._consume_from_kafka(self._bootstrap, topic, max_messages, replay, consumer_group)
        return self._consume_from_sqlite(topic, max_messages, replay, consumer_group)

    def _consume_from_sqlite(
        self,
        topic: str,
        max_messages: int | None,
        replay: bool,
        consumer_group: str,
    ) -> list[dict[str, Any]]:
        with sqlite_connection(self._db_path) as conn:
            if not replay:
                # Serialize concurrent consumers of the same group: reading the
                # cursor and advancing it must be one atomic unit, otherwise two
                # consumers redeliver the same batch (TOCTOU on last_offset).
                conn.execute("BEGIN IMMEDIATE")
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

                query = "SELECT offset, payload FROM event_log WHERE topic = ? AND offset > ? ORDER BY offset ASC"
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
                    offset,
                    topic,
                    exc,
                )
        return events

    def _consume_from_kafka(
        self,
        bootstrap: str,
        topic: str,
        max_messages: int | None,
        replay: bool,
        consumer_group: str,
    ) -> list[dict[str, Any]]:
        """Consumes from Kafka preserving the EventBrokerPort cursor semantics.

        Incremental runs use ``group.id=<consumer_group>`` with
        ``auto.offset.reset=earliest`` and explicitly commit every consumed
        offset. ``earliest`` (not ``latest``) is required so a brand-new
        consumer group reads the existing backlog, exactly like the SQLite
        backend (unknown cursor starts at offset 0): the port promises
        "only events NOT yet consumed by consumer_group", and for a new
        group nothing has been consumed yet. Once offsets are committed,
        subsequent runs are incremental on both backends. Replay runs use an
        ephemeral group id with ``auto.offset.reset=earliest`` and never
        commit, so consumer cursors are left untouched.
        """
        if replay:
            group_id = f"{consumer_group}-replay-{uuid.uuid4().hex}"
        else:
            group_id = consumer_group
        # Both modes reset to earliest (see docstring): a group without
        # committed offsets reads the backlog like the SQLite backend.
        config: dict[str, Any] = {
            "bootstrap.servers": bootstrap,
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
        try:
            if self._consumer_factory is not None:
                consumer: Any = self._consumer_factory(config)
            else:
                from confluent_kafka import Consumer

                consumer = Consumer(config)
        except Exception as exc:
            raise RuntimeError(
                f"Kafka broker unreachable (bootstrap='{bootstrap}'); "
                f"check or unset {KAFKA_ENV_VAR} to use the local event log: {exc}"
            ) from exc

        try:
            try:
                consumer.list_topics(timeout=5)
            except Exception as exc:
                raise RuntimeError(
                    f"Kafka broker unreachable (bootstrap='{bootstrap}'); "
                    f"check or unset {KAFKA_ENV_VAR} to use the local event log: {exc}"
                ) from exc
            consumer.subscribe([topic])

            collected: list[tuple[int, dict[str, Any]]] = []
            consumed: list[Any] = []
            empty_streak = 0
            limit = int(max_messages) if max_messages is not None else None
            started = time.monotonic()
            while True:
                if limit is not None and len(collected) >= limit:
                    break
                if empty_streak >= _MAX_EMPTY_POLLS:
                    break
                if time.monotonic() - started >= _POLL_BUDGET_SECONDS:
                    logger.warning(
                        "Kafka poll budget exhausted on topic '%s'; returning %d events", topic, len(collected)
                    )
                    break
                message = consumer.poll(_POLL_TIMEOUT_SECONDS)
                if message is None:
                    empty_streak += 1
                    continue
                if message.error():
                    logger.warning("Skipping Kafka error on topic '%s': %s", topic, message.error())
                    empty_streak += 1
                    continue
                empty_streak = 0
                consumed.append(message)
                raw = message.value()
                if raw is None:
                    logger.warning("Skipping Kafka tombstone at offset %s on topic '%s'", message.offset(), topic)
                    continue
                try:
                    text = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
                    payload = json.loads(text)
                except (TypeError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                    logger.warning(
                        "Skipping corrupt event payload at offset %s on topic '%s': %s",
                        message.offset(),
                        topic,
                        exc,
                    )
                    continue
                collected.append((int(message.offset()), payload))

            collected.sort(key=lambda item: item[0])
            if not replay:
                # Explicitly commit every consumed offset (including skipped
                # corrupt frames, mirroring the SQLite cursor which advances
                # past them) in ascending order — the order of the returned
                # events — so poison pills are never redelivered forever.
                consumed.sort(key=lambda message: int(message.offset()))
                for message in consumed:
                    try:
                        consumer.commit(message=message)
                    except Exception as exc:
                        logger.warning("Kafka commit failed on topic '%s': %s", topic, exc)
            return [payload for _, payload in collected]
        finally:
            try:
                consumer.close()
            except Exception as exc:
                logger.warning("Error closing Kafka consumer for topic '%s': %s", topic, exc)
