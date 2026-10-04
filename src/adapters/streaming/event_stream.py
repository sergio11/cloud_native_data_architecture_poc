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
#: Poll rounds spent waiting for the broker to assign partitions on replay
#: before falling back to the no-watermark drain. Fake consumers answer
#: immediately; each round blocks at most ``_POLL_TIMEOUT_SECONDS``.
_ASSIGNMENT_WAIT_POLLS = 10


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

        Replay termination: after ``subscribe`` the adapter waits for the
        partition assignment and snapshots the high-watermark end-offsets
        with ``consumer.get_watermark_offsets``; the drain ends when the
        consumed offsets reach those end-offsets on every assigned partition
        (frozen log-end at replay start, so events appended concurrently
        afterwards may be excluded). Neither the poll time budget nor an
        empty-poll streak truncates a watermark-driven replay. When the
        injected consumer exposes no watermark API, replay falls back to
        draining until ``_MAX_EMPTY_POLLS`` consecutive empty polls, still
        ignoring the time budget.
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

            limit = int(max_messages) if max_messages is not None else None
            if replay:
                return self._consume_replay_from_kafka(consumer, topic, limit)
            return self._consume_incremental_from_kafka(consumer, topic, limit)
        finally:
            try:
                consumer.close()
            except Exception as exc:
                logger.warning("Error closing Kafka consumer for topic '%s': %s", topic, exc)

    def _consume_incremental_from_kafka(
        self,
        consumer: Any,
        topic: str,
        limit: int | None,
    ) -> list[dict[str, Any]]:
        """Drains new events for a consumer group (budget + empty streak apply).

        Unchanged incremental semantics: stops at ``max_messages``, after
        ``_MAX_EMPTY_POLLS`` consecutive empty polls, or when the
        ``_POLL_BUDGET_SECONDS`` budget is exhausted, then explicitly commits
        every consumed offset (including skipped corrupt frames, mirroring
        the SQLite cursor which advances past them) in ascending order.
        """
        collected: list[tuple[int, dict[str, Any]]] = []
        consumed: list[Any] = []
        empty_streak = 0
        started = time.monotonic()
        while True:
            if limit is not None and len(collected) >= limit:
                break
            if empty_streak >= _MAX_EMPTY_POLLS:
                break
            if time.monotonic() - started >= _POLL_BUDGET_SECONDS:
                logger.warning("Kafka poll budget exhausted on topic '%s'; returning %d events", topic, len(collected))
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

    def _consume_replay_from_kafka(
        self,
        consumer: Any,
        topic: str,
        limit: int | None,
    ) -> list[dict[str, Any]]:
        """Drains the full topic log from offset 0 without committing.

        Watermark path: ends when consumed offsets reach the end-offsets
        snapshotted at assignment on every assigned partition — never via
        the time budget or an empty-poll streak, so large backlogs and slow
        producers with delivery gaps are returned in full. Fallback path
        (no watermark API on the consumer): drains until
        ``_MAX_EMPTY_POLLS`` consecutive empty polls, still ignoring the
        time budget.
        """
        prefetched: list[Any] = []
        assignment = self._wait_kafka_assignment(consumer, topic, prefetched)
        end_offsets = self._kafka_end_offsets(consumer, assignment) if assignment else None
        if end_offsets is None:
            logger.info(
                "Kafka replay on topic '%s' without watermark support; draining until %d empty polls",
                topic,
                _MAX_EMPTY_POLLS,
            )
            return self._replay_fallback_consume(consumer, topic, limit, prefetched)
        seen: dict[tuple[str, int], int] = {}
        collected: list[tuple[int, dict[str, Any]]] = []
        for message in prefetched:
            if limit is not None and len(collected) >= limit:
                break
            self._accumulate_replay_message(message, topic, collected, seen)
        while not self._replay_complete(seen, end_offsets):
            if limit is not None and len(collected) >= limit:
                break
            message = consumer.poll(_POLL_TIMEOUT_SECONDS)
            if message is None:
                continue
            if message.error():
                logger.warning("Skipping Kafka error on topic '%s': %s", topic, message.error())
                continue
            self._accumulate_replay_message(message, topic, collected, seen)
        collected.sort(key=lambda item: item[0])
        return [payload for _, payload in collected]

    def _replay_fallback_consume(
        self,
        consumer: Any,
        topic: str,
        limit: int | None,
        prefetched: list[Any],
    ) -> list[dict[str, Any]]:
        """Replay drain without watermarks: empty-poll streak only, no budget."""
        collected: list[tuple[int, dict[str, Any]]] = []
        pending = list(prefetched)
        empty_streak = 0
        while True:
            if limit is not None and len(collected) >= limit:
                break
            if empty_streak >= _MAX_EMPTY_POLLS:
                break
            if pending:
                message = pending.pop(0)
            else:
                message = consumer.poll(_POLL_TIMEOUT_SECONDS)
            if message is None:
                empty_streak += 1
                continue
            if message.error():
                logger.warning("Skipping Kafka error on topic '%s': %s", topic, message.error())
                empty_streak += 1
                continue
            empty_streak = 0
            self._accumulate_replay_message(message, topic, collected, {})
        collected.sort(key=lambda item: item[0])
        return [payload for _, payload in collected]

    def _accumulate_replay_message(
        self,
        message: Any,
        topic: str,
        collected: list[tuple[int, dict[str, Any]]],
        seen: dict[tuple[str, int], int],
    ) -> None:
        """Tracks replay progress and appends decodable payloads (never commits)."""
        try:
            offset = int(message.offset())
        except Exception as exc:
            logger.warning("Skipping Kafka message without offset on topic '%s': %s", topic, exc)
            return
        position = self._message_position(message, topic)
        if position in seen:
            seen[position] = max(seen[position], offset)
        else:
            seen[position] = offset
        raw = message.value()
        if raw is None:
            logger.warning("Skipping Kafka tombstone at offset %s on topic '%s'", offset, topic)
            return
        try:
            text = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
            payload = json.loads(text)
        except (TypeError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            logger.warning(
                "Skipping corrupt event payload at offset %s on topic '%s': %s",
                offset,
                topic,
                exc,
            )
            return
        collected.append((offset, payload))

    def _wait_kafka_assignment(
        self,
        consumer: Any,
        topic: str,
        prefetched: list[Any],
    ) -> list[Any]:
        """Polls until the broker assigns topic partitions, buffering early messages.

        Messages arriving before the assignment is visible are appended to
        ``prefetched`` so the replay never loses them; errors and empty
        polls are skipped. Returns the topic assignment (possibly empty).
        """
        assignment_fn = getattr(consumer, "assignment", None)
        if assignment_fn is None:
            return []
        for _ in range(_ASSIGNMENT_WAIT_POLLS):
            try:
                current = assignment_fn()
            except Exception:
                return []
            assignment = [tp for tp in (current or []) if self._tp_topic(tp, topic) == topic]
            if assignment:
                return assignment
            message = consumer.poll(_POLL_TIMEOUT_SECONDS)
            if message is None:
                continue
            if message.error():
                logger.warning("Skipping Kafka error on topic '%s': %s", topic, message.error())
                continue
            prefetched.append(message)
        try:
            current = assignment_fn()
        except Exception:
            return []
        return [tp for tp in (current or []) if self._tp_topic(tp, topic) == topic]

    def _kafka_end_offsets(self, consumer: Any, assignment: list[Any]) -> dict[tuple[str, int], tuple[int, int]] | None:
        """Snapshots (low, high) watermarks per assigned partition, or None when unavailable."""
        watermark_fn = getattr(consumer, "get_watermark_offsets", None)
        if watermark_fn is None:
            return None
        ends: dict[tuple[str, int], tuple[int, int]] = {}
        for tp in assignment:
            key = (self._tp_topic(tp, ""), self._tp_partition(tp))
            try:
                try:
                    low, high = watermark_fn(tp, timeout=5.0)
                except TypeError:
                    low, high = watermark_fn(tp)
            except Exception as exc:
                logger.warning("Kafka watermark lookup failed for %s; using replay fallback: %s", key, exc)
                return None
            ends[key] = (int(low), int(high))
        return ends

    @staticmethod
    def _replay_complete(
        seen: dict[tuple[str, int], int],
        end_offsets: dict[tuple[str, int], tuple[int, int]],
    ) -> bool:
        """True once every assigned partition reached its snapshotted high watermark."""
        for key, (low, high) in end_offsets.items():
            if seen.get(key, int(low) - 1) + 1 < int(high):
                return False
        return True

    @staticmethod
    def _tp_topic(tp: Any, default: str) -> str:
        """Reads a TopicPartition topic, supporting attribute or method shapes."""
        name = getattr(tp, "topic", None)
        if callable(name):
            try:
                return str(name())
            except Exception:
                return default
        return str(name) if name is not None else default

    @staticmethod
    def _tp_partition(tp: Any) -> int:
        """Reads a TopicPartition partition number, supporting attribute or method shapes."""
        part = getattr(tp, "partition", None)
        if callable(part):
            try:
                return int(part())
            except Exception:
                return 0
        try:
            return int(part) if part is not None else 0
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _message_position(message: Any, topic: str) -> tuple[str, int]:
        """Resolves the (topic, partition) of a message across consumer shapes."""
        name = getattr(message, "topic", None)
        if callable(name):
            try:
                resolved = str(name())
            except Exception:
                resolved = topic
        elif name is not None:
            resolved = str(name)
        else:
            resolved = topic
        part = getattr(message, "partition", None)
        if callable(part):
            try:
                partition = int(part())
            except Exception:
                partition = 0
        elif part is not None:
            try:
                partition = int(part)
            except (TypeError, ValueError):
                partition = 0
        else:
            partition = 0
        return (resolved, partition)
