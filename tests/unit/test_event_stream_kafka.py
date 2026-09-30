"""
Modern Data Architecture POC - Unit Tests: Kafka event-stream backend.
Verifies backend selection via KAFKA_BOOTSTRAP_SERVERS and real Kafka consumption
semantics (ascending order, max_messages limit, explicit incremental commits, no
commits on replay, corrupt-payload skip, fail-fast without broker, no double
writes) using a fake confluent_kafka consumer injected through the
consumer_factory seam. Every test runs against isolated databases under tmp_path.
"""

from __future__ import annotations

import json
import sys
import types
from pathlib import Path
from typing import Any

import pytest

from src.adapters.streaming.event_stream import EventStreamAdapter

TOPIC = "unit.kafka"


class FakeMessage:
    """Minimal confluent_kafka.Message stub (valid, corrupt or error frames)."""

    def __init__(
        self,
        topic: str,
        offset: int,
        value: bytes | None,
        error: Any | None = None,
        partition: int = 0,
    ) -> None:
        self._topic = topic
        self._offset = offset
        self._value = value
        self._error = error
        self._partition = partition

    def value(self) -> bytes | None:
        return self._value

    def key(self) -> bytes | None:
        return None

    def offset(self) -> int:
        return self._offset

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def error(self) -> Any | None:
        return self._error


class FakeConsumer:
    """confluent_kafka.Consumer stub backed by a scripted frame list."""

    def __init__(self, config: dict[str, Any], frames: list[Any], fail_list_topics: bool = False) -> None:
        self.config = dict(config)
        self._frames = list(frames)
        self._fail_list_topics = fail_list_topics
        self.subscribed: list[str] = []
        self.committed_offsets: list[int] = []
        self.closed = False

    def list_topics(self, timeout: float | None = None) -> dict[str, Any]:
        if self._fail_list_topics:
            raise Exception("Failed to connect to bootstrap: connection refused")
        return {"topics": {}}

    def subscribe(self, topics: list[str], **kwargs: Any) -> None:
        self.subscribed = list(topics)

    def poll(self, timeout: float | None = None) -> Any | None:
        if not self._frames:
            return None
        return self._frames.pop(0)

    def commit(self, message: Any | None = None, offsets: Any | None = None, asynchronous: bool = False) -> None:
        if message is not None:
            self.committed_offsets.append(int(message.offset()))

    def close(self) -> None:
        self.closed = True


def _valid_frame(topic: str, offset: int, seq: int) -> FakeMessage:
    return FakeMessage(topic, offset, json.dumps({"seq": seq}).encode("utf-8"))


def _make_factory(frames: list[Any], instances: list[FakeConsumer], fail: bool = False, fail_list: bool = False):
    """Builds a consumer_factory returning scripted FakeConsumers (or raising to simulate outage)."""

    def _create(config: dict[str, Any]) -> FakeConsumer:
        if fail:
            raise Exception("Failed to connect to bootstrap: connection refused")
        consumer = FakeConsumer(config, frames, fail_list_topics=fail_list)
        instances.append(consumer)
        return consumer

    return _create


def test_sqlite_backend_without_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Without KAFKA_BOOTSTRAP_SERVERS the adapter uses SQLite and never touches Kafka."""
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)

    def _exploding_factory(config: dict[str, Any]) -> Any:
        raise AssertionError("consumer_factory must not be used on the sqlite path")

    db_path = str(tmp_path / "event_log.db")
    broker = EventStreamAdapter(db_path=db_path, consumer_factory=_exploding_factory)
    assert broker.publish_event(TOPIC, "k1", {"seq": 1}) is True
    assert broker.consume_events(TOPIC, consumer_group="g") == [{"seq": 1}]
    assert broker.consume_events(TOPIC, consumer_group="g") == []  # cursor advanced, nothing fabricated


def test_kafka_incremental_orders_and_commits(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Incremental Kafka runs return ascending offsets and commit every offset delivered.

    A brand-new consumer group starts at ``earliest`` (not ``latest``) so it
    reads the existing backlog, mirroring the SQLite backend where an unknown
    cursor starts at offset 0 (EventBrokerPort contract).
    """
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    frames = [_valid_frame(TOPIC, 2, 2), _valid_frame(TOPIC, 0, 0), _valid_frame(TOPIC, 1, 1)]
    instances: list[FakeConsumer] = []
    broker = EventStreamAdapter(db_path=str(tmp_path / "e.db"), consumer_factory=_make_factory(frames, instances))

    events = broker.consume_events(TOPIC, consumer_group="speed_layer")
    assert [e["seq"] for e in events] == [0, 1, 2]  # sorted by offset before returning
    assert instances[0].subscribed == [TOPIC]
    assert instances[0].config["group.id"] == "speed_layer"
    assert instances[0].config["auto.offset.reset"] == "earliest"
    assert instances[0].config["enable.auto.commit"] is False
    assert instances[0].committed_offsets == [0, 1, 2]
    assert instances[0].closed is True


def test_kafka_replay_never_commits(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Replay uses an ephemeral group id with earliest reset and never commits cursors."""
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    frames = [_valid_frame(TOPIC, 1, 1), _valid_frame(TOPIC, 0, 0)]
    instances: list[FakeConsumer] = []
    broker = EventStreamAdapter(db_path=str(tmp_path / "e.db"), consumer_factory=_make_factory(frames, instances))

    events = broker.consume_events(TOPIC, replay=True, consumer_group="speed_layer")
    assert [e["seq"] for e in events] == [0, 1]
    assert instances[0].config["group.id"].startswith("speed_layer-replay-")
    assert instances[0].config["group.id"] != "speed_layer"
    assert instances[0].config["auto.offset.reset"] == "earliest"
    assert instances[0].committed_offsets == []
    assert instances[0].closed is True


def test_kafka_max_messages_limit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """max_messages caps the batch of returned (and committed) events."""
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    frames = [_valid_frame(TOPIC, i, i) for i in range(5)]
    instances: list[FakeConsumer] = []
    broker = EventStreamAdapter(db_path=str(tmp_path / "e.db"), consumer_factory=_make_factory(frames, instances))

    events = broker.consume_events(TOPIC, max_messages=2, consumer_group="g")
    assert [e["seq"] for e in events] == [0, 1]
    assert instances[0].committed_offsets == [0, 1]


def test_kafka_corrupt_payload_skipped(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Corrupt payloads are warned about and skipped, valid events are still returned in order."""
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    frames: list[Any] = [
        _valid_frame(TOPIC, 0, 0),
        FakeMessage(TOPIC, 1, b"\x00\x01 not-json {{{"),
        _valid_frame(TOPIC, 2, 2),
    ]
    instances: list[FakeConsumer] = []
    broker = EventStreamAdapter(db_path=str(tmp_path / "e.db"), consumer_factory=_make_factory(frames, instances))

    events = broker.consume_events(TOPIC, consumer_group="g")
    assert [e["seq"] for e in events] == [0, 2]


def test_kafka_fail_fast_without_broker(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Unreachable broker raises RuntimeError naming KAFKA_BOOTSTRAP_SERVERS (no silent fallback)."""
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    instances: list[FakeConsumer] = []
    broker = EventStreamAdapter(
        db_path=str(tmp_path / "e.db"), consumer_factory=_make_factory([], instances, fail=True)
    )

    with pytest.raises(RuntimeError, match="KAFKA_BOOTSTRAP_SERVERS"):
        broker.consume_events(TOPIC, consumer_group="g")

    broker_list_fail = EventStreamAdapter(
        db_path=str(tmp_path / "e2.db"), consumer_factory=_make_factory([], instances, fail_list=True)
    )
    with pytest.raises(RuntimeError, match="KAFKA_BOOTSTRAP_SERVERS"):
        broker_list_fail.consume_events(TOPIC, replay=True)


def test_kafka_publish_no_double_write(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Kafka publish goes ONLY to Kafka: no local SQLite file is even created."""
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

    produced: list[dict[str, Any]] = []

    class FakeProducer:
        def __init__(self, config: dict[str, Any]) -> None:
            self.config = config

        def produce(self, topic: str, key: str | None = None, value: str | None = None, **kwargs: Any) -> None:
            produced.append({"topic": topic, "key": key, "value": value})

        def poll(self, timeout: float | None = None) -> int:
            return 0

        def flush(self, timeout: float | None = None) -> int:
            return 0

    fake_module = types.SimpleNamespace(Producer=FakeProducer, Consumer=FakeConsumer)
    monkeypatch.setitem(sys.modules, "confluent_kafka", fake_module)

    db_path = str(tmp_path / "event_log.db")
    broker = EventStreamAdapter(db_path=db_path)
    assert broker.publish_event(TOPIC, "k1", {"seq": 9}) is True
    assert len(produced) == 1 and produced[0]["topic"] == TOPIC
    assert json.loads(produced[0]["value"]) == {"seq": 9}
    assert not Path(db_path).exists()  # no SQLite side effects at all


def test_sqlite_publish_never_imports_kafka(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """SQLite publish works even when confluent_kafka is uninstallable (imports stay lazy)."""
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
    monkeypatch.setitem(sys.modules, "confluent_kafka", None)

    broker = EventStreamAdapter(db_path=str(tmp_path / "event_log.db"))
    assert broker.publish_event(TOPIC, "k1", {"seq": 3}) is True
    assert broker.consume_events(TOPIC, consumer_group="g") == [{"seq": 3}]
