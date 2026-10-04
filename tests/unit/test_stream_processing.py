"""
Modern Data Architecture POC - Unit Tests: Streaming Speed Layer (RF-09 / RF-10).
Verifies real tumbling window bucketing, persistent consumer cursors, full log
replay, deterministic speed-table recomputation and the no-fabrication contract.
Every test runs against isolated event-log and lakehouse databases under tmp_path.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from src.adapters._sqlite import sqlite_connection
from src.adapters.lineage.marquez_lineage import MarquezLineageAdapter
from src.adapters.storage.lakehouse_storage import LakehouseStorageAdapter
from src.adapters.streaming import event_stream as event_stream_module
from src.adapters.streaming.event_stream import EventStreamAdapter
from src.domain.aggregations import BusinessAggregator
from src.use_cases.process_stream import ProcessStreamUseCase
from tests.unit.test_event_stream_kafka import FakeConsumer, FakeMessage, _make_factory

TARGET_TABLE = "stream.sales.orders_windowed"


def _make_use_case(tmp_path: Path):
    storage = LakehouseStorageAdapter(db_path=str(tmp_path / "lakehouse.db"))
    broker = EventStreamAdapter(db_path=str(tmp_path / "event_log.db"))
    lineage = MarquezLineageAdapter(
        marquez_url="http://127.0.0.1:1",
        audit_file_path=str(tmp_path / "lineage.jsonl"),
    )
    use_case = ProcessStreamUseCase(broker=broker, storage=storage, lineage=lineage)
    return use_case, broker, storage


def test_window_bucketing(tmp_path: Path):
    events = [
        {
            "timestamp": "2026-01-15T12:00:00+00:00",
            "status": "confirmed",
            "total_amount": 10.0,
            "order_date": "2026-01-15",
        },
        {
            "timestamp": "2026-01-15T12:03:00+00:00",
            "status": "confirmed",
            "total_amount": 20.0,
            "order_date": "2026-01-15",
        },
        {
            "timestamp": "2026-01-15T12:07:00+00:00",
            "status": "confirmed",
            "total_amount": 40.0,
            "order_date": "2026-01-15",
        },
    ]

    windows = BusinessAggregator.aggregate_tumbling_window(events, window_size_seconds=300)

    assert len(windows) == 2
    # Output sorted by (window_start, status)
    assert [w["window_start"] for w in windows] == sorted(w["window_start"] for w in windows)

    first, second = windows
    assert first["window_start"] == "2026-01-15T12:00:00+00:00"
    assert first["window_end"] == "2026-01-15T12:05:00+00:00"  # exclusive
    assert first["order_date"] == "2026-01-15"
    assert first["status"] == "confirmed"
    assert first["window_orders"] == 2  # 12:00:00 + 12:03:00
    assert first["window_revenue"] == 30.0
    assert first["avg_order_value"] == 15.0
    assert first["source_tier"] == "stream_speed"

    assert second["window_start"] == "2026-01-15T12:05:00+00:00"
    assert second["window_end"] == "2026-01-15T12:10:00+00:00"
    assert second["order_date"] == "2026-01-15"
    assert second["window_orders"] == 1  # 12:07:00
    assert second["window_revenue"] == 40.0
    assert second["avg_order_value"] == 40.0


def test_consume_incremental_no_duplicates(tmp_path: Path):
    broker = EventStreamAdapter(db_path=str(tmp_path / "event_log.db"))
    topic = "unit.incremental"

    for i in range(5):
        broker.publish_event(topic, str(i), {"i": i})

    first = broker.consume_events(topic, consumer_group="group_a")
    assert [e["i"] for e in first] == [0, 1, 2, 3, 4]

    second = broker.consume_events(topic, consumer_group="group_a")
    assert second == []

    for i in range(5, 7):
        broker.publish_event(topic, str(i), {"i": i})

    third = broker.consume_events(topic, consumer_group="group_a")
    assert [e["i"] for e in third] == [5, 6]  # ascending, only new events

    # Independent consumer group starts from the beginning
    other = broker.consume_events(topic, consumer_group="group_b")
    assert [e["i"] for e in other] == [0, 1, 2, 3, 4, 5, 6]


def test_replay_returns_full_log_and_ignores_cursor(tmp_path: Path):
    db_path = str(tmp_path / "event_log.db")
    broker = EventStreamAdapter(db_path=db_path)
    topic = "unit.replay"

    for i in range(5):
        broker.publish_event(topic, str(i), {"i": i})

    partial = broker.consume_events(topic, max_messages=3, consumer_group="group_a")
    assert [e["i"] for e in partial] == [0, 1, 2]

    replayed = broker.consume_events(topic, replay=True)
    assert [e["i"] for e in replayed] == [0, 1, 2, 3, 4]  # full log from offset 0

    with sqlite_connection(db_path) as conn:
        row = conn.execute(
            "SELECT last_offset FROM consumer_offsets WHERE topic = ? AND consumer_group = ?;",
            (topic, "group_a"),
        ).fetchone()
    assert row is not None and row[0] == 3  # replay did NOT advance the cursor

    resumed = broker.consume_events(topic, consumer_group="group_a")
    assert [e["i"] for e in resumed] == [3, 4]  # cursor untouched by replay


def test_process_stream_two_runs_no_duplicate_windows(tmp_path: Path):
    use_case, broker, storage = _make_use_case(tmp_path)
    topic = "unit.two_runs"

    broker.publish_event(
        topic,
        "1",
        {
            "timestamp": "2026-01-15T12:01:00+00:00",
            "status": "confirmed",
            "total_amount": 50.0,
            "order_date": "2026-01-15",
        },
    )
    broker.publish_event(
        topic,
        "2",
        {
            "timestamp": "2026-01-15T12:02:00+00:00",
            "status": "confirmed",
            "total_amount": 70.0,
            "order_date": "2026-01-15",
        },
    )

    first = use_case.execute(topic=topic, target_table=TARGET_TABLE, replay=False)
    assert first["status"] == "SUCCESS"
    assert first["mode"] == "append"
    assert first["windows_generated"] == 1
    rows_after_first = storage.read_table(TARGET_TABLE)
    assert len(rows_after_first) == 1

    second = use_case.execute(topic=topic, target_table=TARGET_TABLE, replay=False)
    assert second["status"] == "NO_EVENTS"
    # Counters report the full-table recompute actually performed (1 window),
    # not zeroes: the idempotent rewrite changed nothing (0 rows written).
    assert second["windows_generated"] == 1
    assert second["windows_written"] == 0
    rows_after_second = storage.read_table(TARGET_TABLE)
    assert len(rows_after_second) == len(rows_after_first)  # no duplicate windows


def test_incremental_batches_share_window_without_fragmentation(tmp_path: Path):
    use_case, broker, storage = _make_use_case(tmp_path)
    topic = "unit.shared_window"

    broker.publish_event(
        topic,
        "1",
        {
            "timestamp": "2026-01-15T12:01:00+00:00",
            "status": "confirmed",
            "total_amount": 50.0,
            "order_date": "2026-01-15",
        },
    )
    broker.publish_event(
        topic,
        "2",
        {
            "timestamp": "2026-01-15T12:02:00+00:00",
            "status": "confirmed",
            "total_amount": 70.0,
            "order_date": "2026-01-15",
        },
    )

    first = use_case.execute(topic=topic, target_table=TARGET_TABLE, replay=False)
    assert first["status"] == "SUCCESS"
    assert first["mode"] == "append"
    assert first["events_consumed"] == 2
    rows_first = storage.read_table(TARGET_TABLE)
    assert len(rows_first) == 1
    assert rows_first[0]["window_start"] == "2026-01-15T12:00:00+00:00"
    assert int(rows_first[0]["window_orders"]) == 2
    assert float(rows_first[0]["window_revenue"]) == 120.0

    broker.publish_event(
        topic,
        "3",
        {
            "timestamp": "2026-01-15T12:03:00+00:00",
            "status": "confirmed",
            "total_amount": 30.0,
            "order_date": "2026-01-15",
        },
    )

    second = use_case.execute(topic=topic, target_table=TARGET_TABLE, replay=False)
    assert second["status"] == "SUCCESS"
    assert second["mode"] == "append"
    assert second["events_consumed"] == 1
    rows_second = storage.read_table(TARGET_TABLE)
    assert len(rows_second) == 1  # no fragmentation: still a single row for W
    keys = [(r["window_start"], r["status"]) for r in rows_second]
    assert len(keys) == len(set(keys))  # no duplicate (window_start, status) rows
    assert rows_second[0]["window_start"] == "2026-01-15T12:00:00+00:00"
    assert int(rows_second[0]["window_orders"]) == 3
    assert float(rows_second[0]["window_revenue"]) == 150.0


def test_process_stream_replay_overwrites(tmp_path: Path):
    use_case, broker, storage = _make_use_case(tmp_path)
    topic = "unit.replay_overwrite"

    for i in range(4):
        broker.publish_event(
            topic,
            str(i),
            {
                "timestamp": f"2026-01-15T12:0{i}:00+00:00",
                "status": "confirmed" if i % 2 == 0 else "pending",
                "total_amount": 10.0 * (i + 1),
                "order_date": "2026-01-15",
            },
        )

    first = use_case.execute(topic=topic, target_table=TARGET_TABLE, replay=True)
    assert first["status"] == "SUCCESS"
    assert first["mode"] == "replay_full"
    assert first["windows_generated"] > 0
    rows_first = storage.read_table(TARGET_TABLE)
    assert len(rows_first) == first["windows_generated"]

    second = use_case.execute(topic=topic, target_table=TARGET_TABLE, replay=True)
    assert second["status"] == "SUCCESS"
    assert second["mode"] == "replay_full"
    rows_second = storage.read_table(TARGET_TABLE)

    def sort_key(row):
        return (row["window_start"], row["status"])

    assert sorted(rows_second, key=sort_key) == sorted(rows_first, key=sort_key)  # deterministic, no growth


def test_empty_log_returns_empty(tmp_path: Path):
    use_case, broker, storage = _make_use_case(tmp_path)
    topic = "unit.empty"

    assert broker.consume_events(topic) == []
    assert broker.consume_events(topic, replay=True) == []
    assert broker.consume_events(topic, max_messages=10, replay=True) == []

    result = use_case.execute(topic=topic, target_table=TARGET_TABLE, replay=False)
    assert result["status"] == "NO_EVENTS"
    assert result["windows_written"] == 0
    assert storage.table_exists(TARGET_TABLE) is False  # nothing written, nothing fabricated


def test_replay_empty_log_clears_stale_speed_rows(tmp_path: Path):
    """Regression (RF-10): replaying an empty log must reconcile the speed
    table to empty instead of reporting SUCCESS over stale rows."""
    use_case, broker, storage = _make_use_case(tmp_path)
    topic = "unit.replay_empty_clears"

    # Simulate rows left behind by a previous log generation.
    storage.write_table(
        TARGET_TABLE,
        [
            {
                "window_start": "2026-01-15T12:00:00+00:00",
                "status": "confirmed",
                "window_orders": 2,
                "window_revenue": 120.0,
            }
        ],
        mode="overwrite",
    )
    assert len(storage.read_table(TARGET_TABLE)) == 1
    assert broker.consume_events(topic, replay=True) == []

    result = use_case.execute(topic=topic, target_table=TARGET_TABLE, replay=True)
    assert result["status"] == "SUCCESS"
    assert result["windows_generated"] == 0
    assert result["windows_written"] == 0
    assert storage.read_table(TARGET_TABLE) == []
    # Time travel still exposes the cleared generation.
    assert len(storage.list_versions(TARGET_TABLE)) == 2


def test_write_failure_does_not_advance_cursor(tmp_path: Path):
    """Regression: the cursor must advance only after a successful speed-table write.

    Simulates a crash between recompute and cursor commit (storage raises on the
    first write). The failed run must leave the cursor untouched so a second
    execution still materializes the pending windows instead of NO_EVENTS.
    """
    real_storage = LakehouseStorageAdapter(db_path=str(tmp_path / "lakehouse.db"))
    broker = EventStreamAdapter(db_path=str(tmp_path / "event_log.db"))
    lineage = MarquezLineageAdapter(
        marquez_url="http://127.0.0.1:1",
        audit_file_path=str(tmp_path / "lineage.jsonl"),
    )

    class FlakyStorage:
        """Delegates to the real storage but fails the first write on purpose."""

        def __init__(self, delegate: LakehouseStorageAdapter) -> None:
            self._delegate = delegate
            self._fail_next = True

        def write_table(self, table_name: str, records: list[dict[str, Any]], mode: str = "append") -> int:
            if self._fail_next:
                self._fail_next = False
                raise RuntimeError("simulated speed-table write failure")
            return self._delegate.write_table(table_name, records, mode=mode)

        def read_table(self, table_name: str, limit: int | None = None, version: int | None = None):
            return self._delegate.read_table(table_name, limit=limit, version=version)

        def table_exists(self, table_name: str) -> bool:
            return self._delegate.table_exists(table_name)

        def get_table_schema(self, table_name: str) -> list[str]:
            return self._delegate.get_table_schema(table_name)

        def list_versions(self, table_name: str):
            return self._delegate.list_versions(table_name)

    use_case = ProcessStreamUseCase(broker=broker, storage=FlakyStorage(real_storage), lineage=lineage)  # type: ignore[arg-type]
    topic = "unit.cursor_after_write"

    broker.publish_event(
        topic,
        "1",
        {
            "timestamp": "2026-01-15T12:01:00+00:00",
            "status": "confirmed",
            "total_amount": 50.0,
            "order_date": "2026-01-15",
        },
    )
    broker.publish_event(
        topic,
        "2",
        {
            "timestamp": "2026-01-15T12:02:00+00:00",
            "status": "confirmed",
            "total_amount": 70.0,
            "order_date": "2026-01-15",
        },
    )

    with pytest.raises(RuntimeError, match="simulated speed-table write failure"):
        use_case.execute(topic=topic, target_table=TARGET_TABLE, replay=False)

    result = use_case.execute(topic=topic, target_table=TARGET_TABLE, replay=False)
    assert result["status"] == "SUCCESS"
    assert result["events_consumed"] == 2  # cursor was NOT falsely advanced by the failed run
    assert result["windows_generated"] == 1
    rows = real_storage.read_table(TARGET_TABLE)
    assert len(rows) == 1
    assert int(rows[0]["window_orders"]) == 2
    assert float(rows[0]["window_revenue"]) == 120.0


def test_process_stream_kafka_replay_recovers_full_backlog_despite_zero_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """End-to-end (A4): a Kafka replay with zero poll budget still rebuilds every window.

    Guards the silent-loss defect where ``_consume_from_kafka`` truncated the
    replay and ``process_stream`` overwrote the Speed table with that partial
    result: all 30 events must be consumed and both tumbling windows rebuilt.
    """
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    monkeypatch.setattr(event_stream_module, "_POLL_BUDGET_SECONDS", 0.0)
    topic = "unit.kafka_replay_full"
    total = 30
    frames: list[Any] = []
    for i in range(total):
        minute = 1 + (i % 8)  # spread over the 12:00-12:05 and 12:05-12:10 windows
        frames.append(
            FakeMessage(
                topic,
                i,
                json.dumps(
                    {
                        "timestamp": f"2026-01-15T12:{minute:02d}:00+00:00",
                        "status": "confirmed",
                        "total_amount": 10.0,
                        "order_date": "2026-01-15",
                    }
                ).encode("utf-8"),
            )
        )
    instances: list[FakeConsumer] = []
    broker = EventStreamAdapter(
        db_path=str(tmp_path / "event_log.db"),
        consumer_factory=_make_factory(frames, instances, watermarks={0: (0, total)}),
    )
    storage = LakehouseStorageAdapter(db_path=str(tmp_path / "lakehouse.db"))
    lineage = MarquezLineageAdapter(
        marquez_url="http://127.0.0.1:1",
        audit_file_path=str(tmp_path / "lineage.jsonl"),
    )
    use_case = ProcessStreamUseCase(broker=broker, storage=storage, lineage=lineage)

    result = use_case.execute(topic=topic, target_table=TARGET_TABLE, replay=True)

    assert result["status"] == "SUCCESS"
    assert result["mode"] == "replay_full"
    assert result["events_consumed"] == total
    assert result["windows_generated"] == 2
    rows = storage.read_table(TARGET_TABLE)
    assert len(rows) == 2
    assert sum(int(r["window_orders"]) for r in rows) == total
