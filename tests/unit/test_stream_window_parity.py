"""Parity contract between the Flink speed layer and the batch aggregation.

``pipelines/stream/window_logic.py`` must produce exactly what
``BusinessAggregator.aggregate_tumbling_window`` produces.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from pipelines.stream import window_logic
from src.domain.aggregations import BusinessAggregator

#: The stream end-to-end latency budget is 5 s. A tumbling window cannot emit
#: before it closes, so the window remainder plus the watermark lag is the
#: worst-case first-emission latency and must stay inside the budget.
RF09_LATENCY_BUDGET_MS = 5_000

BASE_TIME = datetime(2026, 3, 17, 12, 0, 0, tzinfo=UTC)


def _order_payload(
    index: int,
    *,
    offset_ms: int,
    status: str = "confirmed",
    amount: float | None = None,
) -> dict[str, Any]:
    """Builds a ``sales.orders.events`` payload shaped like event_producer.py."""
    event_time = BASE_TIME + timedelta(milliseconds=offset_ms)
    return {
        "event_id": f"evt-{index}",
        "event_type": "created",
        "order_id": 1000 + index,
        "customer_id": 50 + index,
        "total_amount": amount if amount is not None else round(10.0 + index * 3.25, 2),
        "status": status,
        "order_date": event_time.strftime("%Y-%m-%d"),
        "timestamp": event_time.isoformat(),
        "source": "web",
    }


def _clickstream_payload(
    index: int,
    *,
    offset_ms: int,
    event_type: str = "view",
    session_id: str = "sess-1",
) -> dict[str, Any]:
    """Builds a ``marketing.clickstream.raw`` payload like event_producer.py."""
    event_time = BASE_TIME + timedelta(milliseconds=offset_ms)
    return {
        "event_id": f"click-{index}",
        "session_id": session_id,
        "customer_id": 50 + index,
        "page_url": "/catalog",
        "event_type": event_type,
        "referrer": "direct",
        "timestamp": event_time.isoformat(),
    }


def _flink_orders_rows(payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Replays the job's windowing with the pure module, keyed by (window, status)."""
    window_seconds = window_logic.WINDOW_SIZE_MS // 1000
    buckets: dict[tuple[int, str], list[float]] = {}
    for payload in payloads:
        event = window_logic.parse_order_event(payload)
        assert event is not None, "fixture payload must be parseable"
        start = window_logic.window_start_ms(event.event_time_ms)
        buckets.setdefault((start, event.status), []).append(event.total_amount)

    rows = [
        window_logic.build_orders_row(start, status, sum(amounts), len(amounts))
        for (start, status), amounts in buckets.items()
    ]
    rows.sort(key=lambda row: (row["window_start"], row["status"]))
    assert window_seconds * 1000 == window_logic.WINDOW_SIZE_MS
    return rows


def test_window_and_watermark_fit_the_rf09_latency_budget() -> None:
    worst_case_first_emission = window_logic.WINDOW_SIZE_MS + window_logic.MAX_OUT_OF_ORDERNESS_MS
    assert worst_case_first_emission <= RF09_LATENCY_BUDGET_MS, (
        f"worst-case first emission is {worst_case_first_emission} ms, above the RF-09 "
        f"budget of {RF09_LATENCY_BUDGET_MS} ms"
    )
    assert window_logic.ALLOWED_LATENESS_MS > window_logic.MAX_OUT_OF_ORDERNESS_MS, (
        "allowed lateness must exceed the watermark lag, otherwise events that are "
        "merely reordered would be dropped instead of correcting their window"
    )


def test_window_bucketing_matches_the_reference_implementation() -> None:
    offsets = [0, 1_000, 3_999, 4_000, 7_999, 8_000, 12_345]
    for offset in offsets:
        event_time_ms = int((BASE_TIME + timedelta(milliseconds=offset)).timestamp() * 1000)
        start_ms, end_ms = window_logic.window_bounds_ms(event_time_ms)
        start_epoch = int((BASE_TIME + timedelta(milliseconds=offset)).timestamp())
        expected = (start_epoch // (window_logic.WINDOW_SIZE_MS // 1000)) * (window_logic.WINDOW_SIZE_MS // 1000)
        assert start_ms == expected * 1000
        assert end_ms == start_ms + window_logic.WINDOW_SIZE_MS
        assert start_ms % window_logic.WINDOW_SIZE_MS == 0


def test_event_time_parsing_matches_the_reference_implementation() -> None:
    for offset in [0, 1_500, 7_999]:
        payload = _order_payload(offset, offset_ms=offset)
        event = window_logic.parse_order_event(payload)
        assert event is not None
        assert event.event_time_ms == int(BusinessAggregator._event_epoch_seconds(payload) * 1000)


def test_orders_rows_are_identical_to_the_reference_aggregation() -> None:
    payloads = [
        _order_payload(0, offset_ms=0),
        _order_payload(1, offset_ms=1_200),
        _order_payload(2, offset_ms=3_900),
        _order_payload(3, offset_ms=4_100, status="shipped"),
        _order_payload(4, offset_ms=4_800, status="shipped"),
        _order_payload(5, offset_ms=9_000, status="confirmed"),
    ]

    reference = BusinessAggregator.aggregate_tumbling_window(
        payloads, window_size_seconds=window_logic.WINDOW_SIZE_MS // 1000
    )
    assert _flink_orders_rows(payloads) == reference


def test_orders_rows_match_for_randomised_payloads() -> None:
    import random

    rng = random.Random(20260317)
    statuses = ["confirmed", "pending", "cancelled", "shipped"]
    payloads = [
        _order_payload(
            i,
            offset_ms=rng.randrange(0, 30_000),
            status=rng.choice(statuses),
            amount=round(rng.uniform(10.0, 500.0), 2),
        )
        for i in range(250)
    ]

    reference = BusinessAggregator.aggregate_tumbling_window(
        payloads, window_size_seconds=window_logic.WINDOW_SIZE_MS // 1000
    )
    assert _flink_orders_rows(payloads) == reference


def test_orders_columns_are_typed_for_the_iceberg_schema() -> None:
    payload = _order_payload(0, offset_ms=1_000)
    event = window_logic.parse_order_event(payload)
    assert event is not None
    row = window_logic.build_orders_row(
        window_logic.window_start_ms(event.event_time_ms),
        event.status,
        event.total_amount,
        3,
    )
    columns = window_logic.orders_row_to_columns(row)

    assert len(columns) == 8
    window_start, window_end, order_date, status, revenue, orders, average, tier = columns
    assert isinstance(window_start, datetime) and window_start.tzinfo is None
    assert isinstance(window_end, datetime) and window_end.tzinfo is None
    assert window_end - window_start == timedelta(milliseconds=window_logic.WINDOW_SIZE_MS)
    assert order_date == window_start.date()
    assert status == "confirmed"
    assert isinstance(revenue, float)
    assert isinstance(orders, int) and orders == 3
    assert isinstance(average, float)
    assert average == round(revenue / orders, 2)
    assert tier == window_logic.SOURCE_TIER


def test_clickstream_columns_count_events_and_distinct_sessions() -> None:
    payloads = [
        _clickstream_payload(0, offset_ms=0, session_id="s1"),
        _clickstream_payload(1, offset_ms=1_000, session_id="s1"),
        _clickstream_payload(2, offset_ms=2_000, session_id="s2"),
        _clickstream_payload(3, offset_ms=4_000, session_id="s2", event_type="click"),
    ]

    by_window: dict[tuple[int, str], list[str]] = {}
    for payload in payloads:
        event = window_logic.parse_clickstream_event(payload)
        assert event is not None
        start = window_logic.window_start_ms(event.event_time_ms)
        by_window.setdefault((start, event.event_type), []).append(event.session_id)

    rows = [
        window_logic.build_clickstream_row(start, event_type, len(sessions), len(set(sessions)))
        for (start, event_type), sessions in by_window.items()
    ]

    # First window: 3 'view' events across 2 distinct sessions.
    first = next(row for row in rows if row["event_type"] == "view")
    assert first["event_count"] == 3
    assert first["unique_sessions"] == 2
    assert first["order_date"] == "2026-03-17"

    columns = window_logic.clickstream_row_to_columns(first)
    assert len(columns) == 7
    assert columns[3] == "view"
    assert columns[4] == 3
    assert columns[5] == 2
    assert columns[6] == window_logic.SOURCE_TIER


@pytest.mark.parametrize(
    "raw",
    [
        b"not json at all",
        json_bytes := b'["not", "an", "object"]',
        b'{"event_id": "x", "status": "confirmed", "total_amount": 10.0}',
        b'{"timestamp": "not-a-date", "status": "confirmed", "total_amount": 10.0}',
        b'{"timestamp": "2026-03-17T12:00:00+00:00", "total_amount": "abc"}',
        b"\xff\xfe\x00binary garbage",
    ],
)
def test_unusable_order_payloads_are_rejected_instead_of_fabricated(raw: bytes) -> None:
    """No event time means no window: the payload is dropped, never guessed.

    The batch reference implementation falls back to ``order_date`` and finally
    to wall-clock time; the stream job deliberately does neither, because
    placing an event in a wrong window silently corrupts the aggregate.
    """
    assert window_logic.parse_order_event(raw) is None


def test_clickstream_without_session_is_rejected() -> None:
    assert window_logic.parse_clickstream_event(b'{"timestamp": "2026-03-17T12:00:00+00:00"}') is None


def test_naive_timestamps_are_interpreted_as_utc() -> None:
    naive = {"timestamp": "2026-03-17T12:00:00", "status": "confirmed", "total_amount": 42.0}
    aware = {"timestamp": "2026-03-17T12:00:00+00:00", "status": "confirmed", "total_amount": 42.0}
    parsed_naive = window_logic.parse_order_event(naive)
    parsed_aware = window_logic.parse_order_event(aware)
    assert parsed_naive is not None and parsed_aware is not None
    assert parsed_naive.event_time_ms == parsed_aware.event_time_ms
    assert parsed_naive.event_time_ms == int(BASE_TIME.timestamp() * 1000)


def test_zero_suffix_timestamps_are_parsed() -> None:
    payload = {"timestamp": "2026-03-17T12:00:00Z", "status": "confirmed", "total_amount": 42.0}
    event = window_logic.parse_order_event(payload)
    assert event is not None
    assert event.event_time_ms == int(BASE_TIME.timestamp() * 1000)


def test_amounts_are_rounded_like_the_reference() -> None:
    amounts = [10.005, 10.005, 10.005]
    events = [{"timestamp": BASE_TIME.isoformat(), "status": "confirmed", "total_amount": amount} for amount in amounts]
    reference = BusinessAggregator.aggregate_tumbling_window(
        events,
        window_size_seconds=window_logic.WINDOW_SIZE_MS // 1000,
    )[0]

    # Same total, same count, same bucket: the row must come out identical.
    row = window_logic.build_orders_row(
        window_logic.window_start_ms(int(BASE_TIME.timestamp() * 1000)),
        "confirmed",
        sum(amounts),
        len(amounts),
    )

    assert row["window_revenue"] == reference["window_revenue"]
    assert row["avg_order_value"] == reference["avg_order_value"]
    assert row["source_tier"] == reference["source_tier"]
