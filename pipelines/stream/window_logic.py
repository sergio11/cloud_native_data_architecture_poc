"""
window_logic.py - Event-time windowing contract shared by the Flink jobs (RF-09).

Pure Python, no PyFlink imports, so the contract the streaming jobs honour is
unit-testable without a cluster. Row builders are kept byte-identical to
``BusinessAggregator.aggregate_tumbling_window``; tests/unit/test_stream_window_parity.py
locks that in.

Two rules that differ from the batch reference implementation, on purpose:

* Event time is never invented. A payload without a usable ``timestamp`` is
  dead-lettered, whereas the batch code falls back to ``order_date`` and then to
  wall clock. Guessing here would place events in the wrong window.
* Malformed payloads are dead-lettered, never coerced to zero.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any

logger = logging.getLogger("stream.window_logic")

#: Tumbling window size. RF-09 caps end-to-end latency at 5 s and a tumbling
#: window cannot emit before it closes, so the window itself must be under 5 s.
#: Worst-case first emission = window remainder (4 s) + watermark lag (0.5 s).
WINDOW_SIZE_MS: int = 4_000

#: Absorbs the reordering from reading 3 Kafka partitions in parallel. At 0 ms
#: real events would be dropped as late.
MAX_OUT_OF_ORDERNESS_MS: int = 500

#: An event this late still corrects its row (the Iceberg sink upserts on
#: window_start + key) instead of being dropped.
ALLOWED_LATENESS_MS: int = 10_000

#: Marks rows as coming from the streaming layer (consumed by unified_view.sql).
SOURCE_TIER: str = "stream_speed"

_WINDOW_SIZE = timedelta(milliseconds=WINDOW_SIZE_MS)


@dataclass(frozen=True)
class OrderWindowEvent:
    """A parsed ``sales.orders.events`` record, keyed for the speed layer."""

    event_time_ms: int
    status: str
    total_amount: float


@dataclass(frozen=True)
class ClickstreamWindowEvent:
    """A parsed ``marketing.clickstream.raw`` record, keyed for the speed layer."""

    event_time_ms: int
    event_type: str
    session_id: str


def decode_payload(raw: bytes | bytearray | str | dict[str, Any]) -> dict[str, Any] | None:
    """Normalises a Kafka value into a JSON object, or None when unusable.

    Accepts the raw bytes/string produced by the Kafka source as well as an
    already-decoded mapping, so one contract serves the job and its tests.
    """
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, (bytes, bytearray)):
        try:
            text = bytes(raw).decode("utf-8")
        except UnicodeDecodeError:
            logger.warning("Dead-lettering clickstream/orders record: payload is not valid UTF-8")
            return None
    else:
        text = str(raw)
    try:
        payload = json.loads(text)
    except (TypeError, json.JSONDecodeError):
        logger.warning("Dead-lettering orders/clickstream record: payload is not valid JSON")
        return None
    if not isinstance(payload, dict):
        logger.warning("Dead-lettering orders/clickstream record: payload is not a JSON object")
        return None
    return payload


def parse_event_time_ms(raw: Any) -> int | None:
    """Parses an ISO-8601 ``timestamp`` into epoch milliseconds.

    Naive timestamps are interpreted as UTC, matching
    ``BusinessAggregator._event_epoch_seconds``. Returns None when the value is
    absent or unparseable so the caller can dead-letter the record instead of
    inventing an event time.
    """
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        logger.warning("Dead-lettering record with unparseable timestamp %r", text)
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return int(parsed.timestamp() * 1000)


def parse_order_event(raw: bytes | bytearray | str | dict[str, Any]) -> OrderWindowEvent | None:
    """Parses a ``sales.orders.events`` record, or None when it is unusable."""
    payload = decode_payload(raw)
    if payload is None:
        return None
    event_time_ms = parse_event_time_ms(payload.get("timestamp"))
    if event_time_ms is None:
        return None
    try:
        amount = float(payload.get("total_amount", 0.0))
    except (TypeError, ValueError):
        logger.warning("Dead-lettering order record with non-numeric total_amount %r", payload.get("total_amount"))
        return None
    if amount != amount or amount in (float("inf"), float("-inf")):  # NaN / Infinity
        logger.warning("Dead-lettering order record with non-finite total_amount %r", amount)
        return None
    status = str(payload.get("status") or "confirmed")
    return OrderWindowEvent(event_time_ms=event_time_ms, status=status, total_amount=amount)


def parse_clickstream_event(raw: bytes | bytearray | str | dict[str, Any]) -> ClickstreamWindowEvent | None:
    """Parses a ``marketing.clickstream.raw`` record, or None when it is unusable."""
    payload = decode_payload(raw)
    if payload is None:
        return None
    event_time_ms = parse_event_time_ms(payload.get("timestamp"))
    if event_time_ms is None:
        return None
    session_id = payload.get("session_id")
    if not session_id:
        logger.warning("Dead-lettering clickstream record without session_id: %r", payload)
        return None
    event_type = str(payload.get("event_type") or "unknown")
    return ClickstreamWindowEvent(event_time_ms=event_time_ms, event_type=event_type, session_id=str(session_id))


def window_start_ms(event_time_ms: int) -> int:
    """Returns the epoch-aligned start of the tumbling window for an event time."""
    return (int(event_time_ms) // WINDOW_SIZE_MS) * WINDOW_SIZE_MS


def window_bounds_ms(event_time_ms: int) -> tuple[int, int]:
    """Returns ``(window_start_ms, window_end_ms)`` for an event time (end exclusive)."""
    start = window_start_ms(event_time_ms)
    return start, start + WINDOW_SIZE_MS


def _bounds_text(start_ms: int) -> tuple[str, str, str]:
    """Formats window bounds as ``(iso_start, iso_end, order_date)``.

    ``start_ms`` is always a multiple of ``WINDOW_SIZE_MS`` (4 s), so it divides
    evenly into whole seconds and can be converted without float rounding.
    """
    start = datetime.fromtimestamp(start_ms // 1000, tz=UTC)
    return start.isoformat(), (start + _WINDOW_SIZE).isoformat(), start.strftime("%Y-%m-%d")


def build_orders_row(window_start: int, status: str, total_amount: float, order_count: int) -> dict[str, Any]:
    """Builds a ``stream.sales.orders_windowed`` row.

    Byte-identical to the dict produced by
    ``BusinessAggregator.aggregate_tumbling_window`` for the same bucket, so the
    serving layer and the contract tests see one single row shape.
    """
    window_start_iso, window_end_iso, order_date = _bounds_text(window_start)
    rounded_total = round(float(total_amount), 2)
    return {
        "window_start": window_start_iso,
        "window_end": window_end_iso,
        "order_date": order_date,
        "status": status,
        "window_revenue": rounded_total,
        "window_orders": int(order_count),
        "avg_order_value": round(rounded_total / int(order_count), 2) if order_count > 0 else 0.0,
        "source_tier": SOURCE_TIER,
    }


def build_clickstream_row(
    window_start: int,
    event_type: str,
    event_count: int,
    unique_sessions: int,
) -> dict[str, Any]:
    """Builds a ``stream.marketing.clickstream_windowed`` row.

    Mirrors ``build_orders_row`` for the clickstream grain: counting sessions is
    the only divergence from the order schema, so the window bookkeeping stays
    identical and both tables can be fused by the serving layer.
    """
    window_start_iso, window_end_iso, order_date = _bounds_text(window_start)
    return {
        "window_start": window_start_iso,
        "window_end": window_end_iso,
        "order_date": order_date,
        "event_type": event_type,
        "event_count": int(event_count),
        "unique_sessions": int(unique_sessions),
        "source_tier": SOURCE_TIER,
    }


def _naive_utc(iso_text: str) -> datetime:
    """Converts an ISO-8601 UTC instant into a naive datetime.

    Iceberg ``timestamp`` has no zone, so the values are normalised to UTC and
    the Flink session timezone is pinned to UTC by the job. Keeping the
    conversion here makes the column layout unit-testable without PyFlink.
    """
    parsed = datetime.fromisoformat(iso_text)
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(UTC).replace(tzinfo=None)
    return parsed


def orders_row_to_columns(row: dict[str, Any]) -> tuple[datetime, datetime, date, str, float, int, float, str]:
    """Maps an orders row to the Iceberg column order of ``sales.orders_windowed``."""
    return (
        _naive_utc(str(row["window_start"])),
        _naive_utc(str(row["window_end"])),
        date.fromisoformat(str(row["order_date"])),
        str(row["status"]),
        float(row["window_revenue"]),
        int(row["window_orders"]),
        float(row["avg_order_value"]),
        str(row["source_tier"]),
    )


def clickstream_row_to_columns(row: dict[str, Any]) -> tuple[datetime, datetime, date, str, int, int, str]:
    """Maps a clickstream row to the Iceberg column order of ``marketing.clickstream_windowed``."""
    return (
        _naive_utc(str(row["window_start"])),
        _naive_utc(str(row["window_end"])),
        date.fromisoformat(str(row["order_date"])),
        str(row["event_type"]),
        int(row["event_count"]),
        int(row["unique_sessions"]),
        str(row["source_tier"]),
    )
