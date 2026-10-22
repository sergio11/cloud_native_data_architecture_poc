"""
Modern Data Architecture POC - Business Aggregations & Analytical Calculations.
Pure Python implementations of business logic for Gold tier and streaming windows.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import Any

from src.domain.entities import DailySalesMetrics

logger = logging.getLogger("domain.aggregations")


class BusinessAggregator:
    """Calculates business metrics for Gold analytical models."""

    @staticmethod
    def aggregate_daily_sales(orders: list[dict[str, Any]]) -> list[DailySalesMetrics]:
        """Rolls up sales orders by (order_date, status).

        Computes gross revenue, order count and average order value.
        """
        buckets: dict[tuple[str, str], list[float]] = defaultdict(list)

        for o in orders:
            order_date = str(o.get("order_date", "1970-01-01"))
            status = str(o.get("status", "unknown"))
            total_amount = float(o.get("total_amount", 0.0))
            buckets[(order_date, status)].append(total_amount)

        results: list[DailySalesMetrics] = []
        for (order_date, status), amounts in sorted(buckets.items()):
            count = len(amounts)
            gross = sum(amounts)
            aov = (gross / count) if count > 0 else 0.0
            results.append(
                DailySalesMetrics(
                    order_date=order_date,
                    status=status,
                    gross_revenue=gross,
                    total_orders=count,
                    average_order_value=aov,
                )
            )
        return results

    @staticmethod
    def _event_epoch_seconds(event: dict[str, Any]) -> float:
        """Resolves event-time epoch seconds for window bucketing.

        Order: ``timestamp``, then ``order_date`` (midnight UTC), then now.
        """
        ts_raw = event.get("timestamp")
        if ts_raw:
            try:
                parsed = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=UTC)
                return parsed.timestamp()
            except ValueError:
                logger.warning("Unparseable event timestamp %r; falling back to order_date", ts_raw)

        date_raw = event.get("order_date")
        if date_raw:
            try:
                parsed = datetime.fromisoformat(f"{str(date_raw)[:10]}T00:00:00+00:00")
                return parsed.timestamp()
            except ValueError:
                logger.warning("Unparseable order_date %r; falling back to current time", date_raw)

        return datetime.now(UTC).timestamp()

    @staticmethod
    def aggregate_tumbling_window(
        events: list[dict[str, Any]],
        window_size_seconds: int = 300,
    ) -> list[dict[str, Any]]:
        """Aggregates order events into fixed tumbling time windows.

        Windows align to the epoch and are keyed by ``(bucket, status)``.
        Output carries window bounds plus ``order_date``, sorted by window.
        """
        if window_size_seconds <= 0:
            raise ValueError("window_size_seconds must be a positive integer")

        window_buckets: dict[tuple[int, str], list[float]] = defaultdict(list)

        for evt in events:
            try:
                status = str(evt.get("status", "confirmed"))
                amount = float(evt.get("total_amount", 0.0))
                epoch = BusinessAggregator._event_epoch_seconds(evt)
                bucket = int(epoch // window_size_seconds)
            except (TypeError, ValueError) as exc:
                logger.warning("Skipping malformed stream event %r: %s", evt, exc)
                continue
            window_buckets[(bucket, status)].append(amount)

        windows: list[dict[str, Any]] = []
        for (bucket, status), amounts in sorted(window_buckets.items()):
            start_epoch = bucket * window_size_seconds
            window_start = datetime.fromtimestamp(start_epoch, tz=UTC)
            window_end = window_start + timedelta(seconds=window_size_seconds)
            count = len(amounts)
            total = sum(amounts)
            windows.append(
                {
                    "window_start": window_start.isoformat(),
                    "window_end": window_end.isoformat(),
                    "order_date": window_start.strftime("%Y-%m-%d"),
                    "status": status,
                    "window_revenue": round(total, 2),
                    "window_orders": count,
                    "avg_order_value": round(total / count, 2) if count > 0 else 0.0,
                    "source_tier": "stream_speed",
                }
            )

        windows.sort(key=lambda w: (w["window_start"], w["status"]))
        return windows
