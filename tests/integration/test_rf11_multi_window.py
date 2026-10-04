"""
Modern Data Architecture POC - Integration Tests: RF-11 multi-window merge.

Proves the unified serving query aggregates stream tumbling windows to day
grain BEFORE the batch anti-join, so one (order_date, status) key yields
exactly one row. Hermetic: uses the ``world`` fixture (tmp SQLite lakehouse,
offline Marquez stub, TRINO_HOST unset) - no repo-root data/, no network.
"""

from __future__ import annotations

GOLD_METRICS = "gold.sales.daily_metrics"
STREAM_TARGET = "stream.sales.orders_windowed"


def test_stream_windows_merge_to_single_day_row(world) -> None:
    """Given TWO stream windows sharing one (order_date, status) key and no
    batch coverage, When execute() runs, Then exactly
    ONE stream_speed row appears with summed orders/revenue."""
    stream_rows = [
        {
            "order_date": "2026-09-26",
            "status": "confirmed",
            "window_orders": 2,
            "window_revenue": 300.0,
            "avg_order_value": 150.0,
        },
        {
            "order_date": "2026-09-26",
            "status": "confirmed",
            "window_orders": 3,
            "window_revenue": 600.0,
            "avg_order_value": 200.0,
        },
    ]
    assert world.storage.write_table(STREAM_TARGET, stream_rows, mode="append") == 2
    # Seed a non-colliding batch row so the batch physical table exists for
    # the anti-join; it must not shadow the stream-only key under test.
    assert (
        world.storage.write_table(
            GOLD_METRICS,
            [
                {
                    "order_date": "2026-09-20",
                    "status": "shipped",
                    "total_orders": 1,
                    "gross_revenue": 100.0,
                    "average_order_value": 100.0,
                }
            ],
            mode="append",
        )
        == 1
    )

    rows = world.query_serving.execute()

    keyed = [r for r in rows if r["order_date"] == "2026-09-26" and r["status"] == "confirmed"]
    assert len(keyed) == 1
    merged = keyed[0]
    assert merged["source_tier"] == "stream_speed"
    assert merged["total_orders"] == 5
    assert merged["total_revenue"] == 900.0
    assert merged["avg_order_value"] == 180.0


def test_batch_wins_over_merged_stream(world) -> None:
    """Given the same merged stream key covered by batch, When the unified
    query runs, Then the key appears exactly once as batch_historical with
    batch values (stream fully shadowed)."""
    gold_rows = [
        {
            "order_date": "2026-09-26",
            "status": "confirmed",
            "total_orders": 10,
            "gross_revenue": 2000.0,
            "average_order_value": 200.0,
        }
    ]
    stream_rows = [
        {
            "order_date": "2026-09-26",
            "status": "confirmed",
            "window_orders": 2,
            "window_revenue": 300.0,
            "avg_order_value": 150.0,
        },
        {
            "order_date": "2026-09-26",
            "status": "confirmed",
            "window_orders": 3,
            "window_revenue": 600.0,
            "avg_order_value": 200.0,
        },
    ]
    assert world.storage.write_table(GOLD_METRICS, gold_rows, mode="append") == 1
    assert world.storage.write_table(STREAM_TARGET, stream_rows, mode="append") == 2

    rows = world.query_serving.execute()

    keyed = [r for r in rows if r["order_date"] == "2026-09-26" and r["status"] == "confirmed"]
    assert len(keyed) == 1
    assert keyed[0]["source_tier"] == "batch_historical"
    assert keyed[0]["total_orders"] == 10
    assert keyed[0]["total_revenue"] == 2000.0
    assert keyed[0]["avg_order_value"] == 200.0
