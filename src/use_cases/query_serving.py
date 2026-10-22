"""Modern Data Architecture POC - Unified serving layer query use case.

Combines historical batch Gold data with real-time stream data in one query.
"""

from __future__ import annotations

import logging
from typing import Any

from src.domain.naming import gold_table, serving_view_name, stream_table
from src.ports.inbound import QueryServingPort, UnifiedRevenueRow
from src.ports.outbound import QueryExecutorPort

logger = logging.getLogger("use_case.query_serving")


class QueryServingUseCase(QueryServingPort):
    """Orchestrates transparent unification of batch and streaming tiers.

    Batch wins on ``(order_date, status)``; stream rows fill absent keys only.
    """

    def __init__(self, query_executor: QueryExecutorPort) -> None:
        self._query_executor = query_executor

    def execute(
        self,
        order_date_gte: str | None = None,
        limit: int = 20,
    ) -> list[UnifiedRevenueRow]:
        """Executes one unified query over batch Gold and stream Speed tiers.

        Returns typed rows with ``source_tier`` labels.
        """
        logger.info("Executing RF-11 Unified Serving Query (batch + stream)...")
        sql = self._build_unified_sql(order_date_gte=order_date_gte, limit=limit)
        rows = self._query_executor.execute_query(sql)
        return [self._coerce_row(r) for r in rows]

    @staticmethod
    def _build_unified_sql(order_date_gte: str | None, limit: int) -> str:
        """Builds the single no-double-count SQL statement.

        Table names are serving live-view names; the statement runs verbatim.
        """
        batch_table = serving_view_name(gold_table("daily_metrics"))
        stream_table_name = serving_view_name(stream_table())
        sql = (
            "SELECT order_date, status, total_orders, total_revenue, "
            "avg_order_value, source_tier FROM ("
            "SELECT order_date, status, total_orders, "
            "gross_revenue AS total_revenue, "
            "average_order_value AS avg_order_value, "
            "'batch_historical' AS source_tier "
            f"FROM {batch_table} "
            "UNION ALL "
            "SELECT s.order_date AS order_date, s.status AS status, "
            "SUM(s.window_orders) AS total_orders, "
            "SUM(s.window_revenue) AS total_revenue, "
            "SUM(s.window_revenue) / NULLIF(SUM(s.window_orders), 0) "
            "AS avg_order_value, "
            "'stream_speed' AS source_tier "
            f"FROM {stream_table_name} s "
            f"WHERE NOT EXISTS (SELECT 1 FROM {batch_table} b "
            "WHERE b.order_date = s.order_date AND b.status = s.status) "
            "GROUP BY s.order_date, s.status"
            ")"
        )
        if order_date_gte:
            escaped = str(order_date_gte).replace("'", "''")
            sql += f" WHERE order_date >= '{escaped}'"
        sql += f" ORDER BY order_date DESC, status DESC LIMIT {int(limit)}"
        return sql

    @staticmethod
    def _coerce_row(row: dict[str, Any]) -> UnifiedRevenueRow:
        """Physical columns are TEXT; coerce numerics for downstream consumers."""
        return {
            "order_date": row.get("order_date"),
            "status": row.get("status"),
            "total_orders": int(float(row.get("total_orders") or 0)),
            "total_revenue": round(float(row.get("total_revenue") or 0.0), 2),
            "avg_order_value": round(float(row.get("avg_order_value") or 0.0), 2),
            "source_tier": row.get("source_tier"),
        }
