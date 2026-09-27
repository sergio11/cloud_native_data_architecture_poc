"""
Modern Data Architecture POC - SQL Serving Adapter (Outbound / Driven Side).
Implements QueryExecutorPort: the application core hands over ANSI SQL and this
adapter decides which backend serves it, keeping RF-11 unified serving queries
(Trino when explicitly enabled, embedded SQLite otherwise) out of the core.
"""

from __future__ import annotations

import logging
import os
import sqlite3
from typing import Any

from src.domain.naming import bronze_table, gold_table, physical_sql_name, silver_table, stream_table
from src.ports.outbound import QueryExecutorPort

logger = logging.getLogger("adapter.serving.sql")

_GOLD_DAILY_METRICS = gold_table("daily_metrics")
_STREAM_WINDOWED = stream_table()
_SILVER_ORDERS = silver_table("orders")
_BRONZE_ORDERS = bronze_table("orders")

# Interim logical -> physical table-name bridge (embedded SQLite path only).
# Logical 3-part names used by the use cases (gold.sales.daily_metrics,
# stream.sales.orders_windowed, their iceberg.* catalog variants and legacy
# underscored spellings) are rewritten to the normalized physical SQLite table
# names. Logical names and physical spellings derive from src.domain.naming
# (physical_sql_name), while the iceberg.* / legacy alias keys are conserved.
# Data-driven (one dict, one pass) instead of a .replace() chain so every
# occurrence in a statement - including self-joins such as the RF-11 NOT EXISTS
# anti-join - is rewritten consistently. This bridge is temporary: it is removed
# once Trino/Polaris is enabled and the catalogs resolve logical names natively;
# when TRINO_HOST is set the SQL is sent to Trino WITHOUT rewriting for that
# reason.
LOGICAL_TO_PHYSICAL: dict[str, str] = {
    f"iceberg.{_GOLD_DAILY_METRICS}": physical_sql_name(_GOLD_DAILY_METRICS),
    f"iceberg.{_STREAM_WINDOWED}": physical_sql_name(_STREAM_WINDOWED),
    "iceberg.gold.sales_daily_metrics": physical_sql_name(_GOLD_DAILY_METRICS),
    "iceberg.stream.sales_orders_windowed": physical_sql_name(_STREAM_WINDOWED),
    "iceberg.sales.daily_metrics": physical_sql_name(_GOLD_DAILY_METRICS),
    "iceberg.sales.orders_windowed": physical_sql_name(_STREAM_WINDOWED),
    _GOLD_DAILY_METRICS: physical_sql_name(_GOLD_DAILY_METRICS),
    _STREAM_WINDOWED: physical_sql_name(_STREAM_WINDOWED),
    _SILVER_ORDERS: physical_sql_name(_SILVER_ORDERS),
    _BRONZE_ORDERS: physical_sql_name(_BRONZE_ORDERS),
}

# Longest key first so catalog-qualified names win over their bare 3-part
# substrings (e.g. iceberg.gold.sales.daily_metrics before gold.sales.daily_metrics).
_MAPPING_LONGEST_FIRST = sorted(
    LOGICAL_TO_PHYSICAL.items(), key=lambda kv: len(kv[0]), reverse=True
)


class SqlServingAdapter(QueryExecutorPort):
    """
    Outbound adapter executing serving queries (RF-11) through QueryExecutorPort.

    Backend selection is explicit:
    - TRINO_HOST set  -> query Trino first; on failure log a warning and fall
      back to embedded SQLite (explicit, logged - never silent).
    - TRINO_HOST unset -> embedded SQLite only (no hidden network attempts).

    Every execution logs ``Serving query executed by backend=...`` and SQL
    errors RAISE (the port contract forbids returning an empty list on failure).
    """

    def __init__(self, db_path: str = "data/lakehouse.db") -> None:
        self._db_path = db_path

    def execute_query(self, sql: str) -> list[dict[str, Any]]:
        trino_host = os.getenv("TRINO_HOST")
        if trino_host:
            try:
                rows = self._execute_trino(trino_host, sql)
                backend = "trino"
            except Exception as exc:
                logger.warning(
                    "Trino serving query failed (%s); falling back to embedded "
                    "SQLite backend (TRINO_HOST was set explicitly)",
                    exc,
                )
                rows = self._execute_embedded(sql)
                backend = "sqlite_fallback"
        else:
            rows = self._execute_embedded(sql)
            backend = "sqlite"

        logger.info("Serving query executed by backend=%s", backend)
        return rows

    def _execute_trino(self, host: str, sql: str) -> list[dict[str, Any]]:
        import trino

        conn = trino.dbapi.connect(
            host=host,
            port=int(os.getenv("TRINO_PORT", "8080")),
            user=os.getenv("TRINO_USER", "trino"),
            catalog=os.getenv("TRINO_CATALOG", "iceberg"),
        )
        try:
            cur = conn.cursor()
            cur.execute(sql)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r, strict=False)) for r in cur.fetchall()]
        finally:
            conn.close()

    def _execute_embedded(self, sql: str) -> list[dict[str, Any]]:
        conn = sqlite3.connect(self._db_path)
        try:
            conn.row_factory = sqlite3.Row
            rewritten = self._rewrite_logical_names(sql, conn)
            try:
                cur = conn.execute(rewritten)
                rows = cur.fetchall()
            except sqlite3.Error as exc:
                raise RuntimeError(f"serving query failed: {exc}") from exc
            return [dict(r) for r in rows]
        finally:
            conn.close()

    @staticmethod
    def _rewrite_logical_names(sql: str, conn: sqlite3.Connection) -> str:
        """Rewrites every logical table occurrence to its physical name.

        Live-row robustness: storage may or may not carry time-travel columns
        (``_row_superseded``), so we probe the physical table with PRAGMA
        table_info FIRST and only append the ``_row_superseded IS NULL`` live
        filter when that column exists. The same SQL therefore works against
        both the legacy snapshot schema and the versioned schema.
        """
        rewritten = sql
        for logical, physical in _MAPPING_LONGEST_FIRST:
            if logical not in rewritten:
                continue
            replacement = physical
            columns = {
                row[1]
                for row in conn.execute(f'PRAGMA table_info("{physical}")').fetchall()
            }
            if "_row_superseded" in columns:
                replacement = (
                    f"(SELECT * FROM {physical} WHERE _row_superseded IS NULL)"
                )
            rewritten = rewritten.replace(logical, replacement)
        return rewritten
