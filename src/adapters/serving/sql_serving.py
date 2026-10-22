"""Modern Data Architecture POC - SQL Serving Adapter.

Implements QueryExecutorPort, choosing the backend (Trino when enabled,
embedded SQLite otherwise). SQL is executed verbatim on both backends.
"""

from __future__ import annotations

import logging
import os
import sqlite3
from typing import Any

from src.adapters._sqlite import sqlite_connection
from src.domain.naming import bronze_table, gold_table, physical_sql_name, serving_view_name, silver_table, stream_table
from src.ports.outbound import QueryExecutorPort

logger = logging.getLogger("adapter.serving.sql")

# Logical tables behind the serving live-view contract. Views are named
# ``<physical>_live``; Silver/Bronze twins support ad-hoc serving SQL.
_SERVING_LOGICAL_TABLES: tuple[str, ...] = (
    gold_table("daily_metrics"),
    stream_table(),
    silver_table("orders"),
    bronze_table("orders"),
)


def _ensure_serving_views(conn: sqlite3.Connection) -> None:
    """Provisions every ``<physical>_live`` serving view idempotently.

    Live rows only: excludes ``_row_superseded`` when that column exists.
    Missing physical tables are skipped; the query then fails loudly.
    """
    for logical in _SERVING_LOGICAL_TABLES:
        physical = physical_sql_name(logical)
        view = serving_view_name(logical)
        exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type IN ('table', 'view') AND name = ?;",
            (physical,),
        ).fetchone()
        if exists is None:
            continue
        columns = {row[1] for row in conn.execute(f'PRAGMA table_info("{physical}")').fetchall()}
        if "_row_superseded" in columns:
            conn.execute(
                f'CREATE VIEW IF NOT EXISTS "{view}" AS SELECT * FROM "{physical}" WHERE _row_superseded IS NULL;'
            )
        else:
            conn.execute(f'CREATE VIEW IF NOT EXISTS "{view}" AS SELECT * FROM "{physical}";')


class SqlServingAdapter(QueryExecutorPort):
    """Outbound adapter executing serving queries through QueryExecutorPort.

    Backend selection: TRINO_HOST queries Trino first and logs a fallback to
    SQLite on failure; otherwise SQLite only. SQL text is never mutated.
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
            schema=os.getenv("TRINO_SCHEMA", "serving"),
        )
        try:
            cur = conn.cursor()
            cur.execute(sql)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r, strict=False)) for r in cur.fetchall()]
        finally:
            conn.close()

    def _execute_embedded(self, sql: str) -> list[dict[str, Any]]:
        # Shared lifecycle helper: commit on success, rollback on error, close.
        with sqlite_connection(self._db_path, row_factory=sqlite3.Row) as conn:
            _ensure_serving_views(conn)
            try:
                cur = conn.execute(sql)
                rows = cur.fetchall()
            except sqlite3.Error as exc:
                raise RuntimeError(f"serving query failed: {exc}") from exc
            return [dict(r) for r in rows]
