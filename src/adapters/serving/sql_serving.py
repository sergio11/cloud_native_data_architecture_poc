"""
Modern Data Architecture POC - SQL Serving Adapter (Outbound / Driven Side).
Implements QueryExecutorPort: the application core hands over ANSI SQL and this
adapter decides which backend serves it, keeping RF-11 unified serving queries
(Trino when explicitly enabled, embedded SQLite otherwise) out of the core.

No-rewrite contract: the SQL is executed VERBATIM on both backends. The use
case references ``<physical>_live`` serving views (SSOT:
``src.domain.naming.serving_view_name``), never dotted logical names, so there
is no logical-to-physical bridge left in this adapter. Live-row filtering
(``_row_superseded IS NULL``) lives in the view definitions, not in rewriting.
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

# Logical tables behind the serving live-view contract. The views are named
# ``<physical>_live`` via serving_view_name (deterministic SSOT in
# src.domain.naming); the use case only queries Gold + stream, while Silver /
# Bronze twins are provisioned alongside so ad-hoc serving SQL can migrate off
# dotted names without touching this adapter.
_SERVING_LOGICAL_TABLES: tuple[str, ...] = (
    gold_table("daily_metrics"),
    stream_table(),
    silver_table("orders"),
    bronze_table("orders"),
)


def _ensure_serving_views(conn: sqlite3.Connection) -> None:
    """Provisions every ``<physical>_live`` serving view idempotently.

    Serving owns this contract (NOT storage: LakehouseStorageAdapter is left
    untouched, and legacy/raw tables created outside storage still resolve).
    For each logical table whose physical table exists:
    - when the SQLite-only versioning column ``_row_superseded`` exists, the
      view is ``SELECT * FROM <physical> WHERE _row_superseded IS NULL`` so
      superseded row versions stay excluded from serving reads;
    - otherwise the view is a plain pass-through ``SELECT * FROM <physical>``.
    Missing physical tables are skipped: the subsequent query then fails
    loudly (port contract forbids empty stand-ins for failures).
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
    """
    Outbound adapter executing serving queries (RF-11) through QueryExecutorPort.

    Backend selection is explicit:
    - TRINO_HOST set  -> query Trino first; on failure log a warning and fall
      back to embedded SQLite (explicit, logged - never silent).
    - TRINO_HOST unset -> embedded SQLite only (no hidden network attempts).

    The SQL text is NEVER mutated: it is executed exactly as received on both
    backends (the ``<physical>_live`` views it references are provisioned by
    ``_ensure_serving_views`` on SQLite and deployed by
    ``pipelines/serving/unified_view.sql`` on Trino, where bare view names
    resolve against the ``serving`` schema via TRINO_SCHEMA).

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
        # Shared lifecycle helper: commit-on-success, rollback-on-error and
        # close() in every case (raw sqlite3.connect leaks the handle).
        with sqlite_connection(self._db_path, row_factory=sqlite3.Row) as conn:
            _ensure_serving_views(conn)
            try:
                cur = conn.execute(sql)
                rows = cur.fetchall()
            except sqlite3.Error as exc:
                raise RuntimeError(f"serving query failed: {exc}") from exc
            return [dict(r) for r in rows]
