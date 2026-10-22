"""Modern Data Architecture POC - Lakehouse storage adapter.

Implements LakehouseStoragePort on SQLite with content-hash idempotency,
overwrite-as-supersede and snapshot time travel.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any

from src.adapters._files import ensure_parent_dir
from src.adapters._sqlite import sqlite_connection
from src.domain.naming import physical_sql_name
from src.ports.outbound import LakehouseStoragePort

logger = logging.getLogger("adapter.storage.lakehouse")

#: Allowlist for logical table names (dots separate the domain tiers and are
#: normalized to underscores). Anything else is rejected before it can reach
#: an f-string-interpolated SQL identifier (same guard as OltpSourceAdapter).
_TABLE_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*$")

INTERNAL_COLUMN_TYPES: dict[str, str] = {
    "_snapshot_time": "TEXT",
    "_row_version": "INTEGER",
    "_row_superseded": "INTEGER",
    "_content_hash": "TEXT",
}


class LakehouseStorageAdapter(LakehouseStoragePort):
    """ACID lakehouse storage adapter for local and containerized execution.

    Each write records a snapshot; superseded rows stay readable for time
    travel. Missing internal columns are added automatically.
    """

    def __init__(self, db_path: str = "data/lakehouse.db") -> None:
        self._db_path = db_path
        ensure_parent_dir(db_path)
        self._init_metadata()

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        """Yields a managed connection (row tuples, WAL mode, always closed)."""
        with sqlite_connection(self._db_path, row_factory=sqlite3.Row) as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            yield conn

    def _normalize_name(self, table_name: str) -> str:
        if not isinstance(table_name, str) or not _TABLE_NAME_PATTERN.match(table_name):
            raise ValueError(
                f"Invalid table name {table_name!r}: must match {_TABLE_NAME_PATTERN.pattern} "
                "(SQL identifier allowlist)"
            )
        # Single naming rule shared with the domain (dots become underscores).
        return physical_sql_name(table_name)

    def _init_metadata(self) -> None:
        with self._connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS _lakehouse_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_name TEXT NOT NULL,
                    snapshot_version INTEGER NOT NULL,
                    row_count INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    mode TEXT
                );
            """)
            snapshot_columns = {row["name"] for row in conn.execute("PRAGMA table_info(_lakehouse_snapshots);")}
            if "mode" not in snapshot_columns:
                conn.execute("ALTER TABLE _lakehouse_snapshots ADD COLUMN mode TEXT;")
            conn.commit()
            # Uniqueness for (table, version) so racing writers cannot publish
            # the same version; pre-existing duplicates skip the index with a warning.
            try:
                conn.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS ux_snapshots_table_version "
                    "ON _lakehouse_snapshots(table_name, snapshot_version);"
                )
                conn.commit()
            except sqlite3.IntegrityError as exc:
                logger.warning(
                    "LakehouseStorage: legacy duplicate snapshot versions detected, "
                    "uniqueness index skipped (%s); reset state to re-enable it",
                    exc,
                )

    @staticmethod
    def _content_hash(record: dict[str, Any]) -> str:
        payload = {key: value for key, value in record.items() if not key.startswith("_")}
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    @staticmethod
    def _stored_value(value: Any) -> Any:
        """Maps a Python value to its SQLite storage value, preserving numerics."""
        if value is None:
            return None
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, (int, float)):
            return value
        if isinstance(value, (dict, list)):
            return json.dumps(value)
        return str(value)

    @staticmethod
    def _ordered_columns(records: list[dict[str, Any]]) -> list[str]:
        columns: list[str] = []
        seen: set[str] = set()
        for record in records:
            for key in record:
                if key not in seen and key not in INTERNAL_COLUMN_TYPES:
                    seen.add(key)
                    columns.append(key)
        return columns

    @staticmethod
    def _infer_column_affinities(records: list[dict[str, Any]], columns: list[str]) -> dict[str, str]:
        """Infers SQLite column affinities from the Python types of ``records``."""
        affinities: dict[str, str] = {}
        for column in columns:
            has_text = False
            has_real = False
            has_integer = False
            for record in records:
                value = record.get(column)
                if value is None:
                    continue
                if isinstance(value, bool):
                    has_integer = True
                elif isinstance(value, int):
                    has_integer = True
                elif isinstance(value, float):
                    has_real = True
                else:
                    has_text = True
                    break
            if has_text:
                affinities[column] = "TEXT"
            elif has_real:
                affinities[column] = "REAL"
            elif has_integer:
                affinities[column] = "INTEGER"
            else:
                affinities[column] = "TEXT"
        return affinities

    @staticmethod
    def _table_columns(conn: sqlite3.Connection, clean_name: str) -> set[str]:
        cursor = conn.execute(f'PRAGMA table_info("{clean_name}");')
        return {row["name"] for row in cursor.fetchall()}

    def _ensure_internal_columns(self, conn: sqlite3.Connection, clean_name: str) -> None:
        """Adds every missing internal versioning column to ``clean_name``.

        New columns default to NULL, interpreted as a live row at version 0.
        """
        existing = self._table_columns(conn, clean_name)
        for column, column_type in INTERNAL_COLUMN_TYPES.items():
            if column not in existing:
                conn.execute(f'ALTER TABLE "{clean_name}" ADD COLUMN "{column}" {column_type};')

    @staticmethod
    def _table_exists(conn: sqlite3.Connection, clean_name: str) -> bool:
        """Checks the catalog for ``clean_name`` (single home for the lookup SQL)."""
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?;",
            (clean_name,),
        ).fetchone()
        return row is not None

    def _ensure_table(
        self,
        conn: sqlite3.Connection,
        clean_name: str,
        columns: list[str],
        affinities: dict[str, str] | None = None,
    ) -> None:
        """Creates the table (or adds missing columns) honouring affinities."""
        resolved = affinities or {}
        if not self._table_exists(conn, clean_name):
            definitions = [f'"{column}" {resolved.get(column, "TEXT")}' for column in columns]
            definitions.extend(f'"{column}" {column_type}' for column, column_type in INTERNAL_COLUMN_TYPES.items())
            conn.execute(f'CREATE TABLE "{clean_name}" ({", ".join(definitions)});')
            return
        self._ensure_internal_columns(conn, clean_name)
        existing = self._table_columns(conn, clean_name)
        for column in columns:
            if column not in existing:
                conn.execute(f'ALTER TABLE "{clean_name}" ADD COLUMN "{column}" {resolved.get(column, "TEXT")};')

    @staticmethod
    def _next_version(conn: sqlite3.Connection, table_name: str) -> int:
        row = conn.execute(
            "SELECT MAX(snapshot_version) AS max_version FROM _lakehouse_snapshots WHERE table_name = ?;",
            (table_name,),
        ).fetchone()
        max_version = row["max_version"]
        return int(max_version) + 1 if max_version is not None else 1

    def write_table(
        self,
        table_name: str,
        records: list[dict[str, Any]],
        mode: str = "append",
    ) -> int:
        """Writes records with content-hash idempotency and versioning.

        Append inserts only unseen rows; overwrite supersedes live rows without
        deleting them. An empty overwrite reconciles the table to empty.
        """
        if mode not in ("append", "overwrite"):
            raise ValueError(f"Unsupported write mode '{mode}'. Expected 'append' or 'overwrite'.")
        if not records and mode == "append":
            return 0

        clean_name = self._normalize_name(table_name)
        now_iso = datetime.now(UTC).isoformat()

        with self._connection() as conn:
            # Serialize concurrent writers: BEGIN IMMEDIATE avoids a TOCTOU
            # on the snapshot version.
            conn.execute("BEGIN IMMEDIATE")
            if not records:
                # Overwrite-to-empty: supersede every live row under a new
                # snapshot version (row_count 0). Never creates the table.
                if not self._table_exists(conn, clean_name):
                    return 0
                self._ensure_internal_columns(conn, clean_name)
                new_version = self._next_version(conn, table_name)
                conn.execute(
                    f'UPDATE "{clean_name}" SET _row_superseded = ? WHERE _row_superseded IS NULL;',
                    (new_version,),
                )
                conn.execute(
                    "INSERT INTO _lakehouse_snapshots "
                    "(table_name, snapshot_version, row_count, created_at, mode) VALUES (?, ?, ?, ?, ?);",
                    (table_name, new_version, 0, now_iso, mode),
                )
                conn.commit()
                logger.info(
                    "LakehouseStorage: overwrite of '%s' with empty set, table cleared (version=%d)",
                    table_name,
                    new_version,
                )
                return 0

            columns = self._ordered_columns(records)
            hashes = [self._content_hash(record) for record in records]
            self._ensure_table(conn, clean_name, columns, self._infer_column_affinities(records, columns))

            if mode == "append":
                known_hashes = {
                    row["_content_hash"]
                    for row in conn.execute(f'SELECT _content_hash FROM "{clean_name}";')
                    if row["_content_hash"] is not None
                }
                rows_to_insert: list[tuple[dict[str, Any], str]] = []
                seen_hashes = set(known_hashes)
                for record, row_hash in zip(records, hashes, strict=True):
                    if row_hash in seen_hashes:
                        continue
                    seen_hashes.add(row_hash)
                    rows_to_insert.append((record, row_hash))
                if not rows_to_insert:
                    logger.info(
                        "LakehouseStorage: append to '%s' fully idempotent, no snapshot created",
                        table_name,
                    )
                    return 0
            else:
                live_hashes = {
                    row["_content_hash"]
                    for row in conn.execute(f'SELECT _content_hash FROM "{clean_name}" WHERE _row_superseded IS NULL;')
                    if row["_content_hash"] is not None
                }
                if set(hashes) == live_hashes:
                    logger.info(
                        "LakehouseStorage: overwrite of '%s' unchanged, no snapshot created",
                        table_name,
                    )
                    return 0
                rows_to_insert = list(zip(records, hashes, strict=True))

            new_version = self._next_version(conn, table_name)

            if mode == "overwrite":
                conn.execute(
                    f'UPDATE "{clean_name}" SET _row_superseded = ? WHERE _row_superseded IS NULL;',
                    (new_version,),
                )

            insert_columns = columns + list(INTERNAL_COLUMN_TYPES)
            column_names = ", ".join(f'"{column}"' for column in insert_columns)
            placeholders = ", ".join("?" for _ in insert_columns)
            payload_rows: list[tuple[Any, ...]] = []
            for record, row_hash in rows_to_insert:
                values: list[Any] = [self._stored_value(record.get(column)) for column in columns]
                values.extend([now_iso, new_version, None, row_hash])
                payload_rows.append(tuple(values))

            conn.executemany(
                f'INSERT INTO "{clean_name}" ({column_names}) VALUES ({placeholders});',
                payload_rows,
            )
            conn.execute(
                "INSERT INTO _lakehouse_snapshots "
                "(table_name, snapshot_version, row_count, created_at, mode) VALUES (?, ?, ?, ?, ?);",
                (table_name, new_version, len(payload_rows), now_iso, mode),
            )
            conn.commit()

        logger.info(
            "LakehouseStorage: Wrote %d records to '%s' (mode=%s, version=%d)",
            len(payload_rows),
            table_name,
            mode,
            new_version,
        )
        return len(payload_rows)

    def read_table(
        self,
        table_name: str,
        limit: int | None = None,
        version: int | None = None,
    ) -> list[dict[str, Any]]:
        """Reads live rows (``version=None``) or the state as of ``version``.

        Rows are ordered by rowid; internal versioning columns are stripped.
        """
        clean_name = self._normalize_name(table_name)
        with self._connection() as conn:
            if not self._table_exists(conn, clean_name):
                return []

            self._ensure_internal_columns(conn, clean_name)
            params: list[Any] = []
            if version is None:
                where = "WHERE _row_superseded IS NULL"
            else:
                where = "WHERE COALESCE(_row_version, 0) <= ? AND (_row_superseded IS NULL OR _row_superseded > ?)"
                params.extend([int(version), int(version)])

            query = f'SELECT * FROM "{clean_name}" {where} ORDER BY rowid ASC'
            if limit is not None:
                query += " LIMIT ?"
                params.append(int(limit))

            rows = conn.execute(query, params).fetchall()

        results: list[dict[str, Any]] = []
        for row in rows:
            record = {key: value for key, value in dict(row).items() if key not in INTERNAL_COLUMN_TYPES}
            for key, value in record.items():
                if isinstance(value, str) and (value.startswith("{") or value.startswith("[")):
                    try:
                        record[key] = json.loads(value)
                    except (ValueError, TypeError):
                        pass
            results.append(record)
        return results

    def table_exists(self, table_name: str) -> bool:
        clean_name = self._normalize_name(table_name)
        with self._connection() as conn:
            return self._table_exists(conn, clean_name)

    def get_table_schema(self, table_name: str) -> list[str]:
        clean_name = self._normalize_name(table_name)
        with self._connection() as conn:
            cursor = conn.execute(f'PRAGMA table_info("{clean_name}");')
            return [row["name"] for row in cursor.fetchall() if row["name"] not in INTERNAL_COLUMN_TYPES]

    def list_versions(self, table_name: str) -> list[dict[str, Any]]:
        """Lists snapshots of ``table_name`` oldest to newest.

        ``row_count`` is the number of rows written by that snapshot.
        """
        with self._connection() as conn:
            rows = conn.execute(
                "SELECT snapshot_version, row_count, created_at FROM _lakehouse_snapshots "
                "WHERE table_name = ? ORDER BY snapshot_version ASC;",
                (table_name,),
            ).fetchall()
            return [
                {
                    "snapshot_version": int(row["snapshot_version"]),
                    "row_count": int(row["row_count"]),
                    "created_at": row["created_at"],
                }
                for row in rows
            ]
