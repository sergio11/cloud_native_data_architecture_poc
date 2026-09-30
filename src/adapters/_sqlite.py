"""
Modern Data Architecture POC - Shared SQLite connection lifecycle helper.

``sqlite3`` connections used as context managers (``with conn:``) commit or
roll back on exit but are NEVER closed by the context manager: relying on the
garbage collector retains file handles, delays WAL checkpoints and risks
"database is locked" errors under load. Every adapter must therefore acquire
SQLite connections through :func:`sqlite_connection`, which guarantees
commit-on-success, rollback-on-error and ``close()`` in all cases.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any


@contextmanager
def sqlite_connection(
    db_path: str | Path,
    *,
    row_factory: Any | None = None,
    timeout: float = 10.0,
    check_same_thread: bool = True,
    pragmas: tuple[str, ...] = (),
) -> Iterator[sqlite3.Connection]:
    """Yields a SQLite connection that is always committed/rolled back and closed.

    On clean exit the transaction commits; on exception it rolls back. The
    connection is closed in a ``finally`` block either way. Optional
    ``pragmas`` run once right after connecting (outside any transaction).
    """
    conn = sqlite3.connect(str(db_path), timeout=timeout, check_same_thread=check_same_thread)
    try:
        if row_factory is not None:
            conn.row_factory = row_factory
        for pragma in pragmas:
            conn.execute(pragma)
        with conn:
            yield conn
    finally:
        conn.close()
