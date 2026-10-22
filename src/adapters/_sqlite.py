"""Modern Data Architecture POC - Shared SQLite connection lifecycle helper.

``sqlite_connection`` guarantees commit-or-rollback and ``close()``.
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
    """Yields a connection committed on success, rolled back on error, always closed.

    Optional ``pragmas`` run once right after connecting.
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
