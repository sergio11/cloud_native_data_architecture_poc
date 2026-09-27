"""
Modern Data Architecture POC - OLTP Source Adapter.
Extracts operational relational tables from PostgreSQL or local operational seed database.
"""

from __future__ import annotations

import logging
import os
import random
import re
from datetime import UTC, datetime, timedelta
from typing import Any

from src.ports.outbound import OltpSourcePort

logger = logging.getLogger("adapter.source.oltp")

TABLE_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

DEFAULT_SEED = 42

# Fixed reference clock for the synthetic fallback so output is fully
# reproducible per (seed, table_name, count). Wall-clock `now` would make
# two calls with the same seed differ in order_date/created_at timestamps.
SYNTHETIC_REFERENCE_TIME = datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC)


class OltpSourceAdapter(OltpSourcePort):
    """
    Adapter extracting operational relational tables.
    Supports PostgreSQL connection with seamless fallback to embedded operational seed.
    RNF-07: no embedded credentials - PGPASSWORD is read only from the environment;
    when it is unset PostgreSQL is treated as unavailable and the embedded
    synthetic generator serves the local demo profile instead.
    """

    def __init__(
        self,
        pg_host: str | None = None,
        seed: int | None = None,
    ) -> None:
        self._pg_host = pg_host or os.getenv("PGHOST", "localhost")
        if seed is None:
            env_seed = os.getenv("POC_DATA_SEED")
            try:
                self._seed = int(env_seed) if env_seed is not None else DEFAULT_SEED
            except (TypeError, ValueError):
                self._seed = DEFAULT_SEED
        else:
            self._seed = int(seed)

    @staticmethod
    def _validate_identifier(table_name: str) -> None:
        if not isinstance(table_name, str) or not TABLE_NAME_PATTERN.match(table_name):
            raise ValueError(
                f"Invalid table_name {table_name!r}: must match {TABLE_NAME_PATTERN.pattern} (SQL allowlist)"
            )

    @staticmethod
    def _validate_limit(limit: int | None) -> int | None:
        if limit is None:
            return None
        try:
            return int(limit)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid limit {limit!r}: must be an integer") from exc

    def extract_table(self, table_name: str, limit: int | None = None) -> list[dict[str, Any]]:
        # 0. Validate inputs before touching any backend (SQL injection guard)
        self._validate_identifier(table_name)
        limit = self._validate_limit(limit)

        # 1. Try PostgreSQL connection if psycopg2 is installed and a password is configured
        password = os.getenv("PGPASSWORD")
        if not password:
            logger.info(
                "PGPASSWORD not set (RNF-07: no embedded credentials); PostgreSQL source unavailable "
                "for '%s' - using embedded synthetic source",
                table_name,
            )
        else:
            try:
                import psycopg2
                import psycopg2.extras
                conn = psycopg2.connect(
                    host=self._pg_host,
                    port=int(os.getenv("PGPORT", "5432")),
                    dbname=os.getenv("PGDATABASE", "poc_oltp"),
                    user=os.getenv("PGUSER", "postgres"),
                    password=password,
                    connect_timeout=2,
                )
                try:
                    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                        query = f'SELECT * FROM "{table_name}"'
                        if limit:
                            query += f" LIMIT {limit}"
                        cur.execute(query)
                        rows = [dict(r) for r in cur.fetchall()]
                finally:
                    conn.close()
                if rows:
                    logger.info("Extracted %d records from PostgreSQL '%s'", len(rows), table_name)
                    return rows
            except Exception as exc:
                logger.info(
                    "PostgreSQL source unavailable (%s); falling back to embedded synthetic source",
                    exc,
                )

        # 2. Fallback to generating high-fidelity synthetic relational records
        logger.info(
            "SYNTHETIC fallback data: generating embedded demo records for table '%s' "
            "(not live PostgreSQL rows)",
            table_name,
        )
        return self._generate_synthetic_records(table_name, limit or 100)

    def _generate_synthetic_records(self, table_name: str, count: int) -> list[dict[str, Any]]:
        rng = random.Random(self._seed)
        records = []
        now = SYNTHETIC_REFERENCE_TIME

        if table_name == "customers":
            segments = ["premium", "standard", "basic"]
            names = ["Alice Smith", "Bob Jones", "Carol White", "David Brown", "Emma Watson", "Frank Miller"]
            for i in range(1, count + 1):
                name = rng.choice(names)
                slug = name.lower().replace(" ", ".")
                records.append({
                    "id": i,
                    "name": name,
                    "email": f"{slug}{i}@example.com",
                    "segment": rng.choice(segments),
                    "registration_date": (now - timedelta(days=rng.randint(10, 365))).strftime("%Y-%m-%d"),
                    "created_at": (now - timedelta(days=rng.randint(1, 100))).isoformat(),
                })

        elif table_name == "orders":
            statuses = ["confirmed", "pending", "shipped", "cancelled"]
            for i in range(1, count + 1):
                records.append({
                    "id": i,
                    "customer_id": rng.randint(1, min(100, count)),
                    "total_amount": round(rng.uniform(25.0, 450.0), 2),
                    "status": rng.choice(statuses),
                    "order_date": (now - timedelta(days=rng.randint(0, 14))).strftime("%Y-%m-%d"),
                    "created_at": (now - timedelta(hours=rng.randint(1, 72))).isoformat(),
                })

        elif table_name == "campaigns":
            channels = ["email", "social", "search", "display"]
            for i in range(1, count + 1):
                records.append({
                    "id": i,
                    "channel": rng.choice(channels),
                    "budget": round(rng.uniform(500.0, 5000.0), 2),
                    "start_date": (now - timedelta(days=30)).strftime("%Y-%m-%d"),
                    "end_date": now.strftime("%Y-%m-%d"),
                })

        return records
