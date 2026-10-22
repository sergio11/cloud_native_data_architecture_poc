"""Modern Data Architecture POC - Embedded synthetic OLTP records (demo profile).

Deterministic in-repo generator used as the local fallback when PostgreSQL is
unconfigured or unreachable.
"""

from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta
from typing import Any

DEFAULT_SEED = 42

# Fixed reference clock so output is reproducible per (seed, table_name, count).
SYNTHETIC_REFERENCE_TIME = datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC)


def generate_synthetic_records(table_name: str, count: int, *, seed: int = DEFAULT_SEED) -> list[dict[str, Any]]:
    """Generates deterministic demo records for the known demo tables."""
    rng = random.Random(seed)
    records = []
    now = SYNTHETIC_REFERENCE_TIME

    if table_name == "customers":
        segments = ["premium", "standard", "basic"]
        names = ["Alice Smith", "Bob Jones", "Carol White", "David Brown", "Emma Watson", "Frank Miller"]
        for i in range(1, count + 1):
            name = rng.choice(names)
            slug = name.lower().replace(" ", ".")
            records.append(
                {
                    "id": i,
                    "name": name,
                    "email": f"{slug}{i}@example.com",
                    "segment": rng.choice(segments),
                    "registration_date": (now - timedelta(days=rng.randint(10, 365))).strftime("%Y-%m-%d"),
                    "created_at": (now - timedelta(days=rng.randint(1, 100))).isoformat(),
                }
            )

    elif table_name == "orders":
        statuses = ["confirmed", "pending", "shipped", "cancelled"]
        for i in range(1, count + 1):
            records.append(
                {
                    "id": i,
                    "customer_id": rng.randint(1, min(100, count)),
                    "total_amount": round(rng.uniform(25.0, 450.0), 2),
                    "status": rng.choice(statuses),
                    "order_date": (now - timedelta(days=rng.randint(0, 14))).strftime("%Y-%m-%d"),
                    "created_at": (now - timedelta(hours=rng.randint(1, 72))).isoformat(),
                }
            )

    elif table_name == "campaigns":
        channels = ["email", "social", "search", "display"]
        for i in range(1, count + 1):
            records.append(
                {
                    "id": i,
                    "channel": rng.choice(channels),
                    "budget": round(rng.uniform(500.0, 5000.0), 2),
                    "start_date": (now - timedelta(days=30)).strftime("%Y-%m-%d"),
                    "end_date": now.strftime("%Y-%m-%d"),
                }
            )

    return records
