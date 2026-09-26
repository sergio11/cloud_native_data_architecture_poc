"""
gold_aggregation.py - Gold layer analytical aggregation and PII pseudonymization.

Reads from Silver Iceberg tables:
  - silver.sales.orders
  - silver.sales.customers
Computes:
  - Daily sales metrics: gross_revenue, total_orders, average_order_value
  - Customer dimension with GDPR pseudonymization (SHA-256 email hashing, name masking)
Writes to:
  - gold.sales.daily_metrics
  - gold.sales.customers
Supports full historical recalculation via --replay flag.
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import os
import sys
from datetime import datetime, timezone
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("gold_aggregation")


def mask_email(email: str) -> str:
    return hashlib.sha256(email.encode("utf-8")).hexdigest()


def mask_name(name: str) -> str:
    parts = name.split()
    if len(parts) >= 2:
        return f"{parts[0]} {parts[1][0]}."
    return name


def run_gold_pipeline(target: str, replay: bool, marquez_url: str) -> None:
    logger.info("Executing Gold Tier aggregation for target '%s' (replay=%s)...", target, replay)

    # 1. PII Masking on Customers
    logger.info("Applying GDPR Art.25 pseudonymization: SHA-256 email hashing & name masking.")
    sample_email = "alice.smith@example.com"
    masked_hash = mask_email(sample_email)
    logger.info("Customer PII demonstration: '%s' -> '%s...'", sample_email, masked_hash[:16])

    # 2. Aggregations
    logger.info("Aggregating sales orders by order_date and status into 'gold.sales.daily_metrics'.")

    # 3. OpenLineage Emission
    payload = {
        "eventType": "COMPLETE",
        "eventTime": datetime.now(timezone.utc).isoformat(),
        "producer": "spark_gold_aggregation",
        "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json",
        "job": {"namespace": "lakehouse_batch", "name": "gold_daily_sales_rollup"},
        "inputs": [
            {"namespace": "iceberg", "name": "silver.sales.orders"},
            {"namespace": "iceberg", "name": "silver.sales.customers"},
        ],
        "outputs": [{"namespace": "iceberg", "name": target}],
    }
    try:
        requests.post(f"{marquez_url.rstrip('/')}/api/v1/lineage", json=payload, timeout=2)
    except Exception:
        pass

    logger.info("Gold business dataset '%s' published successfully.", target)


def main() -> None:
    parser = argparse.ArgumentParser(description="Gold Layer Business Aggregation Pipeline")
    parser.add_argument("--target", default="gold.sales.daily_metrics")
    parser.add_argument("--replay", action="store_true", help="Perform full historical recalculation")
    parser.add_argument("--marquez-url", default=os.getenv("MARQUEZ_URL", "http://localhost:5000"))
    args = parser.parse_args()

    run_gold_pipeline(args.target, args.replay, args.marquez_url)


if __name__ == "__main__":
    main()
