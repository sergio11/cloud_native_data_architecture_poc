"""
bronze_ingestion.py - Bronze layer raw ingestion pipeline.
Driving Adapter invoking IngestBronzeUseCase via Hexagonal Architecture container.
Extracts operational records, attaches audit technical columns (_ingested_at, _source, _batch_id),
and writes idempotently to Bronze tier lakehouse storage.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

# Ensure repository root is on sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.container import container

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("bronze_ingestion")

SUPPORTED_ENTITIES = ["orders", "customers", "order_items", "campaigns"]


def main() -> None:
    parser = argparse.ArgumentParser(description="Bronze Layer Raw Ingestion Pipeline (Hexagonal)")
    parser.add_argument(
        "--entity",
        default="orders",
        choices=["orders", "customers", "order_items", "campaigns", "all"],
        help="Operational entity to ingest into Bronze tier",
    )
    parser.add_argument("--marquez-url", default=os.getenv("MARQUEZ_URL", "http://localhost:5000"))
    args = parser.parse_args()

    entities = SUPPORTED_ENTITIES if args.entity == "all" else [args.entity]
    for ent in entities:
        logger.info("Triggering Bronze ingestion use case for '%s'...", ent)
        res = container.ingest_bronze.execute(ent)
        logger.info(
            "Bronze ingestion finished: %s -> %d rows written (batch_id: %s)",
            res["target_table"], res["rows_ingested"], res["batch_id"],
        )


if __name__ == "__main__":
    main()
