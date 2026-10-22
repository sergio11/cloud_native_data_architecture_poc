"""
gold_aggregation.py - Gold layer aggregation and PII pseudonymization.

Driving adapter invoking AggregateGoldUseCase: daily sales rollups, GDPR Art.25
pseudonymization and OpenLineage events.
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
logger = logging.getLogger("gold_aggregation")

GOLD_SUITES = {
    "daily_metrics": "quality/expectations/gold_daily_metrics_suite.json",
    "customers": "quality/expectations/gold_customers_suite.json",
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Gold Layer Analytical Modeling & PII Masking Pipeline (Hexagonal)")
    parser.add_argument(
        "--target",
        default="all",
        choices=["all", "daily_metrics", "customers"],
        help="Target Gold product to materialize",
    )
    args = parser.parse_args()

    logger.info("Triggering Gold aggregation use case (target='%s')...", args.target)
    res = container.aggregate_gold.execute(
        target=args.target,
        daily_metrics_suite=GOLD_SUITES["daily_metrics"],
        customers_suite=GOLD_SUITES["customers"],
    )
    logger.info(
        "Gold materialization finished with status: %s (full recomputation from Silver = Lambda batch replay)",
        res.get("status"),
    )
    logger.info("Details: %s", res)


if __name__ == "__main__":
    main()
