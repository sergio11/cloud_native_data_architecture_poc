"""
silver_transform.py - Silver layer transformation, conformance, and quality gate pipeline.
Driving Adapter invoking CleanseSilverUseCase via Hexagonal Architecture container.
Applies deduplication, type casting, schema validation, and Great Expectations quality gates.
A breached quality gate raises QualityGateBreachError and blocks promotion (RF-06);
a missing expectation suite propagates SuiteNotFoundError and also aborts the run.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

# Ensure repository root is on sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.container import container
from src.domain.exceptions import QualityGateBreachError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("silver_transform")


def main() -> None:
    parser = argparse.ArgumentParser(description="Silver Layer Cleaning & Quality Gate Pipeline (Hexagonal)")
    parser.add_argument(
        "--entity",
        default="orders",
        choices=["orders", "customers", "all"],
        help="Entity to cleanse and promote to Silver tier",
    )
    parser.add_argument("--threshold", default=0.95, type=float, help="Quality gate pass threshold (0.0 to 1.0)")
    parser.add_argument("--marquez-url", default=os.getenv("MARQUEZ_URL", "http://localhost:5000"))
    args = parser.parse_args()

    entities = ["orders", "customers"] if args.entity == "all" else [args.entity]
    for ent in entities:
        logger.info("Triggering Silver transformation & quality gate for '%s'...", ent)
        try:
            res = container.cleanse_silver.execute(ent, quality_threshold=args.threshold)
            logger.info(
                "Silver promotion successful for '%s': %d rows conformed (Quality score: %.2f)",
                res["target_table"], res["rows_promoted"], res["quality_pass_rate"]
            )
        except QualityGateBreachError as exc:
            logger.error(
                "Promotion to Silver blocked by Quality Gate: %s (pass rate %.2f < threshold %.2f)",
                exc, exc.pass_rate, exc.threshold,
            )
            sys.exit(1)
        except Exception as exc:
            logger.error("Silver transformation failed: %s", exc)
            sys.exit(1)


if __name__ == "__main__":
    main()
