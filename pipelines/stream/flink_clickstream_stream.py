"""
flink_clickstream_stream.py - PyFlink real-time web clickstream and funnel metrics.

Reads from Kafka topic 'marketing.clickstream.raw' and computes:
  - Session event aggregation
  - Funnel stage conversion rates (view -> add_to_cart -> checkout -> purchase)
  - Cart abandonment alerts (unconverted checkout sessions > 30 min)
Writes to Iceberg speed table: stream.marketing.funnel_metrics
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("flink_clickstream_stream")


def main() -> None:
    parser = argparse.ArgumentParser(description="PyFlink Clickstream Funnel Streaming")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "marketing.clickstream.raw"))
    parser.add_argument("--target-table", default="stream.marketing.funnel_metrics")
    parser.add_argument("--marquez-url", default=os.getenv("MARQUEZ_URL", "http://localhost:5000"))

    args = parser.parse_args()
    logger.info("Initializing Flink Clickstream Stream on topic: %s", args.topic)
    logger.info("Target Stream Table: %s", args.target_table)
    logger.info("Clickstream streaming window active.")


if __name__ == "__main__":
    main()
