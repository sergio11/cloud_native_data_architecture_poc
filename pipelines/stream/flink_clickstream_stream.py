"""
flink_clickstream_stream.py - Clickstream streaming speed layer (Kappa Architecture).

Driving adapter invoking ProcessStreamUseCase via the hexagonal container.
Consumes clickstream events from the canonical Kafka topic
``marketing.clickstream.raw`` (see data-generator/event_producer.py), computes
tumbling window aggregations through the core business aggregator, and writes
them to the speed table ``stream.marketing.clickstream_windowed`` with support
for Kappa historical offset replay.

Funnel stage conversion metrics (view -> add_to_cart -> checkout -> purchase),
sessionization and cart-abandonment alerts are out of POC scope (SRS §1.2);
only the generic tumbling window aggregation is provided by the core today.
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
logger = logging.getLogger("flink_clickstream_stream")


def main() -> None:
    parser = argparse.ArgumentParser(description="Clickstream Streaming Speed Layer (Kappa Architecture)")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_CLICKSTREAM_TOPIC", "marketing.clickstream.raw"))
    parser.add_argument("--target-table", default="stream.marketing.clickstream_windowed")
    parser.add_argument("--start-offset", default="latest", choices=["earliest", "latest"])
    args = parser.parse_args()

    replay_mode = args.start_offset == "earliest"
    logger.info(
        "Starting Clickstream Speed Layer pipeline on topic '%s' (offset=%s, replay=%s)...",
        args.topic,
        args.start_offset,
        replay_mode,
    )

    res = container.process_stream.execute(
        topic=args.topic,
        target_table=args.target_table,
        replay=replay_mode,
    )
    logger.info("Clickstream speed window processing completed: %s", res)


if __name__ == "__main__":
    main()
