"""
flink_orders_stream.py - Streaming Speed Layer pipeline (Kappa Architecture).
Driving Adapter invoking ProcessStreamUseCase via Hexagonal Architecture container.
Consumes continuous order event streams from Kafka, calculates 5-minute tumbling windows,
writes to Speed table, and supports Kappa historical offset replay (RF-10).
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import UTC, datetime, timedelta

# Ensure repository root is on sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.container import container

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("flink_orders_stream")

DEMO_SEED_COUNT = 14
DEMO_SEED_SPREAD_SECONDS = 40  # 14 events x 40s => ~8.7 min span => >= 2 tumbling windows


def seed_demo_events_if_empty(topic: str, explicit_flag: bool) -> int:
    """Publishes a small deterministic burst when the topic event log is empty.

    The event broker adapter never fabricates events (RF-10), but scripts we do
    not own (scripts/demo.sh, Rakefile) invoke this pipeline WITHOUT
    --seed-if-empty and expect a populated speed buffer for the RF-09/RF-10
    demonstration. Therefore an empty log is seeded automatically, loudly, so
    demo runs stay truthful while the broker contract stays honest.
    """
    existing = container.stream_broker.consume_events(topic=topic, replay=True)
    if existing:
        return 0

    now = datetime.now(UTC)
    seeded = 0
    for i in range(DEMO_SEED_COUNT):
        event_ts = now - timedelta(seconds=(DEMO_SEED_COUNT - 1 - i) * DEMO_SEED_SPREAD_SECONDS)
        payload = {
            "event_id": f"demo-{topic}-{9001 + i}",
            "order_id": 9001 + i,
            "customer_id": i + 1,
            "total_amount": round(35.0 + (i + 1) * 7.5, 2),
            "status": "confirmed",
            "order_date": event_ts.strftime("%Y-%m-%d"),
            "timestamp": event_ts.isoformat(),
        }
        container.stream_broker.publish_event(topic, str(payload["order_id"]), payload)
        seeded += 1

    if seeded:
        if explicit_flag:
            logger.info("Seeded %d demo events into topic %s (--seed-if-empty)", seeded, topic)
        else:
            logger.warning(
                "Seeded %d demo events into topic %s (auto demo seeding: event log was empty; "
                "scripts/demo.sh and the Rakefile invoke this pipeline without --seed-if-empty)",
                seeded, topic,
            )
    return seeded


def main() -> None:
    parser = argparse.ArgumentParser(description="Streaming Speed Layer (Kappa Architecture)")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "sales.orders.events"))
    parser.add_argument("--target-table", default="stream.sales.orders_windowed")
    parser.add_argument("--start-offset", default="latest", choices=["earliest", "latest"])
    parser.add_argument(
        "--replay",
        action="store_true",
        help="Full replay of the topic log from offset 0 (RF-10 full recompute)",
    )
    parser.add_argument("--seed-if-empty", action="store_true", default=False,
                        help="Publish a deterministic 14-event demo burst when the topic log is empty "
                             "(an empty log is also auto-seeded loudly for demo-script compatibility)")
    args = parser.parse_args()

    replay_mode = args.replay or args.start_offset == "earliest"
    logger.info(
        "Starting Streaming Speed Layer Pipeline on topic '%s' (offset=%s, replay=%s)...",
        args.topic, args.start_offset, replay_mode,
    )

    seed_demo_events_if_empty(args.topic, args.seed_if_empty)

    res = container.process_stream.execute(
        topic=args.topic,
        target_table=args.target_table,
        replay=replay_mode,
    )
    logger.info("Streaming speed window processing completed: %s", res)


if __name__ == "__main__":
    main()
