"""
event_producer.py - Kafka event producer for e-commerce event streams.
Produces realistic real-time events to Kafka topics:
  - sales.orders.events:       order lifecycle state transitions
  - marketing.clickstream.raw: digital browsing & user session navigation events

All topic names, payload keys, and enums are in English.
"""

from __future__ import annotations

import json
import logging
import random
import sys
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import click

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("event_producer")

DEFAULT_BOOTSTRAP_SERVERS: str = "localhost:9092"
DEFAULT_TOPIC_ORDERS: str = "sales.orders.events"
DEFAULT_TOPIC_CLICKSTREAM: str = "marketing.clickstream.raw"
DEFAULT_RATE: int = 10
MAX_RATE: int = 100
DEFAULT_DURATION: int = 3600

FUNNEL_STAGES: List[str] = ["view", "click", "add_to_cart", "checkout", "purchase"]
PAGES: Dict[str, List[str]] = {
    "view": ["/", "/catalog", "/product/{id}", "/category/{cat}", "/deals"],
    "click": ["/product/{id}", "/search?q={q}", "/brand/{b}"],
    "add_to_cart": ["/cart", "/product/{id}#add"],
    "checkout": ["/checkout/shipping", "/checkout/payment"],
    "purchase": ["/checkout/confirmation", "/order/{id}/thankyou"],
}

REFERRERS: List[str] = [
    "https://www.google.com",
    "https://www.bing.com",
    "https://www.facebook.com",
    "https://www.instagram.com",
    "direct",
    "https://newsletter.company.com",
]

ORDER_EVENT_TYPES: List[str] = ["created", "updated", "cancelled", "shipped"]
ORDER_STATUSES: List[str] = ["confirmed", "pending", "cancelled", "shipped"]
SOURCES: List[str] = ["web", "mobile", "api", "backoffice"]


@dataclass
class OrderEvent:
    event_id: str
    event_type: str
    order_id: int
    customer_id: int
    total_amount: float
    status: str
    order_date: str
    timestamp: str
    source: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ClickstreamEvent:
    event_id: str
    session_id: str
    customer_id: Optional[int]
    page_url: str
    event_type: str
    referrer: str
    timestamp: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _get_faker():
    """Lazily loads Faker so importing this module never requires faker.

    Returns a Faker instance when the package is installed, else None so
    callers keep running in --simulate mode on stdlib random/uuid.
    """
    try:
        from faker import Faker
    except ImportError:
        return None
    return Faker()


def generate_order_event(order_id: int, customer_id: int) -> OrderEvent:
    now = datetime.now(timezone.utc)
    amount = round(random.uniform(10.0, 500.0), 2)
    status = random.choice(ORDER_STATUSES)
    return OrderEvent(
        event_id=str(uuid.uuid4()),
        event_type=random.choice(ORDER_EVENT_TYPES),
        order_id=order_id,
        customer_id=customer_id,
        total_amount=amount,
        status=status,
        order_date=now.strftime("%Y-%m-%d"),
        timestamp=now.isoformat(),
        source=random.choice(SOURCES),
    )


def generate_clickstream_event(session_id: str, customer_id: Optional[int]) -> ClickstreamEvent:
    now = datetime.now(timezone.utc)
    stage = random.choice(FUNNEL_STAGES)
    page_template = random.choice(PAGES[stage])
    page = page_template.replace("{id}", str(random.randint(1, 500))).replace("{cat}", "electronics")
    return ClickstreamEvent(
        event_id=str(uuid.uuid4()),
        session_id=session_id,
        customer_id=customer_id,
        page_url=page,
        event_type=stage,
        referrer=random.choice(REFERRERS),
        timestamp=now.isoformat(),
    )


@click.command()
@click.option("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS, help="Kafka bootstrap brokers")
@click.option("--topic-orders", default=DEFAULT_TOPIC_ORDERS, help="Kafka topic for orders")
@click.option("--topic-clickstream", default=DEFAULT_TOPIC_CLICKSTREAM, help="Kafka topic for clickstream")
@click.option("--rate", default=DEFAULT_RATE, type=int, help="Events per second")
@click.option("--duration", default=10, type=int, help="Execution duration in seconds")
@click.option("--simulate", is_flag=True, default=False,
              help="Print generated events to stdout (one JSON line per event) without requiring Kafka")
def main(bootstrap_servers, topic_orders, topic_clickstream, rate, duration, simulate):
    logger.info("Starting event producer to %s...", bootstrap_servers)
    logger.info("Publishing to topics: '%s' and '%s' at %d events/sec", topic_orders, topic_clickstream, rate)

    producer = None
    if simulate:
        logger.info("Running in --simulate mode: printing events to stdout, Kafka not required.")
    else:
        try:
            from confluent_kafka import Producer
            producer = Producer({"bootstrap.servers": bootstrap_servers})
        except Exception as exc:
            logger.error(
                "Kafka unavailable at '%s': %s. Start the broker, check --bootstrap-servers, "
                "or re-run with --simulate to print events to stdout without Kafka.",
                bootstrap_servers,
                exc,
            )
            sys.exit(1)

    end_time = time.time() + duration
    events_sent = 0
    sessions = [str(uuid.uuid4()) for _ in range(20)]

    while time.time() < end_time:
        order_evt = generate_order_event(random.randint(1, 100000), random.randint(1, 1000))
        click_evt = generate_clickstream_event(random.choice(sessions), random.choice([None, random.randint(1, 1000)]))

        if simulate:
            click.echo(json.dumps(order_evt.to_dict()))
            click.echo(json.dumps(click_evt.to_dict()))
        elif producer:
            try:
                producer.produce(topic_orders, key=str(order_evt.order_id), value=json.dumps(order_evt.to_dict()))
                producer.produce(topic_clickstream, key=click_evt.session_id, value=json.dumps(click_evt.to_dict()))
                producer.poll(0)
            except Exception as e:
                logger.warning("Error producing message: %s", e)
        events_sent += 2
        time.sleep(1.0 / max(1, rate))

    if producer:
        producer.flush(5)
    logger.info("Finished publishing %d events.", events_sent)


if __name__ == "__main__":
    main()
