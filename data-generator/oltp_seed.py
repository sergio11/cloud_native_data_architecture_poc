"""
oltp_seed.py - Synthetic OLTP relational data generator for PostgreSQL.
Generates realistic e-commerce operational datasets:
  - customers:   1,000 records  (PII: name, email)
  - orders:      1,000 records by default (last 90 days, configurable via --records)
  - order_items: ~4,000 records by default (1-5 per order, scales with --records)
  - products:    500 records
  - campaigns:   100 records

Idempotent: uses INSERT ON CONFLICT DO NOTHING.
All table, column, and constraint names are in English.
"""

from __future__ import annotations

import logging
import os
import random
import sys
from datetime import date, timedelta
from typing import List, Tuple

import click

try:
    import numpy as np
except ImportError:  # graceful degradation: seeding helpers need numpy only at call time
    np = None  # type: ignore[assignment]

try:
    import psycopg2
    import psycopg2.extras
except ImportError:  # graceful degradation: DB-backed seeding needs psycopg2 only at call time
    psycopg2 = None  # type: ignore[assignment]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("oltp_seed")

BATCH_SIZE: int = 1_000

CUSTOMER_SEGMENTS: List[str] = ["premium", "standard", "basic"]
SEGMENT_WEIGHTS: List[float] = [0.20, 0.60, 0.20]

ORDER_STATUSES: List[str] = ["confirmed", "pending", "shipped", "cancelled"]
STATUS_WEIGHTS: List[float] = [0.70, 0.15, 0.10, 0.05]

PRODUCT_CATEGORIES: List[str] = [
    "Electronics", "Computers", "Smartphones", "Audio", "Home & Kitchen",
    "Sports & Outdoors", "Books & Media", "Clothing & Fashion", "Health & Beauty", "Toys & Games"
]

CAMPAIGN_CHANNELS: List[str] = ["email", "sms", "display", "social", "search"]

DDL_PRODUCTS = """
CREATE TABLE IF NOT EXISTS products (
    id         SERIAL PRIMARY KEY,
    name       TEXT   NOT NULL,
    category   TEXT   NOT NULL,
    base_price DECIMAL(10, 2) NOT NULL CHECK (base_price > 0)
);
"""

DDL_CUSTOMERS = """
CREATE TABLE IF NOT EXISTS customers (
    id                SERIAL PRIMARY KEY,
    name              TEXT   NOT NULL,
    email             TEXT   NOT NULL UNIQUE,
    segment           TEXT   NOT NULL CHECK (segment IN ('premium', 'standard', 'basic')),
    registration_date DATE   NOT NULL,
    created_at        TIMESTAMP NOT NULL DEFAULT NOW()
);
"""

DDL_ORDERS = """
CREATE TABLE IF NOT EXISTS orders (
    id           SERIAL PRIMARY KEY,
    customer_id  INT    NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
    total_amount DECIMAL(10, 2) NOT NULL CHECK (total_amount >= 0),
    status       TEXT   NOT NULL CHECK (status IN ('confirmed', 'pending', 'cancelled', 'shipped')),
    order_date   DATE   NOT NULL,
    created_at   TIMESTAMP NOT NULL DEFAULT NOW()
);
"""

DDL_ORDER_ITEMS = """
CREATE TABLE IF NOT EXISTS order_items (
    id         SERIAL PRIMARY KEY,
    order_id   INT    NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    product_id INT    NOT NULL REFERENCES products(id),
    quantity   INT    NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(10, 2) NOT NULL CHECK (unit_price > 0)
);
"""

DDL_CAMPAIGNS = """
CREATE TABLE IF NOT EXISTS campaigns (
    id         SERIAL PRIMARY KEY,
    channel    TEXT   NOT NULL CHECK (channel IN ('email', 'sms', 'display', 'social', 'search')),
    budget     DECIMAL(12, 2) NOT NULL CHECK (budget > 0),
    start_date DATE   NOT NULL,
    end_date   DATE   NOT NULL CHECK (end_date >= start_date)
);
"""

DROP_ORDER = [
    "order_items",
    "orders",
    "customers",
    "products",
    "campaigns",
]


def _connect(host: str, port: int, db: str, user: str, password: str) -> psycopg2.extensions.connection:
    if psycopg2 is None:
        logger.error("psycopg2 is not installed. Install it with: pip install psycopg2-binary")
        sys.exit(1)
    try:
        conn = psycopg2.connect(host=host, port=port, dbname=db, user=user, password=password)
        conn.autocommit = False
        return conn
    except Exception as exc:
        logger.error(
            "Failed to connect to PostgreSQL at %s:%d/%s as user '%s': %s. "
            "Verify PostgreSQL is running and reachable, and check PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD.",
            host,
            port,
            db,
            user,
            exc,
        )
        sys.exit(1)


def create_schema(conn: psycopg2.extensions.connection, recreate: bool = False) -> None:
    with conn.cursor() as cur:
        if recreate:
            logger.warning("Dropping existing tables: %s", DROP_ORDER)
            for table in DROP_ORDER:
                cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
            conn.commit()

        logger.info("Creating tables: products, customers, orders, order_items, campaigns...")
        cur.execute(DDL_PRODUCTS)
        cur.execute(DDL_CUSTOMERS)
        cur.execute(DDL_ORDERS)
        cur.execute(DDL_ORDER_ITEMS)
        cur.execute(DDL_CAMPAIGNS)
        conn.commit()
    logger.info("Schema created successfully.")


def seed_products(conn: psycopg2.extensions.connection, n: int = 500) -> List[Tuple[int, float]]:
    logger.info("Seeding %d products...", n)
    from faker import Faker

    faker = Faker()
    rows = []
    for _ in range(n):
        cat = random.choice(PRODUCT_CATEGORIES)
        name = f"{faker.word().capitalize()} {faker.word().capitalize()}"
        price = round(float(np.random.exponential(scale=45.0) + 5.0), 2)
        rows.append((name, cat, price))

    query = """
        INSERT INTO products (name, category, base_price)
        VALUES %s
        ON CONFLICT DO NOTHING
        RETURNING id, base_price;
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, query, rows, page_size=BATCH_SIZE)
        results = cur.fetchall()
        if not results:
            cur.execute("SELECT id, base_price FROM products;")
            results = cur.fetchall()
        conn.commit()
    return [(int(r[0]), float(r[1])) for r in results]


def seed_customers(conn: psycopg2.extensions.connection, n: int = 1_000) -> List[int]:
    logger.info("Seeding %d customers...", n)
    from faker import Faker

    faker = Faker()
    rows = []
    today = date.today()
    start_dt = today - timedelta(days=730)

    for _ in range(n):
        name = faker.name()
        email = f"{faker.user_name()}_{random.randint(100, 99999)}@{faker.free_email_domain()}"
        segment = np.random.choice(CUSTOMER_SEGMENTS, p=SEGMENT_WEIGHTS)
        reg_date = faker.date_between(start_date=start_dt, end_date=today)
        rows.append((name, email, segment, reg_date))

    query = """
        INSERT INTO customers (name, email, segment, registration_date)
        VALUES %s
        ON CONFLICT (email) DO NOTHING
        RETURNING id;
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, query, rows, page_size=BATCH_SIZE)
        results = cur.fetchall()
        if not results:
            cur.execute("SELECT id FROM customers;")
            results = cur.fetchall()
        conn.commit()
    return [int(r[0]) for r in results]


def seed_orders(
    conn: psycopg2.extensions.connection,
    customer_ids: List[int],
    product_pool: List[Tuple[int, float]],
    n_orders: int = 50_000,
) -> None:
    logger.info("Seeding %d orders with order items...", n_orders)
    today = date.today()
    start_dt = today - timedelta(days=90)
    days_span = (today - start_dt).days

    orders_to_insert = []
    items_to_insert = []

    with conn.cursor() as cur:
        for idx in range(1, n_orders + 1):
            cust_id = random.choice(customer_ids)
            status = np.random.choice(ORDER_STATUSES, p=STATUS_WEIGHTS)
            day_offset = random.randint(0, days_span)
            order_dt = start_dt + timedelta(days=day_offset)

            # Generate 1 to 5 order items
            num_items = random.randint(1, 5)
            items = random.sample(product_pool, min(num_items, len(product_pool)))

            total_amount = 0.0
            order_item_records = []
            for prod_id, base_price in items:
                qty = random.randint(1, 3)
                unit_price = round(base_price * random.uniform(0.95, 1.05), 2)
                total_amount += unit_price * qty
                order_item_records.append((idx, prod_id, qty, unit_price))

            total_amount = round(total_amount, 2)
            orders_to_insert.append((idx, cust_id, total_amount, status, order_dt))
            items_to_insert.extend(order_item_records)

            if len(orders_to_insert) >= BATCH_SIZE:
                cur.execute("INSERT INTO orders (id, customer_id, total_amount, status, order_date) VALUES " +
                            ",".join(["(%s,%s,%s,%s,%s)"] * len(orders_to_insert)) + " ON CONFLICT (id) DO NOTHING;",
                            [v for row in orders_to_insert for v in row])
                cur.execute("INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES " +
                            ",".join(["(%s,%s,%s,%s)"] * len(items_to_insert)) + " ON CONFLICT DO NOTHING;",
                            [v for row in items_to_insert for v in row])
                conn.commit()
                orders_to_insert = []
                items_to_insert = []

        if orders_to_insert:
            cur.execute("INSERT INTO orders (id, customer_id, total_amount, status, order_date) VALUES " +
                        ",".join(["(%s,%s,%s,%s,%s)"] * len(orders_to_insert)) + " ON CONFLICT (id) DO NOTHING;",
                        [v for row in orders_to_insert for v in row])
            cur.execute("INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES " +
                        ",".join(["(%s,%s,%s,%s)"] * len(items_to_insert)) + " ON CONFLICT DO NOTHING;",
                        [v for row in items_to_insert for v in row])
            conn.commit()


def seed_campaigns(conn: psycopg2.extensions.connection, n: int = 100) -> None:
    logger.info("Seeding %d marketing campaigns...", n)
    today = date.today()
    rows = []
    for _ in range(n):
        channel = random.choice(CAMPAIGN_CHANNELS)
        budget = round(float(random.uniform(500, 50000)), 2)
        start = today - timedelta(days=random.randint(10, 180))
        end = start + timedelta(days=random.randint(14, 90))
        rows.append((channel, budget, start, end))

    query = """
        INSERT INTO campaigns (channel, budget, start_date, end_date)
        VALUES %s
        ON CONFLICT DO NOTHING;
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, query, rows, page_size=BATCH_SIZE)
        conn.commit()


@click.command()
@click.option("--host", default=lambda: os.getenv("PGHOST", "localhost"))
@click.option("--port", default=lambda: int(os.getenv("PGPORT", "5432")), type=int)
@click.option("--db", default=lambda: os.getenv("PGDATABASE", "ecommerce"))
@click.option("--user", default=lambda: os.getenv("PGUSER", "postgres"))
@click.option("--password", default=lambda: os.getenv("PGPASSWORD"),
              help="PostgreSQL password (or set PGPASSWORD env var). No default: must be provided.")
@click.option("--records", default=1000, type=int, help="Target order record count")
@click.option("--recreate", is_flag=True, default=False)
def main(host, port, db, user, password, records, recreate):
    if psycopg2 is None:
        logger.error("psycopg2 is not installed. Install it with: pip install psycopg2-binary")
        sys.exit(1)
    if not password:
        logger.error("PostgreSQL password is required: set PGPASSWORD env var or pass --password explicitly.")
        sys.exit(2)
    logger.info("Connecting to PostgreSQL at %s:%d/%s...", host, port, db)
    try:
        conn = _connect(host, port, db, user, password)
        create_schema(conn, recreate=recreate)
        products = seed_products(conn, n=500)
        customers = seed_customers(conn, n=1000)
        seed_orders(conn, customers, products, n_orders=records)
        seed_campaigns(conn, n=100)
        conn.close()
        logger.info("OLTP database seeding finished successfully.")
    except SystemExit:
        raise
    except Exception as e:
        logger.error("PostgreSQL seeding failed: %s. Verify PostgreSQL is running and reachable.", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
