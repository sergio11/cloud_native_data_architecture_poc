"""
Modern Data Architecture POC - Centralised logical table naming (pure domain).

Single source of truth for every logical lakehouse table name used by the
hexagonal core. Pure functions only: no I/O, no filesystem knowledge, no
adapter imports. Use cases and the serving logical-to-physical bridge derive
their table names from here instead of hardcoding dotted strings.
"""

from __future__ import annotations

from dataclasses import dataclass

#: Entity -> owning domain mapping (Data Mesh ownership).
DOMAIN_OF_ENTITY: dict[str, str] = {
    "orders": "sales",
    "customers": "sales",
    "campaigns": "marketing",
    "leads": "marketing",
}


@dataclass(frozen=True)
class GoldProduct:
    """Gold product descriptor: the single source of truth for Gold routing.

    ``name`` is the product key used by drivers (``"daily_metrics"``),
    ``contract_entity`` the entity whose contract governs its quality gate
    (contracts live under ``data-products/<entity>/``), and ``result_key``
    the row-count key reported in the Gold result contract.
    """

    name: str
    contract_entity: str
    result_key: str


#: Gold products modelled by the Gold tier (open for extension: adding a
#: product means appending a descriptor here plus its publisher, without
#: touching the aggregation control flow).
GOLD_PRODUCTS: tuple[GoldProduct, ...] = (
    GoldProduct(name="daily_metrics", contract_entity="orders", result_key="daily_metrics_rows"),
    GoldProduct(name="customers", contract_entity="customers", result_key="customers_masked_rows"),
)


def bronze_table(entity: str) -> str:
    """Returns the Bronze logical table name for an entity (e.g. ``bronze.sales.orders``)."""
    return f"bronze.{DOMAIN_OF_ENTITY[entity]}.{entity}"


def silver_table(entity: str) -> str:
    """Returns the Silver logical table name for an entity (e.g. ``silver.sales.orders``)."""
    return f"silver.{DOMAIN_OF_ENTITY[entity]}.{entity}"


def gold_table(product: str) -> str:
    """Returns the Gold logical table name for a product (``gold.sales.daily_metrics``, ``gold.sales.customers``)."""
    return f"gold.sales.{product}"


def stream_table() -> str:
    """Returns the Speed layer logical table name (``stream.sales.orders_windowed``)."""
    return "stream.sales.orders_windowed"


def physical_sql_name(logical: str) -> str:
    """Maps a logical dotted table name to its physical SQL name (dots become underscores)."""
    return logical.replace(".", "_")
