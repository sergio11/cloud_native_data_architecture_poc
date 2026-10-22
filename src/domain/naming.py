"""Modern Data Architecture POC - Centralised logical table naming.

Single source of truth for lakehouse table names used by the core.
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
    """Gold product descriptor: single source of truth for Gold routing.

    ``name`` is the driver key, ``contract_entity`` the governing contract
    entity and ``result_key`` the row-count key in the Gold result.
    """

    name: str
    contract_entity: str
    result_key: str


#: Gold products modelled by the Gold tier.
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


#: Tiers whose tables are PUBLISHED products: the serving layer and the product
#: manifests address them by their bare name, so their Iceberg identifier must
#: not carry the tier.
PUBLISHED_TIERS: frozenset[str] = frozenset({"gold", "stream"})


def iceberg_identifier(logical: str) -> tuple[str, str]:
    """Maps a logical table name to its Iceberg (namespace, table) identifier.

    Published tiers drop the tier; internal tiers keep it as ``<tier>_<name>``
    to avoid collisions. The physical path follows the same rule.
    """
    parts = logical.split(".")
    if len(parts) != 3:
        raise ValueError(f"logical table id must be '<tier>.<domain>.<table>': {logical!r}")
    tier, domain, name = parts
    if tier in PUBLISHED_TIERS:
        return domain, name
    return domain, f"{tier}_{name}"


def contract_entity_for_table(table_name: str) -> str:
    """Returns the data-product entity whose contract governs ``table_name``.

    Gold products map through :data:`GOLD_PRODUCTS`; other tables fall back to
    their last dotted segment.
    """
    for product in GOLD_PRODUCTS:
        if table_name == gold_table(product.name):
            return product.contract_entity
    return table_name.rsplit(".", 1)[-1]


def physical_sql_name(logical: str) -> str:
    """Maps a logical dotted table name to its physical SQL name (dots become underscores)."""
    return logical.replace(".", "_")


def serving_view_name(logical: str) -> str:
    """Maps a logical table name to its serving live-view name.

    Returns ``<physical>_live``; serving SQL references these names so it runs
    verbatim on every backend.
    """
    return f"{physical_sql_name(logical)}_live"
