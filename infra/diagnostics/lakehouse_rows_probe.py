"""
Counts the rows the medallion really left in the Iceberg lakehouse.

Run it with `rake lakehouse:rows`. It is the answer to "did the batch tier
actually write to the lakehouse, or did it only look like it did?", and it
answers it by reading the catalogue and the tables, not the configuration.

Two independent readings, on purpose:

* the CATALOGUE is asked what tables exist (PyIceberg -> Polaris);
* the ADAPTER is asked to read them back, which is the exact path the
  medallion drivers use (LAKEHOUSE_BACKEND=iceberg in the composition root).

If the two disagree, the medallion is writing somewhere the catalog does not
know about, and that is exactly the failure this probe exists to catch.

The tables it looks for are the physical names the medallion writes to. The
mapping from the logical name (bronze.sales.orders) to the Iceberg one
(sales.bronze_orders) is src/domain/naming.py:iceberg_identifier: published
tiers drop the tier, the internal ones keep it.
"""

from __future__ import annotations

import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("lakehouse-rows-probe")

REPO_ROOT = os.environ.get("POC_REPO_ROOT", "/app")
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

#: Logical table name -> the tiers that must have rows for the probe to pass.
#: The stream tier is optional on purpose: the speed layer is a different
#: phase (Flink) and the batch phase must be verifiable without it.
TIER_TABLES: dict[str, list[str]] = {
    "bronze": ["bronze.sales.orders", "bronze.sales.customers"],
    "silver": ["silver.sales.orders", "silver.sales.customers"],
    "gold": ["gold.sales.daily_metrics", "gold.sales.customers"],
    "stream": ["stream.sales.orders_windowed"],
}


def expected_tiers() -> list[str]:
    raw = os.environ.get("EXPECTED_TIERS", "bronze,silver,gold")
    return [tier.strip() for tier in raw.split(",") if tier.strip()]


def main() -> int:
    from src.adapters.storage.iceberg_catalog import _iceberg_catalog
    from src.container import container
    from src.domain.naming import iceberg_identifier

    print("=== PROBE: filas reales en el lakehouse Iceberg ===")
    storage = container.storage
    print(f"implementacion del puerto lakehouse: {type(storage).__name__}")
    if type(storage).__name__ != "IcebergLakehouseStorageAdapter":
        print("FALLO: el container no resolvió el adaptador Iceberg (LAKEHOUSE_BACKEND).")
        return 1

    catalog = _iceberg_catalog()
    print("\n-- tablas del catalogo --")
    catalog_tables: set[str] = set()
    for namespace in catalog.list_namespaces():
        for identifier in catalog.list_tables(namespace):
            # PyIceberg returns either a (namespace, table) tuple or a
            # TableIdentifier; both are "namespace.table" as text.
            if isinstance(identifier, (tuple, list)):
                full = ".".join(str(part) for part in identifier)
            else:
                full = str(identifier)
            catalog_tables.add(full)
            print(f"   {full}")
    if not catalog_tables:
        print("   (el catalogo no tiene ninguna tabla)")

    problems: list[str] = []
    print("\n-- filas por tabla de la medallion --")
    for tier, tables in TIER_TABLES.items():
        for logical in tables:
            namespace, table_id = iceberg_identifier(logical)
            physical = f"{namespace}.{table_id}"
            in_catalog = physical in catalog_tables
            exists = storage.table_exists(logical)
            rows: list[dict] = []
            if exists:
                try:
                    rows = storage.read_table(logical)
                except Exception as exc:  # noqa: BLE001 - the probe reports, it does not raise
                    print(f"   {physical}: ERROR al leer: {exc}")
                    problems.append(f"{physical}: error al leer ({exc})")
                    continue
            print(f"   {physical}: en_catalogo={in_catalog} existe={exists} filas={len(rows)}")
            if in_catalog != exists:
                problems.append(
                    f"{physical}: el catalogo dice {in_catalog} y el adaptador dice {exists}"
                )

    print("\n-- veredicto --")
    for tier in expected_tiers():
        for logical in TIER_TABLES.get(tier, []):
            namespace, table_id = iceberg_identifier(logical)
            physical = f"{namespace}.{table_id}"
            if not storage.table_exists(logical):
                problems.append(f"{physical}: no existe (tier {tier})")
                continue
            count = len(storage.read_table(logical))
            if count == 0:
                problems.append(f"{physical}: existe pero tiene 0 filas (tier {tier})")

    if problems:
        for problem in problems:
            print(f"FALLO: {problem}")
        return 1
    print(f"RESULTADO: OK (tiers comprobados: {', '.join(expected_tiers())})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
