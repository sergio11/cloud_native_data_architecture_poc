"""
Proves that the Iceberg lakehouse adapter reaches the real lakehouse.

Run with `rake lakehouse:probe`; it writes through the composition root
(LAKEHOUSE_BACKEND=iceberg) and leaves `sales.probe_metrics` behind as evidence.
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
log = logging.getLogger("iceberg-adapter-probe")

REPO_ROOT = os.environ.get("POC_REPO_ROOT", "/app")
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

#: Table the probe writes; the `sales` namespace is granted by the Polaris bootstrap.
PROBE_TABLE = "gold.sales.probe_metrics"

PROBE_ROWS = [
    {
        "order_date": "2026-10-18",
        "status": "confirmed",
        "gross_revenue": 120.5,
        "total_orders": 3,
        "average_order_value": 40.16,
    },
    {
        "order_date": "2026-10-18",
        "status": "shipped",
        "gross_revenue": 80.25,
        "total_orders": 2,
        "average_order_value": 40.12,
    },
]


def main() -> int:
    from src.adapters.storage.iceberg_catalog import iceberg_catalog_properties
    from src.container import container
    from src.domain.naming import iceberg_identifier

    print("=== PROBE: adaptador Iceberg del lakehouse ===")
    properties = iceberg_catalog_properties()
    print("propiedades del catalogo:")
    for key in sorted(properties):
        value = properties[key]
        if "secret" in key.lower() or "credential" in key.lower():
            value = "***"
        print(f"   {key} = {value}")

    namespace, table_id = iceberg_identifier(PROBE_TABLE)
    print(f"identificador iceberg de {PROBE_TABLE}: {namespace}.{table_id}")

    storage = container.storage
    print(f"implementacion del puerto lakehouse: {type(storage).__name__}")
    if type(storage).__name__ != "IcebergLakehouseStorageAdapter":
        print("FALLO: el container no resolvió el adaptador Iceberg (LAKEHOUSE_BACKEND).")
        return 1

    # The probe table persists between runs, so a re-run appends only missing rows.
    existed = storage.table_exists(PROBE_TABLE)
    print(f"la tabla ya existia: {existed}")

    written = storage.write_table(PROBE_TABLE, PROBE_ROWS)
    print(f"filas escritas: {written}")

    again = storage.write_table(PROBE_TABLE, PROBE_ROWS)
    print(f"re-escritura identica (debe ser 0): {again}")

    rows = storage.read_table(PROBE_TABLE)
    print(f"filas leidas: {len(rows)}")
    for row in rows:
        print("   ", row)

    versions = storage.list_versions(PROBE_TABLE)
    print(f"snapshots: {versions}")
    print("esquema:", storage.get_table_schema(PROBE_TABLE))
    print("existe:", storage.table_exists(PROBE_TABLE))

    problems: list[str] = []
    # A fresh table takes the whole batch; an existing one takes none.
    expected_first = 0 if existed else len(PROBE_ROWS)
    if written != expected_first:
        problems.append(
            f"la primera escritura devolvio {written} y se esperaban {expected_first} "
            f"(la tabla {'ya existia' if existed else 'no existia'})"
        )
    if again != 0:
        problems.append(f"la re-escritura identica escribio {again} filas y debia escribir 0")
    if len(rows) != len(PROBE_ROWS):
        problems.append(f"se esperaban {len(PROBE_ROWS)} filas leidas y se leyeron {len(rows)}")
    if not storage.table_exists(PROBE_TABLE):
        problems.append("la tabla no existe en el catalogo")
    if not versions:
        problems.append("no hay snapshots: la escritura no se confirmo")

    if problems:
        for problem in problems:
            print(f"FALLO: {problem}")
        return 1
    print("RESULTADO: OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
