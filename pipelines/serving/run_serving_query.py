"""
run_serving_query.py - Interactive serving-layer unified query execution.

Driving adapter invoking QueryServingUseCase to run the single no-double-count
query over the merged Batch + Stream tiers. The backend used (Trino or embedded
SQLite) is reported by the adapter as "Serving query executed by backend=...".
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

# Ensure repository root is on sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.container import container

try:
    from rich.console import Console
    from rich.table import Table

    has_rich = True
except ImportError:
    has_rich = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("serving_query")


def main() -> None:
    parser = argparse.ArgumentParser(description="Unified Serving Layer Query Runner (RF-11)")
    parser.add_argument("--date-gte", default=None, help="Filter order_date greater than or equal to YYYY-MM-DD")
    parser.add_argument("--limit", default=10, type=int, help="Maximum rows to display")
    parser.add_argument(
        "--allow-empty",
        action="store_true",
        help="Exit 0 when the serving query returns no rows (default: exit 1, fail-closed)",
    )
    args = parser.parse_args()

    results = container.query_serving.execute(
        order_date_gte=args.date_gte,
        limit=args.limit,
    )

    if not results:
        print("No records found in serving layer.", file=sys.stderr)
        if not args.allow_empty:
            sys.exit(1)
        return

    if has_rich:
        console = Console()
        table = Table(title="Unified Serving Layer: iceberg.serving.sales_unified_revenue (Batch + Stream)")
        table.add_column("Order Date", style="cyan", justify="center")
        table.add_column("Status", style="magenta")
        table.add_column("Total Orders", style="green", justify="right")
        table.add_column("Total Revenue ($)", style="yellow", justify="right")
        table.add_column("Avg Order Value ($)", style="white", justify="right")
        table.add_column("Source Tier", style="bold blue", justify="center")

        for r in results:
            tot_orders = int(r.get("total_orders") or 0)
            tot_revenue = float(r.get("total_revenue") or 0.0)
            aov = float(r.get("avg_order_value") or 0.0)
            table.add_row(
                str(r.get("order_date")),
                str(r.get("status")),
                f"{tot_orders:,}",
                f"{tot_revenue:,.2f}",
                f"{aov:,.2f}",
                str(r.get("source_tier")),
            )
        console.print(table)
    else:
        print("-" * 80)
        print(f"{'Order Date':<12} | {'Status':<10} | {'Orders':<8} | {'Revenue':<12} | {'Source Tier':<16}")
        print("-" * 80)
        for r in results:
            tot_orders = int(r.get("total_orders") or 0)
            tot_revenue = float(r.get("total_revenue") or 0.0)
            print(
                f"{str(r.get('order_date')):<12} | {str(r.get('status')):<10} | {tot_orders:<8} | "
                f"{tot_revenue:<12.2f} | {str(r.get('source_tier')):<16}"
            )
        print("-" * 80)


if __name__ == "__main__":
    main()
