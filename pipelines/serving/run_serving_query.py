"""
run_serving_query.py - Interactive Serving Layer Unified Query Execution (RF-11).
Driving Adapter invoking QueryServingUseCase (via container.query_serving only)
to run the single no-double-count query over merged Batch + Stream tiers.
Backend visibility (Trino vs embedded SQLite) is reported by the serving
adapter log line "Serving query executed by backend=...".
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
    args = parser.parse_args()

    results = container.query_serving.execute_unified_revenue_query(
        order_date_gte=args.date_gte,
        limit=args.limit,
    )

    if not results:
        print("No records found in serving layer.")
        return

    if has_rich:
        console = Console()
        table = Table(title="Unified Serving Layer: serving.sales.unified_revenue (Batch + Stream)")
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
            print(
                f"{r.get('order_date'):<12} | {r.get('status'):<10} | {r.get('total_orders'):<8} | "
                f"{r.get('total_revenue'):<12.2f} | {r.get('source_tier'):<16}"
            )
        print("-" * 80)


if __name__ == "__main__":
    main()
