#!/usr/bin/env python3
"""
Test the per-SKU suggested order calculation with real data sources.

Usage:
    python scripts/test_suggested_order.py DT01581
    python scripts/test_suggested_order.py DT01581 --stock PT00005=4 PT00013=6 PT00009=2
    python scripts/test_suggested_order.py DT01581 --stock Familiar=4 Mantequilla=6 Integral=2
"""

import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from agent_tasty.config import SKU_CATALOG
from agent_tasty.mssql import get_avg_daily_sales, calculate_suggested_order
from agent_tasty.handy import get_recent_orders, get_visit_history, compute_visit_interval

_CODE_TO_NAME = {s["code"]: s["short_name"] for s in SKU_CATALOG}
_NAME_TO_CODE = {s["short_name"].lower(): s["code"] for s in SKU_CATALOG}


def parse_stock_args(args: list[str]) -> dict[str, int]:
    """Parse stock args like 'PT00005=4' or 'Familiar=4' into {code: qty}."""
    stock = {}
    for arg in args:
        if "=" not in arg:
            continue
        key, val = arg.split("=", 1)
        code = _NAME_TO_CODE.get(key.lower(), key)  # try name first, fallback to raw
        stock[code] = int(val)
    return stock


def main():
    if len(sys.argv) < 2:
        print("Usage: python test_suggested_order.py <client_code> [--stock KEY=VAL ...]")
        sys.exit(1)

    client_code = sys.argv[1]
    stock_by_sku = {}

    if "--stock" in sys.argv:
        idx = sys.argv.index("--stock")
        stock_by_sku = parse_stock_args(sys.argv[idx + 1:])
    else:
        # Default: all 11 SKUs at 0 stock
        stock_by_sku = {s["code"]: 0 for s in SKU_CATALOG}

    print(f"Client: {client_code}")
    print(f"Stock: {len(stock_by_sku)} SKUs")
    for code, qty in stock_by_sku.items():
        print(f"  {_CODE_TO_NAME.get(code, code)}: {qty}")
    print()

    # --- MSSQL (disabled — BI_ANALISIS_VENTAS too slow without indexes) ---
    print("--- MSSQL: skipped (no indexes, queries timeout >60s) ---")
    if "--mssql" in sys.argv:
        t = time.time()
        for code in list(stock_by_sku.keys())[:3]:
            avg = get_avg_daily_sales(client_code, code)
            name = _CODE_TO_NAME.get(code, code)
            print(f"  {name} avg_daily_sales: {avg}")
        print(f"  ({time.time()-t:.1f}s for 3 queries)")
    print()

    # --- HANDY ---
    print("--- HANDY ---")
    t = time.time()
    orders = get_recent_orders(client_code)
    elapsed_orders = time.time() - t
    print(f"  recent_orders: {len(orders)} orders ({elapsed_orders:.1f}s)")

    if orders:
        # Aggregate per-product totals
        product_totals = {}
        for o in orders:
            for pcode, qty in o.get("items", {}).items():
                product_totals[pcode] = product_totals.get(pcode, 0) + qty

        dates = [o["date"] for o in orders if o.get("date")]
        if len(dates) >= 2:
            span = (max(dates) - min(dates)).days or 1
        else:
            span = 90

        print(f"  order date range: {min(dates).date()} to {max(dates).date()} ({span}d)")
        print(f"  per-product totals (from HANDY orders):")
        for code in stock_by_sku:
            total = product_totals.get(code, 0)
            daily = total / span if total > 0 else 0
            name = _CODE_TO_NAME.get(code, code)
            print(f"    {name}: {total} units total, {daily:.2f}/day")

    t = time.time()
    visits = get_visit_history(client_code)
    elapsed_visits = time.time() - t
    interval = compute_visit_interval(visits)
    print(f"  visit_history: {len(visits)} visits ({elapsed_visits:.1f}s)")
    print(f"  visit_interval: {interval:.1f} days")
    print()

    # --- Full calculation ---
    print("--- SUGGESTED ORDER (all tiers) ---")
    t = time.time()
    suggested = calculate_suggested_order(client_code, "", stock_by_sku)
    elapsed = time.time() - t
    print(f"\n  Results ({elapsed:.1f}s):")
    for code, qty in suggested.items():
        name = _CODE_TO_NAME.get(code, code)
        stock = stock_by_sku.get(code, 0)
        print(f"    {name}: stock={stock} -> suggest={qty}")


if __name__ == "__main__":
    main()
