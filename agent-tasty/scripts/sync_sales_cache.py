#!/usr/bin/env python3
"""
Sync sales data from MSSQL BI_ANALISIS_VENTAS (SAP HANA via OPENQUERY)
into the local PostgreSQL sales_cache table.

Queries HANA directly via OPENQUERY with filters pushed inside so HANA
does the filtering. Runs queries in parallel using a thread pool.

Usage:
    # Full sync — iterates all clients from BI_CLIENTES
    python scripts/sync_sales_cache.py

    # Sync specific client
    python scripts/sync_sales_cache.py --client DT01581

    # Sync only clients on a specific route
    python scripts/sync_sales_cache.py --route "Panama Centro 01"

    # Sync last N days
    python scripts/sync_sales_cache.py --days 30

    # Control parallelism (default: 10 concurrent queries)
    python scripts/sync_sales_cache.py --workers 5

    # Dry run (show what would be synced)
    python scripts/sync_sales_cache.py --dry-run
"""

import os
import sys
import time
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from datetime import date, timedelta

import pymssql
from agent_tasty.config import MSSQL_HOST, MSSQL_PORT, MSSQL_USER, MSSQL_PASSWORD, MSSQL_DATABASE
from agent_tasty.db import init_db, engine, SalesCacheRow, ProductPriceCache, ClientMermaCache
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import sessionmaker

SessionLocal = sessionmaker(bind=engine)


def get_sync_connection():
    """MSSQL connection with longer timeouts for slow HANA OPENQUERY views."""
    return pymssql.connect(
        server=MSSQL_HOST,
        port=MSSQL_PORT,
        user=MSSQL_USER,
        password=MSSQL_PASSWORD,
        database=MSSQL_DATABASE,
        login_timeout=30,
        timeout=120,
    )


def get_client_codes(conn, route: str | None = None) -> list[str]:
    """Fetch all client codes from BI_CLIENTES, optionally filtered by route."""
    cursor = conn.cursor()
    if route:
        cursor.execute(
            "SELECT DISTINCT Cod_Cliente FROM BI_CLIENTES WHERE Vendedor = %s ORDER BY Cod_Cliente",
            (route,),
        )
    else:
        cursor.execute("SELECT DISTINCT Cod_Cliente FROM BI_CLIENTES ORDER BY Cod_Cliente")
    return [row[0] for row in cursor.fetchall()]


def fetch_client_sales(client_code: str, days_back: int) -> list[dict]:
    """Fetch sales for one client by querying HANA directly via OPENQUERY.

    Each call opens its own MSSQL connection (thread-safe).
    Returns both positive (sales) and negative (returns/merma) rows.
    """
    safe_code = client_code.replace("'", "''")
    query = (
        "SELECT * FROM OPENQUERY([HANA-005], "
        "'SELECT \"Codigo_Producto\", \"Fecha_Documento\", \"Cantidad_FAC\", \"Cantidad_NDC\", \"Cantidad_NETA\" "
        f"FROM \"20024_JUMBO_C\".\"BI_ANALISIS_VENTAS\" "
        f"WHERE \"Codigo_Cliente\" = ''{safe_code}'' "
        f"AND \"Fecha_Documento\" >= ADD_DAYS(CURRENT_DATE, -{days_back})"
        "')"
    )
    try:
        conn = get_sync_connection()
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        conn.close()
    except Exception as e:
        return [{"error": str(e)}]

    # Aggregate in Python by (product_code, date)
    # agg value: (qty_fac, qty_returned, quantity_neta)
    agg: dict[tuple[str, object], tuple[float, float, float]] = {}
    for row in rows:
        product_code = row[0]
        sale_date = row[1].date() if hasattr(row[1], 'date') else row[1]
        fac  = float(row[2]) if row[2] else 0.0
        ndc  = float(row[3]) if row[3] else 0.0
        neta = float(row[4]) if row[4] else 0.0
        key = (product_code, sale_date)
        prev = agg.get(key, (0.0, 0.0, 0.0))
        agg[key] = (prev[0] + fac, prev[1] + ndc, prev[2] + neta)

    return [
        {
            "client_code": client_code,
            "product_code": k[0],
            "sale_date": k[1],
            "quantity": v[2],       # Cantidad_NETA
            "qty_fac": v[0],        # Cantidad_FAC (gross delivered)
            "qty_returned": v[1],   # Cantidad_NDC (returns/credits)
        }
        for k, v in agg.items()
    ]


def save_client_cache(records: list[dict], client_code: str, days_back: int) -> int:
    """Replace cached records for one client in PostgreSQL.

    Also computes and caches merma (spoilage) rates per product.
    Merma = abs(sum of negative qty) / sum of positive qty.
    """
    with SessionLocal() as session:
        cutoff = date.today() - timedelta(days=days_back)
        deleted = (
            session.query(SalesCacheRow)
            .filter(
                SalesCacheRow.client_code == client_code,
                SalesCacheRow.sale_date >= cutoff,
            )
            .delete()
        )

        for r in records:
            session.add(SalesCacheRow(
                client_code=r["client_code"],
                product_code=r["product_code"],
                sale_date=r["sale_date"],
                quantity=r["quantity"],
                qty_fac=r.get("qty_fac", 0),
                qty_returned=r.get("qty_returned", 0),
            ))

        # Flush so the new rows are visible in the query below
        session.flush()

        # Compute merma from last 21 days (fixed window for suggested order calculation)
        merma_cutoff = date.today() - timedelta(days=21)
        all_rows = (
            session.query(SalesCacheRow)
            .filter(
                SalesCacheRow.client_code == client_code,
                SalesCacheRow.sale_date >= merma_cutoff,
            )
            .all()
        )
        sold: dict[str, float] = {}
        returned: dict[str, float] = {}
        for row in all_rows:
            pcode = row.product_code
            sold[pcode] = sold.get(pcode, 0) + (row.qty_fac or 0)
            returned[pcode] = returned.get(pcode, 0) + (row.qty_returned or 0)

        # Upsert merma cache for this client (idempotent under concurrent workers)
        current_pcodes = set(sold.keys()) | set(returned.keys())
        if current_pcodes:
            rows = []
            for pcode in current_pcodes:
                s = sold.get(pcode, 0)
                r = returned.get(pcode, 0)
                rows.append({
                    "client_code": client_code,
                    "product_code": pcode,
                    "total_sold": s,
                    "total_returned": r,
                    "merma_rate": r / s if s > 0 else 0.0,
                })
            stmt = pg_insert(ClientMermaCache).values(rows)
            stmt = stmt.on_conflict_do_update(
                index_elements=["client_code", "product_code"],
                set_={
                    "total_sold": stmt.excluded.total_sold,
                    "total_returned": stmt.excluded.total_returned,
                    "merma_rate": stmt.excluded.merma_rate,
                },
            )
            session.execute(stmt)

        # Drop stale rows for products no longer in the 21-day window
        if current_pcodes:
            session.query(ClientMermaCache).filter(
                ClientMermaCache.client_code == client_code,
                ~ClientMermaCache.product_code.in_(current_pcodes),
            ).delete(synchronize_session=False)
        else:
            session.query(ClientMermaCache).filter(
                ClientMermaCache.client_code == client_code,
            ).delete(synchronize_session=False)

        session.commit()
        return deleted


def sync_one_client(client_code: str, days_back: int, dry_run: bool) -> tuple[str, int, float, str]:
    """Fetch + save for one client. Returns (code, num_records, elapsed, status)."""
    t = time.time()
    records = fetch_client_sales(client_code, days_back)
    elapsed = time.time() - t

    # Check for error sentinel
    if records and "error" in records[0]:
        return (client_code, 0, elapsed, f"ERROR: {records[0]['error']}")

    if not records:
        return (client_code, 0, elapsed, "ok")

    if not dry_run:
        save_client_cache(records, client_code, days_back)

    return (client_code, len(records), elapsed, "dry-run" if dry_run else "ok")


def sync_product_prices():
    """Sync product prices from SAP HANA BI_RDR1 into ProductPriceCache.

    Uses the most recent order line price per product from BI_RDR1.
    Falls back to FALLBACK_PRICES from config if HANA query fails.
    """
    from agent_tasty.config import FALLBACK_PRICES, SKU_CATALOG

    sku_codes = {s["code"] for s in SKU_CATALOG}
    prices: dict[str, float] = {}

    # Fetch latest prices from BI_RDR1 (order line items)
    try:
        conn = get_sync_connection()
        cursor = conn.cursor()
        query = (
            "SELECT * FROM OPENQUERY([HANA-005], "
            "'SELECT \"ItemCode\", \"Price\" "
            "FROM \"20024_JUMBO_C\".\"BI_RDR1\" "
            "WHERE \"Price\" > 0 "
            "ORDER BY \"ShipDate\" DESC')"
        )
        cursor.execute(query)
        for row in cursor.fetchall():
            code = row[0]
            price = float(row[1]) if row[1] else 0.0
            # Keep first (most recent) price per product
            if code in sku_codes and code not in prices and price > 0:
                prices[code] = price
        conn.close()
        print(f"[prices] Fetched {len(prices)} prices from HANA BI_RDR1")
    except Exception as e:
        print(f"[prices] HANA query failed ({e}), using fallback prices")

    # Fill missing with fallback
    for code, fallback_price in FALLBACK_PRICES.items():
        if code not in prices:
            prices[code] = fallback_price

    # Save to PG
    with SessionLocal() as session:
        session.query(ProductPriceCache).delete()
        for code, price in prices.items():
            session.add(ProductPriceCache(
                product_code=code,
                unit_price_usd=price,
            ))
        session.commit()

    print(f"[prices] Saved {len(prices)} product prices to cache")


def main():
    parser = argparse.ArgumentParser(description="Sync MSSQL sales data to PostgreSQL cache")
    parser.add_argument("--client", type=str, help="Sync only this client code (e.g. DT01581)")
    parser.add_argument("--route", type=str, help="Sync only clients on this route (Vendedor)")
    parser.add_argument("--days", type=int, default=90, help="Days of history to sync (default: 90)")
    parser.add_argument("--workers", type=int, default=50, help="Parallel workers (default: 50)")
    parser.add_argument("--dry-run", action="store_true", help="Fetch from MSSQL but don't write to PG")
    args = parser.parse_args()

    init_db()

    # Sync product prices first (fast, 11 rows)
    print("Syncing product prices...")
    sync_product_prices()
    print()

    # Build client list
    if args.client:
        client_codes = [args.client]
    else:
        print("Connecting to MSSQL BI_JUMBO...")
        conn = get_sync_connection()
        print("Fetching client list from BI_CLIENTES...")
        client_codes = get_client_codes(conn, route=args.route)
        conn.close()
        print(f"Found {len(client_codes)} clients")

    total_records = 0
    synced_clients = 0
    failed_clients = 0
    done_count = 0
    start = time.time()

    print(f"Syncing {len(client_codes) if not args.client else 1} clients with {args.workers} parallel workers...\n")

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(sync_one_client, code, args.days, args.dry_run): code
            for code in client_codes
        }

        for future in as_completed(futures):
            code, num_records, elapsed, status = future.result()
            done_count += 1

            if status.startswith("ERROR"):
                failed_clients += 1
                print(f"[{done_count}/{len(client_codes)}] {code}... {status} ({elapsed:.1f}s)")
            elif num_records > 0:
                synced_clients += 1
                total_records += num_records
                tag = " [dry-run]" if args.dry_run else ""
                print(f"[{done_count}/{len(client_codes)}] {code}... {num_records} rows ({elapsed:.1f}s){tag}")
            # Skip printing clients with 0 rows to reduce noise

    total_elapsed = time.time() - start
    print(f"\nDone in {total_elapsed:.0f}s — {synced_clients} clients with data, "
          f"{total_records} records, {failed_clients} errors")


if __name__ == "__main__":
    main()
