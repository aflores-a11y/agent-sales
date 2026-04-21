#!/usr/bin/env python3
"""
Explore MSSQL sales-related tables to determine the right query
for calculating average daily sales per client.

Usage:
    python agent-tasty/scripts/explore_sales_tables.py
"""

import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from agent_tasty.mssql import get_mssql_connection

SEPARATOR = "=" * 70

SALES_TABLES = [
    "BI_ANALISIS_VENTAS",
    "CR_VENTAS_X_ARTICULO",
    "BI_ORDR",
    "BI_RDR1",
    "BI_OITM",
    "BI_OITW",
]


def run_query(cursor, query, description, max_rows=20):
    print(f"\n{SEPARATOR}")
    print(f"  {description}")
    print(f"  SQL: {query}")
    print(SEPARATOR)
    try:
        cursor.execute(query)
        rows = cursor.fetchmany(max_rows)
        if not rows:
            print("  (no results)")
            return rows
        cols = [desc[0] for desc in cursor.description]
        header = " | ".join(f"{c:<30}" for c in cols)
        print(f"  {header}")
        print(f"  {'-' * len(header)}")
        for row in rows:
            print(f"  {' | '.join(str(v)[:30].ljust(30) for v in row)}")
        remaining = cursor.fetchall()
        total = len(rows) + len(remaining)
        if total > max_rows:
            print(f"  ... ({total} total rows, showing first {max_rows})")
        else:
            print(f"  ({total} rows)")
        return rows
    except Exception as e:
        print(f"  ERROR: {e}")
        return []


def main():
    print("Connecting to BI_JUMBO...")
    try:
        conn = get_mssql_connection()
    except Exception as e:
        print(f"Connection failed: {e}")
        print("\nMake sure MSSQL_* vars are set in agent-tasty/.env")
        sys.exit(1)

    cursor = conn.cursor()
    print("Connected!\n")

    for table in SALES_TABLES:
        # Schema
        run_query(cursor,
            f"SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE "
            f"FROM INFORMATION_SCHEMA.COLUMNS "
            f"WHERE TABLE_NAME = '{table}' "
            f"ORDER BY ORDINAL_POSITION",
            f"Schema: {table}",
            max_rows=50)

        # Sample rows
        run_query(cursor,
            f"SELECT TOP 5 * FROM [{table}]",
            f"Sample rows: {table}",
            max_rows=5)

    conn.close()

    print(f"\n\n{'#' * 70}")
    print("  DONE — Review output to identify:")
    print("  1. Which table has sales qty by client and date")
    print("  2. Column names for client code, qty, and date")
    print("  3. How to filter by date range (last 90 days)")
    print(f"{'#' * 70}\n")


if __name__ == "__main__":
    main()
