#!/usr/bin/env python3
"""
Explore the BI_JUMBO MSSQL database to find client/store tables
and recommend queries for client validation during salesrep check-in.

Usage:
    # Set MSSQL env vars in agent-tasty/.env first:
    #   MSSQL_HOST=...
    #   MSSQL_USER=...
    #   MSSQL_PASSWORD=...
    #   MSSQL_DATABASE=BI_JUMBO
    #   MSSQL_PORT=1433

    python agent-tasty/scripts/explore_mssql.py
"""

import sys
sys.path.insert(0, "agent-tasty/src")

from agent_tasty.mssql import get_mssql_connection

SEPARATOR = "=" * 70


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
        # Print column headers
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

    # 1. List all tables
    run_query(cursor,
        "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE "
        "FROM INFORMATION_SCHEMA.TABLES "
        "ORDER BY TABLE_SCHEMA, TABLE_NAME",
        "Step 1: All tables in BI_JUMBO",
        max_rows=100)

    # 2. Find tables with 'client' or 'cliente' in the name
    run_query(cursor,
        "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
        "WHERE TABLE_NAME LIKE '%client%' OR TABLE_NAME LIKE '%cliente%' "
        "OR TABLE_NAME LIKE '%customer%' OR TABLE_NAME LIKE '%cust%' "
        "ORDER BY TABLE_NAME",
        "Step 2: Tables matching 'client/cliente/customer'")

    # 3. Find tables with 'ruta' or 'route' in the name
    run_query(cursor,
        "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
        "WHERE TABLE_NAME LIKE '%ruta%' OR TABLE_NAME LIKE '%route%' "
        "OR TABLE_NAME LIKE '%vendor%' OR TABLE_NAME LIKE '%vendedor%' "
        "OR TABLE_NAME LIKE '%sales%' "
        "ORDER BY TABLE_NAME",
        "Step 3: Tables matching 'ruta/route/vendor/vendedor/sales'")

    # 4. Find tables with 'tienda' or 'store' or 'comercio'
    run_query(cursor,
        "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
        "WHERE TABLE_NAME LIKE '%tienda%' OR TABLE_NAME LIKE '%store%' "
        "OR TABLE_NAME LIKE '%comercio%' OR TABLE_NAME LIKE '%sucursal%' "
        "OR TABLE_NAME LIKE '%punto%' "
        "ORDER BY TABLE_NAME",
        "Step 4: Tables matching 'tienda/store/comercio/sucursal'")

    # 5. Find tables with 'product' or 'producto'
    run_query(cursor,
        "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
        "WHERE TABLE_NAME LIKE '%product%' OR TABLE_NAME LIKE '%producto%' "
        "OR TABLE_NAME LIKE '%item%' OR TABLE_NAME LIKE '%articulo%' "
        "ORDER BY TABLE_NAME",
        "Step 5: Tables matching 'product/producto/item'")

    # 6. For each candidate table found in steps 2-4, show columns
    print(f"\n\n{'#' * 70}")
    print("  Step 6: Column details for candidate tables")
    print(f"{'#' * 70}")

    # Gather candidate table names from steps 2-4
    cursor.execute(
        "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
        "WHERE TABLE_NAME LIKE '%client%' OR TABLE_NAME LIKE '%cliente%' "
        "OR TABLE_NAME LIKE '%customer%' OR TABLE_NAME LIKE '%cust%' "
        "OR TABLE_NAME LIKE '%ruta%' OR TABLE_NAME LIKE '%route%' "
        "OR TABLE_NAME LIKE '%tienda%' OR TABLE_NAME LIKE '%store%' "
        "OR TABLE_NAME LIKE '%comercio%' OR TABLE_NAME LIKE '%sucursal%' "
        "OR TABLE_NAME LIKE '%vendor%' OR TABLE_NAME LIKE '%vendedor%' "
        "OR TABLE_NAME LIKE '%sales%' "
        "ORDER BY TABLE_NAME"
    )
    candidate_tables = cursor.fetchall()

    for schema, table in candidate_tables:
        run_query(cursor,
            f"SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE "
            f"FROM INFORMATION_SCHEMA.COLUMNS "
            f"WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}' "
            f"ORDER BY ORDINAL_POSITION",
            f"Columns in [{schema}].[{table}]",
            max_rows=50)

        # Sample 5 rows
        run_query(cursor,
            f"SELECT TOP 5 * FROM [{schema}].[{table}]",
            f"Sample rows from [{schema}].[{table}]",
            max_rows=5)

    # 7. If no candidate tables found, do a broader search
    if not candidate_tables:
        print("\n  No obvious client/store tables found. Doing broader column search...")
        run_query(cursor,
            "SELECT t.TABLE_NAME, c.COLUMN_NAME "
            "FROM INFORMATION_SCHEMA.COLUMNS c "
            "JOIN INFORMATION_SCHEMA.TABLES t ON c.TABLE_NAME = t.TABLE_NAME "
            "WHERE c.COLUMN_NAME LIKE '%client%' OR c.COLUMN_NAME LIKE '%cliente%' "
            "OR c.COLUMN_NAME LIKE '%store%' OR c.COLUMN_NAME LIKE '%tienda%' "
            "OR c.COLUMN_NAME LIKE '%nombre%' OR c.COLUMN_NAME LIKE '%name%' "
            "ORDER BY t.TABLE_NAME, c.COLUMN_NAME",
            "Step 7: Columns containing 'client/store/nombre/name' across all tables",
            max_rows=100)

    conn.close()

    print(f"\n\n{'#' * 70}")
    print("  DONE — Review the output above to identify:")
    print("  1. Which table holds client/store master data")
    print("  2. Column for client code and client name")
    print("  3. Whether clients are linked to routes/salesreps")
    print(f"{'#' * 70}\n")


if __name__ == "__main__":
    main()
