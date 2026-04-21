"""MSSQL connection to BI_JUMBO and order calculation logic."""

import math
from dataclasses import dataclass, field

import pymssql


@dataclass
class OrderResult:
    suggested_by_sku: dict[str, int] = field(default_factory=dict)
    canasta_upgrades: dict[str, dict] = field(default_factory=dict)
    # {unit_code: {"n_canastas": 1, "canasta_code": "PT00024", "short_name": "Canasta Familiar",
    #              "total_units": 14, "price_usd": 15.60}}
    total_value_usd: float = 0.0
    warnings: list[str] = field(default_factory=list)
    skipped_skus: dict[str, str] = field(default_factory=dict)  # {code: reason}
    segment: str = "Bronze"

from agent_tasty.config import MSSQL_HOST, MSSQL_PORT, MSSQL_USER, MSSQL_PASSWORD, MSSQL_DATABASE


def get_mssql_connection(timeout: int = 30):
    return pymssql.connect(
        server=MSSQL_HOST,
        port=MSSQL_PORT,
        user=MSSQL_USER,
        password=MSSQL_PASSWORD,
        database=MSSQL_DATABASE,
        login_timeout=15,
        timeout=timeout,
    )


def search_clients(salesrep_id: str, query: str) -> list[dict]:
    """Search clients in BI_CLIENTES by name (LIKE) or code (exact), filtered by route.
    TEST salesrep_id bypasses the route filter."""
    conn = get_mssql_connection()
    try:
        c = conn.cursor()
        if salesrep_id in ("TEST", "Panama Este 01", "Panama Oeste 04"):
            c.execute(
                "SELECT Cod_Cliente, Nombre_Cliente FROM BI_CLIENTES "
                "WHERE Nombre_Cliente LIKE %s OR Cod_Cliente = %s "
                "ORDER BY Nombre_Cliente",
                (f"%{query}%", query),
            )
        elif salesrep_id == "DT_UT":
            c.execute(
                "SELECT Cod_Cliente, Nombre_Cliente FROM BI_CLIENTES "
                "WHERE (Cod_Cliente LIKE 'DT%' OR Cod_Cliente LIKE 'UT%') "
                "AND (Nombre_Cliente LIKE %s OR Cod_Cliente = %s) "
                "ORDER BY Nombre_Cliente",
                (f"%{query}%", query),
            )
        else:
            c.execute(
                "SELECT Cod_Cliente, Nombre_Cliente FROM BI_CLIENTES "
                "WHERE Vendedor = %s AND (Nombre_Cliente LIKE %s OR Cod_Cliente = %s) "
                "ORDER BY Nombre_Cliente",
                (salesrep_id, f"%{query}%", query),
            )
        return [{"code": row[0], "name": row[1]} for row in c.fetchall()]
    finally:
        conn.close()


def get_avg_daily_sales(client_code: str, product_code: str | None = None, days_back: int = 90) -> float | None:
    """Query MSSQL for average daily sales (units) for a client over the last N days.

    If product_code is given, filters to that specific product.
    Uses BI_ANALISIS_VENTAS table. Returns None if no data or on error.
    """
    try:
        conn = get_mssql_connection()
    except Exception:
        return None
    try:
        c = conn.cursor()
        if product_code:
            c.execute(
                "SELECT COALESCE(SUM(Cantidad_NETA), 0) AS total_qty, "
                "  DATEDIFF(day, MIN(Fecha_Documento), GETDATE()) + 1 AS span_days "
                "FROM BI_ANALISIS_VENTAS "
                "WHERE Codigo_Cliente = %s AND Codigo_Producto = %s "
                "  AND Fecha_Documento >= DATEADD(day, %s, GETDATE())",
                (client_code, product_code, -days_back),
            )
        else:
            c.execute(
                "SELECT COALESCE(SUM(Cantidad_NETA), 0) AS total_qty, "
                "  DATEDIFF(day, MIN(Fecha_Documento), GETDATE()) + 1 AS span_days "
                "FROM BI_ANALISIS_VENTAS "
                "WHERE Codigo_Cliente = %s AND Fecha_Documento >= DATEADD(day, %s, GETDATE())",
                (client_code, -days_back),
            )
        row = c.fetchone()
        if not row or row[0] == 0 or row[1] is None or row[1] == 0:
            return None
        return float(row[0]) / float(row[1])
    except Exception:
        return None
    finally:
        conn.close()


def get_last_order_qty(client_code: str) -> int | None:
    """Get the most recent order total qty for a client from MSSQL. Secondary signal."""
    try:
        conn = get_mssql_connection()
    except Exception:
        return None
    try:
        c = conn.cursor()
        c.execute(
            "SELECT TOP 1 Cantidad_NETA FROM BI_ANALISIS_VENTAS "
            "WHERE Codigo_Cliente = %s ORDER BY Fecha_Documento DESC",
            (client_code,),
        )
        row = c.fetchone()
        return int(row[0]) if row else None
    except Exception:
        return None
    finally:
        conn.close()


def _handy_daily_by_product(handy_orders: list[dict]) -> dict[str, float]:
    """Compute per-product daily sales estimate from HANDY order history.

    Canasta orders (e.g. PT00024 Canasta Familiar = 14x PT00005) are expanded
    to their unit equivalents so demand is not underestimated.
    """
    from agent_tasty.config import CANASTA_MAPPING

    # Reverse map: canasta_code -> (unit_code, case_size)
    canasta_to_unit = {
        cm["canasta_code"]: (unit_code, cm["case_size"])
        for unit_code, cm in CANASTA_MAPPING.items()
    }

    if not handy_orders:
        return {}

    dates = [o["date"] for o in handy_orders if o.get("date")]
    if len(dates) >= 2:
        span = max((max(dates) - min(dates)).days, 90)
    else:
        span = 90

    product_totals: dict[str, int] = {}
    for o in handy_orders:
        for pcode, qty in o.get("items", {}).items():
            if pcode in canasta_to_unit:
                unit_code, case_size = canasta_to_unit[pcode]
                product_totals[unit_code] = product_totals.get(unit_code, 0) + qty * case_size
            else:
                product_totals[pcode] = product_totals.get(pcode, 0) + qty

    return {code: total / span for code, total in product_totals.items() if total > 0}


def calculate_suggested_order(client_code: str, store_name: str, stock_by_sku: dict[str, int]) -> OrderResult:
    """Calculate suggested order per SKU with business rules.

    Tier 1: Local PostgreSQL sales cache (synced from MSSQL/SAP HANA)
    Tier 2: HANDY order history (per-product daily avg)
    Tier 3: Fallback stub max(0, 30 - stock)

    All tiers use: max(0, ceil(daily * visit_interval) - shelf_stock)

    Business rules applied post-calculation:
    1. Merma filter: skip SKUs where client merma_rate >= 8%
    2. Minimum $15 total order value check
    """
    from agent_tasty.handy import get_recent_orders, get_visit_history, compute_visit_interval
    from agent_tasty.db import get_cached_daily_sales, get_product_prices, get_client_merma_rates, compute_rfm_segment
    from agent_tasty.config import SEGMENT_CONFIG, UT_SEGMENT_CONFIG, DT_COVERAGE_TARGETS, UT_COVERAGE_TARGETS, CANASTA_MAPPING

    # Compute RFM segment
    rfm = compute_rfm_segment(client_code)
    tier = rfm["tier"]
    is_ut = client_code.startswith("UT")
    is_dt = client_code.startswith("DT")
    seg_cfg = UT_SEGMENT_CONFIG[tier] if is_ut else SEGMENT_CONFIG[tier]
    multiplier = seg_cfg["multiplier"]
    lookback_days = seg_cfg["lookback_days"]
    min_floor = seg_cfg["min_floor"]
    print(f"[order-calc] RFM segment={tier} (R={rfm['r']},F={rfm['f']},M={rfm['m']},sum={rfm['composite']}) multiplier={multiplier} lookback={lookback_days}d")

    # Get HANDY visit interval (shared across all SKUs)
    visit_interval = 7.0
    try:
        visits = get_visit_history(client_code)
        visit_interval = compute_visit_interval(visits)
    except Exception:
        pass

    # Tier 1: Local PG sales cache (using segment lookback)
    pg_daily = {}
    try:
        pg_daily = get_cached_daily_sales(client_code, days_back=lookback_days)
    except Exception:
        pass

    # Tier 2: HANDY order history
    handy_daily = {}
    try:
        handy_orders = get_recent_orders(client_code, days_back=365)
        handy_daily = _handy_daily_by_product(handy_orders)
    except Exception:
        pass

    # Load business rule data
    prices = {}
    try:
        prices = get_product_prices()
    except Exception:
        pass

    merma_rates = {}
    try:
        merma_rates = get_client_merma_rates(client_code)
    except Exception:
        pass

    PASITAS_CODE = "PT00019"
    order = OrderResult()
    order.segment = tier

    for product_code, shelf_stock in stock_by_sku.items():
        # Business rule: skip SKU if merma > 15%
        merma = merma_rates.get(product_code, 0.0)
        if merma > 0.15:
            order.suggested_by_sku[product_code] = 0
            order.skipped_skus[product_code] = f"merma {merma:.0%}"
            print(f"[order-calc] {product_code} SKIPPED: merma={merma:.1%}")
            continue

        daily = pg_daily.get(product_code)
        data_tier = "PG-cache"

        if not daily or daily <= 0:
            daily = handy_daily.get(product_code)
            data_tier = "HANDY"

        if daily and daily > 0:
            if product_code == PASITAS_CODE:
                eff_multiplier = multiplier * 1.25
                suggested = max(0, math.ceil(daily * visit_interval * eff_multiplier) - shelf_stock)
                if min_floor > 0:
                    suggested = max(suggested, min_floor)
                print(f"[order-calc] {product_code} {data_tier} PASITAS-BOOST 1.25x seg={tier}: daily={daily:.2f}, interval={visit_interval:.1f}, mult={eff_multiplier:.2f}, stock={shelf_stock} -> {suggested}")
            else:
                suggested = max(0, math.ceil(daily * visit_interval * multiplier) - shelf_stock)
                if min_floor > 0:
                    suggested = max(suggested, min_floor)
                print(f"[order-calc] {product_code} {data_tier} seg={tier}: daily={daily:.2f}, interval={visit_interval:.1f}, mult={multiplier}, stock={shelf_stock} -> {suggested}")
        else:
            suggested = 0
            print(f"[order-calc] {product_code} no history: suggest=0")

        # Canasta upgrade: for DT clients, round up to full canastas when close to case_size.
        # Only applies for PG-cache data — HANDY daily is already derived from canasta unit expansion
        # so applying the upgrade again would double-count the case_size and inflate the order.
        if suggested > 0 and data_tier == "PG-cache" and client_code.startswith("DT") and product_code in CANASTA_MAPPING:
            cm = CANASTA_MAPPING[product_code]
            case_size = cm["case_size"]
            if suggested >= case_size - 2:
                n_canastas = math.ceil(suggested / case_size)
                order.canasta_upgrades[product_code] = {
                    "n_canastas": n_canastas,
                    "canasta_code": cm["canasta_code"],
                    "short_name": cm["short_name"],
                    "total_units": n_canastas * case_size,
                    "price_usd": cm["price_usd"],
                }
                order.suggested_by_sku[product_code] = 0  # covered by canasta
                print(f"[order-calc] {product_code} canasta-upgrade: {suggested}u → {n_canastas}x {cm['short_name']} ({n_canastas * case_size}u)")
                continue  # skip storing in suggested_by_sku below

        # Coverage floor: nudge DT/UT clients to stock target SKUs
        if suggested == 0 and shelf_stock == 0:
            if is_dt and product_code in DT_COVERAGE_TARGETS:
                suggested = DT_COVERAGE_TARGETS[product_code]
                print(f"[order-calc] {product_code} cobertura-DT floor={suggested}")
            elif is_ut and product_code in UT_COVERAGE_TARGETS:
                suggested = UT_COVERAGE_TARGETS[product_code]
                print(f"[order-calc] {product_code} cobertura-UT floor={suggested}")

        # Minimum order quantity: if ordering at all, must be at least 2 units
        if 0 < suggested < 2:
            suggested = 2
            print(f"[order-calc] {product_code} min-qty floor: raised to 2")

        order.suggested_by_sku[product_code] = suggested

    # Compute total order value in USD
    for product_code, qty in order.suggested_by_sku.items():
        if qty > 0 and product_code not in order.skipped_skus:
            price = prices.get(product_code, 0.0)
            order.total_value_usd += qty * price

    for unit_code, upgrade in order.canasta_upgrades.items():
        order.total_value_usd += upgrade["n_canastas"] * upgrade["price_usd"]

    # Business rule: Dynamic minimum order based on blended devol rate
    # min_order = DROP_COST / (gross_margin - D_blended)
    # At avg 13.8% devol: min = $7 / (0.60 - 0.138) = $15.15
    DROP_COST = 7.0
    GROSS_MARGIN = 0.60
    wnum = wden = 0.0
    OVERALL_DEVOL = 0.138
    for product_code, qty in order.suggested_by_sku.items():
        if qty > 0 and product_code not in order.skipped_skus:
            price = prices.get(product_code, 0.0)
            val = qty * price
            wnum += val * merma_rates.get(product_code, OVERALL_DEVOL)
            wden += val
    for unit_code, upgrade in order.canasta_upgrades.items():
        val = upgrade["n_canastas"] * upgrade["price_usd"]
        wnum += val * merma_rates.get(unit_code, OVERALL_DEVOL)
        wden += val
    blend = wnum / wden if wden > 0 else OVERALL_DEVOL
    min_order = DROP_COST / (GROSS_MARGIN - blend) if blend < GROSS_MARGIN else float("inf")
    if order.total_value_usd > 0 and order.total_value_usd < min_order:
        order.warnings.append(
            f"Pedido por debajo del minimo de ${min_order:.2f} (devol={blend:.1%}, total: ${order.total_value_usd:.2f})"
        )
        print(f"[order-calc] WARNING: total ${order.total_value_usd:.2f} < ${min_order:.2f} dynamic minimum (devol blend={blend:.1%})")

    print(f"[order-calc] Total order value: ${order.total_value_usd:.2f}")
    return order
