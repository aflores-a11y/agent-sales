"""Daily supervisor compliance report — PDF via WhatsApp.

Data sources:
- Bot visits + Sug$  : PostgreSQL salesrep_visits / salesrep_visit_items
- Inicio/Fin/T.Avg   : HANDY API  /user/{id}/salesOrder  (today, last pages)
- V.Mes / T.Prom Mes : sales_cache (BI_ANALISIS_VENTAS synced to PostgreSQL)
"""

import base64
import json
import math
from datetime import date, datetime, timedelta, timezone

import pymssql
import psycopg2
import redis
import requests
from fpdf import FPDF

from agent_tasty.config import (
    CANASTA_MAPPING,
    DATABASE_URL,
    FALLBACK_PRICES,
    HANDY_API_TOKEN,
    HANDY_BASE_URL,
    MONTH_ABBREV,
    MSSQL_DATABASE,
    MSSQL_HOST,
    MSSQL_PASSWORD,
    MSSQL_PORT,
    MSSQL_USER,
    REDIS_URL,
    SKU_CATALOG,
    get_routes_for_report,
    get_supervisors,
)

# Reverse map: canasta_code → (unit_code, case_size)
_CANASTA_TO_UNIT = {
    cm["canasta_code"]: (unit_code, cm["case_size"])
    for unit_code, cm in CANASTA_MAPPING.items()
}

# Short names for SKUs
_SKU_SHORT = {s["code"]: s["short_name"] for s in SKU_CATALOG}

PANAMA_TZ = timezone(timedelta(hours=-5))
OUTGOING_QUEUE = "queue:outgoing"
DROP_COST = 7.0
GROSS_MARGIN = 0.60
OVERALL_DEVOL = 0.138
MIN_PROFITABLE_ORDER = DROP_COST / (GROSS_MARGIN - OVERALL_DEVOL)  # ~$15.15


# ── Data fetchers ─────────────────────────────────────────────────────────────

def get_handy_route_data(handy_user_id: int, report_date: date) -> dict:
    """Fetch HANDY orders for a salesrep user on report_date.

    Returns inicio, fin, n_orders, venta_handy, avg_min, and per-client order details.
    Scans backwards from the last page until all orders for report_date are found.
    """
    empty = {"inicio": "-", "fin": "-", "n_orders": 0, "n_visitas": 0, "venta_handy": 0.0, "avg_min": 0, "client_orders": []}
    if not HANDY_API_TOKEN or not HANDY_BASE_URL:
        return empty

    base = HANDY_BASE_URL.rstrip("/")
    headers = {"Authorization": f"Bearer {HANDY_API_TOKEN}"}

    try:
        r = requests.get(
            f"{base}/api/v2/user/{handy_user_id}/salesOrder?page=1",
            headers=headers, timeout=10,
        )
        total_pages = r.json().get("pagination", {}).get("totalPages", 1)
    except Exception as e:
        print(f"[reports] HANDY pagination error (user {handy_user_id}): {e}")
        return empty

    orders = []
    client_orders = []  # per-client detail for page 2

    for page in range(total_pages, max(total_pages - 5, 0), -1):
        try:
            r2 = requests.get(
                f"{base}/api/v2/user/{handy_user_id}/salesOrder?page={page}",
                headers=headers, timeout=10,
            )
            found_on_page = False
            for o in r2.json().get("salesOrders", []):
                dt_str = o.get("mobileDateCreated", "")
                if not dt_str:
                    continue
                dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00")).astimezone(PANAMA_TZ)
                if dt.date() == report_date:
                    total_sales = float(o.get("totalSales", 0))
                    orders.append((dt, total_sales))
                    cust = o.get("customer", {})
                    # Extract per-item with canasta → unit expansion
                    items_by_sku = {}
                    for item in o.get("items", []):
                        if item.get("isReturn", False):
                            continue
                        pcode = item.get("product", {}).get("code", "")
                        qty = int(item.get("quantity", 0))
                        if pcode in _CANASTA_TO_UNIT:
                            unit_code, case_size = _CANASTA_TO_UNIT[pcode]
                            items_by_sku[unit_code] = items_by_sku.get(unit_code, 0) + qty * case_size
                        elif pcode:
                            items_by_sku[pcode] = items_by_sku.get(pcode, 0) + qty
                    client_orders.append({
                        "client_code": cust.get("code", ""),
                        "client_name": cust.get("description", ""),
                        "handy_usd": total_sales,
                        "time": dt.strftime("%H:%M"),
                        "handy_items": items_by_sku,
                    })
                    found_on_page = True
                elif dt.date() < report_date:
                    pass
            if not found_on_page:
                break
        except Exception as e:
            print(f"[reports] HANDY page {page} error (user {handy_user_id}): {e}")

    if not orders:
        return empty

    orders.sort()
    total_min = int((orders[-1][0] - orders[0][0]).total_seconds() / 60)
    n = len(orders)

    # Fetch visits (includes non-effective) for efectividad calculation
    n_visitas = n  # fallback: assume all visits are orders
    try:
        rv = requests.get(
            f"{base}/api/v2/user/{handy_user_id}/visit?page=1",
            headers=headers, timeout=10,
        )
        visit_total_pages = rv.json().get("pagination", {}).get("totalPages", 1)
        visit_count = 0
        for vpage in range(visit_total_pages, max(visit_total_pages - 5, 0), -1):
            rv2 = requests.get(
                f"{base}/api/v2/user/{handy_user_id}/visit?page={vpage}",
                headers=headers, timeout=10,
            )
            found = False
            for v in rv2.json().get("visits", []):
                start_str = v.get("start", "")
                if not start_str:
                    continue
                vdt = datetime.fromisoformat(start_str.replace("Z", "+00:00")).astimezone(PANAMA_TZ)
                if vdt.date() == report_date:
                    visit_count += 1
                    found = True
                elif vdt.date() < report_date:
                    pass
            if not found:
                break
        if visit_count > 0:
            n_visitas = visit_count
    except Exception as e:
        print(f"[reports] HANDY visits error (user {handy_user_id}): {e}")

    return {
        "inicio":        orders[0][0].strftime("%H:%M"),
        "fin":           orders[-1][0].strftime("%H:%M"),
        "n_orders":      n,
        "n_visitas":     n_visitas,
        "venta_handy":   sum(v for _, v in orders),
        "avg_min":       total_min // n if n else 0,
        "client_orders": client_orders,
    }


def get_bot_route_data(lids: tuple, report_date: date) -> dict:
    """Fetch bot visits and suggested order value (USD) from PostgreSQL.
    Also returns per-client suggested totals for deviation analysis.
    """
    bot_visits = 0
    sug_usd = 0.0
    client_suggestions = {}  # {client_code: sug_usd}
    client_sug_by_sku = {}   # {client_code: {product_code: qty}}

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        ph = ",".join(["%s"] * len(lids))

        cur.execute(
            f"SELECT COUNT(DISTINCT v.id) FROM salesrep_visits v "
            f"WHERE v.phone_number IN ({ph}) AND DATE(v.visit_date) = %s",
            lids + (report_date,),
        )
        bot_visits = cur.fetchone()[0]

        cur.execute(
            f"SELECT i.product_code, SUM(i.suggested_order) "
            f"FROM salesrep_visits v "
            f"JOIN salesrep_visit_items i ON i.visit_id = v.id "
            f"WHERE v.phone_number IN ({ph}) AND DATE(v.visit_date) = %s "
            f"GROUP BY i.product_code",
            lids + (report_date,),
        )
        sug_usd = sum(
            float(qty) * FALLBACK_PRICES.get(code, 0)
            for code, qty in cur.fetchall()
        )

        # Per-client per-SKU suggested quantities + totals
        cur.execute(
            f"SELECT v.client_code, i.product_code, i.suggested_order "
            f"FROM salesrep_visits v "
            f"JOIN salesrep_visit_items i ON i.visit_id = v.id "
            f"WHERE v.phone_number IN ({ph}) AND DATE(v.visit_date) = %s",
            lids + (report_date,),
        )
        for client_code, product_code, suggested in cur.fetchall():
            if client_code and suggested:
                qty = int(suggested)
                price = FALLBACK_PRICES.get(product_code, 0)
                client_suggestions[client_code] = client_suggestions.get(client_code, 0) + qty * price
                if client_code not in client_sug_by_sku:
                    client_sug_by_sku[client_code] = {}
                client_sug_by_sku[client_code][product_code] = client_sug_by_sku[client_code].get(product_code, 0) + qty

        conn.close()
    except Exception as e:
        print(f"[reports] PostgreSQL error: {e}")

    return {
        "bot_visits": bot_visits, "sug_usd": sug_usd,
        "client_suggestions": client_suggestions, "client_sug_by_sku": client_sug_by_sku,
    }


def get_route_client_counts() -> dict[str, int]:
    """Get total client count per route from MSSQL BI_CLIENTES."""
    route_ids = [r["salesrep_id"] for r in get_routes_for_report()]
    counts = {}
    try:
        ms = pymssql.connect(
            server=MSSQL_HOST, port=MSSQL_PORT,
            user=MSSQL_USER, password=MSSQL_PASSWORD,
            database=MSSQL_DATABASE,
            login_timeout=15, timeout=30,
        )
        cur = ms.cursor()
        placeholders = ",".join(["%s"] * len(route_ids))
        cur.execute(
            f"SELECT Vendedor, COUNT(*) FROM BI_CLIENTES WHERE Vendedor IN ({placeholders}) GROUP BY Vendedor",
            tuple(route_ids),
        )
        for vendedor, cnt in cur.fetchall():
            counts[vendedor] = cnt
        ms.close()
    except Exception as e:
        print(f"[reports] MSSQL client count error: {e}")
    return counts


def get_bi_all_routes(report_date: date) -> dict:
    """Compute BI revenue metrics from PostgreSQL sales_cache + product_price_cache."""
    from agent_tasty.config import get_routes_for_report
    route_ids = [r["salesrep_id"] for r in get_routes_for_report()]
    month_start = report_date.replace(day=1)
    result: dict[str, dict] = {}

    client_to_route: dict[str, str] = {}
    try:
        ms = pymssql.connect(
            server=MSSQL_HOST, port=MSSQL_PORT,
            user=MSSQL_USER, password=MSSQL_PASSWORD,
            database=MSSQL_DATABASE,
            login_timeout=15, timeout=30,
        )
        cur = ms.cursor()
        placeholders = ",".join(["%s"] * len(route_ids))
        cur.execute(
            f"SELECT Cod_Cliente, Vendedor FROM BI_CLIENTES WHERE Vendedor IN ({placeholders})",
            tuple(route_ids),
        )
        for cod, vendedor in cur.fetchall():
            client_to_route[cod] = vendedor
        ms.close()
        print(f"[reports] BI_CLIENTES: {len(client_to_route)} clients mapped to {len(route_ids)} routes.")
    except Exception as e:
        print(f"[reports] MSSQL BI_CLIENTES error: {e}")
        return result

    if not client_to_route:
        return result

    prices: dict[str, float] = {}
    try:
        pg = psycopg2.connect(DATABASE_URL)
        cur2 = pg.cursor()
        cur2.execute("SELECT product_code, unit_price_usd FROM product_price_cache")
        prices = {row[0]: float(row[1]) for row in cur2.fetchall()}

        client_list = list(client_to_route.keys())
        ph = ",".join(["%s"] * len(client_list))
        cur2.execute(
            f"SELECT client_code, product_code, sale_date, quantity "
            f"FROM sales_cache "
            f"WHERE sale_date >= %s AND sale_date <= %s AND client_code IN ({ph})",
            (month_start, report_date) + tuple(client_list),
        )
        rows = cur2.fetchall()
        pg.close()
    except Exception as e:
        print(f"[reports] PostgreSQL error fetching sales cache: {e}")
        return result

    route_daily: dict[str, dict] = {}
    for client_code, product_code, sale_date, quantity in rows:
        vendedor = client_to_route.get(client_code)
        if not vendedor:
            continue
        price = prices.get(product_code, 0.0)
        revenue = float(quantity or 0) * price
        if vendedor not in route_daily:
            route_daily[vendedor] = {}
        route_daily[vendedor][sale_date] = route_daily[vendedor].get(sale_date, 0.0) + revenue

    for vendedor, daily in route_daily.items():
        venta_mes = sum(daily.values())
        dias = len(daily)
        result[vendedor] = {
            "venta_mes":       round(venta_mes, 2),
            "ticket_prom_mes": round(venta_mes / dias, 2) if dias else 0.0,
        }

    print(f"[reports] Sales cache: {len(result)} routes with revenue data.")
    return result


def _empty_bi() -> dict:
    return {"venta_mes": 0.0, "ticket_prom_mes": 0.0}


# ── Report data builder ───────────────────────────────────────────────────────

def build_report_data(report_date: date, route_filter: str | None = None) -> list[dict]:
    """Collect all column data for each active route."""
    routes = get_routes_for_report()
    if route_filter:
        routes = [r for r in routes if route_filter.lower() in r["salesrep_id"].lower()]

    bi_by_route = get_bi_all_routes(report_date)
    client_counts = get_route_client_counts()

    # Load stored visit stats for efectividad (captured at 16:45)
    from agent_tasty.visit_capture import get_visit_stats
    visit_stats_list = get_visit_stats(report_date)
    visit_stats_by_route = {vs["salesrep_id"]: vs for vs in visit_stats_list}

    rows = []
    for rep in routes:
        print(f"[reports] Fetching {rep['salesrep_id']}...", flush=True)
        lids = tuple(x for x in [rep.get("phone"), rep.get("lid")] if x)
        handy = get_handy_route_data(rep["handy_user_id"], report_date)
        bot   = get_bot_route_data(lids, report_date)
        bi    = bi_by_route.get(rep["salesrep_id"], _empty_bi())

        n_orders = handy["n_orders"]
        total_clients = client_counts.get(rep["salesrep_id"], 0)

        # Efectividad, inicio/fin, t_avg from stored visit stats (calendarEvent + unscheduled)
        vs = visit_stats_by_route.get(rep["salesrep_id"])
        if vs and vs["total_stops"] > 0:
            n_visitas = vs["total_stops"]
            efectividad = round(vs["efectividad"])
            # Use visit times (earliest/latest stop) instead of first/last sale
            if vs.get("inicio"):
                handy["inicio"] = vs["inicio"]
            if vs.get("fin"):
                handy["fin"] = vs["fin"]
            # Recalculate avg_min as total route time / total stops
            if vs.get("inicio") and vs.get("fin"):
                h1, m1 = map(int, vs["inicio"].split(":"))
                h2, m2 = map(int, vs["fin"].split(":"))
                total_min = (h2 * 60 + m2) - (h1 * 60 + m1)
                handy["avg_min"] = total_min // vs["total_stops"] if total_min > 0 else 0
        else:
            # Fallback: no visit_capture data — use HANDY visits endpoint
            n_visitas = handy.get("n_visitas", 0) or n_orders
            efectividad = round(n_orders / n_visitas * 100) if n_visitas > 0 else 0

        # OEE: Availability × Performance × Quality
        # Shift = 8h (480 min), theoretical cycle = 15 min/visit → 32 visits/day
        work_min = 0
        if handy["inicio"] != "-" and handy["fin"] != "-":
            try:
                h1, m1 = map(int, handy["inicio"].split(":"))
                h2, m2 = map(int, handy["fin"].split(":"))
                work_min = (h2 * 60 + m2) - (h1 * 60 + m1)
            except ValueError:
                work_min = 0
        availability = min(work_min / 480, 1.0) if work_min > 0 else 0.0
        theoretical_visits = work_min / 15 if work_min > 0 else 0
        performance = min(n_visitas / theoretical_visits, 1.0) if theoretical_visits > 0 else 0.0
        good_orders = sum(1 for co in handy["client_orders"] if co["handy_usd"] >= MIN_PROFITABLE_ORDER)
        quality = (good_orders / n_orders) if n_orders > 0 else 0.0
        oee = availability * performance * quality

        # Compliance = bot / pedidos (bot adoption)
        compliance = round(bot["bot_visits"] / n_orders * 100) if n_orders > 0 else 0
        # Cumplimiento Ruta = pedidos / total clients in route (route coverage)
        cumpl_ruta = round(n_orders / total_clients * 100) if total_clients > 0 else 0

        # Build per-client deviation list with per-SKU detail
        client_details = []
        for co in handy["client_orders"]:
            cc = co["client_code"]
            sug = bot["client_suggestions"].get(cc, 0)
            deviation = co["handy_usd"] - sug if sug > 0 else 0

            # Per-SKU comparison (canasta-expanded)
            handy_items = co.get("handy_items", {})
            sug_items = bot["client_sug_by_sku"].get(cc, {})
            skipped_skus = []
            for sku, sug_qty in sug_items.items():
                real_qty = handy_items.get(sku, 0)
                if real_qty < sug_qty:
                    diff = real_qty - sug_qty
                    skipped_skus.append({
                        "sku": sku,
                        "name": _SKU_SHORT.get(sku, sku),
                        "sug": sug_qty,
                        "real": real_qty,
                        "diff_usd": round(diff * FALLBACK_PRICES.get(sku, 0), 2),
                    })
            skipped_skus.sort(key=lambda x: x["diff_usd"])

            client_details.append({
                "client_code": cc,
                "client_name": co["client_name"],
                "time": co["time"],
                "handy_usd": co["handy_usd"],
                "sug_usd": round(sug, 2),
                "deviation": round(deviation, 2),
                "unprofitable": co["handy_usd"] < MIN_PROFITABLE_ORDER,
                "skipped_skus": skipped_skus,
            })

        rows.append({
            "ruta":            rep["salesrep_id"],
            "inicio":          handy["inicio"],
            "fin":             handy["fin"],
            "visitas":         n_visitas,
            "pedidos":         n_orders,
            "total_clients":   total_clients,
            "bot":             bot["bot_visits"],
            "efectividad":     efectividad,
            "compliance":      compliance,
            "cumpl_ruta":      cumpl_ruta,
            "t_avg_str":       f"{handy['avg_min']}m" if handy["avg_min"] else "-",
            "t_avg_min":       handy["avg_min"],
            "venta_handy":     round(handy["venta_handy"], 2),
            "ticket_hoy":      round(handy["venta_handy"] / n_orders, 2) if n_orders else 0.0,
            "ticket_prom_mes": bi["ticket_prom_mes"],
            "venta_mes":       bi["venta_mes"],
            "availability":    round(availability * 100),
            "performance":     round(performance * 100),
            "quality":         round(quality * 100),
            "oee":             round(oee * 100),
            "work_min":        work_min,
            "good_orders":     good_orders,
            "client_details":  client_details,
        })

    return rows


# ── PDF builder ───────────────────────────────────────────────────────────────

def build_pdf(rows: list[dict], report_date: date) -> bytes:
    pdf = FPDF(orientation="L", unit="mm", format="A4")
    pdf.set_auto_page_break(auto=True, margin=10)
    pdf.add_page()

    # ── Page 1: Summary table ──
    pdf.set_font("Helvetica", "B", 13)
    pdf.cell(0, 8, f"Reporte de Supervision  -  {report_date.strftime('%d/%m/%Y')}", ln=True, align="C")
    pdf.ln(2)

    cols = [
        ("Ruta",           40, "L"),
        ("Inicio",         14, "C"),
        ("Fin",            14, "C"),
        ("Visitas",        13, "C"),
        ("Pedidos",        14, "C"),
        ("Efect %",        13, "C"),
        ("Compl %",        13, "C"),
        ("C.Ruta %",       14, "C"),
        ("T.Avg",          12, "C"),
        ("Avail %",        13, "C"),
        ("Perf %",         13, "C"),
        ("Qual %",         13, "C"),
        ("OEE %",          13, "C"),
        ("V.Handy",        20, "R"),
        ("T.Prom Hoy",     18, "R"),
        ("V.Mes",          20, "R"),
    ]

    # Header
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_fill_color(30, 80, 160)
    pdf.set_text_color(255, 255, 255)
    for header, w, align in cols:
        pdf.cell(w, 7, header, border=1, align=align, fill=True)
    pdf.ln()

    # Data rows
    pdf.set_font("Helvetica", "", 8)
    totals = {
        "visitas": 0, "pedidos": 0, "bot": 0, "total_clients": 0,
        "venta_handy": 0.0, "venta_mes": 0.0,
        "ticket_hoy_sum": 0.0, "ticket_hoy_n": 0,
        "t_avg_sum": 0, "t_avg_n": 0,
        "work_min": 0, "good_orders": 0, "routes_with_time": 0,
    }

    for i, row in enumerate(rows):
        pdf.set_fill_color(240, 245, 255) if i % 2 == 0 else pdf.set_fill_color(255, 255, 255)
        pdf.set_text_color(0, 0, 0)

        values = [
            row["ruta"],
            row["inicio"],
            row["fin"],
            str(row["visitas"]),
            str(row["pedidos"]),
            f"{row['efectividad']}%",
            f"{row['compliance']}%",
            f"{row['cumpl_ruta']}%",
            row["t_avg_str"],
            f"{row['availability']}%",
            f"{row['performance']}%",
            f"{row['quality']}%",
            f"{row['oee']}%",
            f"${row['venta_handy']:.2f}" if row["venta_handy"] else "-",
            f"${row['ticket_hoy']:.2f}" if row["ticket_hoy"] else "-",
            f"${row['venta_mes']:.2f}",
        ]
        for (_, w, align), val in zip(cols, values):
            pdf.cell(w, 6, val, border=1, align=align, fill=(i % 2 == 0))
        pdf.ln()

        totals["visitas"]        += row["visitas"]
        totals["pedidos"]        += row["pedidos"]
        totals["bot"]            += row["bot"]
        totals["total_clients"]  += row["total_clients"]
        totals["venta_handy"]    += row["venta_handy"]
        totals["venta_mes"]      += row["venta_mes"]
        if row["t_avg_min"]:
            totals["t_avg_sum"] += row["t_avg_min"]
            totals["t_avg_n"]   += 1
        if row["ticket_hoy"]:
            totals["ticket_hoy_sum"] += row["ticket_hoy"]
            totals["ticket_hoy_n"]   += 1
        if row["work_min"] > 0:
            totals["work_min"]         += row["work_min"]
            totals["routes_with_time"] += 1
        totals["good_orders"] += row["good_orders"]

    # Totals row
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_fill_color(200, 220, 255)
    pdf.set_text_color(0, 0, 0)
    avg_t_avg = round(totals["t_avg_sum"] / totals["t_avg_n"]) if totals["t_avg_n"] else 0
    avg_ticket_hoy = totals["ticket_hoy_sum"] / totals["ticket_hoy_n"] if totals["ticket_hoy_n"] else 0
    total_efect = round(totals["pedidos"] / totals["visitas"] * 100) if totals["visitas"] > 0 else 0
    total_compliance = round(totals["bot"] / totals["pedidos"] * 100) if totals["pedidos"] > 0 else 0
    total_cumpl_ruta = round(totals["pedidos"] / totals["total_clients"] * 100) if totals["total_clients"] > 0 else 0

    # OEE totals: aggregate across routes using combined work_min
    budget_min = 480 * totals["routes_with_time"]
    total_avail = min(totals["work_min"] / budget_min, 1.0) if budget_min > 0 else 0.0
    theo_visits = totals["work_min"] / 15 if totals["work_min"] > 0 else 0
    total_perf  = min(totals["visitas"] / theo_visits, 1.0) if theo_visits > 0 else 0.0
    total_qual  = (totals["good_orders"] / totals["pedidos"]) if totals["pedidos"] > 0 else 0.0
    total_oee   = total_avail * total_perf * total_qual

    total_values = [
        "TOTAL", "", "",
        str(totals["visitas"]),
        str(totals["pedidos"]),
        f"{total_efect}%",
        f"{total_compliance}%",
        f"{total_cumpl_ruta}%",
        f"{avg_t_avg}m" if avg_t_avg else "-",
        f"{round(total_avail*100)}%",
        f"{round(total_perf*100)}%",
        f"{round(total_qual*100)}%",
        f"{round(total_oee*100)}%",
        f"${totals['venta_handy']:.2f}",
        f"${avg_ticket_hoy:.2f}" if avg_ticket_hoy else "-",
        f"${totals['venta_mes']:.2f}",
    ]
    for (_, w, align), val in zip(cols, total_values):
        pdf.cell(w, 7, val, border=1, align=align, fill=True)
    pdf.ln()

    # Footer note
    pdf.set_font("Helvetica", "I", 7)
    pdf.set_text_color(100, 100, 100)
    pdf.ln(2)
    pdf.cell(
        0, 5,
        f"Efect% = pedidos/visitas. Compl% = bot/pedidos. C.Ruta% = pedidos/clientes en ruta. "
        f"Avail% = jornada/8h. Perf% = visitas/(jornada/15min). Qual% = pedidos>=${MIN_PROFITABLE_ORDER:.2f}/total. "
        f"OEE = Avail x Perf x Qual.",
        align="C",
    )

    # ── Page 2+: Per-route flagged clients ──
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 13)
    pdf.cell(0, 8, f"Detalle por Ruta  -  {report_date.strftime('%d/%m/%Y')}", ln=True, align="C")
    pdf.ln(2)

    detail_cols = [
        ("Cliente",        55, "L"),
        ("Hora",           14, "C"),
        ("Pedido $",       20, "R"),
        ("Sugerido $",     20, "R"),
        ("Desviacion $",   22, "R"),
        ("Alerta",         30, "L"),
    ]

    for row in rows:
        details = row["client_details"]
        if not details:
            continue

        # Flag clients: unprofitable OR biggest negative deviations
        # Max 20% of the route's daily clients
        max_flags = max(1, math.ceil(len(details) * 0.20))

        flagged = []
        for d in details:
            reasons = []
            if d["unprofitable"]:
                reasons.append("< minimo")
            if d["sug_usd"] > 0 and d["deviation"] < 0:
                reasons.append(f"desvio {d['deviation']:+.2f}")
            if reasons:
                d["alert"] = ", ".join(reasons)
                flagged.append(d)

        # Sort by severity: unprofitable first, then worst deviation
        flagged.sort(key=lambda x: (not x["unprofitable"], x.get("deviation", 0)))
        flagged = flagged[:max_flags]

        if not flagged:
            continue

        # Route header
        pdf.set_font("Helvetica", "B", 9)
        pdf.set_text_color(30, 80, 160)
        pdf.cell(0, 7, f"{row['ruta']}  ({row['pedidos']} pedidos, {len(flagged)} alertas)", ln=True)

        # Detail header
        pdf.set_font("Helvetica", "B", 7.5)
        pdf.set_fill_color(30, 80, 160)
        pdf.set_text_color(255, 255, 255)
        for header, w, align in detail_cols:
            pdf.cell(w, 6, header, border=1, align=align, fill=True)
        pdf.ln()

        # Detail rows with per-SKU breakdown
        pdf.set_font("Helvetica", "", 7.5)
        for j, d in enumerate(flagged):
            pdf.set_fill_color(255, 235, 235) if d["unprofitable"] else (
                pdf.set_fill_color(240, 245, 255) if j % 2 == 0 else pdf.set_fill_color(255, 255, 255)
            )
            pdf.set_text_color(190, 0, 0) if d["unprofitable"] else pdf.set_text_color(0, 0, 0)

            name = d["client_name"][:35] if len(d["client_name"]) > 35 else d["client_name"]
            vals = [
                f"{d['client_code']} {name}",
                d["time"],
                f"${d['handy_usd']:.2f}",
                f"${d['sug_usd']:.2f}" if d["sug_usd"] > 0 else "-",
                f"${d['deviation']:+.2f}" if d["sug_usd"] > 0 else "-",
                d.get("alert", ""),
            ]
            for (_, w, align), val in zip(detail_cols, vals):
                pdf.cell(w, 5.5, val, border=1, align=align, fill=True)
            pdf.ln()

            # Per-SKU skipped items (canasta-aware) — compact inline
            skipped = d.get("skipped_skus", [])
            if skipped:
                # Build a compact string: "SKU sug→real, SKU sug→real"
                sku_parts = []
                for sk in skipped[:5]:
                    sku_parts.append(f"{sk['name']} {sk['sug']}>{sk['real']}")
                sku_text = "  SKUs faltantes:  " + "  |  ".join(sku_parts)

                pdf.set_font("Helvetica", "I", 6.5)
                pdf.set_text_color(100, 100, 100)
                pdf.set_fill_color(248, 248, 255)
                # Span across the full detail row width
                total_w = sum(w for _, w, _ in detail_cols)
                pdf.cell(total_w, 4.5, sku_text, border="LRB", align="L", fill=True)
                pdf.ln()
                pdf.set_font("Helvetica", "", 7.5)

        pdf.ln(2)

    return bytes(pdf.output())


# ── WhatsApp sender ───────────────────────────────────────────────────────────

def send_report_via_whatsapp(supervisor_phone: str, pdf_bytes: bytes, report_date: date):
    r = redis.from_url(REDIS_URL)
    filename = f"reporte_{report_date.isoformat()}.pdf"
    payload = json.dumps({
        "to": supervisor_phone,
        "body": f"Reporte de Supervision — {report_date.strftime('%d/%m/%Y')}",
        "attachment": {
            "mimetype": "application/pdf",
            "data": base64.b64encode(pdf_bytes).decode(),
            "filename": filename,
        },
    })
    r.lpush(OUTGOING_QUEUE, payload)
    print(f"[reports] PDF queued for {supervisor_phone} ({filename})")


# ── Orchestrator ──────────────────────────────────────────────────────────────

def generate_and_send_report(report_date: date | None = None, route_filter: str | None = None):
    """Build and send the report. Called by scheduler (17:00) or on-demand by supervisor."""
    if report_date is None:
        report_date = datetime.now(PANAMA_TZ).date()

    label = f" (ruta: {route_filter})" if route_filter else ""
    print(f"[reports] Generating report for {report_date}{label}")

    rows = build_report_data(report_date, route_filter=route_filter)
    # Filter to DT routes only (exclude UT)
    rows = [r for r in rows if not r["ruta"].startswith("UT")]
    if not rows:
        print("[reports] No data found — skipping.")
        return

    pdf_bytes = build_pdf(rows, report_date)
    supervisors = get_supervisors()
    for sup in supervisors:
        send_report_via_whatsapp(sup["phone"], pdf_bytes, report_date)

    print(f"[reports] Sent to {len(supervisors)} supervisor(s).")
