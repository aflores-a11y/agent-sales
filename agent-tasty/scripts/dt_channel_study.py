"""DT Channel Client Diagnostic Study - 90-day analysis with monthly breakdown.

Generates a PDF report with:
  1. Executive Summary
  2. Sales Analysis (top/bottom clients, product mix)
  3. Coverage & Visits
  4. Profitability (unprofitable clients, merma offenders)
  5. Stock Turnover Heatmap (client x SKU, color-coded)

Usage:
    python scripts/dt_channel_study.py
    python scripts/dt_channel_study.py --days 90 --output /tmp/dt_study.pdf
"""
import sys, os, argparse, io
from datetime import date, timedelta
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import psycopg2
from fpdf import FPDF

from agent_tasty.config import (
    DATABASE_URL, SKU_CATALOG, FALLBACK_PRICES,
    DT_COVERAGE_TARGETS, SEGMENT_CONFIG,
)
from agent_tasty.mssql import get_mssql_connection
from agent_tasty.db import compute_rfm_segment

SKU_CODES = [s["code"] for s in SKU_CATALOG if not s.get("dt_only")]
SKU_CODES_ALL = [s["code"] for s in SKU_CATALOG]
SKU_SHORT = {s["code"]: s["short_name"] for s in SKU_CATALOG}
PRICES = dict(FALLBACK_PRICES)

DROP_COST = 7.0
GROSS_MARGIN = 0.60
OVERALL_DEVOL = 0.138
MIN_PROFITABLE_ORDER = DROP_COST / (GROSS_MARGIN - OVERALL_DEVOL)


# ── Data Fetching ────────────────────────────────────────────────────────────

def fetch_dt_clients() -> list[dict]:
    try:
        conn = get_mssql_connection()
        c = conn.cursor()
        c.execute(
            "SELECT Cod_Cliente, Nombre_Cliente, Vendedor "
            "FROM BI_CLIENTES WHERE Cod_Cliente LIKE 'DT%' "
            "ORDER BY Vendedor, Nombre_Cliente"
        )
        clients = [{"code": r[0], "name": r[1], "route": r[2]} for r in c.fetchall()]
        conn.close()
        print(f"[study] {len(clients)} DT clients from MSSQL")
        return clients
    except Exception as e:
        print(f"[study] MSSQL error: {e}")
        return []


def fetch_sales_data(client_codes: list[str], start: date, end: date) -> list[dict]:
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    ph = ",".join(["%s"] * len(client_codes))
    cur.execute(
        f"SELECT client_code, product_code, sale_date, quantity, "
        f"COALESCE(qty_fac, 0), COALESCE(qty_returned, 0) "
        f"FROM sales_cache "
        f"WHERE client_code IN ({ph}) AND sale_date >= %s AND sale_date <= %s",
        client_codes + [start, end],
    )
    rows = [
        {
            "client": r[0], "product": r[1], "date": r[2],
            "qty_net": float(r[3]), "qty_fac": float(r[4]), "qty_ret": float(r[5]),
        }
        for r in cur.fetchall()
    ]
    conn.close()
    print(f"[study] {len(rows)} sales rows from PostgreSQL ({start} to {end})")
    return rows


def fetch_merma_data(client_codes: list[str]) -> dict:
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    ph = ",".join(["%s"] * len(client_codes))
    cur.execute(
        f"SELECT client_code, product_code, total_sold, total_returned, merma_rate "
        f"FROM client_merma_cache WHERE client_code IN ({ph})",
        client_codes,
    )
    result = {}
    for r in cur.fetchall():
        key = (r[0], r[1])
        result[key] = {"sold": float(r[2]), "returned": float(r[3]), "rate": float(r[4])}
    conn.close()
    return result


def fetch_visit_data(client_codes: list[str], start: date) -> list[dict]:
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    ph = ",".join(["%s"] * len(client_codes))
    cur.execute(
        f"SELECT v.client_code, v.visit_date, i.product_code, i.shelf_stock "
        f"FROM salesrep_visits v "
        f"JOIN salesrep_visit_items i ON i.visit_id = v.id "
        f"WHERE v.client_code IN ({ph}) AND v.visit_date >= %s",
        client_codes + [start],
    )
    rows = [
        {"client": r[0], "date": r[1], "product": r[2], "shelf": int(r[3] or 0)}
        for r in cur.fetchall()
    ]
    conn.close()
    print(f"[study] {len(rows)} visit items from PostgreSQL")
    return rows


# ── Metrics Computation ──────────────────────────────────────────────────────

def get_month_key(d) -> str:
    if isinstance(d, date):
        return d.strftime("%Y-%m")
    return str(d)[:7]


def compute_metrics(sales: list, visits: list, merma: dict, clients: list, months: list[str]):
    client_map = {c["code"]: c for c in clients}
    codes = set(c["code"] for c in clients)

    # ── Sales by client x month ──
    client_month_revenue = defaultdict(lambda: defaultdict(float))
    client_month_orders = defaultdict(lambda: defaultdict(int))
    client_total_revenue = defaultdict(float)
    product_month_units = defaultdict(lambda: defaultdict(float))
    product_month_revenue = defaultdict(lambda: defaultdict(float))
    client_product_qty = defaultdict(lambda: defaultdict(float))
    order_dates_by_client = defaultdict(set)

    for s in sales:
        mk = get_month_key(s["date"])
        rev = s["qty_net"] * PRICES.get(s["product"], 0)
        client_month_revenue[s["client"]][mk] += rev
        client_total_revenue[s["client"]] += rev
        order_dates_by_client[s["client"]].add(s["date"])
        product_month_units[s["product"]][mk] += s["qty_net"]
        product_month_revenue[s["product"]][mk] += rev
        client_product_qty[s["client"]][s["product"]] += s["qty_net"]

    for c in order_dates_by_client:
        for d in order_dates_by_client[c]:
            client_month_orders[c][get_month_key(d)] += 1

    # ── RFM segments ──
    rfm_dist = defaultdict(int)
    client_rfm = {}
    for c in clients[:200]:
        try:
            seg = compute_rfm_segment(c["code"])
            rfm_dist[seg["tier"]] += 1
            client_rfm[c["code"]] = seg["tier"]
        except Exception:
            rfm_dist["Bronze"] += 1
            client_rfm[c["code"]] = "Bronze"

    # ── Visit frequency ──
    client_visit_dates = defaultdict(set)
    client_sku_shelf = defaultdict(lambda: defaultdict(list))
    for v in visits:
        client_visit_dates[v["client"]].add(str(v["date"])[:10])
        if v["shelf"] is not None:
            client_sku_shelf[v["client"]][v["product"]].append(v["shelf"])

    # ── Turnover matrix ──
    span_days = 90
    turnover = {}
    for c in codes:
        turnover[c] = {}
        for sku in SKU_CODES_ALL:
            total_qty = client_product_qty[c].get(sku, 0)
            daily_vel = total_qty / span_days if total_qty > 0 else 0
            shelves = client_sku_shelf.get(c, {}).get(sku, [])
            avg_shelf = sum(shelves) / len(shelves) if shelves else 0
            if avg_shelf > 0 and daily_vel > 0:
                turnover[c][sku] = round(daily_vel / avg_shelf, 2)
            elif daily_vel > 0:
                turnover[c][sku] = -1  # has sales but no shelf data
            else:
                turnover[c][sku] = 0

    # ── Profitability ──
    unprofitable = []
    for c in codes:
        total_orders = sum(client_month_orders[c].values())
        total_rev = client_total_revenue.get(c, 0)
        if total_orders > 0:
            avg_ticket = total_rev / total_orders
            if avg_ticket < MIN_PROFITABLE_ORDER:
                unprofitable.append({
                    "code": c, "name": client_map.get(c, {}).get("name", ""),
                    "orders": total_orders, "revenue": total_rev,
                    "avg_ticket": avg_ticket,
                })
    unprofitable.sort(key=lambda x: x["avg_ticket"])

    # ── Merma offenders ──
    merma_offenders = []
    for (cc, pc), m in merma.items():
        if m["rate"] > 0.15 and cc in codes:
            merma_offenders.append({
                "client": cc, "name": client_map.get(cc, {}).get("name", ""),
                "product": SKU_SHORT.get(pc, pc), "rate": m["rate"],
                "sold": m["sold"], "returned": m["returned"],
            })
    merma_offenders.sort(key=lambda x: -x["rate"])

    # ── Top/Bottom clients ──
    sorted_clients = sorted(codes, key=lambda c: -client_total_revenue.get(c, 0))
    top_clients = []
    for c in sorted_clients[:15]:
        monthly = [client_month_revenue[c].get(m, 0) for m in months]
        top_clients.append({
            "code": c, "name": client_map.get(c, {}).get("name", ""),
            "route": client_map.get(c, {}).get("route", ""),
            "monthly": monthly, "total": client_total_revenue.get(c, 0),
            "visits": len(client_visit_dates.get(c, set())),
        })

    bottom_clients = []
    active_codes = [c for c in sorted_clients if client_total_revenue.get(c, 0) > 0]
    for c in active_codes[-10:]:
        monthly = [client_month_revenue[c].get(m, 0) for m in months]
        bottom_clients.append({
            "code": c, "name": client_map.get(c, {}).get("name", ""),
            "route": client_map.get(c, {}).get("route", ""),
            "monthly": monthly, "total": client_total_revenue.get(c, 0),
        })

    # ── Coverage by route ──
    route_clients = defaultdict(set)
    route_visited = defaultdict(set)
    for c in clients:
        route_clients[c["route"]].add(c["code"])
        if c["code"] in client_visit_dates and client_visit_dates[c["code"]]:
            route_visited[c["route"]].add(c["code"])

    coverage_by_route = []
    for route in sorted(route_clients.keys()):
        total = len(route_clients[route])
        visited = len(route_visited.get(route, set()))
        coverage_by_route.append({
            "route": route, "total": total, "visited": visited,
            "pct": round(visited / total * 100) if total > 0 else 0,
        })

    return {
        "total_clients": len(codes),
        "total_revenue": sum(client_total_revenue.values()),
        "monthly_revenue": {m: sum(client_month_revenue[c].get(m, 0) for c in codes) for m in months},
        "rfm_dist": dict(rfm_dist),
        "top_clients": top_clients,
        "bottom_clients": bottom_clients,
        "product_month_units": dict(product_month_units),
        "product_month_revenue": dict(product_month_revenue),
        "coverage_by_route": coverage_by_route,
        "unprofitable": unprofitable[:30],
        "merma_offenders": merma_offenders[:30],
        "turnover": turnover,
        "client_map": client_map,
        "months": months,
        "client_visit_count": {c: len(d) for c, d in client_visit_dates.items()},
        "client_total_revenue": dict(client_total_revenue),
    }


# ── PDF Builder ──────────────────────────────────────────────────────────────

def _header(pdf, text):
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_fill_color(30, 80, 160)
    pdf.set_text_color(255, 255, 255)
    return text


def _reset(pdf):
    pdf.set_font("Helvetica", "", 7)
    pdf.set_text_color(0, 0, 0)


def build_pdf(metrics: dict, start: date, end: date) -> bytes:
    pdf = FPDF(orientation="L", unit="mm", format="A4")
    pdf.set_auto_page_break(auto=True, margin=10)
    months = metrics["months"]
    month_labels = months

    # ── Page 1: Executive Summary ──
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 10, "Estudio Diagnostico Canal DT", new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.set_font("Helvetica", "", 10)
    pdf.cell(0, 6, f"Periodo: {start.strftime('%d/%m/%Y')} - {end.strftime('%d/%m/%Y')}", new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.ln(4)

    pdf.set_font("Helvetica", "B", 10)
    pdf.cell(0, 7, "Resumen Ejecutivo", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 9)
    pdf.cell(100, 6, f"Total clientes DT: {metrics['total_clients']}")
    pdf.cell(100, 6, f"Revenue total: ${metrics['total_revenue']:,.2f}", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(2)

    # Monthly revenue
    cols = [("Mes", 40, "L")] + [(m, 35, "R") for m in month_labels]
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_fill_color(30, 80, 160)
    pdf.set_text_color(255, 255, 255)
    for h, w, a in cols:
        pdf.cell(w, 7, h, border=1, align=a, fill=True)
    pdf.ln()
    _reset(pdf)
    pdf.cell(40, 6, "Revenue", border=1)
    for m in months:
        pdf.cell(35, 6, f"${metrics['monthly_revenue'].get(m, 0):,.2f}", border=1, align="R")
    pdf.ln()
    pdf.ln(4)

    # RFM distribution
    pdf.set_font("Helvetica", "B", 10)
    pdf.cell(0, 7, "Distribucion RFM (primeros 200 clientes)", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 9)
    for tier in ["Platinum", "Gold", "Silver", "Bronze"]:
        cnt = metrics["rfm_dist"].get(tier, 0)
        pdf.cell(50, 6, f"  {tier}: {cnt}", new_x="LMARGIN", new_y="NEXT")

    # ── Page 2: Sales Analysis ──
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 12)
    pdf.cell(0, 8, "Analisis de Ventas", new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.ln(2)

    # Top 15
    pdf.set_font("Helvetica", "B", 9)
    pdf.cell(0, 6, "Top 15 Clientes por Revenue", new_x="LMARGIN", new_y="NEXT")

    top_cols = [("Cliente", 60, "L"), ("Ruta", 35, "L")]
    for m in month_labels:
        top_cols.append((m, 25, "R"))
    top_cols += [("Total", 28, "R"), ("Visitas", 15, "C")]

    pdf.set_font("Helvetica", "B", 7)
    pdf.set_fill_color(30, 80, 160)
    pdf.set_text_color(255, 255, 255)
    for h, w, a in top_cols:
        pdf.cell(w, 6, h, border=1, align=a, fill=True)
    pdf.ln()
    _reset(pdf)

    for i, c in enumerate(metrics["top_clients"]):
        fill = i % 2 == 0
        if fill:
            pdf.set_fill_color(240, 245, 255)
        name = (c["name"] or "")[:30]
        pdf.cell(60, 5, f"{c['code']} {name}", border=1, fill=fill)
        pdf.cell(35, 5, (c["route"] or "")[:18], border=1, fill=fill)
        for v in c["monthly"]:
            pdf.cell(25, 5, f"${v:,.0f}" if v else "-", border=1, align="R", fill=fill)
        pdf.cell(28, 5, f"${c['total']:,.0f}", border=1, align="R", fill=fill)
        pdf.cell(15, 5, str(c["visits"]), border=1, align="C", fill=fill)
        pdf.ln()

    pdf.ln(4)

    # Product mix
    pdf.set_font("Helvetica", "B", 9)
    pdf.cell(0, 6, "Mix de Productos (unidades y revenue por mes)", new_x="LMARGIN", new_y="NEXT")

    prod_cols = [("Producto", 40, "L")]
    for m in month_labels:
        prod_cols += [(f"Und {m[-2:]}", 20, "R"), (f"$ {m[-2:]}", 22, "R")]
    prod_cols.append(("Total $", 25, "R"))

    pdf.set_font("Helvetica", "B", 7)
    pdf.set_fill_color(30, 80, 160)
    pdf.set_text_color(255, 255, 255)
    for h, w, a in prod_cols:
        pdf.cell(w, 6, h, border=1, align=a, fill=True)
    pdf.ln()
    _reset(pdf)

    for sku in SKU_CODES_ALL:
        name = SKU_SHORT.get(sku, sku)
        pdf.cell(40, 5, name, border=1)
        total_rev = 0
        for m in months:
            units = metrics["product_month_units"].get(sku, {}).get(m, 0)
            rev = metrics["product_month_revenue"].get(sku, {}).get(m, 0)
            total_rev += rev
            pdf.cell(20, 5, f"{units:.0f}" if units else "-", border=1, align="R")
            pdf.cell(22, 5, f"${rev:,.0f}" if rev else "-", border=1, align="R")
        pdf.cell(25, 5, f"${total_rev:,.0f}", border=1, align="R")
        pdf.ln()

    # ── Page 3: Coverage & Visits ──
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 12)
    pdf.cell(0, 8, "Cobertura y Visitas", new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.ln(2)

    cov_cols = [("Ruta", 50, "L"), ("Total Clientes", 25, "C"), ("Visitados", 25, "C"), ("Cobertura %", 25, "C")]
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_fill_color(30, 80, 160)
    pdf.set_text_color(255, 255, 255)
    for h, w, a in cov_cols:
        pdf.cell(w, 7, h, border=1, align=a, fill=True)
    pdf.ln()
    _reset(pdf)

    total_total = 0
    total_visited = 0
    for cr in metrics["coverage_by_route"]:
        pdf.cell(50, 5, cr["route"], border=1)
        pdf.cell(25, 5, str(cr["total"]), border=1, align="C")
        pdf.cell(25, 5, str(cr["visited"]), border=1, align="C")
        pdf.cell(25, 5, f"{cr['pct']}%", border=1, align="C")
        pdf.ln()
        total_total += cr["total"]
        total_visited += cr["visited"]

    pdf.set_font("Helvetica", "B", 8)
    pdf.set_fill_color(200, 220, 255)
    pdf.set_text_color(0, 0, 0)
    pdf.cell(50, 6, "TOTAL", border=1, fill=True)
    pdf.cell(25, 6, str(total_total), border=1, align="C", fill=True)
    pdf.cell(25, 6, str(total_visited), border=1, align="C", fill=True)
    pct = round(total_visited / total_total * 100) if total_total else 0
    pdf.cell(25, 6, f"{pct}%", border=1, align="C", fill=True)
    pdf.ln()

    # ── Page 4: Profitability ──
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 12)
    pdf.cell(0, 8, "Rentabilidad", new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.ln(2)

    pdf.set_font("Helvetica", "B", 9)
    pdf.cell(0, 6, f"Clientes con ticket promedio < ${MIN_PROFITABLE_ORDER:.2f} (no rentables)", new_x="LMARGIN", new_y="NEXT")

    unp_cols = [("Cliente", 70, "L"), ("Pedidos", 20, "C"), ("Revenue", 30, "R"), ("Ticket Prom", 30, "R")]
    pdf.set_font("Helvetica", "B", 7)
    pdf.set_fill_color(30, 80, 160)
    pdf.set_text_color(255, 255, 255)
    for h, w, a in unp_cols:
        pdf.cell(w, 6, h, border=1, align=a, fill=True)
    pdf.ln()
    _reset(pdf)

    for u in metrics["unprofitable"][:25]:
        pdf.set_fill_color(255, 230, 230)
        name = f"{u['code']} {(u['name'] or '')[:30]}"
        pdf.cell(70, 5, name, border=1, fill=True)
        pdf.cell(20, 5, str(u["orders"]), border=1, align="C", fill=True)
        pdf.cell(30, 5, f"${u['revenue']:,.2f}", border=1, align="R", fill=True)
        pdf.cell(30, 5, f"${u['avg_ticket']:,.2f}", border=1, align="R", fill=True)
        pdf.ln()

    pdf.ln(6)
    pdf.set_font("Helvetica", "B", 9)
    pdf.cell(0, 6, "Merma > 15% (por cliente x producto)", new_x="LMARGIN", new_y="NEXT")

    merma_cols = [("Cliente", 60, "L"), ("Producto", 30, "L"), ("Vendido", 20, "R"), ("Devuelto", 20, "R"), ("Merma %", 20, "R")]
    pdf.set_font("Helvetica", "B", 7)
    pdf.set_fill_color(30, 80, 160)
    pdf.set_text_color(255, 255, 255)
    for h, w, a in merma_cols:
        pdf.cell(w, 6, h, border=1, align=a, fill=True)
    pdf.ln()
    _reset(pdf)

    for m in metrics["merma_offenders"][:25]:
        name = f"{m['client']} {(m['name'] or '')[:25]}"
        pdf.cell(60, 5, name, border=1)
        pdf.cell(30, 5, m["product"], border=1)
        pdf.cell(20, 5, f"{m['sold']:.0f}", border=1, align="R")
        pdf.cell(20, 5, f"{m['returned']:.0f}", border=1, align="R")
        pdf.cell(20, 5, f"{m['rate']*100:.0f}%", border=1, align="R")
        pdf.ln()

    # ── Pages 5+: Stock Turnover Heatmap ──
    sorted_clients = sorted(
        metrics["turnover"].keys(),
        key=lambda c: -metrics["client_total_revenue"].get(c, 0),
    )
    active_clients = [c for c in sorted_clients if metrics["client_total_revenue"].get(c, 0) > 0]

    sku_list = SKU_CODES_ALL
    sku_headers = [SKU_SHORT.get(s, s)[:10] for s in sku_list]
    per_page = 30
    cell_w = 16
    name_w = 55

    for page_idx in range(0, len(active_clients), per_page):
        pdf.add_page()
        pdf.set_font("Helvetica", "B", 12)
        page_num = page_idx // per_page + 1
        total_pages = (len(active_clients) + per_page - 1) // per_page
        pdf.cell(0, 8, f"Rotacion de Inventario - Heatmap ({page_num}/{total_pages})", new_x="LMARGIN", new_y="NEXT", align="C")
        pdf.set_font("Helvetica", "I", 7)
        pdf.set_text_color(100, 100, 100)
        pdf.cell(0, 4, "Verde=alta rotacion (>0.5)  Amarillo=moderada (0.1-0.5)  Rojo=baja (<0.1)  Gris=sin datos", new_x="LMARGIN", new_y="NEXT", align="C")
        pdf.ln(2)

        # Header row
        pdf.set_font("Helvetica", "B", 6)
        pdf.set_fill_color(30, 80, 160)
        pdf.set_text_color(255, 255, 255)
        pdf.cell(name_w, 6, "Cliente", border=1, fill=True)
        for sh in sku_headers:
            pdf.cell(cell_w, 6, sh, border=1, align="C", fill=True)
        pdf.ln()

        batch = active_clients[page_idx:page_idx + per_page]
        pdf.set_font("Helvetica", "", 6)

        for c in batch:
            cmap = metrics["client_map"].get(c, {})
            label = f"{c} {(cmap.get('name', '') or '')[:22]}"
            pdf.set_text_color(0, 0, 0)
            pdf.set_fill_color(255, 255, 255)
            pdf.cell(name_w, 5, label, border=1)

            for sku in sku_list:
                val = metrics["turnover"].get(c, {}).get(sku, 0)
                if val > 0.5:
                    pdf.set_fill_color(76, 175, 80)
                    pdf.set_text_color(255, 255, 255)
                elif val > 0.1:
                    pdf.set_fill_color(255, 235, 59)
                    pdf.set_text_color(0, 0, 0)
                elif val == -1:
                    pdf.set_fill_color(200, 200, 200)
                    pdf.set_text_color(80, 80, 80)
                elif val > 0:
                    pdf.set_fill_color(244, 67, 54)
                    pdf.set_text_color(255, 255, 255)
                else:
                    pdf.set_fill_color(220, 220, 220)
                    pdf.set_text_color(150, 150, 150)

                display = f"{val:.1f}" if val > 0 else ("-" if val == -1 else "0")
                pdf.cell(cell_w, 5, display, border=1, align="C", fill=True)
            pdf.ln()

    # Footer
    pdf.set_font("Helvetica", "I", 7)
    pdf.set_text_color(100, 100, 100)
    pdf.ln(2)
    pdf.cell(
        0, 5,
        f"Rotacion = velocidad_diaria / stock_promedio_anaquel. "
        f"Min. rentable = ${MIN_PROFITABLE_ORDER:.2f}. Merma umbral = 15%.",
        align="C",
    )

    return pdf.output()


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="DT Channel Diagnostic Study")
    parser.add_argument("--days", type=int, default=90, help="Lookback days (default 90)")
    parser.add_argument("--output", type=str, default=None, help="Output PDF path")
    args = parser.parse_args()

    end = date.today()
    start = end - timedelta(days=args.days)

    # Compute month boundaries
    months = []
    d = start.replace(day=1)
    while d <= end:
        months.append(d.strftime("%Y-%m"))
        if d.month == 12:
            d = d.replace(year=d.year + 1, month=1)
        else:
            d = d.replace(month=d.month + 1)

    print(f"[study] Period: {start} to {end}, months: {months}")

    clients = fetch_dt_clients()
    if not clients:
        print("[study] No DT clients found, aborting.")
        return

    codes = [c["code"] for c in clients]
    sales = fetch_sales_data(codes, start, end)
    visits = fetch_visit_data(codes, start)
    merma = fetch_merma_data(codes)

    metrics = compute_metrics(sales, visits, merma, clients, months)

    pdf_bytes = build_pdf(metrics, start, end)
    out_path = args.output or f"/tmp/dt_channel_study_{end.strftime('%Y%m%d')}.pdf"
    with open(out_path, "wb") as f:
        f.write(pdf_bytes)
    print(f"[study] PDF saved: {out_path} ({len(pdf_bytes):,} bytes)")


if __name__ == "__main__":
    main()
