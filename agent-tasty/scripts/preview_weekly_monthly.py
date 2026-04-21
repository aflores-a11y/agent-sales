#!/usr/bin/env python3
"""Generate draft PDFs for weekly and monthly supervisor reports using real data.

Requires: MSSQL, PostgreSQL, HANDY API connections (run inside Docker or with .env).

Usage:
    docker compose exec agent-tasty python scripts/preview_weekly_monthly.py
    # Outputs copied to /app/ inside container, then docker cp to host.
"""

import io
import math
import os
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from fpdf import FPDF

from agent_tasty.reports import build_report_data
from agent_tasty.scheduler import is_panama_business_day

COLORS = {
    "primary":    (30, 80, 160),
    "header_bg":  (30, 80, 160),
    "header_fg":  (255, 255, 255),
    "row_even":   (240, 245, 255),
    "row_odd":    (255, 255, 255),
    "total_bg":   (200, 220, 255),
    "red":        (200, 40, 40),
    "green":      (30, 140, 60),
    "orange":     (220, 140, 20),
    "gray":       (120, 120, 120),
}

DAY_NAMES_ES = ["Lun", "Mar", "Mie", "Jue", "Vie", "Sab", "Dom"]
MONTH_NAMES_ES = ["", "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
                  "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]


# ── Data collection ──────────────────────────────────────────────────────────

def collect_multi_day_data(days: list[date]) -> dict[date, list[dict]]:
    """Call build_report_data for each business day, return {date: [route_rows]}."""
    result = {}
    for d in days:
        if not is_panama_business_day(d):
            continue
        print(f"  Fetching data for {d}...", flush=True)
        try:
            rows = build_report_data(d)
            rows = [r for r in rows if not r["ruta"].startswith("UT")]
            if rows:
                result[d] = rows
        except Exception as e:
            print(f"  ERROR on {d}: {e}")
    return result


def _aggregate_routes(multi_day: dict[date, list[dict]]) -> list[dict]:
    """Aggregate daily route data into per-route summaries."""
    route_data: dict[str, list[dict]] = {}
    for d, rows in multi_day.items():
        for row in rows:
            ruta = row["ruta"]
            if ruta not in route_data:
                route_data[ruta] = []
            route_data[ruta].append(row)

    summary = []
    for ruta, days_list in route_data.items():
        n = len(days_list)
        total_pedidos = sum(r["pedidos"] for r in days_list)
        total_visitas = sum(r["visitas"] for r in days_list)
        total_venta = sum(r["venta_handy"] for r in days_list)
        total_bot = sum(r["bot"] for r in days_list)
        total_clients = days_list[0]["total_clients"] if days_list else 0
        avg_t = [r["t_avg_min"] for r in days_list if r["t_avg_min"]]

        summary.append({
            "ruta": ruta,
            "dias": n,
            "total_pedidos": total_pedidos,
            "total_visitas": total_visitas,
            "prom_pedidos": round(total_pedidos / n) if n else 0,
            "total_venta": round(total_venta, 2),
            "prom_venta": round(total_venta / n, 2) if n else 0,
            "prom_efectividad": round(total_pedidos / total_visitas * 100) if total_visitas else 0,
            "prom_compliance": round(total_bot / total_pedidos * 100) if total_pedidos else 0,
            "prom_cumpl_ruta": round(total_pedidos / n / total_clients * 100) if total_clients and n else 0,
            "prom_ticket": round(total_venta / total_pedidos, 2) if total_pedidos else 0,
            "prom_t_avg": round(sum(avg_t) / len(avg_t)) if avg_t else 0,
            "total_clients": total_clients,
            "venta_mes": days_list[-1].get("venta_mes", 0),
        })
    summary.sort(key=lambda x: x["total_venta"], reverse=True)
    return summary


def _daily_totals(multi_day: dict[date, list[dict]]) -> dict[date, dict]:
    """Per-day totals across all routes."""
    result = {}
    for d, rows in sorted(multi_day.items()):
        result[d] = {
            "pedidos": sum(r["pedidos"] for r in rows),
            "visitas": sum(r["visitas"] for r in rows),
            "venta": sum(r["venta_handy"] for r in rows),
            "ticket": (sum(r["venta_handy"] for r in rows) / sum(r["pedidos"] for r in rows)
                       if sum(r["pedidos"] for r in rows) else 0),
        }
    return result


def _per_route_daily(multi_day: dict[date, list[dict]], metric: str) -> dict[str, dict[date, float]]:
    """Extract {ruta: {date: value}} for a given metric."""
    result: dict[str, dict[date, float]] = {}
    for d, rows in multi_day.items():
        for row in rows:
            ruta = row["ruta"]
            if ruta not in result:
                result[ruta] = {}
            result[ruta][d] = row.get(metric, 0)
    return result


# ── Chart builders ───────────────────────────────────────────────────────────

def _chart_daily_ventas(multi_day: dict[date, list[dict]], title: str) -> bytes:
    daily = _daily_totals(multi_day)
    dates = sorted(daily.keys())
    totals = [daily[d]["venta"] for d in dates]
    labels = [f"{DAY_NAMES_ES[d.weekday()]} {d.day}" for d in dates]

    fig, ax = plt.subplots(figsize=(max(7, len(dates) * 0.6), 2.8))
    bars = ax.bar(labels, totals, color="#1E50A0", width=0.6)
    ax.set_ylabel("Venta Total ($)", fontsize=9)
    ax.set_title(title, fontsize=11, fontweight="bold")
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter('$%.0f'))
    for bar, val in zip(bars, totals):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 20,
                f"${val:,.0f}", ha="center", va="bottom", fontsize=6)
    plt.xticks(fontsize=7, rotation=45 if len(dates) > 8 else 0)
    plt.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150)
    plt.close(fig)
    return buf.getvalue()


def _chart_metric_by_route(summary: list[dict], metric: str, label: str, title: str, fmt: str = "{:.0f}%") -> bytes:
    sorted_routes = sorted(summary, key=lambda x: x[metric])
    names = [r["ruta"] for r in sorted_routes]
    values = [r[metric] for r in sorted_routes]

    avg = sum(values) / len(values) if values else 0
    colors = ["#C82828" if v < avg * 0.8 else "#1E8C3C" if v > avg * 1.1 else "#1E50A0" for v in values]

    fig, ax = plt.subplots(figsize=(7, max(3, len(names) * 0.35)))
    bars = ax.barh(names, values, color=colors, height=0.6)
    ax.axvline(x=avg, color="#666", linestyle="--", linewidth=1, label=f"Promedio {fmt.format(avg)}")
    ax.set_xlabel(label, fontsize=9)
    ax.set_title(title, fontsize=11, fontweight="bold")
    ax.legend(fontsize=8)
    for bar, val in zip(bars, values):
        ax.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height() / 2,
                fmt.format(val), ha="left", va="center", fontsize=7)
    plt.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150)
    plt.close(fig)
    return buf.getvalue()


def _chart_ticket_trend(multi_day: dict[date, list[dict]], title: str) -> bytes:
    daily = _daily_totals(multi_day)
    dates = sorted(daily.keys())
    tickets = [daily[d]["ticket"] for d in dates]
    labels = [f"{d.day}/{d.month}" for d in dates]

    fig, ax = plt.subplots(figsize=(max(7, len(dates) * 0.5), 2.5))
    ax.plot(labels, tickets, marker="o", color="#1E50A0", linewidth=2, markersize=4)
    ax.fill_between(range(len(labels)), tickets, alpha=0.1, color="#1E50A0")
    ax.set_ylabel("Ticket Promedio ($)", fontsize=9)
    ax.set_title(title, fontsize=11, fontweight="bold")
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter('$%.2f'))
    if len(labels) > 10:
        show_labels = [l if i % 3 == 0 else "" for i, l in enumerate(labels)]
        ax.set_xticks(range(len(show_labels)))
        ax.set_xticklabels(show_labels, fontsize=7)
    plt.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150)
    plt.close(fig)
    return buf.getvalue()


# ── Outlier detection ────────────────────────────────────────────────────────

def _find_outliers(summary: list[dict]) -> list[dict]:
    outliers = []
    for metric, label, fmt, direction in [
        ("prom_efectividad", "Efectividad", "{:.0f}%", "both"),
        ("prom_compliance",  "Compliance Bot", "{:.0f}%", "low"),
        ("prom_venta",       "Venta Diaria", "${:.2f}", "both"),
        ("prom_ticket",      "Ticket Promedio", "${:.2f}", "low"),
        ("prom_cumpl_ruta",  "Cumplimiento Ruta", "{:.0f}%", "low"),
    ]:
        vals = [(r["ruta"], r[metric]) for r in summary]
        vals.sort(key=lambda x: x[1])
        avg = sum(v for _, v in vals) / len(vals) if vals else 0

        if direction in ("both", "low") and vals and vals[0][1] < avg * 0.75:
            outliers.append({
                "ruta": vals[0][0], "metric": label,
                "value": fmt.format(vals[0][1]),
                "avg": fmt.format(avg), "type": "bajo",
            })
        if direction == "both" and vals and vals[-1][1] > avg * 1.2:
            outliers.append({
                "ruta": vals[-1][0], "metric": label,
                "value": fmt.format(vals[-1][1]),
                "avg": fmt.format(avg), "type": "alto",
            })
    return outliers


# ── PDF builders ─────────────────────────────────────────────────────────────

def _write_summary_table(pdf: FPDF, summary: list[dict]):
    """Write the route summary table to the PDF."""
    cols = [
        ("Ruta",        35, "L"),
        ("Dias",        12, "C"),
        ("Pedidos",     16, "C"),
        ("P/Dia",       14, "C"),
        ("Venta $",     22, "R"),
        ("V/Dia $",     20, "R"),
        ("Efect%",      14, "C"),
        ("Compl%",      14, "C"),
        ("C.Ruta%",     14, "C"),
        ("Ticket $",    18, "R"),
    ]

    pdf.set_font("Helvetica", "B", 7.5)
    pdf.set_fill_color(*COLORS["header_bg"])
    pdf.set_text_color(*COLORS["header_fg"])
    for header, w, align in cols:
        pdf.cell(w, 7, header, border=1, align=align, fill=True)
    pdf.ln()

    pdf.set_font("Helvetica", "", 7.5)
    for i, row in enumerate(summary):
        bg = COLORS["row_even"] if i % 2 == 0 else COLORS["row_odd"]
        pdf.set_fill_color(*bg)
        pdf.set_text_color(0, 0, 0)
        vals = [
            row["ruta"], str(row["dias"]), str(row["total_pedidos"]),
            str(row["prom_pedidos"]), f"${row['total_venta']:,.2f}",
            f"${row['prom_venta']:,.2f}", f"{row['prom_efectividad']}%",
            f"{row['prom_compliance']}%", f"{row['prom_cumpl_ruta']}%",
            f"${row['prom_ticket']:.2f}",
        ]
        for (_, w, align), val in zip(cols, vals):
            pdf.cell(w, 6, val, border=1, align=align, fill=True)
        pdf.ln()

    # Totals row
    n = len(summary)
    if not n:
        return
    pdf.set_font("Helvetica", "B", 7.5)
    pdf.set_fill_color(*COLORS["total_bg"])
    pdf.set_text_color(0, 0, 0)
    total_pedidos = sum(r["total_pedidos"] for r in summary)
    total_visitas = sum(r["total_visitas"] for r in summary)
    total_venta = sum(r["total_venta"] for r in summary)
    tot_vals = [
        "TOTAL", "", str(total_pedidos), "",
        f"${total_venta:,.2f}", "",
        f"{round(total_pedidos / total_visitas * 100) if total_visitas else 0}%",
        f"{round(sum(r['prom_compliance'] * r['total_pedidos'] for r in summary) / total_pedidos) if total_pedidos else 0}%",
        f"{round(sum(r['prom_cumpl_ruta'] for r in summary) / n)}%",
        f"${total_venta / total_pedidos:.2f}" if total_pedidos else "-",
    ]
    for (_, w, align), val in zip(cols, tot_vals):
        pdf.cell(w, 7, val, border=1, align=align, fill=True)
    pdf.ln()


def _write_outliers_table(pdf: FPDF, outliers: list[dict]):
    if outliers:
        pdf.set_font("Helvetica", "B", 8)
        pdf.set_fill_color(*COLORS["header_bg"])
        pdf.set_text_color(*COLORS["header_fg"])
        for header, w in [("Ruta", 45), ("Metrica", 35), ("Valor", 25), ("Promedio", 25), ("Tipo", 20)]:
            pdf.cell(w, 7, header, border=1, align="C", fill=True)
        pdf.ln()

        pdf.set_font("Helvetica", "", 8)
        for i, o in enumerate(outliers):
            bg = COLORS["row_even"] if i % 2 == 0 else COLORS["row_odd"]
            pdf.set_fill_color(*bg)
            is_bad = o["type"] == "bajo"
            pdf.set_text_color(*(COLORS["red"] if is_bad else COLORS["green"]))
            pdf.cell(45, 6, o["ruta"], border=1, align="L", fill=True)
            pdf.cell(35, 6, o["metric"], border=1, align="L", fill=True)
            pdf.cell(25, 6, o["value"], border=1, align="C", fill=True)
            pdf.cell(25, 6, o["avg"], border=1, align="C", fill=True)
            pdf.cell(20, 6, "BAJO" if is_bad else "ALTO", border=1, align="C", fill=True)
            pdf.ln()
    else:
        pdf.set_font("Helvetica", "", 10)
        pdf.set_text_color(0, 0, 0)
        pdf.cell(0, 8, "No se encontraron outliers significativos.", ln=True, align="C")


def _write_tasks(pdf: FPDF, outliers: list[dict]):
    """Write tasks based on outlier analysis."""
    low_efect = [o for o in outliers if o["metric"] == "Efectividad" and o["type"] == "bajo"]
    low_comp = [o for o in outliers if o["metric"] == "Compliance Bot" and o["type"] == "bajo"]
    low_ticket = [o for o in outliers if o["metric"] == "Ticket Promedio" and o["type"] == "bajo"]
    low_cumpl = [o for o in outliers if o["metric"] == "Cumplimiento Ruta" and o["type"] == "bajo"]

    tasks = []
    if low_efect:
        rutas = ", ".join(o["ruta"] for o in low_efect)
        tasks.append(f"Acompanar en ruta a vendedores con baja efectividad: {rutas}. "
                     f"Verificar tecnica de venta y cobertura de clientes.")
    if low_comp:
        rutas = ", ".join(o["ruta"] for o in low_comp)
        tasks.append(f"Reforzar uso del bot con: {rutas}. "
                     f"Revisar que estan reportando stock en cada visita.")
    if low_ticket:
        rutas = ", ".join(o["ruta"] for o in low_ticket)
        tasks.append(f"Revisar mezcla de productos en: {rutas}. "
                     f"Asegurar que ofrecen catalogo completo (especialmente Integral, Pasitas, Sandwich).")
    if low_cumpl:
        rutas = ", ".join(o["ruta"] for o in low_cumpl)
        tasks.append(f"Mejorar cobertura de ruta en: {rutas}. "
                     f"Verificar que visitan todos los clientes asignados.")

    tasks.append("Revisar merma por ruta - dar seguimiento a clientes con alta devolucion.")
    tasks.append("Verificar que todas las rutas inician antes de las 08:00.")

    pdf.set_font("Helvetica", "", 9)
    pdf.set_text_color(0, 0, 0)
    for i, task in enumerate(tasks, 1):
        pdf.set_font("Helvetica", "B", 9)
        pdf.cell(8, 6, f"{i}.", align="R")
        pdf.set_font("Helvetica", "", 9)
        pdf.multi_cell(170, 6, f" {task}")
        pdf.ln(1)


def _save_chart(chart_bytes: bytes, path: str):
    with open(path, "wb") as f:
        f.write(chart_bytes)


def build_weekly_pdf(report_monday: date, multi_day: dict[date, list[dict]]) -> bytes:
    prev_monday = report_monday - timedelta(days=7)
    week_label = f"{prev_monday.strftime('%d/%m/%Y')} - {(prev_monday + timedelta(days=5)).strftime('%d/%m/%Y')}"

    summary = _aggregate_routes(multi_day)
    outliers = _find_outliers(summary)

    pdf = FPDF(orientation="P", unit="mm", format="A4")
    pdf.set_auto_page_break(auto=True, margin=15)

    # ── Page 1: Summary Table ──
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 16)
    pdf.set_text_color(*COLORS["primary"])
    pdf.cell(0, 10, "Reporte Semanal de Supervision", ln=True, align="C")
    pdf.set_font("Helvetica", "", 11)
    pdf.set_text_color(*COLORS["gray"])
    pdf.cell(0, 6, f"Semana: {week_label}", ln=True, align="C")
    pdf.cell(0, 6, f"Generado: {report_monday.strftime('%d/%m/%Y')} 07:00", ln=True, align="C")
    pdf.ln(4)

    _write_summary_table(pdf, summary)

    # ── Page 2: Charts ──
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 13)
    pdf.set_text_color(*COLORS["primary"])
    pdf.cell(0, 8, "Graficos Semanales", ln=True, align="C")
    pdf.ln(2)

    tmp = "/tmp"
    c1 = _chart_daily_ventas(multi_day, "Venta Total Diaria")
    c2 = _chart_metric_by_route(summary, "prom_efectividad", "Efectividad %", "Efectividad por Ruta")
    _save_chart(c1, f"{tmp}/_wc1.png")
    _save_chart(c2, f"{tmp}/_wc2.png")
    pdf.image(f"{tmp}/_wc1.png", x=10, w=190)
    pdf.ln(3)
    pdf.image(f"{tmp}/_wc2.png", x=10, w=190)

    # ── Page 3: More Charts ──
    pdf.add_page()
    c3 = _chart_metric_by_route(summary, "prom_compliance", "Compliance Bot %", "Compliance Bot por Ruta")
    c4 = _chart_ticket_trend(multi_day, "Ticket Promedio Diario")
    _save_chart(c3, f"{tmp}/_wc3.png")
    _save_chart(c4, f"{tmp}/_wc4.png")
    pdf.image(f"{tmp}/_wc3.png", x=10, w=190)
    pdf.ln(3)
    pdf.image(f"{tmp}/_wc4.png", x=10, w=190)

    # ── Page 4: Outliers + Tasks ──
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 13)
    pdf.set_text_color(*COLORS["primary"])
    pdf.cell(0, 8, "Alertas y Outliers", ln=True, align="C")
    pdf.ln(2)

    _write_outliers_table(pdf, outliers)

    pdf.ln(6)
    pdf.set_font("Helvetica", "B", 13)
    pdf.set_text_color(*COLORS["primary"])
    pdf.cell(0, 8, "Tareas para la Proxima Semana", ln=True, align="C")
    pdf.ln(2)

    _write_tasks(pdf, outliers)

    for i in range(1, 5):
        p = f"{tmp}/_wc{i}.png"
        if os.path.exists(p):
            os.remove(p)

    return bytes(pdf.output())


def build_monthly_pdf(last_monday: date, multi_day: dict[date, list[dict]]) -> bytes:
    report_month = last_monday.month
    report_year = last_monday.year
    month_label = f"{MONTH_NAMES_ES[report_month]} {report_year}"
    next_month = report_month + 1 if report_month < 12 else 1
    next_year = report_year if report_month < 12 else report_year + 1

    summary = _aggregate_routes(multi_day)
    outliers = _find_outliers(summary)

    pdf = FPDF(orientation="P", unit="mm", format="A4")
    pdf.set_auto_page_break(auto=True, margin=15)

    # ── Page 1: Summary Table ──
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 16)
    pdf.set_text_color(*COLORS["primary"])
    pdf.cell(0, 10, "Reporte Mensual de Supervision", ln=True, align="C")
    pdf.set_font("Helvetica", "", 11)
    pdf.set_text_color(*COLORS["gray"])
    pdf.cell(0, 6, f"Mes: {month_label}", ln=True, align="C")
    pdf.cell(0, 6, f"Generado: {last_monday.strftime('%d/%m/%Y')} 07:00", ln=True, align="C")
    pdf.ln(4)

    _write_summary_table(pdf, summary)

    # ── Page 2: Charts ──
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 13)
    pdf.set_text_color(*COLORS["primary"])
    pdf.cell(0, 8, f"Graficos del Mes - {month_label}", ln=True, align="C")
    pdf.ln(2)

    tmp = "/tmp"
    c1 = _chart_daily_ventas(multi_day, f"Venta Total Diaria - {month_label}")
    c2 = _chart_metric_by_route(summary, "prom_efectividad", "Efectividad %", f"Efectividad por Ruta - {month_label}")
    _save_chart(c1, f"{tmp}/_mc1.png")
    _save_chart(c2, f"{tmp}/_mc2.png")
    pdf.image(f"{tmp}/_mc1.png", x=10, w=190)
    pdf.ln(3)
    pdf.image(f"{tmp}/_mc2.png", x=10, w=190)

    # ── Page 3: More Charts ──
    pdf.add_page()
    c3 = _chart_metric_by_route(summary, "prom_compliance", "Compliance Bot %", f"Compliance Bot - {month_label}")
    c4 = _chart_ticket_trend(multi_day, f"Ticket Promedio Diario - {month_label}")
    _save_chart(c3, f"{tmp}/_mc3.png")
    _save_chart(c4, f"{tmp}/_mc4.png")
    pdf.image(f"{tmp}/_mc3.png", x=10, w=190)
    pdf.ln(3)
    pdf.image(f"{tmp}/_mc4.png", x=10, w=190)

    # ── Page 4: Outliers ──
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 13)
    pdf.set_text_color(*COLORS["primary"])
    pdf.cell(0, 8, "Alertas y Outliers del Mes", ln=True, align="C")
    pdf.ln(2)

    _write_outliers_table(pdf, outliers)

    # ── Page 5: Goals for next month ──
    pdf.ln(6)
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 13)
    pdf.set_text_color(*COLORS["primary"])
    pdf.cell(0, 8, f"Metas para {MONTH_NAMES_ES[next_month]} {next_year}", ln=True, align="C")
    pdf.ln(4)

    n = len(summary) or 1
    total_venta = sum(r["total_venta"] for r in summary)
    total_pedidos = sum(r["total_pedidos"] for r in summary)
    total_visitas = sum(r["total_visitas"] for r in summary)
    avg_efect = round(total_pedidos / total_visitas * 100) if total_visitas else 0
    avg_comp = round(sum(r["prom_compliance"] for r in summary) / n)
    avg_cumpl = round(sum(r["prom_cumpl_ruta"] for r in summary) / n)
    avg_ticket = total_venta / total_pedidos if total_pedidos else 0

    low_efect = [o for o in outliers if o["metric"] == "Efectividad" and o["type"] == "bajo"]
    low_comp = [o for o in outliers if o["metric"] == "Compliance Bot" and o["type"] == "bajo"]
    low_ticket = [o for o in outliers if o["metric"] == "Ticket Promedio" and o["type"] == "bajo"]
    low_cumpl = [o for o in outliers if o["metric"] == "Cumplimiento Ruta" and o["type"] == "bajo"]

    goals = []

    if avg_efect < 85:
        goals.append({
            "title": f"Subir efectividad general de {avg_efect}% a {min(avg_efect + 5, 90)}%",
            "detail": "Enfocarse en tecnica de cierre de venta. Reducir visitas sin pedido.",
            "rutas_foco": [o["ruta"] for o in low_efect],
        })

    if avg_comp < 85:
        goals.append({
            "title": f"Aumentar compliance del bot de {avg_comp}% a {min(avg_comp + 10, 95)}%",
            "detail": "Todos los vendedores deben reportar stock en cada visita via bot.",
            "rutas_foco": [o["ruta"] for o in low_comp],
        })

    if avg_cumpl < 40:
        goals.append({
            "title": f"Mejorar cobertura de ruta de {avg_cumpl}% a {min(avg_cumpl + 5, 50)}%",
            "detail": "Revisar listado de clientes por ruta. Eliminar inactivos y agregar nuevos.",
            "rutas_foco": [o["ruta"] for o in low_cumpl],
        })

    goals.append({
        "title": f"Incrementar venta total mensual en 5% (meta: ${total_venta * 1.05:,.0f})",
        "detail": "Introducir SKUs faltantes en clientes con catalogo incompleto. Impulsar Integral y Pasitas.",
        "rutas_foco": [o["ruta"] for o in low_ticket],
    })

    goals.append({
        "title": f"Subir ticket promedio de ${avg_ticket:.2f} a ${avg_ticket * 1.08:.2f}",
        "detail": "Sugerir productos complementarios. Ofrecer canastas en vez de unidades donde aplique.",
        "rutas_foco": [],
    })

    goals.append({
        "title": "Reducir merma (devolucion) a menos de 12%",
        "detail": "Revisar clientes con alta devolucion. Ajustar pedidos sugeridos para evitar exceso.",
        "rutas_foco": [],
    })

    pdf.set_font("Helvetica", "", 9)
    pdf.set_text_color(0, 0, 0)
    for i, goal in enumerate(goals, 1):
        pdf.set_font("Helvetica", "B", 10)
        pdf.set_text_color(*COLORS["primary"])
        pdf.cell(8, 7, f"{i}.", align="R")
        pdf.multi_cell(170, 7, f" {goal['title']}")

        pdf.set_font("Helvetica", "", 9)
        pdf.set_text_color(60, 60, 60)
        pdf.cell(8, 5, "")
        pdf.multi_cell(170, 5, f"   {goal['detail']}")

        if goal["rutas_foco"]:
            pdf.set_font("Helvetica", "I", 8)
            pdf.set_text_color(*COLORS["red"])
            pdf.cell(8, 5, "")
            pdf.multi_cell(170, 5, f"   Rutas foco: {', '.join(goal['rutas_foco'])}")
        pdf.ln(2)

    pdf.ln(4)
    pdf.set_font("Helvetica", "I", 8)
    pdf.set_text_color(*COLORS["gray"])
    pdf.cell(0, 5, "Las metas se basan en los resultados del mes actual y las areas de mejora identificadas.", align="C")

    for i in range(1, 5):
        p = f"{tmp}/_mc{i}.png"
        if os.path.exists(p):
            os.remove(p)

    return bytes(pdf.output())


def main():
    # Weekly: report for week of April 6-11 (generated Monday April 13)
    print("=" * 60)
    print("WEEKLY REPORT - Week of April 6-11, 2026")
    print("=" * 60)
    report_monday = date(2026, 4, 13)
    prev_monday = report_monday - timedelta(days=7)
    week_days = [prev_monday + timedelta(days=i) for i in range(6)]  # Mon-Sat
    weekly_data = collect_multi_day_data(week_days)
    print(f"Collected data for {len(weekly_data)} days.")

    if weekly_data:
        weekly_pdf = build_weekly_pdf(report_monday, weekly_data)
        out1 = "/app/draft_reporte_semanal.pdf"
        with open(out1, "wb") as f:
            f.write(weekly_pdf)
        print(f"Weekly report saved: {out1} ({len(weekly_pdf):,} bytes)")
    else:
        print("No data found for weekly report.")

    # Monthly: use all April data available (simulating last Monday of April)
    print()
    print("=" * 60)
    print("MONTHLY REPORT - Abril 2026")
    print("=" * 60)
    last_monday = date(2026, 4, 27)
    month_start = date(2026, 4, 1)
    # Collect all April days up to today (or April 13)
    today = date(2026, 4, 13)
    month_days = []
    d = month_start
    while d <= today:
        month_days.append(d)
        d += timedelta(days=1)
    monthly_data = collect_multi_day_data(month_days)
    print(f"Collected data for {len(monthly_data)} days.")

    if monthly_data:
        monthly_pdf = build_monthly_pdf(last_monday, monthly_data)
        out2 = "/app/draft_reporte_mensual.pdf"
        with open(out2, "wb") as f:
            f.write(monthly_pdf)
        print(f"Monthly report saved: {out2} ({len(monthly_pdf):,} bytes)")
    else:
        print("No data found for monthly report.")


if __name__ == "__main__":
    main()
