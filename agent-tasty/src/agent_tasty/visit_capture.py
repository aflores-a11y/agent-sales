"""Capture daily visit stats from HANDY calendarEvent API.

The calendarEvent endpoint only returns today's scheduled visits with their
execution results (effective/not effective + reason). This module captures
that data into PostgreSQL daily_visit_stats so it can be used for
efectividad calculations in daily, weekly, and monthly reports.
"""

import json
from datetime import date, datetime, timedelta, timezone

import requests

from agent_tasty.config import (
    HANDY_API_TOKEN,
    HANDY_BASE_URL,
    get_routes_for_report,
)
from agent_tasty.db import DailyVisitStats, SessionLocal

PANAMA_TZ = timezone(timedelta(hours=-5))


def _get_calendar_visits(handy_user_id: int) -> dict:
    """Fetch today's calendarEvent data for a salesrep.

    Returns {
        scheduled: int,
        visited_effective: int,
        visited_not_effective: int,
        calendar_client_codes: set,  # all scheduled client codes
        effective_client_codes: set,  # scheduled clients with a sale
        no_sale_reasons: dict,        # {reason: count}
    }
    """
    empty = {
        "scheduled": 0, "visited_effective": 0, "visited_not_effective": 0,
        "calendar_client_codes": set(), "effective_client_codes": set(),
        "no_sale_reasons": {}, "visit_times": [],
    }
    if not HANDY_API_TOKEN or not HANDY_BASE_URL:
        return empty

    base = HANDY_BASE_URL.rstrip("/")
    headers = {"Authorization": f"Bearer {HANDY_API_TOKEN}"}

    try:
        r = requests.get(
            f"{base}/api/v2/user/{handy_user_id}/calendarEvent?page=1",
            headers=headers, timeout=15,
        )
        total_pages = r.json().get("pagination", {}).get("totalPages", 1)
    except Exception as e:
        print(f"[visit_capture] calendarEvent pagination error (user {handy_user_id}): {e}")
        return empty

    scheduled = 0
    visited_effective = 0
    visited_not_effective = 0
    calendar_clients = set()
    effective_clients = set()
    reasons = {}
    visit_times = []  # list of datetime for all actual visits

    for page in range(1, total_pages + 1):
        try:
            r2 = requests.get(
                f"{base}/api/v2/user/{handy_user_id}/calendarEvent?page={page}",
                headers=headers, timeout=15,
            )
            for ev in r2.json().get("calendarEvents", []):
                scheduled += 1
                cc = ev.get("customer", {}).get("code", "")
                calendar_clients.add(cc)

                visit = ev.get("visit")
                if visit and isinstance(visit, dict):
                    # Capture visit start time
                    start_str = visit.get("start", "")
                    if start_str:
                        try:
                            vdt = datetime.fromisoformat(start_str.replace("Z", "+00:00")).astimezone(PANAMA_TZ)
                            visit_times.append(vdt)
                        except Exception:
                            pass

                    result = visit.get("result", {})
                    if result.get("effective"):
                        visited_effective += 1
                        effective_clients.add(cc)
                    else:
                        visited_not_effective += 1
                        desc = result.get("description", "Otro")
                        reasons[desc] = reasons.get(desc, 0) + 1
        except Exception as e:
            print(f"[visit_capture] calendarEvent page {page} error (user {handy_user_id}): {e}")

    return {
        "scheduled": scheduled,
        "visited_effective": visited_effective,
        "visited_not_effective": visited_not_effective,
        "calendar_client_codes": calendar_clients,
        "effective_client_codes": effective_clients,
        "no_sale_reasons": reasons,
        "visit_times": visit_times,
    }


def _get_handy_order_clients(handy_user_id: int, report_date: date) -> set:
    """Get set of client codes with HANDY orders on report_date."""
    if not HANDY_API_TOKEN or not HANDY_BASE_URL:
        return set()

    base = HANDY_BASE_URL.rstrip("/")
    headers = {"Authorization": f"Bearer {HANDY_API_TOKEN}"}

    try:
        r = requests.get(
            f"{base}/api/v2/user/{handy_user_id}/salesOrder?page=1",
            headers=headers, timeout=15,
        )
        total_pages = r.json().get("pagination", {}).get("totalPages", 1)
    except Exception as e:
        print(f"[visit_capture] salesOrder pagination error (user {handy_user_id}): {e}")
        return set()

    clients = set()
    for page in range(total_pages, max(total_pages - 15, 0), -1):
        try:
            r2 = requests.get(
                f"{base}/api/v2/user/{handy_user_id}/salesOrder?page={page}",
                headers=headers, timeout=15,
            )
            found_target = False
            found_older = False
            for o in r2.json().get("salesOrders", []):
                dt_str = o.get("mobileDateCreated", "")
                if not dt_str:
                    continue
                dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00")).astimezone(PANAMA_TZ)
                if dt.date() == report_date:
                    clients.add(o.get("customer", {}).get("code", ""))
                    found_target = True
                elif dt.date() < report_date:
                    found_older = True
            if found_older and not found_target:
                break
        except Exception as e:
            print(f"[visit_capture] salesOrder page {page} error (user {handy_user_id}): {e}")

    return clients


def capture_daily_visit_stats(report_date: date | None = None):
    """Capture visit stats for all routes and save to PostgreSQL.

    Should be called daily before the report (e.g. 16:45 Panama time).
    The calendarEvent API only returns today's data, so report_date
    must be today for calendar data to be available.
    """
    if report_date is None:
        report_date = datetime.now(PANAMA_TZ).date()

    routes = get_routes_for_report()
    print(f"[visit_capture] Capturing visit stats for {report_date}, {len(routes)} routes...")

    session = SessionLocal()
    try:
        for rep in routes:
            salesrep_id = rep["salesrep_id"]
            handy_user_id = rep.get("handy_user_id")
            if not handy_user_id:
                continue

            print(f"[visit_capture] {salesrep_id}...", flush=True)

            # Get calendar visit data (only works for today)
            cal = _get_calendar_visits(handy_user_id)

            # Get HANDY order clients
            order_clients = _get_handy_order_clients(handy_user_id, report_date)

            # Unscheduled sales = orders NOT on the calendar
            unscheduled = order_clients - cal["calendar_client_codes"]

            total_stops = cal["visited_effective"] + cal["visited_not_effective"] + len(unscheduled)
            total_sales = len(order_clients)
            efectividad = round(total_sales / total_stops * 100, 1) if total_stops > 0 else 0

            # Earliest/latest visit times
            visit_times = sorted(cal["visit_times"])
            inicio = visit_times[0].strftime("%H:%M") if visit_times else None
            fin = visit_times[-1].strftime("%H:%M") if visit_times else None

            # Upsert (delete + insert)
            session.query(DailyVisitStats).filter(
                DailyVisitStats.report_date == report_date,
                DailyVisitStats.salesrep_id == salesrep_id,
            ).delete()

            session.add(DailyVisitStats(
                report_date=report_date,
                salesrep_id=salesrep_id,
                scheduled=cal["scheduled"],
                visited_effective=cal["visited_effective"],
                visited_not_effective=cal["visited_not_effective"],
                unscheduled_sales=len(unscheduled),
                total_stops=total_stops,
                total_sales=total_sales,
                efectividad=efectividad,
                inicio=inicio,
                fin=fin,
                no_sale_reasons=json.dumps(cal["no_sale_reasons"], ensure_ascii=False) if cal["no_sale_reasons"] else None,
            ))

            print(f"[visit_capture]   {salesrep_id}: {total_stops} stops, {total_sales} sales, {efectividad}% efectividad")

        session.commit()
        print(f"[visit_capture] Done — {len(routes)} routes saved.")
    except Exception as e:
        session.rollback()
        print(f"[visit_capture] Error: {e}")
        raise
    finally:
        session.close()


def get_visit_stats(report_date: date, salesrep_id: str | None = None) -> list[dict]:
    """Read stored visit stats from PostgreSQL.

    Returns list of dicts with all DailyVisitStats fields.
    """
    session = SessionLocal()
    try:
        q = session.query(DailyVisitStats).filter(DailyVisitStats.report_date == report_date)
        if salesrep_id:
            q = q.filter(DailyVisitStats.salesrep_id == salesrep_id)
        rows = q.all()
        return [
            {
                "report_date": r.report_date,
                "salesrep_id": r.salesrep_id,
                "scheduled": r.scheduled,
                "visited_effective": r.visited_effective,
                "visited_not_effective": r.visited_not_effective,
                "unscheduled_sales": r.unscheduled_sales,
                "total_stops": r.total_stops,
                "total_sales": r.total_sales,
                "efectividad": r.efectividad,
                "inicio": r.inicio,
                "fin": r.fin,
                "no_sale_reasons": json.loads(r.no_sale_reasons) if r.no_sale_reasons else {},
            }
            for r in rows
        ]
    finally:
        session.close()


def get_visit_stats_range(start_date: date, end_date: date) -> list[dict]:
    """Read stored visit stats for a date range."""
    session = SessionLocal()
    try:
        rows = (
            session.query(DailyVisitStats)
            .filter(
                DailyVisitStats.report_date >= start_date,
                DailyVisitStats.report_date <= end_date,
            )
            .order_by(DailyVisitStats.report_date, DailyVisitStats.salesrep_id)
            .all()
        )
        return [
            {
                "report_date": r.report_date,
                "salesrep_id": r.salesrep_id,
                "scheduled": r.scheduled,
                "visited_effective": r.visited_effective,
                "visited_not_effective": r.visited_not_effective,
                "unscheduled_sales": r.unscheduled_sales,
                "total_stops": r.total_stops,
                "total_sales": r.total_sales,
                "efectividad": r.efectividad,
                "inicio": r.inicio,
                "fin": r.fin,
                "no_sale_reasons": json.loads(r.no_sale_reasons) if r.no_sale_reasons else {},
            }
            for r in rows
        ]
    finally:
        session.close()
