"""Background scheduler for automated tasks."""

import subprocess
import sys
from datetime import date, timedelta

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

# Panama public holidays (fixed dates, updated yearly as needed)
PANAMA_HOLIDAYS = {
    # 2026
    date(2026, 1, 1),    # Año Nuevo
    date(2026, 1, 9),    # Día de los Mártires
    date(2026, 2, 16),   # Lunes de Carnaval
    date(2026, 2, 17),   # Martes de Carnaval
    date(2026, 4, 3),    # Viernes Santo
    date(2026, 5, 1),    # Día del Trabajo
    date(2026, 11, 3),   # Día de la Separación
    date(2026, 11, 5),   # Día de Colón (observed)
    date(2026, 11, 10),  # Primer Grito de Independencia
    date(2026, 11, 28),  # Independencia de España
    date(2026, 12, 8),   # Día de las Madres
    date(2026, 12, 25),  # Navidad
}


def is_panama_business_day(d: date) -> bool:
    """Return True if the date is a Panama business day (not Sunday, not a holiday)."""
    if d.weekday() == 6:  # Sunday
        return False
    if d in PANAMA_HOLIDAYS:
        return False
    return True


def _run_visit_capture():
    """Capture daily visit stats from HANDY calendarEvent API."""
    panama_tz = pytz.timezone("America/Panama")
    from datetime import datetime
    today = datetime.now(panama_tz).date()
    if not is_panama_business_day(today):
        print(f"[scheduler] Skipping visit capture — {today} is not a business day.")
        return
    print("[scheduler] Starting visit stats capture...")
    try:
        from agent_tasty.visit_capture import capture_daily_visit_stats
        capture_daily_visit_stats()
        print("[scheduler] Visit capture complete.")
    except Exception as e:
        print(f"[scheduler] Visit capture failed: {e}")


def _run_daily_report():
    """Generate and send the supervisor compliance report (only on business days)."""
    panama_tz = pytz.timezone("America/Panama")
    from datetime import datetime
    today = datetime.now(panama_tz).date()
    if not is_panama_business_day(today):
        print(f"[scheduler] Skipping daily report — {today} is not a business day.")
        return
    print("[scheduler] Starting daily supervisor report...")
    try:
        from agent_tasty.reports import generate_and_send_report
        generate_and_send_report()
        print("[scheduler] Daily report complete.")
    except Exception as e:
        print(f"[scheduler] Daily report failed: {e}")


def _run_sales_sync(days: int = 10):
    """Run sync_sales_cache.py as a subprocess — syncs last N days from MSSQL to PG."""
    print(f"[scheduler] Starting sales cache sync (--days {days})...")
    try:
        result = subprocess.run(
            [sys.executable, "scripts/sync_sales_cache.py", "--days", str(days)],
            capture_output=True,
            text=True,
            timeout=3600,
        )
        last_line = result.stdout.strip().splitlines()[-1] if result.stdout.strip() else "(no output)"
        print(f"[scheduler] Sales sync complete: {last_line}")
        if result.returncode != 0:
            print(f"[scheduler] Sales sync stderr: {result.stderr[-500:]}")
    except Exception as e:
        print(f"[scheduler] Sales sync failed: {e}")


def start_scheduler() -> BackgroundScheduler:
    """Start background scheduler. Call once from main() before the Redis loop.

    Jobs:
    - Daily at 02:00 Panama time (Mon–Sun): sync MSSQL sales → PG cache
    Runs in a daemon thread — no conflict with the blocking brpop loop.
    """
    panama_tz = pytz.timezone("America/Panama")
    scheduler = BackgroundScheduler(timezone=panama_tz)
    scheduler.add_job(
        _run_sales_sync,
        CronTrigger(hour=2, minute=0, timezone=panama_tz),
        id="sales_sync",
        replace_existing=True,
    )
    scheduler.add_job(
        lambda: _run_sales_sync(days=2),
        CronTrigger(hour=16, minute=30, timezone=panama_tz),
        id="sales_sync_presync",
        replace_existing=True,
    )
    scheduler.add_job(
        _run_visit_capture,
        CronTrigger(hour=16, minute=45, timezone=panama_tz),
        id="visit_capture",
        replace_existing=True,
    )
    scheduler.add_job(
        _run_daily_report,
        CronTrigger(hour=17, minute=0, timezone=panama_tz),
        id="daily_report",
        replace_existing=True,
    )
    scheduler.start()
    print("[scheduler] Scheduler started (sales sync 02:00 PA | pre-sync 16:30 PA | visit capture 16:45 PA | report 17:00 PA)")
    return scheduler
