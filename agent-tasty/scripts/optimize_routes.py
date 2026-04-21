"""Daily route optimizer: select 31 clients per route, optimize visit order via Google Routes API,
generate Google Maps navigation URL, and send via WhatsApp at 7 AM.

Usage:
    python scripts/optimize_routes.py                  # optimize all test routes
    python scripts/optimize_routes.py --send           # optimize + send WhatsApp
    python scripts/optimize_routes.py --route "Panama Centro 01"  # single route
"""
import sys, os, json, math, argparse
from datetime import date, datetime, timedelta, timezone
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import requests
import redis

from agent_tasty.config import (
    DATABASE_URL, REDIS_URL, FALLBACK_PRICES,
    SEGMENT_CONFIG, SALESREP_ROSTER,
)
from agent_tasty.mssql import get_mssql_connection
from agent_tasty.db import (
    SessionLocal, SalesCacheRow, ClientMermaCache,
    compute_rfm_segment, init_db,
)
from sqlalchemy import func, and_

# ── Config ──────────────────────────────────────────────────────────────────

GOOGLE_ROUTES_API_KEY = os.getenv("GOOGLE_ROUTES_API_KEY", "")
HANDY_BASE_URL = os.getenv("HANDY_BASE_URL", "").rstrip("/")
HANDY_API_TOKEN = os.getenv("HANDY_API_TOKEN", "")

PANAMA_TZ = timezone(timedelta(hours=-5))
TARGET_CLIENTS_PER_DAY = 31
MIN_PROFITABLE_ORDER = 15.15
OUTGOING_QUEUE = "queue:outgoing"

# Optimal visit intervals by RFM tier
INTERVAL_BY_TIER = {"Platinum": 5, "Gold": 7, "Silver": 10, "Bronze": 14}
MULTIPLIER_BY_TIER = {"Platinum": 1.20, "Gold": 1.10, "Silver": 1.00, "Bronze": 0.90}

# Test phase: only Alejandro gets the route
TEST_ROUTES = [
    {
        "phone": "50766718022@c.us",
        "name": "Alejandro Flores",
        "route_clients": "Panama Centro 01",  # use this route's clients
    },
]


# ── Client selection ────────────────────────────────────────────────────────

def get_working_days_remaining(today: date) -> int:
    """Count Mon-Sat remaining in the month including today."""
    days = 0
    d = today
    last_day = today.replace(day=28) + timedelta(days=4)
    last_day = last_day.replace(day=1) - timedelta(days=1)  # last day of month
    while d <= last_day:
        if d.weekday() < 6:  # Mon=0 .. Sat=5
            days += 1
        d += timedelta(days=1)
    return max(days, 1)


def get_route_clients(route_name: str) -> list[dict]:
    """Get all clients for a route from MSSQL."""
    conn = get_mssql_connection()
    try:
        c = conn.cursor()
        c.execute(
            "SELECT Cod_Cliente, Nombre_Cliente FROM BI_CLIENTES WHERE Vendedor = %s",
            (route_name,),
        )
        return [{"code": r[0], "name": r[1]} for r in c.fetchall()]
    finally:
        conn.close()


def get_client_gps(client_code: str) -> tuple[float, float] | None:
    """Fetch lat/lon from Handy API."""
    if not HANDY_BASE_URL or not HANDY_API_TOKEN:
        return None
    try:
        r = requests.get(
            f"{HANDY_BASE_URL}/api/v2/customer/{client_code}",
            headers={"Authorization": f"Bearer {HANDY_API_TOKEN}"},
            timeout=10,
        )
        if r.status_code == 200:
            data = r.json()
            lat, lon = data.get("latitude", 0), data.get("longitude", 0)
            if lat and lon and abs(lat) > 0.1 and abs(lon) > 0.1:
                return (lat, lon)
    except Exception:
        pass
    return None


def get_batch_client_gps(client_codes: list[str]) -> dict[str, tuple[float, float]]:
    """Fetch GPS for multiple clients. Uses Handy customer list API for efficiency."""
    gps = {}
    if not HANDY_BASE_URL or not HANDY_API_TOKEN:
        return gps

    headers = {"Authorization": f"Bearer {HANDY_API_TOKEN}"}
    codes_set = set(client_codes)

    # Paginate through all customers
    page = 1
    while True:
        try:
            r = requests.get(
                f"{HANDY_BASE_URL}/api/v2/customer?page={page}",
                headers=headers, timeout=15,
            )
            if r.status_code != 200:
                break
            data = r.json()
            for c in data.get("customers", []):
                code = c.get("code", "")
                if code in codes_set:
                    lat, lon = c.get("latitude", 0), c.get("longitude", 0)
                    if lat and lon and abs(lat) > 0.1 and abs(lon) > 0.1:
                        gps[code] = (lat, lon)
            tp = data.get("pagination", {}).get("totalPages", 1)
            if page >= tp:
                break
            page += 1
        except Exception as e:
            print(f"[optimizer] GPS fetch error page {page}: {e}")
            break

    return gps


def select_clients(route_name: str, today: date) -> list[dict]:
    """Select 31 clients for today using priority + mandatory 2x/month logic."""
    clients = get_route_clients(route_name)
    if not clients:
        print(f"[optimizer] No clients for route {route_name}")
        return []

    codes = [c["code"] for c in clients]
    code_to_name = {c["code"]: c["name"] for c in clients}

    # Get last sale dates from sales_cache
    with SessionLocal() as session:
        last_sales = dict(
            session.query(SalesCacheRow.client_code, func.max(SalesCacheRow.sale_date))
            .filter(SalesCacheRow.client_code.in_(codes))
            .group_by(SalesCacheRow.client_code)
            .all()
        )

        # Count visits this month (sales days in current month)
        month_start = today.replace(day=1)
        monthly_visits = dict(
            session.query(
                SalesCacheRow.client_code,
                func.count(func.distinct(SalesCacheRow.sale_date)),
            )
            .filter(and_(
                SalesCacheRow.client_code.in_(codes),
                SalesCacheRow.sale_date >= month_start,
                SalesCacheRow.sale_date <= today,
            ))
            .group_by(SalesCacheRow.client_code)
            .all()
        )

    # Get daily sales and merma per client
    scored = []
    for code in codes:
        rfm = compute_rfm_segment(code)
        tier = rfm["tier"]
        interval = INTERVAL_BY_TIER[tier]
        multiplier = MULTIPLIER_BY_TIER[tier]

        # Daily USD from cached sales
        from agent_tasty.db import get_cached_daily_sales
        daily_sales = get_cached_daily_sales(code, days_back=SEGMENT_CONFIG[tier]["lookback_days"])
        daily_usd = sum(qty * FALLBACK_PRICES.get(p, 0) for p, qty in daily_sales.items())

        # Merma
        merma = {}
        try:
            from agent_tasty.db import get_client_merma_rates
            merma = get_client_merma_rates(code)
        except Exception:
            pass
        avg_merma = sum(merma.values()) / len(merma) if merma else 0

        # Last visit
        last_sale = last_sales.get(code)
        days_since = (today - last_sale).days if last_sale else 999

        # Monthly visit count
        month_visits = monthly_visits.get(code, 0)

        # Expected ticket
        expected_ticket = daily_usd * interval * multiplier

        # Priority score
        if daily_usd > 0:
            urgency = days_since / interval if interval > 0 else 0
            priority = urgency * expected_ticket * (1 - avg_merma)
        else:
            priority = 0.1 if days_since > 30 else 0.01

        # Portfolio breadth boost: fewer SKUs = more opportunity
        n_skus = len(daily_sales)
        if 0 < n_skus < 5:
            priority *= 1.15

        # Status
        if daily_usd > 0 and days_since >= 30:
            status = "RECUPERAR"
        elif daily_usd == 0:
            status = "NUEVO"
        elif days_since > interval:
            status = "VENCIDO"
        else:
            status = "Activo"

        scored.append({
            "code": code,
            "name": code_to_name[code],
            "tier": tier,
            "daily_usd": daily_usd,
            "expected_ticket": round(expected_ticket, 2),
            "days_since": days_since,
            "interval": interval,
            "priority": priority,
            "month_visits": month_visits,
            "status": status,
            "avg_merma": avg_merma,
            "profitable": expected_ticket >= MIN_PROFITABLE_ORDER,
        })

    # ── Slot allocation ──
    # Exclude clients visited yesterday or today
    yesterday = today - timedelta(days=1)
    eligible = [s for s in scored if s["days_since"] >= 1]

    # 1. Mandatory: clients needing visits to hit 2x/month
    working_days_left = get_working_days_remaining(today)
    needs_visit = [s for s in eligible if s["month_visits"] < 2]
    must_visit_n = math.ceil(len(needs_visit) / working_days_left)
    must_visit_n = min(must_visit_n, TARGET_CLIENTS_PER_DAY)

    # Sort mandatory by expected ticket descending (best ones first)
    needs_visit.sort(key=lambda x: (-x["expected_ticket"], -x["days_since"]))
    mandatory = needs_visit[:must_visit_n]
    mandatory_codes = {s["code"] for s in mandatory}

    # 2. Priority: fill remaining slots
    remaining = TARGET_CLIENTS_PER_DAY - len(mandatory)
    candidates = [s for s in eligible if s["code"] not in mandatory_codes]

    # Profitable first, then unprofitable
    profitable = [s for s in candidates if s["profitable"]]
    unprofitable = [s for s in candidates if not s["profitable"]]
    profitable.sort(key=lambda x: -x["priority"])
    unprofitable.sort(key=lambda x: -x["priority"])

    priority_picks = profitable[:remaining]
    if len(priority_picks) < remaining:
        priority_picks.extend(unprofitable[:remaining - len(priority_picks)])

    # Combine
    selected = mandatory + priority_picks
    selected = selected[:TARGET_CLIENTS_PER_DAY]

    n_rec = sum(1 for s in selected if s["status"] == "RECUPERAR")
    n_ven = sum(1 for s in selected if s["status"] == "VENCIDO")
    n_act = sum(1 for s in selected if s["status"] == "Activo")
    n_new = sum(1 for s in selected if s["status"] == "NUEVO")
    print(f"[optimizer] {route_name}: {len(selected)} selected "
          f"({len(mandatory)} mandatory, {remaining} priority) "
          f"— {n_act} activos, {n_ven} vencidos, {n_rec} recuperar, {n_new} nuevos")

    return selected


# ── Google Routes API ───────────────────────────────────────────────────────

def _google_optimize_segment(waypoints: list[dict]) -> list[dict]:
    """Optimize a segment of up to 27 waypoints (origin + 25 intermediates + destination)."""
    if len(waypoints) <= 2:
        return waypoints

    origin = waypoints[0]
    destination = waypoints[-1]
    intermediates = waypoints[1:-1]

    body = {
        "origin": {"location": {"latLng": {"latitude": origin["lat"], "longitude": origin["lon"]}}},
        "destination": {"location": {"latLng": {"latitude": destination["lat"], "longitude": destination["lon"]}}},
        "intermediates": [
            {"location": {"latLng": {"latitude": w["lat"], "longitude": w["lon"]}}}
            for w in intermediates
        ],
        "travelMode": "DRIVE",
        "optimizeWaypointOrder": True,
    }

    r = requests.post(
        f"https://routes.googleapis.com/directions/v2:computeRoutes?key={GOOGLE_ROUTES_API_KEY}",
        json=body,
        headers={
            "Content-Type": "application/json",
            "X-Goog-FieldMask": "routes.optimizedIntermediateWaypointIndex,routes.duration,routes.distanceMeters",
        },
        timeout=30,
    )
    if r.status_code != 200:
        raise Exception(f"HTTP {r.status_code}: {r.text[:200]}")

    data = r.json()
    route = data.get("routes", [{}])[0]
    opt_indices = route.get("optimizedIntermediateWaypointIndex", [])
    duration = route.get("duration", "0s")
    distance = route.get("distanceMeters", 0)

    if opt_indices and len(opt_indices) == len(intermediates):
        optimized = [origin]
        for idx in opt_indices:
            optimized.append(intermediates[idx])
        optimized.append(destination)
        return optimized, distance, duration

    return waypoints, distance, duration


def optimize_route_google(waypoints: list[dict]) -> list[dict]:
    """Optimize route via Google Routes API. Splits into segments if >27 waypoints."""
    if len(waypoints) < 2:
        return waypoints

    try:
        if len(waypoints) <= 27:
            # Single call
            optimized, dist, dur = _google_optimize_segment(waypoints)
            print(f"[optimizer] Google Routes: {dist/1000:.1f}km, {dur}")
            return optimized

        # Split: first use local solver for rough order, then optimize each half with Google
        print(f"[optimizer] {len(waypoints)} waypoints > 27, splitting into 2 segments...")
        rough = optimize_route_local(waypoints)
        mid = len(rough) // 2

        seg1, dist1, dur1 = _google_optimize_segment(rough[:mid + 1])
        seg2, dist2, dur2 = _google_optimize_segment(rough[mid:])

        # Combine: seg1 (without last) + seg2 (the midpoint connects them)
        combined = seg1[:-1] + seg2
        total_dist = (dist1 + dist2) / 1000
        print(f"[optimizer] Google Routes (2 segments): {total_dist:.1f}km total")
        return combined

    except Exception as e:
        print(f"[optimizer] Google Routes API error: {e}")

    return waypoints  # fallback


def optimize_route_local(waypoints: list[dict]) -> list[dict]:
    """Fallback: nearest-neighbor + 2-opt using haversine distance."""
    if len(waypoints) <= 2:
        return waypoints

    def haversine(a, b):
        R = 6371
        dlat = math.radians(b["lat"] - a["lat"])
        dlon = math.radians(b["lon"] - a["lon"])
        h = math.sin(dlat/2)**2 + math.cos(math.radians(a["lat"])) * math.cos(math.radians(b["lat"])) * math.sin(dlon/2)**2
        return R * 2 * math.asin(math.sqrt(h))

    # Nearest neighbor
    remaining = list(waypoints)
    route = [remaining.pop(0)]
    while remaining:
        last = route[-1]
        nearest = min(remaining, key=lambda w: haversine(last, w))
        route.append(nearest)
        remaining.remove(nearest)

    # 2-opt improvement
    improved = True
    while improved:
        improved = False
        for i in range(1, len(route) - 2):
            for j in range(i + 1, len(route)):
                d_before = haversine(route[i-1], route[i]) + haversine(route[j-1], route[j])
                d_after = haversine(route[i-1], route[j-1]) + haversine(route[i], route[j])
                if d_after < d_before - 0.01:
                    route[i:j] = reversed(route[i:j])
                    improved = True

    return route


# ── Google Maps URL ─────────────────────────────────────────────────────────

def build_route_map_url(waypoints: list[dict], route_name: str,
                        salesrep: str, plan_date: date, selected: list[dict]) -> str:
    """Store route data and return a URL to the interactive map page."""
    import hashlib
    from agent_tasty.route_server import store_route

    route_id = hashlib.md5(f"{route_name}-{plan_date}".encode()).hexdigest()[:12]

    n_rec = sum(1 for s in selected if s["status"] == "RECUPERAR")
    n_ven = sum(1 for s in selected if s["status"] == "VENCIDO")
    n_act = sum(1 for s in selected if s["status"] in ("Activo", "NUEVO"))

    store_route(route_id, {
        "waypoints": waypoints,
        "route_name": route_name,
        "salesrep": salesrep,
        "plan_date": str(plan_date),
        "n_recovery": n_rec,
        "n_overdue": n_ven,
        "n_active": n_act,
    })

    # Use the server's public URL
    server_host = os.getenv("ROUTE_SERVER_HOST", "localhost:8080")
    return f"http://{server_host}/route/{route_id}"


# ── WhatsApp sender ─────────────────────────────────────────────────────────

def send_route_via_whatsapp(phone: str, name: str, selected: list[dict],
                            map_url: str, route_name: str, today: date):
    """Send optimized route to salesrep via WhatsApp."""
    r = redis.from_url(REDIS_URL)

    n_rec = sum(1 for s in selected if s["status"] == "RECUPERAR")
    n_ven = sum(1 for s in selected if s["status"] == "VENCIDO")
    n_act = sum(1 for s in selected if s["status"] == "Activo")
    n_new = sum(1 for s in selected if s["status"] == "NUEVO")
    total_ticket = sum(s["expected_ticket"] for s in selected)

    body = (
        f"Buenos dias {name}!\n"
        f"Tu ruta optimizada para hoy ({len(selected)} clientes):\n\n"
        f"🗺 {map_url}\n\n"
        f"Recuperar: {n_rec} | Vencidos: {n_ven} | Activos: {n_act} | Nuevos: {n_new}\n"
        f"Venta esperada: ${total_ticket:.2f}"
    )

    payload = json.dumps({"to": phone, "body": body})
    r.lpush(OUTGOING_QUEUE, payload)
    print(f"[optimizer] Route sent to {name} ({phone})")


# ── Main ────────────────────────────────────────────────────────────────────

def optimize_and_send(send: bool = False, route_filter: str | None = None):
    today = datetime.now(PANAMA_TZ).date()
    print(f"[optimizer] Running for {today}")

    routes = TEST_ROUTES
    if route_filter:
        routes = [r for r in routes if route_filter.lower() in r["route_clients"].lower()]

    for route_cfg in routes:
        route_name = route_cfg["route_clients"]
        print(f"\n[optimizer] === {route_name} (test: {route_cfg['name']}) ===")

        # 1. Select clients
        selected = select_clients(route_name, today)
        if not selected:
            continue

        # 2. Fetch GPS
        codes = [s["code"] for s in selected]
        print(f"[optimizer] Fetching GPS for {len(codes)} clients...")
        gps = get_batch_client_gps(codes)

        waypoints = []
        no_gps = []
        for s in selected:
            coords = gps.get(s["code"])
            if coords:
                waypoints.append({
                    "code": s["code"],
                    "name": s["name"],
                    "lat": coords[0],
                    "lon": coords[1],
                    "status": s["status"],
                    "expected_ticket": s["expected_ticket"],
                })
            else:
                no_gps.append(s["code"])

        if no_gps:
            print(f"[optimizer] {len(no_gps)} clients missing GPS: {no_gps[:5]}...")

        if len(waypoints) < 2:
            print(f"[optimizer] Not enough waypoints, skipping")
            continue

        # 3. Optimize route
        print(f"[optimizer] Optimizing {len(waypoints)} waypoints via Google Routes API...")
        optimized = optimize_route_google(waypoints)
        if optimized == waypoints:
            print(f"[optimizer] Falling back to local solver...")
            optimized = optimize_route_local(waypoints)

        # 4. Build map URL
        map_url = build_route_map_url(optimized, route_name, route_cfg["name"], today, selected)
        print(f"[optimizer] Map URL: {map_url}")

        # 5. Print summary
        print(f"\n  {'#':>2s}  {'Code':<10s} {'Client':<35s} {'Status':<10s} {'Ticket':>7s}")
        print(f"  {'-'*70}")
        for i, w in enumerate(optimized, 1):
            print(f"  {i:>2d}  {w['code']:<10s} {w['name'][:35]:<35s} {w['status']:<10s} ${w['expected_ticket']:>6.2f}")

        # 6. Send via WhatsApp
        if send:
            send_route_via_whatsapp(
                route_cfg["phone"], route_cfg["name"],
                selected, map_url, route_name, today,
            )
        else:
            print(f"\n  [DRY RUN] Would send to {route_cfg['name']} ({route_cfg['phone']})")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--send", action="store_true", help="Actually send via WhatsApp")
    parser.add_argument("--route", type=str, help="Filter to specific route")
    args = parser.parse_args()

    from agent_tasty.config import HANDY_BASE_URL as _hbu, HANDY_API_TOKEN as _hat
    HANDY_BASE_URL = _hbu.rstrip("/")
    HANDY_API_TOKEN = _hat

    optimize_and_send(send=args.send, route_filter=args.route)
