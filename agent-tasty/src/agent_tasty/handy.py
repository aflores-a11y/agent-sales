"""HANDY API client for order history and visit frequency data."""

from datetime import datetime, timedelta, timezone

import requests

from agent_tasty.config import HANDY_API_TOKEN, HANDY_BASE_URL


def _handy_get(endpoint: str, params: dict | None = None) -> dict | None:
    """Make a GET request to the HANDY API v2. Returns None on any failure."""
    if not HANDY_API_TOKEN or not HANDY_BASE_URL:
        return None
    base = HANDY_BASE_URL.rstrip("/")
    url = f"{base}/api/v2/{endpoint.lstrip('/')}"
    try:
        resp = requests.get(
            url,
            params=params,
            headers={"Authorization": f"Bearer {HANDY_API_TOKEN}"},
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None


def get_recent_orders(client_code: str, days_back: int = 90) -> list[dict]:
    """Fetch recent sales orders for a client from HANDY.

    Returns list of {"date": datetime, "total_qty": int}, or [] on failure.
    Paginates up to 10 pages.
    """
    orders = []
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)

    # First request to get total pages
    data = _handy_get(f"customer/{client_code}/salesOrder", {"page": 1})
    if not data:
        return []

    total_pages = data.get("pagination", {}).get("totalPages", 1)

    # Paginate backwards from last page (newest orders are on later pages)
    pages_fetched = 0
    for page in range(total_pages, 0, -1):
        if page != 1:  # page 1 already fetched above but we want newest first
            data = _handy_get(f"customer/{client_code}/salesOrder", {"page": page})
            if not data:
                break

        page_orders = data.get("salesOrders", [])
        if not page_orders:
            break

        hit_cutoff = False
        for order in reversed(page_orders):  # newest first within page
            try:
                date_str = order.get("mobileDateCreated") or order.get("dateCreated") or ""
                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00")) if date_str else None

                if dt and dt < cutoff:
                    hit_cutoff = True
                    break

                items_by_product = {}
                total_qty = 0
                for item in order.get("items", []):
                    if not item.get("isReturn", False):
                        qty = int(item.get("quantity", 0))
                        product_code = item.get("product", {}).get("code", "")
                        if product_code:
                            items_by_product[product_code] = items_by_product.get(product_code, 0) + qty
                        total_qty += qty

                orders.append({"date": dt, "total_qty": total_qty, "items": items_by_product})
            except (ValueError, TypeError):
                continue

        pages_fetched += 1
        if hit_cutoff or pages_fetched >= 10:
            break

    return orders


def get_visit_history(client_code: str, days_back: int = 90) -> list[datetime]:
    """Fetch visit dates for a client from HANDY.

    Returns list of datetime objects, or [] on failure.
    """
    data = _handy_get(f"customer/{client_code}/visit")
    if not data:
        return []

    items = data.get("visits", [])
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)
    visits = []

    for item in items:
        try:
            date_str = item.get("mobileDateCreated") or item.get("dateCreated") or item.get("date") or ""
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            if dt >= cutoff:
                visits.append(dt)
        except (ValueError, TypeError):
            continue

    visits.sort()
    return visits


def compute_visit_interval(visits: list[datetime], default: float = 7.0) -> float:
    """Compute average days between visits. Returns default if insufficient data."""
    if len(visits) < 2:
        return default
    gaps = [(visits[i] - visits[i - 1]).total_seconds() / 86400 for i in range(1, len(visits))]
    avg = sum(gaps) / len(gaps)
    return max(1.0, avg)  # at least 1 day
