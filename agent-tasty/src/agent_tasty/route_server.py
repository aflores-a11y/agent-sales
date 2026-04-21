"""Lightweight HTTP server for route map pages.

Serves optimized route maps as interactive Google Maps pages.
Each route plan gets a unique URL: /route/{route_id}
"""

import json
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse

# In-memory store: route_id -> route data
_ROUTE_STORE: dict[str, dict] = {}

GOOGLE_MAPS_JS_KEY = ""
SERVER_PORT = 8080


def store_route(route_id: str, data: dict):
    """Store a route plan for serving."""
    _ROUTE_STORE[route_id] = data


def _build_html(data: dict) -> str:
    """Build an interactive Google Maps HTML page with numbered stops."""
    waypoints = data.get("waypoints", [])
    route_name = data.get("route_name", "Ruta")
    salesrep = data.get("salesrep", "")
    plan_date = data.get("plan_date", "")
    n_rec = data.get("n_recovery", 0)
    n_ven = data.get("n_overdue", 0)
    n_act = data.get("n_active", 0)

    # Center map on centroid
    avg_lat = sum(w["lat"] for w in waypoints) / len(waypoints) if waypoints else 9.0
    avg_lon = sum(w["lon"] for w in waypoints) / len(waypoints) if waypoints else -79.5

    markers_js = ""
    waypoints_js = ""
    info_items = ""
    for i, w in enumerate(waypoints, 1):
        color = "#CC0000" if w.get("status") == "RECUPERAR" else "#FF8800" if w.get("status") == "VENCIDO" else "#006600"
        nav_url = f"https://www.google.com/maps/dir/?api=1&destination={w['lat']},{w['lon']}&travelmode=driving"

        markers_js += f"""
        new google.maps.Marker({{
            position: {{lat: {w['lat']}, lng: {w['lon']}}},
            map: map,
            label: {{text: '{i}', color: 'white', fontWeight: 'bold', fontSize: '11px'}},
            icon: {{
                path: google.maps.SymbolPath.CIRCLE,
                scale: 14,
                fillColor: '{color}',
                fillOpacity: 1,
                strokeColor: 'white',
                strokeWeight: 2,
            }},
            title: '{i}. {w["name"][:40]}'
        }}).addListener('click', function() {{
            window.open('{nav_url}', '_blank');
        }});
        """

        if i < len(waypoints):
            waypoints_js += f"{{location: {{lat: {w['lat']}, lng: {w['lon']}}}}},\n"

        status_badge = f'<span style="color:{color};font-weight:bold">{w.get("status", "")}</span>'
        ticket = w.get("expected_ticket", 0)
        info_items += f"""
        <div style="display:flex;align-items:center;padding:6px 0;border-bottom:1px solid #eee;font-size:13px">
            <div style="min-width:24px;height:24px;border-radius:50%;background:{color};color:white;display:flex;align-items:center;justify-content:center;font-weight:bold;font-size:11px;margin-right:8px">{i}</div>
            <div style="flex:1">
                <div style="font-weight:500">{w['code']} {w['name'][:35]}</div>
                <div style="font-size:11px;color:#666">{status_badge} · ${ticket:.2f}</div>
            </div>
            <a href="{nav_url}" target="_blank" style="background:#1a73e8;color:white;padding:4px 10px;border-radius:4px;text-decoration:none;font-size:11px;white-space:nowrap">Ir</a>
        </div>
        """

    last = waypoints[-1] if waypoints else {"lat": avg_lat, "lon": avg_lon}

    return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{route_name} - {plan_date}</title>
<style>
    * {{ margin:0; padding:0; box-sizing:border-box; }}
    body {{ font-family: -apple-system, BlinkMacSystemFont, sans-serif; }}
    #map {{ width:100%; height:55vh; }}
    #list {{ padding:8px 12px; max-height:45vh; overflow-y:auto; }}
    .header {{ background:#1a3a6b; color:white; padding:10px 12px; font-size:14px; }}
    .stats {{ display:flex; gap:12px; margin-top:4px; font-size:12px; opacity:0.9; }}
</style>
</head>
<body>
<div class="header">
    <div><strong>{route_name}</strong> — {plan_date}</div>
    <div class="stats">
        <span>{len(waypoints)} clientes</span>
        <span>🟢 {n_act} activos</span>
        <span>🟠 {n_ven} vencidos</span>
        <span>🔴 {n_rec} recuperar</span>
    </div>
</div>
<div id="map"></div>
<div id="list">{info_items}</div>

<script>
function initMap() {{
    const map = new google.maps.Map(document.getElementById('map'), {{
        center: {{lat: {avg_lat}, lng: {avg_lon}}},
        zoom: 13,
        mapTypeControl: false,
        streetViewControl: false,
    }});

    // Markers
    {markers_js}

    // Route line
    const directionsService = new google.maps.DirectionsService();
    const directionsRenderer = new google.maps.DirectionsRenderer({{
        map: map,
        suppressMarkers: true,
        polylineOptions: {{strokeColor: '#1a73e8', strokeWeight: 3, strokeOpacity: 0.7}}
    }});

    directionsService.route({{
        origin: {{lat: {waypoints[0]['lat']}, lng: {waypoints[0]['lon']}}},
        destination: {{lat: {last['lat']}, lng: {last['lon']}}},
        waypoints: [{waypoints_js}],
        travelMode: 'DRIVING',
        optimizeWaypoints: false,
    }}, function(result, status) {{
        if (status === 'OK') directionsRenderer.setDirections(result);
    }});
}}
</script>
<script src="https://maps.googleapis.com/maps/api/js?key={GOOGLE_MAPS_JS_KEY}&callback=initMap" async defer></script>
</body>
</html>"""


class RouteHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = urlparse(self.path).path
        if path.startswith("/route/"):
            route_id = path.split("/route/", 1)[1].strip("/")
            data = _ROUTE_STORE.get(route_id)
            if data:
                html = _build_html(data)
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                self.wfile.write(html.encode())
                return

        self.send_response(404)
        self.end_headers()
        self.wfile.write(b"Not found")

    def log_message(self, format, *args):
        pass  # suppress logs


def start_server(port: int = SERVER_PORT):
    """Start the route map server in a background thread."""
    server = HTTPServer(("0.0.0.0", port), RouteHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    print(f"[route-server] Listening on port {port}")
    return server
