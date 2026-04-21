import json
import threading
import redis
from langchain_core.messages import HumanMessage, AIMessage

from agent_tasty.config import REDIS_URL, get_salesrep, get_supervisor, SKU_CATALOG
from agent_tasty.graph import build_graph, SalesRepState, CurrentVisit
from agent_tasty.db import init_db, save_message, save_visit
from agent_tasty.scheduler import start_scheduler

_CODE_TO_NAME = {s["code"]: s["short_name"] for s in SKU_CATALOG}

INCOMING_QUEUE = "queue:incoming"
OUTGOING_QUEUE = "queue:outgoing"

conversations: dict[str, SalesRepState] = {}


def _new_state(phone: str, rep: dict) -> SalesRepState:
    return SalesRepState(
        messages=[],
        user_input="",
        response="",
        phase="check_in",
        current_visit=CurrentVisit(),
        visits_today=[],
        phone_number=phone,
        salesrep_name=rep["name"],
        salesrep_id=rep["salesrep_id"],
        pending_matches=[],
        phase_start_msg_count=0,
    )


SUPERVISOR_HELP = (
    "Comandos disponibles:\n"
    "• *REPORTE GENERAL* — reporte de todas las rutas\n"
    "• *REPORTE [ruta]* — reporte de una ruta específica\n"
    "  Ej: _REPORTE Panama Centro 01_"
)


def _run_report_async(r: redis.Redis, sender: str, route_filter: str | None):
    """Run generate_and_send_report in a daemon thread so it doesn't block the Redis loop."""
    def _target():
        try:
            from agent_tasty.reports import generate_and_send_report
            generate_and_send_report(route_filter=route_filter)
        except Exception as e:
            r.lpush(OUTGOING_QUEUE, json.dumps({"to": sender, "body": f"Error generando reporte: {e}"}))
            print(f"[reports] Async report error: {e}")
    t = threading.Thread(target=_target, daemon=True)
    t.start()


def _handle_supervisor_message(r: redis.Redis, sender: str, body: str) -> bool:
    """Route supervisor WhatsApp commands to report generation.

    Returns True if message was handled, False to fall through to salesrep flow.
    """
    cmd = body.strip().upper()

    def reply(text: str):
        r.lpush(OUTGOING_QUEUE, json.dumps({"to": sender, "body": text}))

    if cmd == "REPORTE GENERAL":
        reply("Generando reporte general, un momento...")
        _run_report_async(r, sender, route_filter=None)
        return True

    elif cmd.startswith("REPORTE "):
        route_filter = body.strip()[8:].strip()
        reply(f"Generando reporte para _{route_filter}_...")
        _run_report_async(r, sender, route_filter=route_filter)
        return True

    else:
        return False  # not a supervisor command — fall through to salesrep flow


def main():
    init_db()
    r = redis.from_url(REDIS_URL)
    graph = build_graph()
    start_scheduler()

    # Start route map server (serves optimized route pages on port 8080)
    from agent_tasty.route_server import start_server
    start_server()

    print("agent-tasty (salesrep) running, waiting for messages...")

    while True:
        result = r.brpop(INCOMING_QUEUE, timeout=0)
        if result is None:
            continue

        raw = result[1]
        if isinstance(raw, bytes):
            raw = raw.decode()
        message = json.loads(raw)

        sender = message["from"]
        body = message.get("body", "")

        if not body:
            continue

        print(f"Incoming from {sender}: {body}")

        # Supervisor commands — if recognized, handle and skip salesrep flow
        sup = get_supervisor(sender)
        if sup and _handle_supervisor_message(r, sender, body):
            continue

        rep = get_salesrep(sender)
        if not rep:
            print(f"Unknown sender {sender}, ignoring.")
            continue

        if sender not in conversations:
            conversations[sender] = _new_state(sender, rep)

        state = conversations[sender]
        state["user_input"] = body

        try:
            result_state = graph.invoke(state)
        except Exception as e:
            print(f"[main] Error processing message from {sender}: {e}")
            r.lpush(OUTGOING_QUEUE, json.dumps({"to": sender, "body": "Lo siento, ocurrió un error. Por favor intenta de nuevo."}))
            continue
        conversations[sender] = result_state
        response_body = result_state["response"]

        save_message(sender, "human", body, result_state["phase"])
        save_message(sender, "ai", response_body, result_state["phase"])

        # If a visit was just completed (phase looped back to check_in with new visits)
        visits = result_state.get("visits_today", [])
        prev_visits = state.get("visits_today", [])
        if len(visits) > len(prev_visits):
            latest = visits[-1]
            save_visit(
                sender,
                latest.get("store_name", ""),
                latest.get("client_code", ""),
                latest.get("stock_by_sku", {}),
                latest.get("suggested_by_sku", {}),
                sku_names=_CODE_TO_NAME,
            )

        outgoing = json.dumps({"to": sender, "body": response_body})
        r.lpush(OUTGOING_QUEUE, outgoing)
        print(f"[{result_state['phase']}] Outgoing to {sender}: {response_body[:100]}...")


if __name__ == "__main__":
    main()
