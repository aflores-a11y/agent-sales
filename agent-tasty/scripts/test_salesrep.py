#!/usr/bin/env python3
"""
Test harness for the salesrep route assistant flow.
Bypasses Redis/WhatsApp and drives the graph directly.

Usage:
    python agent-tasty/scripts/test_salesrep.py              # scripted run
    python agent-tasty/scripts/test_salesrep.py --interactive # scripted with pauses
    python agent-tasty/scripts/test_salesrep.py --chat        # interactive chat
"""

import sys
import json
import argparse

sys.path.insert(0, "agent-tasty/src")

from agent_tasty.graph import build_graph, SalesRepState, CurrentVisit

SCRIPT = [
    "Buenas, llegue al Balboa Market",
    "Hay 12 panes en el anaquel",
    "Estoy en bodega juliana",
    "Quedan 25",
]


def new_state():
    return SalesRepState(
        messages=[],
        user_input="",
        response="",
        phase="check_in",
        current_visit=CurrentVisit(),
        visits_today=[],
        phone_number="test_salesrep@c.us",
        salesrep_name="Carlos Julio",
        salesrep_id="Panama Centro 01",
        pending_matches=[],
        phase_start_msg_count=0,
    )


def print_state(state):
    print(f"  [phase: {state['phase']}]")
    visit = state.get("current_visit", {})
    if visit:
        print(f"  [current_visit: {json.dumps(visit, ensure_ascii=False)}]")
    pending = state.get("pending_matches", [])
    if pending:
        print(f"  [pending_matches: {len(pending)} options]")
    visits = state.get("visits_today", [])
    if visits:
        print(f"  [visits_today: {len(visits)} visits]")
        for v in visits:
            print(f"    - {v.get('store_name')} ({v.get('client_code')}): stock={v.get('shelf_stock')}, sugerencia={v.get('suggested_order')}")


def run_scripted(graph, interactive=False):
    state = new_state()

    print("=" * 60)
    print("SALESREP ROUTE ASSISTANT TEST")
    print("=" * 60)

    for i, message in enumerate(SCRIPT, 1):
        print(f"\n{'─' * 60}")
        print(f"Step {i}/{len(SCRIPT)}")
        print(f"  Vendedor: {message}")

        state["user_input"] = message
        state = graph.invoke(state)

        print(f"  Poly: {state['response']}")
        print_state(state)

        if interactive:
            input("\nPress Enter to continue...")

    print(f"\n{'=' * 60}")
    print("FINAL STATE")
    print("=" * 60)
    print(f"Phase: {state['phase']}")
    print(f"Visits today: {len(state.get('visits_today', []))}")
    for v in state.get("visits_today", []):
        print(f"  - {v.get('store_name')} ({v.get('client_code')}): stock={v.get('shelf_stock')}, sugerencia={v.get('suggested_order')}")


def run_chat(graph):
    state = new_state()

    print("=" * 60)
    print("SALESREP INTERACTIVE CHAT — type your messages, Ctrl+C to quit")
    print("Commands: /visits  /phase  /reset  /quit")
    print("=" * 60)

    try:
        while True:
            try:
                message = input("\nVendedor: ").strip()
            except EOFError:
                break

            if not message:
                continue

            if message.startswith("/"):
                cmd = message.lower()
                if cmd == "/quit":
                    break
                elif cmd == "/reset":
                    state = new_state()
                    print("[Reset — starting fresh]")
                    continue
                elif cmd == "/visits":
                    visits = state.get("visits_today", [])
                    if visits:
                        for v in visits:
                            print(f"  - {v.get('store_name')} ({v.get('client_code')}): stock={v.get('shelf_stock')}, sugerencia={v.get('suggested_order')}")
                    else:
                        print("  No visits yet.")
                    continue
                elif cmd == "/phase":
                    print(f"Current phase: {state['phase']}")
                    continue
                else:
                    print(f"Unknown command: {message}")
                    continue

            state["user_input"] = message
            state = graph.invoke(state)

            print(f"\nPoly: {state['response']}")
            print_state(state)

    except KeyboardInterrupt:
        print("\n")

    print(f"\n{'=' * 60}")
    print("FINAL STATE")
    print("=" * 60)
    print(f"Phase: {state['phase']}")
    print(f"Visits today: {len(state.get('visits_today', []))}")


def main():
    parser = argparse.ArgumentParser(description="Test salesrep route assistant flow")
    parser.add_argument("--chat", action="store_true", help="Interactive chat mode")
    parser.add_argument("--interactive", action="store_true", help="Scripted with pauses between steps")
    args = parser.parse_args()

    graph = build_graph()
    if args.chat:
        run_chat(graph)
    else:
        run_scripted(graph, interactive=args.interactive)


if __name__ == "__main__":
    main()
