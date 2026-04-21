#!/usr/bin/env python3
"""
Test harness for the maid interview conversation flow.
Bypasses Redis/WhatsApp and drives the graph directly.

Usage:
    python agent-tasty/scripts/test_conversation.py              # scripted run
    python agent-tasty/scripts/test_conversation.py --chat       # interactive chat
    python agent-tasty/scripts/test_conversation.py --interactive # scripted with pauses
"""

import sys
import json
import argparse

# Ensure agent_tasty is importable
sys.path.insert(0, "agent-tasty/src")

from agent_tasty.graph import build_graph, InterviewState, MaidProfile
from agent_tasty.patron_graph import build_patron_graph, PatronInterviewState, PatronProfile

SCRIPT = [
    "Hola buenas tardes",
    "Sí, me interesa mucho",
    "Me llamo María García, tengo 35 años",
    "Soy de Colombia, vine a México buscando mejores oportunidades para mi familia",
    "Trabajé 2 años con la familia Rodríguez en Polanco, cuidaba a dos niños y hacía la limpieza general. Me fui porque se mudaron a otra ciudad.",
    "Antes de eso trabajé año y medio con la señora Martínez en Condesa, cocinaba y limpiaba. Dejé ese trabajo porque me ofrecieron mejor sueldo con los Rodríguez.",
    "Sé cocinar comida mexicana y colombiana, hago limpieza profunda, sé planchar, y tengo experiencia cuidando niños desde bebés hasta 10 años",
    "Prefiero tiempo completo de lunes a viernes, de entrada por salida",
    "Podría empezar la próxima semana",
]


def new_state():
    return InterviewState(
        messages=[],
        user_input="",
        response="",
        phase="intro",
        profile=MaidProfile(),
        phone_number="test_user@c.us",
        declined=False,
        phase_start_msg_count=0,
    )


def print_state(state):
    print(f"  [phase: {state['phase']}]")
    if state["profile"]:
        print(f"  [profile: {json.dumps(state['profile'], ensure_ascii=False)}]")


def run_chat(graph):
    state = new_state()

    print("=" * 60)
    print("INTERACTIVE CHAT — type your messages, Ctrl+C to quit")
    print("Commands: /profile  /phase  /reset  /quit")
    print("=" * 60)

    try:
        while True:
            if state["phase"] == "ended":
                print("\n[Interview ended. Type /reset to start over or /quit to exit]")

            try:
                message = input("\nTú: ").strip()
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
                    print("[Reset — starting fresh interview]")
                    continue
                elif cmd == "/profile":
                    print(json.dumps(state["profile"], ensure_ascii=False, indent=2))
                    continue
                elif cmd == "/phase":
                    print(f"Current phase: {state['phase']}")
                    continue
                else:
                    print(f"Unknown command: {message}")
                    continue

            if state["phase"] == "ended":
                print("[Interview already ended. Use /reset to start over.]")
                continue

            state["user_input"] = message
            state = graph.invoke(state)

            print(f"\nPoly: {state['response']}")
            print_state(state)

    except KeyboardInterrupt:
        print("\n")

    print(f"\n{'=' * 60}")
    print("FINAL PROFILE")
    print("=" * 60)
    print(json.dumps(state["profile"], ensure_ascii=False, indent=2))
    print(f"Final phase: {state['phase']}")


def run_scripted(graph, interactive=False):
    state = new_state()

    print("=" * 60)
    print("MAID INTERVIEW TEST HARNESS")
    print("=" * 60)

    for i, message in enumerate(SCRIPT, 1):
        print(f"\n{'─' * 60}")
        print(f"Step {i}/{len(SCRIPT)}")
        print(f"  Candidata: {message}")

        state["user_input"] = message
        state = graph.invoke(state)

        print(f"  Poly: {state['response']}")
        print_state(state)

        if state["phase"] == "ended":
            print("\n[Interview ended]")
            break

        if interactive:
            input("\nPress Enter to continue...")

    print(f"\n{'=' * 60}")
    print("FINAL PROFILE")
    print("=" * 60)
    print(json.dumps(state["profile"], ensure_ascii=False, indent=2))
    print(f"\nFinal phase: {state['phase']}")
    print(f"Declined: {state.get('declined', False)}")


PATRON_SCRIPT = [
    "Buenas tardes, necesito una empleada para mi casa",
    "Soy Roberto Méndez, vivo en Punta Pacífica",
    "Somos mi esposa y yo, tenemos dos hijos de 4 y 7 años, y un perro labrador",
    "Es un apartamento de 3 recámaras, no hay necesidades especiales",
    "Necesito alguien que cocine, limpie y cuide a los niños cuando no estamos",
    "Prefiero entrada por salida, de lunes a viernes, puede empezar cuando quiera",
]


def new_patron_state():
    return PatronInterviewState(
        messages=[],
        user_input="",
        response="",
        phase="intro",
        profile=PatronProfile(),
        phone_number="test_patron@c.us",
        declined=False,
        phase_start_msg_count=0,
    )


def run_patron_scripted(graph, interactive=False):
    state = new_patron_state()

    print("=" * 60)
    print("PATRON INTERVIEW TEST HARNESS")
    print("=" * 60)

    for i, message in enumerate(PATRON_SCRIPT, 1):
        print(f"\n{'─' * 60}")
        print(f"Step {i}/{len(PATRON_SCRIPT)}")
        print(f"  Patrón: {message}")

        state["user_input"] = message
        state = graph.invoke(state)

        print(f"  Poly: {state['response']}")
        print_state(state)

        if state["phase"] == "ended":
            print("\n[Interview ended]")
            break

        if interactive:
            input("\nPress Enter to continue...")

    print(f"\n{'=' * 60}")
    print("FINAL PATRON PROFILE")
    print("=" * 60)
    print(json.dumps(state["profile"], ensure_ascii=False, indent=2))
    print(f"\nFinal phase: {state['phase']}")
    print(f"Declined: {state.get('declined', False)}")


def run_patron_chat(graph):
    state = new_patron_state()

    print("=" * 60)
    print("PATRON INTERACTIVE CHAT — type your messages, Ctrl+C to quit")
    print("Commands: /profile  /phase  /reset  /quit")
    print("=" * 60)

    try:
        while True:
            if state["phase"] == "ended":
                print("\n[Interview ended. Type /reset to start over or /quit to exit]")

            try:
                message = input("\nTú: ").strip()
            except EOFError:
                break

            if not message:
                continue

            if message.startswith("/"):
                cmd = message.lower()
                if cmd == "/quit":
                    break
                elif cmd == "/reset":
                    state = new_patron_state()
                    print("[Reset — starting fresh patron interview]")
                    continue
                elif cmd == "/profile":
                    print(json.dumps(state["profile"], ensure_ascii=False, indent=2))
                    continue
                elif cmd == "/phase":
                    print(f"Current phase: {state['phase']}")
                    continue
                else:
                    print(f"Unknown command: {message}")
                    continue

            if state["phase"] == "ended":
                print("[Interview already ended. Use /reset to start over.]")
                continue

            state["user_input"] = message
            state = graph.invoke(state)

            print(f"\nPoly: {state['response']}")
            print_state(state)

    except KeyboardInterrupt:
        print("\n")

    print(f"\n{'=' * 60}")
    print("FINAL PATRON PROFILE")
    print("=" * 60)
    print(json.dumps(state["profile"], ensure_ascii=False, indent=2))
    print(f"Final phase: {state['phase']}")


def main():
    parser = argparse.ArgumentParser(description="Test maid interview conversation flow")
    parser.add_argument("--chat", action="store_true", help="Interactive chat mode")
    parser.add_argument("--interactive", action="store_true", help="Scripted with pauses between steps")
    parser.add_argument("--patron", action="store_true", help="Run patron interview instead of maid")
    args = parser.parse_args()

    if args.patron:
        graph = build_patron_graph()
        if args.chat:
            run_patron_chat(graph)
        else:
            run_patron_scripted(graph, interactive=args.interactive)
    else:
        graph = build_graph()
        if args.chat:
            run_chat(graph)
        else:
            run_scripted(graph, interactive=args.interactive)


if __name__ == "__main__":
    main()
