"""
Interactive CLI to review and approve/reject pending match requests.
On approve: sends text message + vCard to patron via Redis.
On reject: sends polite unavailability message to patron.
"""

import json
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import redis

from agent_tasty.config import REDIS_URL
from agent_tasty.db import (
    init_db,
    get_pending_match_requests,
    get_match_request,
    update_match_request,
)

OUTGOING_QUEUE = "queue:outgoing"


def format_vcard(name: str, phone: str) -> str:
    """Generate a vCard string for WhatsApp contact sharing."""
    # Strip @c.us suffix and any non-digit chars for the phone number
    digits = phone.replace("@c.us", "").replace("+", "")
    return (
        "BEGIN:VCARD\n"
        "VERSION:3.0\n"
        f"FN:{name}\n"
        f"TEL;type=CELL;type=VOICE;waid={digits}:+{digits}\n"
        "END:VCARD"
    )


def display_requests(requests: list[dict]):
    if not requests:
        print("\nNo hay solicitudes pendientes.")
        return

    print(f"\n{'='*60}")
    print(f"  SOLICITUDES PENDIENTES ({len(requests)})")
    print(f"{'='*60}")

    for req in requests:
        patron_name = req["patron_profile"].get("name", "Sin nombre")
        patron_location = req["patron_profile"].get("location", "")
        maid_name = req["maid_profile"].get("name", "Sin nombre")
        maid_skills = ", ".join(req["maid_profile"].get("skills", [])) or "N/A"

        print(f"\n  ID: {req['id']}")
        print(f"  Patrón: {patron_name} ({patron_location}) — {req['patron_phone']}")
        print(f"  Candidata: {maid_name} — {req['maid_phone']}")
        print(f"  Habilidades: {maid_skills}")
        print(f"  Creado: {req['created_at']}")
        print(f"  {'-'*50}")


def handle_request(req: dict, r):
    patron_name = req["patron_profile"].get("name", "el patrón")
    maid_name = req["maid_profile"].get("name", "la candidata")
    maid_phone = req["maid_phone"]

    print(f"\n  Patrón: {patron_name} ({req['patron_phone']})")
    print(f"  Perfil patrón: {json.dumps(req['patron_profile'], ensure_ascii=False, indent=4)}")
    print(f"\n  Candidata: {maid_name} ({maid_phone})")
    print(f"  Perfil candidata: {json.dumps(req['maid_profile'], ensure_ascii=False, indent=4)}")

    while True:
        action = input("\n  [a]probar / [r]echazar / [s]altar? ").strip().lower()
        if action in ("a", "aprobar"):
            approve_request(req, maid_name, maid_phone, r)
            break
        elif action in ("r", "rechazar"):
            reject_request(req, maid_name, r)
            break
        elif action in ("s", "saltar"):
            print("  Saltando...")
            break
        else:
            print("  Opción no válida. Use 'a', 'r', o 's'.")


def approve_request(req: dict, maid_name: str, maid_phone: str, r):
    # Send text message to patron
    text_msg = json.dumps({
        "to": req["patron_phone"],
        "body": (
            f"¡Buenas noticias! Le hemos asignado a {maid_name}. "
            f"Compartimos su contacto para que coordinen."
        ),
    })
    r.lpush(OUTGOING_QUEUE, text_msg)

    # Send vCard to patron
    vcard = format_vcard(maid_name, maid_phone)
    vcard_msg = json.dumps({
        "to": req["patron_phone"],
        "body": vcard,
        "delay": 3.0,
    })
    r.lpush(OUTGOING_QUEUE, vcard_msg)

    update_match_request(req["id"], "approved")
    print(f"  APROBADO — Mensaje y vCard enviados a {req['patron_phone']}")


def reject_request(req: dict, maid_name: str, r):
    text_msg = json.dumps({
        "to": req["patron_phone"],
        "body": (
            f"Lamentablemente {maid_name} no está disponible en este momento. "
            f"Le avisaremos cuando tengamos otra candidata que se ajuste."
        ),
    })
    r.lpush(OUTGOING_QUEUE, text_msg)

    update_match_request(req["id"], "rejected")
    print(f"  RECHAZADO — Mensaje enviado a {req['patron_phone']}")


def main():
    init_db()
    r = redis.from_url(REDIS_URL)

    while True:
        requests = get_pending_match_requests()
        display_requests(requests)

        if not requests:
            break

        request_id = input("\nIngrese ID de solicitud (o 'q' para salir): ").strip()
        if request_id.lower() == "q":
            break

        try:
            rid = int(request_id)
        except ValueError:
            print("ID no válido.")
            continue

        req = get_match_request(rid)
        if not req:
            print(f"Solicitud {rid} no encontrada.")
            continue
        if req["status"] != "pending":
            print(f"Solicitud {rid} ya fue procesada ({req['status']}).")
            continue

        handle_request(req, r)

    print("\n¡Hasta luego!")


if __name__ == "__main__":
    main()
