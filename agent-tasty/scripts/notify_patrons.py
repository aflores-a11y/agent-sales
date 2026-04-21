"""
Periodic script to send personalized match suggestions to patrons.
Run manually ~every 6h. Uses LLM to compare patron profiles against
completed maid profiles and sends numbered suggestions via WhatsApp.
"""

import json
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import redis
from langchain_core.messages import HumanMessage, SystemMessage

from agent_tasty.config import REDIS_URL, get_llm
from agent_tasty.db import (
    init_db,
    get_completed_patron_profiles,
    get_completed_profiles,
    get_pending_requests_for_patron,
)

OUTGOING_QUEUE = "queue:outgoing"

MATCH_SUGGESTION_SYSTEM = """\
Eres una asistente de una agencia de empleo doméstico. Se te da el perfil de un patrón \
(empleador) y una lista de candidatas disponibles.

Tu tarea: sugiere las 1-3 mejores candidatas para este patrón, explicando brevemente \
por qué cada una encaja. Numera las candidatas. Al final, pregunta si desea que le \
conectemos con alguna.

Responde en español, de forma amable y profesional. Sé conciso.

Perfil del patrón:
{patron_profile}

Candidatas disponibles:
{maid_profiles}
"""


def format_patron_profile(profile: dict) -> str:
    lines = []
    for key, val in profile.items():
        if val:
            lines.append(f"- {key}: {val}")
    return "\n".join(lines) if lines else "Sin información"


def format_maid_profiles(profiles: list[dict]) -> str:
    if not profiles:
        return "No hay candidatas disponibles."
    lines = []
    for i, p in enumerate(profiles, 1):
        prof = p["profile"]
        name = prof.get("name", "Sin nombre")
        nationality = prof.get("nationality", "")
        skills = ", ".join(prof.get("skills", [])) or "No especificadas"
        schedule = prof.get("schedule", "")
        modality = prof.get("live_in_or_out", "")
        lines.append(
            f"{i}. {name} — {nationality}\n"
            f"   Habilidades: {skills}\n"
            f"   Horario: {schedule} | Modalidad: {modality}"
        )
    return "\n".join(lines)


def main():
    init_db()
    r = redis.from_url(REDIS_URL)
    llm = get_llm()

    patron_profiles = get_completed_patron_profiles()
    maid_profiles = get_completed_profiles()

    if not patron_profiles:
        print("No completed patron profiles found.")
        return

    if not maid_profiles:
        print("No completed maid profiles found.")
        return

    print(f"Found {len(patron_profiles)} patron(s) and {len(maid_profiles)} maid(s).")

    sent_count = 0
    skipped_count = 0

    for patron in patron_profiles:
        phone = patron["phone_number"]

        # Skip patrons with pending match requests
        pending = get_pending_requests_for_patron(phone)
        if pending:
            print(f"  Skipping {phone} — has {len(pending)} pending request(s)")
            skipped_count += 1
            continue

        patron_text = format_patron_profile(patron["profile"])
        maids_text = format_maid_profiles(maid_profiles)

        prompt = MATCH_SUGGESTION_SYSTEM.format(
            patron_profile=patron_text,
            maid_profiles=maids_text,
        )

        result = llm.invoke([
            SystemMessage(content=prompt),
            HumanMessage(content="Genera las sugerencias de candidatas para este patrón."),
        ])

        message_body = result.content

        outgoing = json.dumps({"to": phone, "body": message_body})
        r.lpush(OUTGOING_QUEUE, outgoing)
        print(f"  Sent suggestions to {phone}")
        sent_count += 1

    print(f"\nDone. Sent: {sent_count}, Skipped: {skipped_count}")


if __name__ == "__main__":
    main()
