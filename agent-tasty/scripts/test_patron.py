#!/usr/bin/env python3
"""
Interactive patron chat — ask about available maid profiles.

Connects to PostgreSQL to fetch completed profiles, then lets you chat
with the LLM to browse candidates. No Redis/WhatsApp needed.

Usage:
    python agent-tasty/scripts/test_patron.py
"""

import sys
import json

sys.path.insert(0, "agent-tasty/src")

from langchain_core.messages import SystemMessage, HumanMessage, AIMessage

from agent_tasty.config import get_llm
from agent_tasty.db import init_db, get_completed_profiles


def _format_profiles_for_context(profiles: list[dict]) -> str:
    if not profiles:
        return "No hay perfiles de candidatas completados en la base de datos."

    sections = []
    for i, p in enumerate(profiles, 1):
        prof = p["profile"]
        phone = p["phone_number"]
        lines = [f"Candidata #{i} (tel: {phone})"]
        for key, val in prof.items():
            if val:
                if isinstance(val, list):
                    val = ", ".join(str(v) for v in val)
                lines.append(f"  {key}: {val}")
        sections.append("\n".join(lines))

    return "\n\n".join(sections)


SYSTEM_PROMPT = """\
Eres Poly, la asistente de una agencia de empleo doméstico. Estás hablando con un patrón/empleador que busca contratar personal doméstico.

Tu trabajo:
1. Saludar y preguntar qué tipo de ayuda necesita (limpieza, cocina, niñera, cuidado de adultos mayores, etc.)
2. Entender sus necesidades: horario, modalidad (entrada por salida o planta), zona, presupuesto, requisitos especiales.
3. Buscar en los perfiles disponibles y recomendar las mejores opciones.

PERFILES DISPONIBLES:
{profiles}

REGLAS:
- Habla en español, tono profesional pero cálido.
- Si no hay perfiles disponibles, díselo honestamente y dile que pronto tendremos candidatas.
- Si hay perfiles, recomienda los que mejor coincidan con lo que busca.
- Puedes resumir perfiles sin dar el teléfono directamente — dile que la agencia se encarga de coordinar.
- Si el patrón pide más detalles de una candidata, dáselos del perfil.
- Sé concisa, no repitas información que ya dijiste.
"""


def main():
    init_db()
    llm = get_llm()

    profiles = get_completed_profiles()
    profile_text = _format_profiles_for_context(profiles)

    print("=" * 60)
    print(f"PATRON CHAT — {len(profiles)} perfil(es) disponibles")
    print("Commands: /profiles  /reload  /quit")
    print("=" * 60)

    system_msg = SystemMessage(content=SYSTEM_PROMPT.format(profiles=profile_text))
    messages = [system_msg]

    try:
        while True:
            try:
                user_input = input("\nPatrón: ").strip()
            except EOFError:
                break

            if not user_input:
                continue

            if user_input.startswith("/"):
                cmd = user_input.lower()
                if cmd == "/quit":
                    break
                elif cmd == "/profiles":
                    print(f"\n{profile_text}")
                    continue
                elif cmd == "/reload":
                    profiles = get_completed_profiles()
                    profile_text = _format_profiles_for_context(profiles)
                    messages[0] = SystemMessage(content=SYSTEM_PROMPT.format(profiles=profile_text))
                    print(f"[Reloaded — {len(profiles)} perfil(es)]")
                    continue
                else:
                    print(f"Unknown command: {user_input}")
                    continue

            messages.append(HumanMessage(content=user_input))
            response = llm.invoke(messages)
            messages.append(response)

            print(f"\nPoly: {response.content}")

    except KeyboardInterrupt:
        print("\n")

    print("Hasta luego!")


if __name__ == "__main__":
    main()
