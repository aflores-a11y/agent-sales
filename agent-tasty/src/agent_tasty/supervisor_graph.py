"""LangGraph for supervisor conversational interface.

Every inbound message triggers a fresh report query and returns the WhatsApp
table for today's DT route activity. No session state is persisted.
"""

from datetime import datetime, timezone
from typing import TypedDict

from langgraph.graph import END, START, StateGraph

from agent_tasty.reports import format_whatsapp_table, get_todays_metrics


class SupervisorState(TypedDict):
    user_input: str
    response: str
    phone_number: str
    supervisor_name: str


def supervisor_node(state: SupervisorState) -> SupervisorState:
    """Fetch today's DT metrics and format as WhatsApp table."""
    metrics = get_todays_metrics()
    table = format_whatsapp_table(metrics, datetime.now(timezone.utc))
    return {**state, "response": table}


def build_supervisor_graph():
    g: StateGraph = StateGraph(SupervisorState)
    g.add_node("supervisor", supervisor_node)
    g.add_edge(START, "supervisor")
    g.add_edge("supervisor", END)
    return g.compile()
