import re
import time
from typing import Literal, TypedDict

from langgraph.graph import StateGraph, START, END
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage

from agent_tasty.config import get_llm, SKU_CATALOG, get_sku_catalog
from agent_tasty.prompts import PHASE_PROMPTS
from agent_tasty.extraction import extract_phase_data, stock_data_to_sku_dict
from agent_tasty.mssql import calculate_suggested_order, search_clients, OrderResult

Phase = Literal["check_in", "stock_report"]

# Lookup: product_code -> short_name
_CODE_TO_NAME = {s["code"]: s["short_name"] for s in SKU_CATALOG}


class CurrentVisit(TypedDict, total=False):
    store_name: str
    client_code: str
    stock_by_sku: dict[str, int]       # {product_code: shelf_stock}
    suggested_by_sku: dict[str, int]   # {product_code: suggested_order}


class SalesRepState(TypedDict):
    messages: list
    user_input: str
    response: str
    phase: Phase
    current_visit: CurrentVisit
    visits_today: list[CurrentVisit]
    phone_number: str
    salesrep_name: str
    salesrep_id: str
    pending_matches: list[dict]
    phase_start_msg_count: int


def _conversation_text(messages: list) -> str:
    parts = []
    for m in messages:
        if isinstance(m, HumanMessage):
            parts.append(f"Vendedor: {m.content}")
        elif isinstance(m, AIMessage):
            parts.append(f"Poly: {m.content}")
    return "\n".join(parts)


def _visit_context(state: SalesRepState) -> str:
    visits = state.get("visits_today", [])
    if not visits:
        return "No ha visitado tiendas hoy."
    lines = ["Visitas de hoy:"]
    for v in visits:
        store = v.get("store_name", "?")
        stock = v.get("stock_by_sku", {})
        suggested = v.get("suggested_by_sku", {})
        if stock:
            items = ", ".join(f"{_CODE_TO_NAME.get(c, c)}:{stock[c]}" for c in stock)
            lines.append(f"- {store}: {items}")
        else:
            lines.append(f"- {store}")
    return "\n".join(lines)


def _chat(state: SalesRepState, system_prompt: str) -> str:
    llm = get_llm()
    context = _visit_context(state)
    current_visit = state.get("current_visit", {})
    store_name = current_visit.get("store_name", "")
    client_code = current_visit.get("client_code", "")
    product_list = ", ".join(s["short_name"] for s in get_sku_catalog(client_code))
    prompt = system_prompt.format(
        salesrep_name=state.get("salesrep_name", ""),
        store_name=store_name,
        context=context,
        product_list=product_list,
    )
    phase_start = state.get("phase_start_msg_count", 0)
    recent_messages = state["messages"][phase_start:]
    msgs = [SystemMessage(content=prompt)] + recent_messages + [HumanMessage(content=state["user_input"])]
    for attempt in range(3):
        try:
            result = llm.invoke(msgs)
            return result.content
        except Exception as e:
            if "overloaded" in str(e).lower() or "529" in str(e):
                wait = 10 * (attempt + 1)
                print(f"[chat] API overloaded, retrying in {wait}s (attempt {attempt+1}/3)...")
                time.sleep(wait)
            else:
                raise
    raise RuntimeError("LLM API overloaded after 3 retries")


def _parse_selection(text: str) -> int | None:
    """Try to parse a number selection from user input (e.g. '2', 'la 3', '#1')."""
    m = re.search(r'(\d+)', text.strip())
    if m:
        return int(m.group(1))
    return None


def _format_matches(matches: list[dict]) -> str:
    """Format client matches as a numbered list."""
    lines = []
    for i, m in enumerate(matches, 1):
        lines.append(f"{i}. *{m['name']}* ({m['code']})")
    return "\n".join(lines)


_SEGMENT_BADGE = {
    "Platinum": "🏆 Platino",
    "Gold":     "⭐ Oro",
    "Silver":   "🔵 Plata",
    "Bronze":   "🔸 Bronce",
}


def _format_suggestion_table(stock_by_sku: dict[str, int], order_result: OrderResult) -> str:
    """Format per-SKU stock and suggestion as a readable list."""
    badge = _SEGMENT_BADGE.get(order_result.segment, f"🔸 {order_result.segment}")
    lines = [badge, ""]
    for code in stock_by_sku:
        name = _CODE_TO_NAME.get(code, code)
        stock = stock_by_sku[code]
        if code in order_result.skipped_skus:
            lines.append(f"- {name}: {stock} en anaquel → *0* ⚠ {order_result.skipped_skus[code]}")
        elif code in order_result.canasta_upgrades:
            up = order_result.canasta_upgrades[code]
            lines.append(f"- {name}: {stock} en anaquel → *{up['n_canastas']} {up['short_name']}* ({up['total_units']}u)")
        else:
            suggested = order_result.suggested_by_sku.get(code, 0)
            lines.append(f"- {name}: {stock} en anaquel → *{suggested}*")

    if order_result.total_value_usd > 0:
        lines.append(f"\nTotal pedido: *${order_result.total_value_usd:.2f}*")

    for warning in order_result.warnings:
        lines.append(f"⚠ {warning}")

    return "\n".join(lines)


# --- Graph Nodes ---

def check_in_node(state: SalesRepState) -> dict:
    user_input = state["user_input"]
    pending = state.get("pending_matches", [])
    current_visit = dict(state.get("current_visit", {}))
    salesrep_id = state.get("salesrep_id", "")

    # If we showed options and the rep is selecting one
    if pending:
        selection = _parse_selection(user_input)
        if selection and 1 <= selection <= len(pending):
            picked = pending[selection - 1]
            current_visit["store_name"] = picked["name"]
            current_visit["client_code"] = picked["code"]
            response = f"Perfecto, registrado en *{picked['name']}* ({picked['code']}). Cuantos panes hay en el anaquel de cada producto?"
            new_messages = state["messages"] + [HumanMessage(content=user_input), AIMessage(content=response)]
            return {
                **state,
                "messages": new_messages,
                "response": response,
                "current_visit": current_visit,
                "pending_matches": [],
            }
        else:
            response = f"Elige un numero del 1 al {len(pending)}, o dime el nombre de otra tienda."
            new_messages = state["messages"] + [HumanMessage(content=user_input), AIMessage(content=response)]
            return {**state, "messages": new_messages, "response": response}

    # If input looks like a client code (e.g. DT01521), search directly
    code_match = re.match(r'^([A-Z]{2}\d+)$', user_input.strip(), re.IGNORECASE)
    if code_match:
        search_query = code_match.group(1).upper()
        try:
            matches = search_clients(salesrep_id, search_query)
        except Exception as e:
            print(f"MSSQL search error: {e}")
            matches = []

        if len(matches) == 1:
            picked = matches[0]
            current_visit["store_name"] = picked["name"]
            current_visit["client_code"] = picked["code"]
            response = f"Llegaste a *{picked['name']}* ({picked['code']}). Cuantos panes hay en el anaquel de cada producto?"
            new_messages = state["messages"] + [HumanMessage(content=user_input), AIMessage(content=response)]
            return {
                **state,
                "messages": new_messages,
                "response": response,
                "current_visit": current_visit,
                "pending_matches": [],
            }
        elif len(matches) > 1:
            options = _format_matches(matches[:10])
            response = f"Encontre varios clientes:\n{options}\n\nCual es? Responde con el numero."
            new_messages = state["messages"] + [HumanMessage(content=user_input), AIMessage(content=response)]
            return {
                **state,
                "messages": new_messages,
                "response": response,
                "pending_matches": matches[:10],
            }
        else:
            response = f"No encontre el codigo \"{search_query}\" en tu ruta. Verifica el codigo del cliente."
            new_messages = state["messages"] + [HumanMessage(content=user_input), AIMessage(content=response)]
            return {**state, "messages": new_messages, "response": response}

    # Extract store name from message (name-based search via LLM)
    phase_start = state.get("phase_start_msg_count", 0)
    recent_messages = state["messages"][phase_start:]
    conv_text = _conversation_text(recent_messages + [HumanMessage(content=user_input)])
    extracted = extract_phase_data("check_in", conv_text)
    search_query = extracted.get("store_name", "")

    if not search_query:
        response = _chat(state, PHASE_PROMPTS["check_in"])
        new_messages = state["messages"] + [HumanMessage(content=user_input), AIMessage(content=response)]
        return {**state, "messages": new_messages, "response": response}

    # Search MSSQL for matching clients
    try:
        matches = search_clients(salesrep_id, search_query)
    except Exception as e:
        print(f"MSSQL search error: {e}")
        matches = []

    if len(matches) == 1:
        picked = matches[0]
        current_visit["store_name"] = picked["name"]
        current_visit["client_code"] = picked["code"]
        response = f"Llegaste a *{picked['name']}* ({picked['code']}). Cuantos panes hay en el anaquel de cada producto?"
        new_messages = state["messages"] + [HumanMessage(content=user_input), AIMessage(content=response)]
        return {
            **state,
            "messages": new_messages,
            "response": response,
            "current_visit": current_visit,
            "pending_matches": [],
        }
    elif len(matches) > 1:
        options = _format_matches(matches[:10])
        response = f"Encontre varios clientes:\n{options}\n\nCual es? Responde con el numero."
        new_messages = state["messages"] + [HumanMessage(content=user_input), AIMessage(content=response)]
        return {
            **state,
            "messages": new_messages,
            "response": response,
            "pending_matches": matches[:10],
        }
    else:
        response = f"No encontre \"{search_query}\" en tu ruta. Verifica el nombre o codigo del cliente."
        new_messages = state["messages"] + [HumanMessage(content=user_input), AIMessage(content=response)]
        return {**state, "messages": new_messages, "response": response}


_CLOSED_KEYWORDS = ("cerrada", "cerrado", "closed", "no abrió", "no abrio", "no abre", "estaba cerrada", "estaba cerrado")


def stock_report_node(state: SalesRepState) -> dict:
    user_input_lower = state["user_input"].lower()
    if any(kw in user_input_lower for kw in _CLOSED_KEYWORDS):
        current_visit = dict(state.get("current_visit", {}))
        store_name = current_visit.get("store_name", "la tienda")
        response = f"Entendido, {store_name} estaba cerrada. Escríbeme cuando llegues a la siguiente parada."
        new_messages = state["messages"] + [HumanMessage(content=state["user_input"]), AIMessage(content=response)]
        visits_today = list(state.get("visits_today", []))
        visits_today.append({**current_visit, "closed": True})
        return {
            **state,
            "messages": new_messages,
            "response": response,
            "phase": "check_in",
            "current_visit": CurrentVisit(),
            "visits_today": visits_today,
            "pending_matches": [],
            "phase_start_msg_count": len(new_messages),
        }

    response = _chat(state, PHASE_PROMPTS["stock_report"])
    new_messages = state["messages"] + [HumanMessage(content=state["user_input"]), AIMessage(content=response)]

    phase_start = state.get("phase_start_msg_count", 0)
    recent_messages = new_messages[phase_start:]
    conv_text = _conversation_text(recent_messages)

    current_visit = dict(state.get("current_visit", {}))
    client_code = current_visit.get("client_code", "")
    extracted = extract_phase_data("stock_report", conv_text, client_code=client_code)

    # Merge extracted SKU stock into current visit
    new_stock = stock_data_to_sku_dict(extracted)
    if new_stock:
        existing = dict(current_visit.get("stock_by_sku", {}))
        existing.update(new_stock)
        current_visit["stock_by_sku"] = existing

    return {
        **state,
        "messages": new_messages,
        "response": response,
        "current_visit": current_visit,
    }


# --- Phase Transition ---

def phase_transition(state: SalesRepState) -> dict:
    phase = state["phase"]
    current_visit = state.get("current_visit", {})

    if phase == "check_in":
        if current_visit.get("client_code"):
            return {**state, "phase": "stock_report", "phase_start_msg_count": len(state["messages"])}

    elif phase == "stock_report":
        stock_by_sku = current_visit.get("stock_by_sku", {})
        if stock_by_sku:
            store_name = current_visit.get("store_name", "")
            client_code = current_visit.get("client_code", "")

            # Pad ALL applicable SKUs not reported by salesrep (assume 0 on shelf)
            for sku in get_sku_catalog(client_code):
                if sku["code"] not in stock_by_sku:
                    stock_by_sku[sku["code"]] = 0

            order_result = calculate_suggested_order(client_code, store_name, stock_by_sku)
            current_visit["suggested_by_sku"] = order_result.suggested_by_sku

            table = _format_suggestion_table(stock_by_sku, order_result)
            suggestion_msg = (
                f"Listo! En *{store_name}* ({client_code}):\n"
                f"{table}\n\n"
                f"Registrado. Escribeme cuando llegues a la siguiente parada."
            )
            new_messages = state["messages"] + [AIMessage(content=suggestion_msg)]

            visits_today = list(state.get("visits_today", []))
            visits_today.append(current_visit)

            return {
                **state,
                "messages": new_messages,
                "response": suggestion_msg,
                "phase": "check_in",
                "current_visit": CurrentVisit(),
                "visits_today": visits_today,
                "pending_matches": [],
                "phase_start_msg_count": len(new_messages),
            }

    return state


# --- Router ---

def phase_router(state: SalesRepState) -> str:
    phase = state["phase"]
    return {
        "check_in": "check_in_node",
        "stock_report": "stock_report_node",
    }.get(phase, "check_in_node")


def build_graph():
    graph = StateGraph(SalesRepState)

    graph.add_node("check_in_node", check_in_node)
    graph.add_node("stock_report_node", stock_report_node)
    graph.add_node("phase_transition", phase_transition)

    graph.add_conditional_edges(
        START,
        phase_router,
        {
            "check_in_node": "check_in_node",
            "stock_report_node": "stock_report_node",
        },
    )

    graph.add_edge("check_in_node", "phase_transition")
    graph.add_edge("stock_report_node", "phase_transition")
    graph.add_edge("phase_transition", END)

    return graph.compile()
