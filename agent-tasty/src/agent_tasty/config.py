import os
from dotenv import load_dotenv

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "openai")
LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4o")
FALLBACK_PROVIDER = os.getenv("FALLBACK_PROVIDER", "anthropic")
FALLBACK_MODEL = os.getenv("FALLBACK_MODEL", "claude-haiku-4-5-20251001")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://poly:poly_dev@localhost:5432/agencia_poly")

# MSSQL (BI_JUMBO) connection
MSSQL_HOST = os.getenv("MSSQL_HOST", "localhost")
MSSQL_PORT = int(os.getenv("MSSQL_PORT", "1433"))
MSSQL_USER = os.getenv("MSSQL_USER", "")
MSSQL_PASSWORD = os.getenv("MSSQL_PASSWORD", "")
MSSQL_DATABASE = os.getenv("MSSQL_DATABASE", "BI_JUMBO")

# HANDY API
HANDY_BASE_URL = os.getenv("HANDY_BASE_URL", "")
HANDY_API_TOKEN = os.getenv("HANDY_API_TOKEN", "")

# Core product SKUs tracked for stock reports.
# Customize this list with your own products, codes, and prices.
SKU_CATALOG = [
    # Example products — replace with your own
    {"code": "SKU001", "short_name": "Product A",     "full_name": "Product A 500g",    "price_usd": 1.50},
    {"code": "SKU002", "short_name": "Product B",     "full_name": "Product B 500g",    "price_usd": 2.00},
    {"code": "SKU003", "short_name": "Product C",     "full_name": "Product C 250g",    "price_usd": 1.00},
    {"code": "SKU004", "short_name": "Product D",     "full_name": "Product D 1kg",     "price_usd": 3.00},

    # Channel-specific products (dt_only: True = only for DT-prefixed clients)
    {"code": "SKU010", "short_name": "Bulk Product X", "full_name": "Bulk Product X 12u", "price_usd": 5.00, "dt_only": True},
]


def get_sku_catalog(client_code: str = "") -> list[dict]:
    """Return applicable SKUs. DT-only SKUs excluded for non-DT/UT clients."""
    if client_code.startswith("DT") or client_code.startswith("UT"):
        return SKU_CATALOG
    return [s for s in SKU_CATALOG if not s.get("dt_only")]


# Maps unit SKU -> bulk/case equivalent.
# case_size: units per case. Upgrade triggers when suggested >= case_size - 2.
# Customize with your own product bundles.
CANASTA_MAPPING = {
    # "SKU001": {"canasta_code": "SKU001-CASE", "case_size": 12, "short_name": "Case Product A", "price_usd": 15.00},
}

# Fallback prices from SKU_CATALOG (used when HANA prices unavailable)
FALLBACK_PRICES = {s["code"]: s["price_usd"] for s in SKU_CATALOG}

# Month abbreviations used in BI_ANALISIS_VENTAS
MONTH_ABBREV = {1:"ENE",2:"FEB",3:"MAR",4:"ABR",5:"MAY",6:"JUN",7:"JUL",8:"AGO",9:"SEP",10:"OCT",11:"NOV",12:"DIC"}

# Report hour in Panama time
REPORT_HOUR_PANAMA = 17

# Salesrep roster — maps WhatsApp phone/lid to salesrep identity and route.
# Customize with your own salesreps. Each entry needs at minimum: phone, name, salesrep_id.
# - phone: WhatsApp number in format "COUNTRYCODE+NUMBER@c.us"
# - lid: WhatsApp linked device ID (alternative identifier)
# - salesrep_id: route name matching your BI system's Vendedor field, or "TEST"
# - handy_user_id: (optional) HANDY CRM user ID for visit/order data
# - supervisor: (optional) True for users who receive daily reports
SALESREP_ROSTER = [
    {"phone": "1234567890@c.us", "lid": "000000000000@lid", "name": "Test User",     "salesrep_id": "TEST", "supervisor": True},
    {"phone": "1234567891@c.us", "lid": "000000000001@lid", "name": "Sales Rep 1",   "salesrep_id": "Route 01", "handy_user_id": 10001},
    {"phone": "1234567892@c.us", "lid": "000000000002@lid", "name": "Sales Rep 2",   "salesrep_id": "Route 02", "handy_user_id": 10002},
]


SEGMENT_CONFIG = {
    "Platinum": {"multiplier": 1.20, "lookback_days": 90,  "min_floor": 2},
    "Gold":     {"multiplier": 1.10, "lookback_days": 180, "min_floor": 1},
    "Silver":   {"multiplier": 1.00, "lookback_days": 365, "min_floor": 0},
    "Bronze":   {"multiplier": 0.90, "lookback_days": 365, "min_floor": 0},
}

UT_SEGMENT_CONFIG = {
    "Platinum": {"multiplier": 1.30, "lookback_days": 90,  "min_floor": 2},
    "Gold":     {"multiplier": 1.20, "lookback_days": 180, "min_floor": 1},
    "Silver":   {"multiplier": 1.10, "lookback_days": 365, "min_floor": 0},
    "Bronze":   {"multiplier": 1.00, "lookback_days": 365, "min_floor": 0},
}

# Coverage targets for wholesale (UT) channel.
# Applied when: client has no history/stock for a SKU. Values = minimum units to suggest.
UT_COVERAGE_TARGETS = {
    # "SKU001": 10,  # Suggest 10 units of Product A for new UT clients
}

# Coverage targets for retail (DT) channel.
# Applied when: client has no history/stock for a SKU. Values = minimum units to suggest.
DT_COVERAGE_TARGETS = {
    # "SKU001": 1,  # Suggest 1 unit of Product A for new DT clients
}


def get_salesrep(sender: str) -> dict | None:
    for rep in SALESREP_ROSTER:
        if rep["phone"] == sender or rep.get("lid") == sender:
            return rep
    return None


def get_supervisor(sender: str) -> dict | None:
    for rep in SALESREP_ROSTER:
        if rep.get("supervisor") and (rep["phone"] == sender or rep.get("lid") == sender):
            return rep
    return None


def get_supervisors() -> list[dict]:
    return [rep for rep in SALESREP_ROSTER if rep.get("supervisor")]


def get_routes_for_report() -> list[dict]:
    """Return active salesrep entries that have a handy_user_id and are not supervisors/TEST."""
    return [
        rep for rep in SALESREP_ROSTER
        if rep.get("handy_user_id") and not rep.get("supervisor") and rep.get("salesrep_id") != "TEST"
    ]


def _build_llm(provider: str, model: str):
    if provider == "anthropic":
        from langchain_anthropic import ChatAnthropic
        return ChatAnthropic(model=model, api_key=ANTHROPIC_API_KEY)
    else:
        from langchain_openai import ChatOpenAI
        return ChatOpenAI(model=model, api_key=OPENAI_API_KEY)


def _has_key(provider: str) -> bool:
    return bool(ANTHROPIC_API_KEY) if provider == "anthropic" else bool(OPENAI_API_KEY)


def get_llm():
    primary = _build_llm(LLM_PROVIDER, LLM_MODEL)
    if FALLBACK_PROVIDER and FALLBACK_PROVIDER != LLM_PROVIDER and _has_key(FALLBACK_PROVIDER):
        fallback = _build_llm(FALLBACK_PROVIDER, FALLBACK_MODEL)
        return primary.with_fallbacks([fallback])
    return primary


def get_structured_llm(model_cls):
    primary = _build_llm(LLM_PROVIDER, LLM_MODEL).with_structured_output(model_cls)
    if FALLBACK_PROVIDER and FALLBACK_PROVIDER != LLM_PROVIDER and _has_key(FALLBACK_PROVIDER):
        fallback = _build_llm(FALLBACK_PROVIDER, FALLBACK_MODEL).with_structured_output(model_cls)
        return primary.with_fallbacks([fallback])
    return primary
