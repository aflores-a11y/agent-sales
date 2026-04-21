"""Pydantic models and structured extraction for salesrep flow."""

import warnings
warnings.filterwarnings("ignore", message="Pydantic serializer warnings")

from pydantic import BaseModel, Field
from langchain_core.messages import HumanMessage, SystemMessage

from agent_tasty.config import get_structured_llm, get_sku_catalog


class CheckInData(BaseModel):
    store_name: str | None = Field(None, description="Nombre de la tienda o comercio que visita el vendedor")


class StockReportData(BaseModel):
    familiar: int | None = Field(None, description="Familiar 560")
    integral: int | None = Field(None, description="Integral 560")
    mantequilla: int | None = Field(None, description="Mantequilla 560")
    pasitas: int | None = Field(None, description="Pan Pasitas")
    bolita_huevo: int | None = Field(None, description="Bolita de Huevo 8u")
    bolita_pasitas: int | None = Field(None, description="Bolita Pasitas 8u")
    hamburguesa: int | None = Field(None, description="Hamburguesa S/A 8u")
    hot_dog: int | None = Field(None, description="Hot Dog S/A 8u")
    sandwich: int | None = Field(None, description="Jumbo Sandwich Plain 1040")
    # DT-only (null for non-DT; excluded from prompt for non-DT clients)
    burger_ca_12u:   int | None = Field(None, description="J Burger C/A 12u")
    hotdog_sa_12u:   int | None = Field(None, description="J HotDog S/A 12u")
    michita_10u:     int | None = Field(None, description="PAN MICHITA 10 UND")
    burger_sa_12u:   int | None = Field(None, description="J Burger S/A 12u")
    hotdog_ca_12u:   int | None = Field(None, description="J HotDog C/A 12u")


# Map StockReportData field names to product codes
_FIELD_TO_CODE = {
    "familiar":      "PT00005",
    "integral":      "PT00009",
    "mantequilla":   "PT00013",
    "pasitas":       "PT00019",
    "bolita_huevo":  "PT00001",
    "bolita_pasitas": "PT00003",
    "hamburguesa":   "PT00006",
    "hot_dog":       "PT00007",
    "sandwich":      "PT00012",
    "burger_ca_12u": "PT00010",
    "hotdog_sa_12u": "PT00011",
    "michita_10u":   "PT00016",
    "burger_sa_12u": "PT00031",
    "hotdog_ca_12u": "PT00077",
}


EXTRACTION_SYSTEM = (
    "Extrae la informacion estructurada del mensaje del vendedor. "
    "Solo extrae lo que este claramente mencionado. Deja como null lo que no se haya dicho. "
    "Responde en espanol."
)

PHASE_MODELS = {
    "check_in": CheckInData,
    "stock_report": StockReportData,
}


def extract_phase_data(phase: str, conversation_text: str, client_code: str = "") -> dict:
    """Extract structured data from conversation using with_structured_output."""
    model_cls = PHASE_MODELS.get(phase)
    if not model_cls:
        return {}

    structured_llm = get_structured_llm(model_cls)

    if phase == "stock_report":
        sku_names = ", ".join(s["short_name"] for s in get_sku_catalog(client_code))
        system = (
            "Extrae las cantidades de productos en anaquel reportadas por el vendedor. "
            f"Los productos validos son: {sku_names}. "
            "El vendedor puede usar nombres cortos o variaciones (ej: 'Pasita' = Pasitas, 'Bolita' = Bolita Huevo, 'HD' = Hot Dog). "
            "Solo extrae lo que este claramente mencionado. Deja como null lo que no se haya dicho."
        )
    else:
        system = EXTRACTION_SYSTEM

    result = structured_llm.invoke([
        SystemMessage(content=system),
        HumanMessage(content=f"Conversacion:\n{conversation_text}"),
    ])

    return result.model_dump(exclude_none=True)


def stock_data_to_sku_dict(extracted: dict) -> dict[str, int]:
    """Convert extracted StockReportData fields to {product_code: qty} dict."""
    result = {}
    for field_name, code in _FIELD_TO_CODE.items():
        val = extracted.get(field_name)
        if val is not None:
            result[code] = val
    return result
