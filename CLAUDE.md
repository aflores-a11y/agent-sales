# agent-tasty

Two-service WhatsApp AI agent for route-based sales and distribution.

## Architecture

```
[WhatsApp Salesreps] <-> [wapp-gateway (Node.js)] <-> [Redis Queues] <-> [agent-tasty (Python)]
                                                                              |
                                                                    [PostgreSQL] + [MSSQL (optional)]
```

- **wapp-gateway**: WhatsApp Web client using whatsapp-web.js. Reads/sends messages, bridges to Redis. Includes QR code web page for easy auth.
- **agent-tasty**: Python AI agent using LangGraph. Guides salesreps through check-in and stock reporting at merchant stores.
- **Redis**: Message broker via two lists — `queue:incoming` and `queue:outgoing`.
- **PostgreSQL**: Stores salesrep visits, conversation logs, sales cache, and merma data.
- **MSSQL**: Optional external BI database for client master data.

## Salesrep Flow

1. **check_in** — Salesrep arrives at store, reports store name/code. Bot validates against client database, confirms client, asks for shelf stock.
2. **stock_report** — Salesrep reports shelf stock per product. Bot calculates suggested order using RFM segmentation, logs the visit. Phase resets to check_in.

## Setup

```bash
cp agent-tasty/.env.example agent-tasty/.env    # Edit with your credentials
cp wapp-gateway/.env.example wapp-gateway/.env  # Edit Redis URL if needed
docker compose up -d
```

Open http://localhost:3100/qr to scan WhatsApp QR code.

## Configuration

All business-specific config is in `agent-tasty/src/agent_tasty/config.py`:
- **SKU_CATALOG** — Your product list with codes, names, and prices
- **SALESREP_ROSTER** — Phone numbers mapped to salesrep names and routes
- **CANASTA_MAPPING** — Bulk/case product bundles
- **SEGMENT_CONFIG** — RFM tier multipliers for order suggestions
- **COVERAGE_TARGETS** — Minimum order floors per channel

## Environment Variables

See `agent-tasty/.env.example` for full list.

## Project Structure

```
agent-tasty/
  src/agent_tasty/
    config.py        — Products, salesreps, segments, LLM factory
    graph.py         — LangGraph StateGraph: check_in -> stock_report loop
    extraction.py    — Pydantic models + structured output extraction
    prompts.py       — System prompts per phase (Spanish by default)
    mssql.py         — MSSQL connection, client search, order calculation
    db.py            — SQLAlchemy models, RFM segmentation
    main.py          — Redis consumer, salesrep routing, scheduler
    reports.py       — PDF report generation (daily, with OEE metrics)
  scripts/
    sync_sales_cache.py  — Sync sales data from MSSQL to PostgreSQL
    dt_channel_study.py  — Channel diagnostic study (PDF)

wapp-gateway/
  src/index.js       — WhatsApp bridge with QR web page
```
