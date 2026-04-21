# Agent Tasty

WhatsApp AI sales agent for route-based product distribution. Built with LangGraph, whatsapp-web.js, and Redis.

## What it does

An AI-powered WhatsApp bot that assists field salesreps during their daily store visits:

1. **Check-in** — Salesrep arrives at a store and reports the store name/code. The bot validates the client and asks for shelf stock.
2. **Stock Report** — Salesrep reports how many of each product are on the shelf. The bot calculates a suggested order based on sales history, RFM segmentation, and coverage targets.
3. **Daily Reports** — Automated PDF reports with route performance, OEE metrics, and visit analytics sent to supervisors via WhatsApp.

## Architecture

```
[WhatsApp Salesreps] <-> [wapp-gateway (Node.js)] <-> [Redis] <-> [agent-tasty (Python/LangGraph)]
                                                                          |
                                                                  [PostgreSQL] + [MSSQL]
```

| Service | Role |
|---|---|
| **agent-tasty** | AI agent — conversation flow, order calculation, reports |
| **wapp-gateway** | WhatsApp Web bridge with QR code web page |
| **PostgreSQL** | Visits, conversations, sales cache, merma data |
| **Redis** | Message queue between WhatsApp and the agent |
| **MSSQL** | Optional external BI database for client master data |

## Quick Start

```bash
# 1. Clone and configure
git clone https://github.com/aflores-a11y/agent-tasty-os.git
cd agent-tasty-os
cp agent-tasty/.env.example agent-tasty/.env     # Edit with your API keys
cp wapp-gateway/.env.example wapp-gateway/.env

# 2. Start all services
docker compose up -d

# 3. Connect WhatsApp
# Open http://localhost:3100/qr and scan the QR code
```

## Configuration

Edit `agent-tasty/src/agent_tasty/config.py` to customize:

- **SKU_CATALOG** — Your products (code, name, price)
- **SALESREP_ROSTER** — Map WhatsApp phone numbers to salesrep names and routes
- **CANASTA_MAPPING** — Bulk/case product bundles
- **SEGMENT_CONFIG** — RFM tier multipliers (Platinum, Gold, Silver, Bronze)
- **COVERAGE_TARGETS** — Minimum suggested order per channel

## Environment Variables

| Variable | Description |
|---|---|
| `LLM_PROVIDER` | `openai` or `anthropic` |
| `LLM_MODEL` | Model name (e.g. `gpt-4o-mini`) |
| `OPENAI_API_KEY` | OpenAI API key |
| `ANTHROPIC_API_KEY` | Anthropic API key (optional fallback) |
| `DATABASE_URL` | PostgreSQL connection string |
| `MSSQL_HOST/PORT/USER/PASSWORD` | External BI database |
| `HANDY_BASE_URL/API_TOKEN` | HANDY CRM integration (optional) |
| `WHISPER_MODEL` | Voice note transcription model size |

See `agent-tasty/.env.example` for the complete list.

## Features

- **LLM with fallback** — Primary + fallback LLM provider (e.g., OpenAI primary, Anthropic fallback)
- **RFM Segmentation** — Automatic client tiering based on Recency, Frequency, Monetary value
- **Suggested Orders** — AI calculates optimal order quantities per product per client
- **Merma Detection** — Flags clients with high spoilage/return rates
- **Voice Notes** — Transcribes salesrep voice messages using Whisper
- **Daily PDF Reports** — Automated supervisor reports with OEE metrics
- **QR Web Auth** — Browser-based WhatsApp QR scanning (works on cloud platforms)
- **Channel Studies** — Diagnostic PDF reports for client analysis

## Deploying to Railway

This project runs well on [Railway](https://railway.com):

1. Create a project with PostgreSQL and Redis plugins
2. Create two services from the repo:
   - **agent-tasty** (root: `/agent-tasty`)
   - **wapp-gateway** (root: `/wapp-gateway`)
3. Set environment variables (Railway auto-provides `DATABASE_URL` and `REDIS_URL`)
4. Open the wapp-gateway public URL `/qr` to scan WhatsApp QR code

## License

MIT
