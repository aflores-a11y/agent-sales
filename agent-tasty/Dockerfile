FROM python:3.13-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends cron && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml ./
RUN mkdir -p src/agent_tasty && touch src/agent_tasty/__init__.py \
    && pip install --no-cache-dir -e .

COPY src/ src/
COPY scripts/ scripts/
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

RUN echo "0 7 * * * root . /etc/environment && cd /app && /usr/local/bin/python3 scripts/sync_sales_cache.py --days 10 >> /var/log/sync_sales.log 2>&1" \
    > /etc/cron.d/sync-sales \
    && chmod 0644 /etc/cron.d/sync-sales

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["/entrypoint.sh"]
CMD ["python", "-m", "agent_tasty.main"]
