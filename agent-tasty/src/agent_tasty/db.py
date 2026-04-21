from datetime import datetime, timedelta, timezone, date
from zoneinfo import ZoneInfo

PANAMA_TZ = ZoneInfo("America/Panama")  # UTC-5, no DST

from sqlalchemy import (
    Column, Integer, Float, Text, Date, DateTime, ForeignKey, Index,
    UniqueConstraint, create_engine, func,
)
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

from agent_tasty.config import DATABASE_URL

Base = declarative_base()


class SalesRepVisitRow(Base):
    __tablename__ = "salesrep_visits"

    id = Column(Integer, primary_key=True)
    phone_number = Column(Text, nullable=False, index=True)
    store_name = Column(Text)
    client_code = Column(Text, nullable=True)
    visit_date = Column(DateTime(timezone=True), server_default=func.now())
    items = relationship("SalesRepVisitItem", back_populates="visit")


class SalesRepVisitItem(Base):
    __tablename__ = "salesrep_visit_items"

    id = Column(Integer, primary_key=True)
    visit_id = Column(Integer, ForeignKey("salesrep_visits.id"), nullable=False)
    product_code = Column(Text, nullable=False)
    product_name = Column(Text)
    shelf_stock = Column(Integer, nullable=True)
    suggested_order = Column(Integer, nullable=True)
    visit = relationship("SalesRepVisitRow", back_populates="items")


class ConversationMessage(Base):
    __tablename__ = "conversation_messages"

    id = Column(Integer, primary_key=True)
    phone_number = Column(Text, nullable=False, index=True)
    role = Column(Text, nullable=False)  # "human" | "ai"
    content = Column(Text, nullable=False)
    phase = Column(Text)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class SalesCacheRow(Base):
    """Local cache of daily sales data from MSSQL BI_ANALISIS_VENTAS (SAP HANA)."""
    __tablename__ = "sales_cache"

    id = Column(Integer, primary_key=True)
    client_code = Column(Text, nullable=False)
    product_code = Column(Text, nullable=False)
    sale_date = Column(Date, nullable=False)
    quantity = Column(Float, nullable=False, default=0)   # Cantidad_NETA (net after returns)
    qty_fac = Column(Float, nullable=True, default=0)     # Cantidad_FAC (gross delivered)
    qty_returned = Column(Float, nullable=True, default=0) # Cantidad_NDC (returns/credits)

    __table_args__ = (
        Index("ix_sales_cache_client_product", "client_code", "product_code"),
        Index("ix_sales_cache_date", "sale_date"),
    )


class ProductPriceCache(Base):
    """Cached unit prices per SKU from SAP HANA."""
    __tablename__ = "product_price_cache"

    id = Column(Integer, primary_key=True)
    product_code = Column(Text, nullable=False, unique=True, index=True)
    unit_price_usd = Column(Float, nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now())


class ClientMermaCache(Base):
    """Cached merma (spoilage) rate per client+product from SAP HANA."""
    __tablename__ = "client_merma_cache"

    id = Column(Integer, primary_key=True)
    client_code = Column(Text, nullable=False)
    product_code = Column(Text, nullable=False)
    total_sold = Column(Float, nullable=False, default=0)
    total_returned = Column(Float, nullable=False, default=0)
    merma_rate = Column(Float, nullable=False, default=0)

    __table_args__ = (
        Index("ix_merma_client_product", "client_code", "product_code"),
        UniqueConstraint("client_code", "product_code", name="uq_merma_client_product"),
    )


class DailyVisitStats(Base):
    """Daily visit statistics per route, captured from HANDY calendarEvent API."""
    __tablename__ = "daily_visit_stats"

    id = Column(Integer, primary_key=True)
    report_date = Column(Date, nullable=False)
    salesrep_id = Column(Text, nullable=False)          # e.g. "Panama Centro 01"
    scheduled = Column(Integer, nullable=False, default=0)           # total calendar events
    visited_effective = Column(Integer, nullable=False, default=0)   # visited + sale
    visited_not_effective = Column(Integer, nullable=False, default=0)  # visited, no sale
    unscheduled_sales = Column(Integer, nullable=False, default=0)   # HANDY orders not on calendar
    total_stops = Column(Integer, nullable=False, default=0)         # effective + not_effective + unscheduled
    total_sales = Column(Integer, nullable=False, default=0)         # HANDY orders
    efectividad = Column(Float, nullable=False, default=0)           # total_sales / total_stops
    inicio = Column(Text, nullable=True)                             # earliest visit time "HH:MM"
    fin = Column(Text, nullable=True)                                # latest visit time "HH:MM"
    no_sale_reasons = Column(Text, nullable=True)                    # JSON: {"reason": count}

    __table_args__ = (
        UniqueConstraint("report_date", "salesrep_id", name="uq_visit_stats_date_route"),
        Index("ix_visit_stats_date", "report_date"),
    )


engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)


def init_db():
    Base.metadata.create_all(engine)
    # Add new columns to existing tables if they don't exist (forward migration)
    from sqlalchemy import text
    with engine.connect() as conn:
        for col, typedef in [("qty_fac", "FLOAT DEFAULT 0"), ("qty_returned", "FLOAT DEFAULT 0")]:
            try:
                conn.execute(text(f"ALTER TABLE sales_cache ADD COLUMN {col} {typedef}"))
                conn.commit()
            except Exception:
                pass  # Column already exists


def compute_rfm_segment(client_code: str) -> dict:
    """Compute RFM tier for a client using local sales_cache.

    Returns {"tier": "Gold", "r": 3, "f": 2, "m": 3, "composite": 8}
    """
    cutoff_365 = date.today() - timedelta(days=365)
    with SessionLocal() as session:
        from sqlalchemy import and_
        row = (
            session.query(
                func.max(SalesCacheRow.sale_date).label("last_sale"),
                func.count(func.distinct(SalesCacheRow.sale_date)).label("distinct_dates"),
                func.sum(SalesCacheRow.quantity).label("total_qty"),
            )
            .filter(and_(
                SalesCacheRow.client_code == client_code,
                SalesCacheRow.sale_date >= cutoff_365,
                SalesCacheRow.quantity > 0,
            ))
            .one()
        )

    last_sale = row.last_sale
    distinct_dates = row.distinct_dates or 0
    total_qty = float(row.total_qty or 0)

    if last_sale is None:
        return {"tier": "Bronze", "r": 1, "f": 1, "m": 1, "composite": 3}

    recency_days = (date.today() - last_sale).days
    if recency_days <= 14:
        r = 4
    elif recency_days <= 30:
        r = 3
    elif recency_days <= 90:
        r = 2
    else:
        r = 1

    if distinct_dates >= 24:
        f = 4
    elif distinct_dates >= 12:
        f = 3
    elif distinct_dates >= 4:
        f = 2
    else:
        f = 1

    if total_qty >= 500:
        m = 4
    elif total_qty >= 200:
        m = 3
    elif total_qty >= 50:
        m = 2
    else:
        m = 1

    composite = r + f + m
    if composite >= 10:
        tier = "Platinum"
    elif composite >= 7:
        tier = "Gold"
    elif composite >= 5:
        tier = "Silver"
    else:
        tier = "Bronze"

    return {"tier": tier, "r": r, "f": f, "m": m, "composite": composite}


def save_visit(phone: str, store_name: str, client_code: str,
               stock_by_sku: dict[str, int], suggested_by_sku: dict[str, int],
               sku_names: dict[str, str] | None = None):
    """Save a visit with per-SKU stock and suggested order data."""
    with SessionLocal() as session:
        row = SalesRepVisitRow(
            phone_number=phone,
            store_name=store_name,
            client_code=client_code,
        )
        session.add(row)
        session.flush()  # get row.id

        for product_code, shelf_stock in stock_by_sku.items():
            item = SalesRepVisitItem(
                visit_id=row.id,
                product_code=product_code,
                product_name=(sku_names or {}).get(product_code, product_code),
                shelf_stock=shelf_stock,
                suggested_order=suggested_by_sku.get(product_code, 0),
            )
            session.add(item)

        session.commit()


def get_today_visits(phone: str) -> list[dict]:
    today_start = datetime.now(PANAMA_TZ).replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc)
    with SessionLocal() as session:
        rows = (
            session.query(SalesRepVisitRow)
            .filter(
                SalesRepVisitRow.phone_number == phone,
                SalesRepVisitRow.visit_date >= today_start,
            )
            .order_by(SalesRepVisitRow.id)
            .all()
        )
        return [
            {
                "store_name": r.store_name,
                "client_code": r.client_code,
            }
            for r in rows
        ]


def save_message(phone: str, role: str, content: str, phase: str | None = None):
    with SessionLocal() as session:
        msg = ConversationMessage(
            phone_number=phone, role=role, content=content, phase=phase,
        )
        session.add(msg)
        session.commit()


def get_cached_daily_sales(client_code: str, product_code: str | None = None, days_back: int = 1095) -> dict[str, float]:
    """Query local sales cache for avg daily sales per product.

    Returns {product_code: avg_daily_qty}. Uses the sales_cache table
    which is synced from MSSQL BI_ANALISIS_VENTAS.
    Uses qty_fac (Cantidad_FAC, gross delivered) as the primary signal — this
    reflects actual deliveries regardless of returns. Falls back to quantity
    (Cantidad_NETA) for older rows that predate the qty_fac column.
    Span is at least days_back to prevent a single recent row from inflating the rate.
    """
    cutoff = date.today() - timedelta(days=days_back)
    with SessionLocal() as session:
        from sqlalchemy import and_, or_
        filters = [
            SalesCacheRow.client_code == client_code,
            SalesCacheRow.sale_date >= cutoff,
            or_(SalesCacheRow.qty_fac > 0, SalesCacheRow.quantity > 0),
        ]
        if product_code:
            filters.append(SalesCacheRow.product_code == product_code)

        rows = (
            session.query(
                SalesCacheRow.product_code,
                func.sum(SalesCacheRow.qty_fac).label("total_fac"),
                func.sum(SalesCacheRow.quantity).label("total_neta"),
                func.min(SalesCacheRow.sale_date).label("first_date"),
            )
            .filter(and_(*filters))
            .group_by(SalesCacheRow.product_code)
            .all()
        )

        if not rows:
            return {}

        today = date.today()
        result = {}
        for pcode, total_fac, total_neta, first_date in rows:
            # Prefer qty_fac (gross); fall back to quantity (net) for legacy rows
            total = total_fac if (total_fac and total_fac > 0) else total_neta
            if total and total > 0 and first_date:
                span = max((today - first_date).days, days_back)
                result[pcode] = float(total) / span
        return result


def load_messages(phone: str) -> list[dict]:
    with SessionLocal() as session:
        rows = (
            session.query(ConversationMessage)
            .filter_by(phone_number=phone)
            .order_by(ConversationMessage.id)
            .all()
        )
        return [
            {"role": r.role, "content": r.content, "phase": r.phase}
            for r in rows
        ]


def get_product_prices() -> dict[str, float]:
    """Get cached product prices {product_code: unit_price_usd}.

    Falls back to FALLBACK_PRICES from config if cache is empty.
    """
    from agent_tasty.config import FALLBACK_PRICES

    with SessionLocal() as session:
        rows = session.query(ProductPriceCache).all()
        prices = dict(FALLBACK_PRICES)  # base: config prices for all SKUs
        for r in rows:
            prices[r.product_code] = r.unit_price_usd  # DB overrides config
        return prices


def get_client_merma_rates(client_code: str) -> dict[str, float]:
    """Get merma rates for a client {product_code: merma_rate}."""
    with SessionLocal() as session:
        rows = (
            session.query(ClientMermaCache)
            .filter(ClientMermaCache.client_code == client_code)
            .all()
        )
        return {r.product_code: r.merma_rate for r in rows}
