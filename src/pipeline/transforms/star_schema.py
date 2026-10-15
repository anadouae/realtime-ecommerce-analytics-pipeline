"""Star-schema builders for e-commerce analytics.

Complexity notes (transform jobs)
---------------------------------
* ``build_dim_product`` / ``build_dim_customer``: O(n) over reference rows;
  SCD Type-1 upsert keyed by natural key. Deterministic surrogate keys via
  stable hash so batch re-runs are idempotent.
* ``build_fact_clicks``: O(n) filter + projection of clickstream events;
  late events are accepted if ``event_ts`` is within the watermark window
  (default 24h) — no shuffle required for pure Python path.
* ``build_fact_orders``: O(n) over PURCHASE events with O(1) product lookup
  for price enrichment. Aggregation by ``order_id`` is O(n) hash-group.
* Spark path (``SparkStarTransform``): same logic as DataFrame mapPartitions /
  groupBy; shuffle cost is O(n) network for dim lookups broadcast join when
  dimensions fit in memory (typical for catalog < 1M SKUs).

These pure-Python builders are the source of truth for unit tests and the
local DuckDB sink. Spark jobs call the same column contracts.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Optional

from pipeline.schema.events import (
    CUSTOMER_SEED,
    PRODUCT_CATALOG,
    ClickstreamEvent,
    EventType,
    OrderEvent,
)


def _surrogate_key(*parts: str) -> int:
    """Stable positive int64-ish surrogate from natural key parts.

    Complexity: O(k) where k is total key length — constant for fixed IDs.
    """
    digest = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
    return int(digest[:15], 16)


def _as_utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def build_dim_product(
    products: Optional[Iterable[dict[str, Any]]] = None,
    as_of: Optional[datetime] = None,
) -> list[dict[str, Any]]:
    """Build ``dim_product`` SCD1 rows. O(n) over catalog size."""
    as_of = as_of or datetime.now(timezone.utc)
    rows: list[dict[str, Any]] = []
    for p in products or PRODUCT_CATALOG:
        rows.append(
            {
                "product_sk": _surrogate_key("product", p["product_id"]),
                "product_id": p["product_id"],
                "sku": p["sku"],
                "product_name": p["name"],
                "category": p["category"],
                "brand": p["brand"],
                "unit_price": float(p["unit_price"]),
                "is_active": True,
                "effective_from": as_of.isoformat(),
                "effective_to": None,
            }
        )
    return rows


def build_dim_customer(
    customers: Optional[Iterable[dict[str, Any]]] = None,
    as_of: Optional[datetime] = None,
) -> list[dict[str, Any]]:
    """Build ``dim_customer`` SCD1 rows. O(n) over customer seed."""
    as_of = as_of or datetime.now(timezone.utc)
    rows: list[dict[str, Any]] = []
    for c in customers or CUSTOMER_SEED:
        rows.append(
            {
                "customer_sk": _surrogate_key("customer", c["customer_id"]),
                "customer_id": c["customer_id"],
                "email": c["email"],
                "country": c["country"],
                "segment": c["segment"],
                "is_active": True,
                "effective_from": as_of.isoformat(),
                "effective_to": None,
            }
        )
    return rows


def _product_price_index(
    products: Optional[Iterable[dict[str, Any]]] = None,
) -> dict[str, float]:
    catalog = products or PRODUCT_CATALOG
    return {p["product_id"]: float(p["unit_price"]) for p in catalog}


def enrich_purchase_to_order(
    event: ClickstreamEvent | dict[str, Any],
    price_index: Optional[dict[str, float]] = None,
) -> OrderEvent:
    """Map a PURCHASE clickstream event to an OrderEvent with price enrichment.

    Complexity: O(1) dict lookup for unit_price.
    """
    if isinstance(event, dict):
        event = ClickstreamEvent.model_validate(event)
    if event.event_type != EventType.PURCHASE:
        raise ValueError(f"Expected PURCHASE, got {event.event_type}")
    if not event.customer_id or not event.product_id:
        raise ValueError("PURCHASE requires customer_id and product_id")

    prices = price_index or _product_price_index()
    unit_price = prices.get(event.product_id)
    if unit_price is None:
        raise KeyError(f"Unknown product_id={event.product_id}")

    qty = event.quantity or 1
    order_id = event.metadata.get("order_id") if event.metadata else None
    return OrderEvent(
        order_id=order_id or f"O-{event.event_id[:12]}",
        event_id=event.event_id,
        order_ts=_as_utc(event.event_ts),
        customer_id=event.customer_id,
        product_id=event.product_id,
        quantity=qty,
        unit_price=unit_price,
        channel=event.device or "web",
    )


def build_fact_clicks(
    events: Iterable[ClickstreamEvent | dict[str, Any]],
    watermark: Optional[timedelta] = None,
    now: Optional[datetime] = None,
) -> list[dict[str, Any]]:
    """Project clickstream → ``fact_clicks``.

    Complexity: O(n) single pass. Watermark drop is O(1) per event.
    """
    watermark = watermark or timedelta(hours=24)
    now = _as_utc(now or datetime.now(timezone.utc))
    cutoff = now - watermark
    facts: list[dict[str, Any]] = []

    for raw in events:
        ev = raw if isinstance(raw, ClickstreamEvent) else ClickstreamEvent.model_validate(raw)
        ts = _as_utc(ev.event_ts)
        if ts < cutoff:
            continue  # late / out-of-window — dropped here; production would route to DLQ
        facts.append(
            {
                "click_sk": _surrogate_key("click", ev.event_id),
                "event_id": ev.event_id,
                "event_type": ev.event_type.value,
                "event_ts": ts.isoformat(),
                "session_id": ev.session_id,
                "customer_sk": _surrogate_key("customer", ev.customer_id) if ev.customer_id else None,
                "customer_id": ev.customer_id,
                "product_sk": _surrogate_key("product", ev.product_id) if ev.product_id else None,
                "product_id": ev.product_id,
                "page_url": ev.page_url,
                "search_query": ev.search_query,
                "quantity": ev.quantity,
                "device": ev.device,
                "country": ev.country,
                "referrer": ev.referrer,
            }
        )
    return facts


def build_fact_orders(
    events: Iterable[ClickstreamEvent | dict[str, Any]],
    products: Optional[Iterable[dict[str, Any]]] = None,
) -> list[dict[str, Any]]:
    """Extract PURCHASE events → ``fact_orders`` with line totals.

    Complexity: O(n) scan + O(1) price lookups. Grouping by order_id is
    unnecessary for line-grain facts (one row per purchase line).
    """
    prices = _product_price_index(products)
    facts: list[dict[str, Any]] = []
    for raw in events:
        ev = raw if isinstance(raw, ClickstreamEvent) else ClickstreamEvent.model_validate(raw)
        if ev.event_type != EventType.PURCHASE:
            continue
        order = enrich_purchase_to_order(ev, prices)
        facts.append(
            {
                "order_sk": _surrogate_key("order", order.order_id, order.product_id),
                "order_id": order.order_id,
                "event_id": order.event_id,
                "order_ts": order.order_ts.isoformat(),
                "customer_sk": _surrogate_key("customer", order.customer_id),
                "customer_id": order.customer_id,
                "product_sk": _surrogate_key("product", order.product_id),
                "product_id": order.product_id,
                "quantity": order.quantity,
                "unit_price": order.unit_price,
                "line_total": order.line_total,
                "currency": order.currency,
                "status": order.status,
                "channel": order.channel,
            }
        )
    return facts


class StarSchemaBuilder:
    """Orchestrates land → transform for a batch of clickstream events.

    Designed for Airflow task bodies and local smoke tests without Spark.
    """

    def __init__(
        self,
        products: Optional[list[dict[str, Any]]] = None,
        customers: Optional[list[dict[str, Any]]] = None,
        watermark_hours: int = 24,
    ) -> None:
        self.products = list(products or PRODUCT_CATALOG)
        self.customers = list(customers or CUSTOMER_SEED)
        self.watermark = timedelta(hours=watermark_hours)

    def transform(
        self,
        events: Iterable[ClickstreamEvent | dict[str, Any]],
        now: Optional[datetime] = None,
    ) -> dict[str, list[dict[str, Any]]]:
        event_list = [
            e if isinstance(e, ClickstreamEvent) else ClickstreamEvent.model_validate(e)
            for e in events
        ]
        return {
            "dim_product": build_dim_product(self.products, as_of=now),
            "dim_customer": build_dim_customer(self.customers, as_of=now),
            "fact_clicks": build_fact_clicks(event_list, watermark=self.watermark, now=now),
            "fact_orders": build_fact_orders(event_list, products=self.products),
        }
