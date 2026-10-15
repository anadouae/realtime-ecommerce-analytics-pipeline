from datetime import datetime, timedelta, timezone

import pytest

from pipeline.schema.events import ClickstreamEvent, EventType
from pipeline.transforms.star_schema import (
    StarSchemaBuilder,
    build_dim_customer,
    build_dim_product,
    build_fact_clicks,
    build_fact_orders,
    enrich_purchase_to_order,
)


NOW = datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)


def _event(etype: EventType, **kwargs) -> ClickstreamEvent:
    defaults = dict(
        event_type=etype,
        event_ts=NOW,
        session_id="S-99",
        customer_id="C001",
        product_id="P1001",
        device="web",
        country="FR",
    )
    defaults.update(kwargs)
    return ClickstreamEvent(**defaults)


def test_dim_product_surrogate_stable():
    a = build_dim_product(as_of=NOW)
    b = build_dim_product(as_of=NOW)
    assert len(a) == 8
    assert a[0]["product_sk"] == b[0]["product_sk"]
    assert a[0]["product_id"] == "P1001"


def test_dim_customer_count():
    rows = build_dim_customer(as_of=NOW)
    assert len(rows) == 8
    assert {r["segment"] for r in rows} >= {"premium", "standard"}


def test_enrich_purchase():
    purchase = _event(EventType.PURCHASE, quantity=2, metadata={"order_id": "O-TEST"})
    order = enrich_purchase_to_order(purchase)
    assert order.order_id == "O-TEST"
    assert order.line_total == round(2 * 29.99, 2)


def test_enrich_rejects_non_purchase():
    with pytest.raises(ValueError):
        enrich_purchase_to_order(_event(EventType.PAGE_VIEW))


def test_fact_clicks_watermark_drops_late():
    fresh = _event(EventType.PAGE_VIEW, product_id=None)
    late = _event(
        EventType.PAGE_VIEW,
        event_ts=NOW - timedelta(hours=48),
        product_id=None,
        event_id="late-1",
    )
    facts = build_fact_clicks([fresh, late], watermark=timedelta(hours=24), now=NOW)
    assert len(facts) == 1
    assert facts[0]["event_id"] == fresh.event_id


def test_fact_orders_only_purchases():
    events = [
        _event(EventType.PRODUCT_VIEW),
        _event(EventType.PURCHASE, quantity=1, metadata={"order_id": "O-1"}),
        _event(EventType.ADD_TO_CART, quantity=1),
        _event(EventType.PURCHASE, quantity=2, product_id="P1005", metadata={"order_id": "O-2"}),
    ]
    facts = build_fact_orders(events)
    assert len(facts) == 2
    assert all(f["line_total"] > 0 for f in facts)
    assert {f["order_id"] for f in facts} == {"O-1", "O-2"}


def test_star_schema_builder_end_to_end():
    events = [
        _event(EventType.SEARCH, search_query="mug", product_id=None, page_url="/search"),
        _event(EventType.PURCHASE, quantity=1, metadata={"order_id": "O-X"}),
    ]
    star = StarSchemaBuilder().transform(events, now=NOW)
    assert set(star) == {"dim_product", "dim_customer", "fact_clicks", "fact_orders"}
    assert len(star["fact_orders"]) == 1
    assert len(star["fact_clicks"]) == 2
    # Surrogate keys on facts must resolve against dims
    product_sks = {r["product_sk"] for r in star["dim_product"]}
    assert star["fact_orders"][0]["product_sk"] in product_sks
