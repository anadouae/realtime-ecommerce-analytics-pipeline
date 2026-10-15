from datetime import datetime, timezone

from pipeline.schema.events import ClickstreamEvent, EventType, OrderEvent


def test_clickstream_roundtrip():
    ev = ClickstreamEvent(
        event_type=EventType.PRODUCT_VIEW,
        session_id="S-1",
        customer_id="C001",
        product_id="P1001",
        event_ts=datetime(2026, 1, 15, 12, 0, tzinfo=timezone.utc),
    )
    payload = ev.to_kafka_dict()
    assert payload["event_type"] == "product_view"
    assert payload["event_ts"].endswith("Z")
    restored = ClickstreamEvent.model_validate(payload)
    assert restored.product_id == "P1001"


def test_order_line_total():
    order = OrderEvent(
        event_id="e1",
        order_ts=datetime.now(timezone.utc),
        customer_id="C001",
        product_id="P1001",
        quantity=3,
        unit_price=10.0,
    )
    assert order.line_total == 30.0
