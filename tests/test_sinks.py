from datetime import datetime, timezone

from pipeline.schema.events import ClickstreamEvent, EventType
from pipeline.sinks.duckdb_sink import DuckDBSink
from pipeline.sinks.snowflake_stub import SnowflakeAdapter
from pipeline.transforms.star_schema import StarSchemaBuilder


def test_duckdb_load_star(tmp_path):
    db = tmp_path / "test.duckdb"
    events = [
        ClickstreamEvent(
            event_type=EventType.PURCHASE,
            event_ts=datetime.now(timezone.utc),
            session_id="S-1",
            customer_id="C002",
            product_id="P1003",
            quantity=2,
            metadata={"order_id": "O-DB"},
        ),
        ClickstreamEvent(
            event_type=EventType.PRODUCT_VIEW,
            event_ts=datetime.now(timezone.utc),
            session_id="S-1",
            customer_id="C002",
            product_id="P1003",
        ),
    ]
    star = StarSchemaBuilder().transform(events)
    sink = DuckDBSink(str(db))
    try:
        counts = sink.load_star(star)
        assert counts["dim_product"] == 8
        assert counts["fact_orders"] == 1
        assert counts["fact_clicks"] == 2
        gmv = sink.query("SELECT SUM(line_total) FROM fact_orders")[0][0]
        assert abs(gmv - 29.0) < 0.01  # 2 * 14.50
        # Idempotent reload
        counts2 = sink.load_star(star)
        assert counts2["fact_orders"] == 1
        assert sink.query("SELECT COUNT(*) FROM fact_orders")[0][0] == 1
    finally:
        sink.close()


def test_snowflake_offline_records_plan():
    adapter = SnowflakeAdapter(account=None, user=None, password=None)
    assert adapter.mode == "offline"
    assert "***REDACTED***" in repr(adapter) or adapter.password is None
    star = StarSchemaBuilder().transform(
        [
            ClickstreamEvent(
                event_type=EventType.PURCHASE,
                event_ts=datetime.now(timezone.utc),
                session_id="S-2",
                customer_id="C001",
                product_id="P1001",
                quantity=1,
                metadata={"order_id": "O-SF"},
            )
        ]
    )
    try:
        counts = adapter.load_star(star)
        assert counts["fact_orders"] == 1
        assert any(p.operation == "upsert_dim" for p in adapter.plan)
        assert any(p.operation == "append_fact" for p in adapter.plan)
    finally:
        adapter.close()
