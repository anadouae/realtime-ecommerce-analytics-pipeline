from pathlib import Path

from pipeline.orchestration.tasks import land_events, transform_and_load
from pipeline.producer.simulator import ClickstreamSimulator
from pipeline.schema.events import EventType


def test_simulator_generates_valid_mix():
    sim = ClickstreamSimulator(seed=7, bootstrap="file")
    types = [sim.generate_event().event_type for _ in range(200)]
    assert EventType.PURCHASE in types
    assert EventType.PAGE_VIEW in types


def test_simulator_writes_ndjson(tmp_path):
    out = tmp_path / "events.ndjson"
    sim = ClickstreamSimulator(seed=1, bootstrap="file", output_path=str(out))
    n = sim.run(count=25, rate_per_sec=0)
    assert n == 25
    lines = out.read_text().strip().splitlines()
    assert len(lines) == 25


def test_land_transform_pipeline(tmp_path):
    raw = tmp_path / "clickstream.ndjson"
    db = tmp_path / "analytics.duckdb"
    land = land_events(count=40, rate=0, output=str(raw), seed=99)
    assert land["published"] == 40
    assert Path(raw).exists()
    counts = transform_and_load(input_path=str(raw), duckdb_path=str(db))
    assert counts["dim_customer"] == 8
    assert counts["fact_clicks"] == 40
    assert counts["fact_orders"] >= 0
