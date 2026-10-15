"""Integration smokes against real sinks (DuckDB file) and optional Kafka/Redpanda."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
from pathlib import Path

import pytest

from pipeline.orchestration.tasks import land_events, transform_and_load
from pipeline.producer.simulator import ClickstreamSimulator
from pipeline.sinks.duckdb_sink import DuckDBSink

ROOT = Path(__file__).resolve().parents[1]
COMPOSE_FILE = ROOT / "docker-compose.yml"


def test_duckdb_file_sink_end_to_end(tmp_path):
    """Real DuckDB file on disk — land → transform → assert row counts."""
    raw = tmp_path / "clickstream.ndjson"
    db = tmp_path / "analytics.duckdb"
    land = land_events(count=60, rate=0, output=str(raw), seed=11)
    assert land["published"] == 60
    assert raw.is_file()
    counts = transform_and_load(input_path=str(raw), duckdb_path=str(db))
    assert db.is_file()
    assert counts["fact_clicks"] == 60
    assert counts["dim_product"] >= 1
    assert counts["dim_customer"] >= 1

    sink = DuckDBSink(str(db))
    try:
        clicks = sink.query("SELECT COUNT(*) FROM fact_clicks")[0][0]
        orders = sink.query("SELECT COUNT(*) FROM fact_orders")[0][0]
        gmv = sink.query("SELECT COALESCE(SUM(line_total), 0) FROM fact_orders")[0][0]
    finally:
        sink.close()

    assert clicks == 60
    assert orders == counts["fact_orders"]
    assert gmv >= 0


def _compose_available() -> bool:
    if not shutil.which("docker"):
        return False
    try:
        subprocess.run(
            ["docker", "compose", "version"],
            check=True,
            capture_output=True,
            timeout=30,
        )
        return COMPOSE_FILE.is_file()
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError):
        return False


def _compose(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE_FILE), *args],
        cwd=str(ROOT),
        check=check,
        capture_output=True,
        text=True,
        timeout=180,
    )


@pytest.mark.integration
def test_kafka_redpanda_produce_consume_smoke():
    """Bring up Redpanda via compose and smoke produce/consume; skip if compose fails."""
    if not _compose_available():
        pytest.skip("docker compose unavailable")

    up = _compose("up", "-d", "redpanda", check=False)
    if up.returncode != 0:
        pytest.skip(f"compose up redpanda failed: {up.stderr or up.stdout}")

    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092")
    topic = "clickstream.events.smoke"
    try:
        # Wait for broker readiness (compose healthcheck can take a bit).
        ready = False
        last_err = ""
        for _ in range(45):
            try:
                from kafka import KafkaProducer  # type: ignore

                p = KafkaProducer(
                    bootstrap_servers=bootstrap,
                    request_timeout_ms=2000,
                )
                p.close()
                ready = True
                break
            except Exception as exc:  # noqa: BLE001
                last_err = str(exc)
                time.sleep(2)
        if not ready:
            pytest.skip(f"Redpanda not ready: {last_err}")

        from kafka import KafkaConsumer, KafkaProducer  # type: ignore

        producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=3,
            request_timeout_ms=5000,
        )
        sim = ClickstreamSimulator(
            seed=3,
            topic=topic,
            bootstrap=bootstrap,
            output_path=str(ROOT / "data" / "raw" / "_kafka_smoke.ndjson"),
        )
        # Force the already-connected path by injecting producer after generate
        payload_count = 5
        for _ in range(payload_count):
            event = sim.generate_event()
            producer.send(topic, key=event.session_id, value=event.to_kafka_dict())
        producer.flush(timeout=10)
        producer.close()

        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap,
            auto_offset_reset="earliest",
            consumer_timeout_ms=10000,
            group_id=f"smoke-{int(time.time())}",
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        )
        received = list(consumer)
        consumer.close()
        assert len(received) >= payload_count
        assert "event_id" in received[0].value
    finally:
        _compose("down", "-v", check=False)
