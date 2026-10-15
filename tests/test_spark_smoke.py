"""Optional Spark local smoke — skipped when PySpark/Java unavailable."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.spark.batch_job import run_python_batch, run_spark_batch


def test_python_batch_engine(tmp_path):
    raw = tmp_path / "e.ndjson"
    events = [
        {
            "event_id": "e1",
            "event_type": "purchase",
            "event_ts": "2026-03-01T12:00:00Z",
            "session_id": "S-1",
            "customer_id": "C001",
            "product_id": "P1001",
            "quantity": 1,
            "device": "web",
            "country": "FR",
            "metadata": {"order_id": "O-1"},
        }
    ]
    raw.write_text("\n".join(json.dumps(e) for e in events) + "\n")
    db = tmp_path / "w.duckdb"
    counts = run_python_batch(str(raw), "duckdb", str(db))
    assert counts["fact_orders"] == 1


def test_spark_local_smoke(tmp_path):
    pyspark = pytest.importorskip("pyspark")
    # Java required for Spark local
    import shutil

    if not shutil.which("java"):
        pytest.skip("Java runtime not available")

    raw = tmp_path / "spark_events.ndjson"
    payload = {
        "event_id": "spark-1",
        "event_type": "purchase",
        "event_ts": "2026-03-01T12:00:00Z",
        "session_id": "S-SP",
        "customer_id": "C003",
        "product_id": "P1005",
        "quantity": 1,
        "device": "ios",
        "country": "DE",
        "metadata": {"order_id": "O-SP"},
    }
    raw.write_text(json.dumps(payload) + "\n")
    db = tmp_path / "spark.duckdb"
    counts = run_spark_batch(str(raw), "duckdb", str(db))
    assert counts.get("fact_orders") == 1
