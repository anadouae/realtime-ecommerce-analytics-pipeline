"""Land → transform → load task functions callable from Airflow or CLI."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from pipeline.producer.simulator import ClickstreamSimulator
from pipeline.spark.batch_job import run_python_batch

logger = logging.getLogger(__name__)


def land_events(
    count: int = 200,
    rate: float = 50.0,
    output: str = "data/raw/clickstream.ndjson",
    seed: int = 42,
    clear: bool = True,
) -> dict[str, Any]:
    """Simulate / land clickstream events to NDJSON (Kafka fallback path)."""
    path = Path(output)
    if clear and path.exists():
        path.unlink()
    path.parent.mkdir(parents=True, exist_ok=True)
    sim = ClickstreamSimulator(seed=seed, output_path=str(path), bootstrap="file")
    published = sim.run(count=count, rate_per_sec=rate, flush=False)
    return {"published": published, "path": str(path)}


def transform_and_load(
    input_path: str = "data/raw/clickstream.ndjson",
    sink_backend: str = "duckdb",
    duckdb_path: str = "data/warehouse/analytics.duckdb",
) -> dict[str, int]:
    """Run star-schema transform and load into configured sink."""
    counts = run_python_batch(input_path, sink_backend, duckdb_path)
    logger.info("transform_and_load done: %s", counts)
    return counts
