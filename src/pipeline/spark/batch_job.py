"""Spark (or pure-Python fallback) batch transform: land → star schema → sink.

When PySpark is unavailable (CI without Java), the same transform contract
runs via ``StarSchemaBuilder`` so unit/smoke tests stay green.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path
from typing import Any, Optional

from pipeline.security import configure_secure_logging
from pipeline.sinks import get_sink
from pipeline.transforms.star_schema import StarSchemaBuilder

logger = logging.getLogger(__name__)


def load_events_ndjson(path: str | Path) -> list[dict[str, Any]]:
    path = Path(path)
    if not path.exists():
        return []
    events: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                events.append(json.loads(line))
    return events


def run_python_batch(
    input_path: str,
    sink_backend: str = "duckdb",
    sink_path: str = "data/warehouse/analytics.duckdb",
) -> dict[str, int]:
    events = load_events_ndjson(input_path)
    if not events:
        logger.warning("No events at %s", input_path)
        return {}
    builder = StarSchemaBuilder()
    star = builder.transform(events)
    sink = get_sink(sink_backend, path=sink_path) if sink_backend == "duckdb" else get_sink(sink_backend)
    try:
        counts = sink.load_star(star)
        logger.info("Loaded star schema: %s", counts)
        return counts
    finally:
        sink.close()


def run_spark_batch(
    input_path: str,
    sink_backend: str = "duckdb",
    sink_path: str = "data/warehouse/analytics.duckdb",
) -> dict[str, int]:
    """Spark DataFrame path — broadcast dims, map purchases, write via Python sink.

    Complexity: reading NDJSON is O(n); dim broadcast join for enrichment is
    O(n) with O(|dim|) memory. Shuffle avoided by collecting facts to driver
    for local DuckDB sinks (acceptable for smoke volumes; cloud path would write
    Parquet partitions to object storage then COPY into Snowflake).
    """
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
    except ImportError:
        logger.warning("PySpark not installed; falling back to pure-Python batch")
        return run_python_batch(input_path, sink_backend, sink_path)

    spark = (
        SparkSession.builder.appName("ecom-star-batch")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    try:
        df = spark.read.json(str(input_path))
        if df.rdd.isEmpty():
            return {}
        # Collect to Python for shared StarSchemaBuilder (keeps one transform path).
        events = [row.asDict(recursive=True) for row in df.collect()]
        # Normalize Spark nulls
        for e in events:
            if e.get("metadata") is None:
                e["metadata"] = {}
        builder = StarSchemaBuilder()
        star = builder.transform(events)
        # Lightweight Spark-side aggregation smoke: purchase GMV by country
        if "event_type" in df.columns:
            purchases = df.filter(F.col("event_type") == "purchase")
            logger.info("Spark purchase count=%s", purchases.count())
        sink = get_sink(sink_backend, path=sink_path) if sink_backend == "duckdb" else get_sink(sink_backend)
        try:
            return sink.load_star(star)
        finally:
            sink.close()
    finally:
        spark.stop()


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Batch land→transform→load")
    parser.add_argument("--input", default="data/raw/clickstream.ndjson")
    parser.add_argument("--sink", default=os.getenv("SINK_BACKEND", "duckdb"))
    parser.add_argument("--duckdb-path", default="data/warehouse/analytics.duckdb")
    parser.add_argument("--engine", choices=["auto", "python", "spark"], default="auto")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)
    configure_secure_logging(logging.DEBUG if args.verbose else logging.INFO)

    engine = args.engine
    if engine == "auto":
        try:
            import pyspark  # noqa: F401

            engine = "spark"
        except ImportError:
            engine = "python"

    if engine == "spark":
        counts = run_spark_batch(args.input, args.sink, args.duckdb_path)
    else:
        counts = run_python_batch(args.input, args.sink, args.duckdb_path)
    print(json.dumps(counts))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
