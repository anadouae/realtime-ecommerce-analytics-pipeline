"""Structured Streaming skeleton: Kafka → micro-batch star transform.

Requires a running Kafka/Redpanda cluster and PySpark with the Kafka
connector. Without a cluster, use ``batch_job.py`` on NDJSON landed by the
producer file path (``scripts/run_pipeline.sh``).
"""

from __future__ import annotations

import argparse
import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)


def run_streaming(
    bootstrap: str = "localhost:9092",
    topic: str = "clickstream.events",
    checkpoint: str = "data/checkpoints/clickstream",
    sink_backend: str = "duckdb",
    sink_path: str = "data/warehouse/analytics.duckdb",
) -> None:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        IntegerType,
        MapType,
        StringType,
        StructField,
        StructType,
    )

    from pipeline.sinks import get_sink
    from pipeline.transforms.star_schema import StarSchemaBuilder

    schema = StructType(
        [
            StructField("event_id", StringType()),
            StructField("event_type", StringType()),
            StructField("event_ts", StringType()),
            StructField("session_id", StringType()),
            StructField("customer_id", StringType()),
            StructField("product_id", StringType()),
            StructField("page_url", StringType()),
            StructField("search_query", StringType()),
            StructField("quantity", IntegerType()),
            StructField("device", StringType()),
            StructField("country", StringType()),
            StructField("referrer", StringType()),
            StructField("metadata", MapType(StringType(), StringType())),
        ]
    )

    spark = (
        SparkSession.builder.appName("ecom-clickstream-stream")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )

    events = (
        raw.select(F.col("value").cast("string").alias("json"))
        .select(F.from_json("json", schema).alias("e"))
        .select("e.*")
        .withColumn("event_ts_ts", F.to_timestamp("event_ts"))
        .withWatermark("event_ts_ts", "24 hours")
    )

    builder = StarSchemaBuilder()

    def foreach_batch(batch_df, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return
        rows = [r.asDict(recursive=True) for r in batch_df.drop("event_ts_ts").collect()]
        for r in rows:
            if r.get("metadata") is None:
                r["metadata"] = {}
        star = builder.transform(rows)
        sink = (
            get_sink(sink_backend, path=sink_path)
            if sink_backend == "duckdb"
            else get_sink(sink_backend)
        )
        try:
            counts = sink.load_star(star)
            logger.info("batch_id=%s loaded=%s", batch_id, counts)
        finally:
            sink.close()

    query = (
        events.writeStream.foreachBatch(foreach_batch)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime="10 seconds")
        .start()
    )
    logger.info("Streaming query started; awaiting termination")
    query.awaitTermination()


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Spark Structured Streaming job")
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
    parser.add_argument("--topic", default="clickstream.events")
    parser.add_argument("--checkpoint", default="data/checkpoints/clickstream")
    parser.add_argument("--sink", default="duckdb")
    parser.add_argument("--duckdb-path", default="data/warehouse/analytics.duckdb")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    run_streaming(args.bootstrap, args.topic, args.checkpoint, args.sink, args.duckdb_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
