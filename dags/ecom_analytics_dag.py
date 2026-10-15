"""Airflow DAG: land → transform → load for e-commerce clickstream.

Astronomer-style / Airflow 2.x compatible. Runnable locally with the
Astronomer Runtime image or vanilla Airflow when ``PIPELINE_HOME`` is on
``PYTHONPATH``.

Schedule: hourly micro-batch (streaming is a separate Spark job).
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

# Ensure src/ is importable when Airflow mounts only the dags/ folder.
_PIPELINE_HOME = os.getenv("PIPELINE_HOME", os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
_SRC = os.path.join(_PIPELINE_HOME, "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except ImportError:  # pragma: no cover — allows importing helpers without Airflow installed
    DAG = None  # type: ignore
    PythonOperator = None  # type: ignore

from pipeline.orchestration.tasks import land_events, transform_and_load


DEFAULT_ARGS = {
    "owner": "douae-el-assal",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def _land(**_context):
    return land_events(
        count=int(os.getenv("LAND_EVENT_COUNT", "200")),
        rate=float(os.getenv("LAND_EVENT_RATE", "100")),
        output=os.getenv("CLICKSTREAM_PATH", f"{_PIPELINE_HOME}/data/raw/clickstream.ndjson"),
        seed=int(os.getenv("LAND_SEED", "42")),
    )


def _transform_load(**_context):
    return transform_and_load(
        input_path=os.getenv("CLICKSTREAM_PATH", f"{_PIPELINE_HOME}/data/raw/clickstream.ndjson"),
        sink_backend=os.getenv("SINK_BACKEND", "duckdb"),
        duckdb_path=os.getenv("DUCKDB_PATH", f"{_PIPELINE_HOME}/data/warehouse/analytics.duckdb"),
    )


def _snowflake_adapter_load(**_context):
    """Optional Snowflake adapter after local DuckDB load (offline without creds)."""
    from pipeline.sinks import SnowflakeAdapter
    from pipeline.spark.batch_job import load_events_ndjson
    from pipeline.transforms.star_schema import StarSchemaBuilder

    path = os.getenv("CLICKSTREAM_PATH", f"{_PIPELINE_HOME}/data/raw/clickstream.ndjson")
    events = load_events_ndjson(path)
    star = StarSchemaBuilder().transform(events)
    adapter = SnowflakeAdapter()
    try:
        counts = adapter.load_star(star)
        return {"mode": adapter.mode, "counts": counts, "plan_steps": len(adapter.plan)}
    finally:
        adapter.close()


if DAG is not None:
    with DAG(
        dag_id="ecom_clickstream_land_transform_load",
        default_args=DEFAULT_ARGS,
        description="Hourly land→transform→load of clickstream into star schema",
        schedule_interval=timedelta(hours=1),
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=["ecommerce", "spark", "snowflake", "star-schema"],
    ) as dag:
        land = PythonOperator(task_id="land_clickstream", python_callable=_land)
        transform = PythonOperator(task_id="transform_and_load_duckdb", python_callable=_transform_load)
        snowflake = PythonOperator(
            task_id="snowflake_adapter_optional",
            python_callable=_snowflake_adapter_load,
        )

        land >> transform >> snowflake
