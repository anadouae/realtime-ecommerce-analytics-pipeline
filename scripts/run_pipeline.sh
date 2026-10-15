#!/usr/bin/env bash
# First-class local pipeline: produce → transform → load star schema → assert counts.
# DuckDB is the default sink; Snowflake is an optional cloud adapter (see .env.example).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

export PYTHONPATH="${ROOT}/src${PYTHONPATH:+:$PYTHONPATH}"
COUNT="${LAND_EVENT_COUNT:-150}"
RATE="${LAND_EVENT_RATE:-200}"
SEED="${LAND_SEED:-42}"
RAW="${CLICKSTREAM_PATH:-data/raw/clickstream.ndjson}"
DB="${DUCKDB_PATH:-data/warehouse/analytics.duckdb}"
SINK="${SINK_BACKEND:-duckdb}"

mkdir -p "$(dirname "$RAW")" "$(dirname "$DB")"
rm -f "$RAW" "$DB"

echo "==> [1/3] Land clickstream events (count=${COUNT})"
python -m pipeline.producer.simulator \
  --count "$COUNT" \
  --rate "$RATE" \
  --seed "$SEED" \
  --bootstrap file \
  --output "$RAW"

echo "==> [2/3] Transform + load star schema (sink=${SINK})"
python -m pipeline.spark.batch_job \
  --engine python \
  --input "$RAW" \
  --sink "$SINK" \
  --duckdb-path "$DB"

echo "==> [3/3] Assert warehouse row counts"
python - <<PY
import json
import sys
from pathlib import Path

from pipeline.sinks.duckdb_sink import DuckDBSink

db = Path(${DB@Q})
raw = Path(${RAW@Q})
expected_events = sum(1 for line in raw.read_text().splitlines() if line.strip())
assert db.exists(), f"missing DuckDB file: {db}"

sink = DuckDBSink(str(db))
try:
    dims_p = sink.query("SELECT COUNT(*) FROM dim_product")[0][0]
    dims_c = sink.query("SELECT COUNT(*) FROM dim_customer")[0][0]
    clicks = sink.query("SELECT COUNT(*) FROM fact_clicks")[0][0]
    orders = sink.query("SELECT COUNT(*) FROM fact_orders")[0][0]
    gmv = sink.query("SELECT COALESCE(ROUND(SUM(line_total), 2), 0) FROM fact_orders")[0][0]
finally:
    sink.close()

assert dims_p >= 1, f"dim_product empty: {dims_p}"
assert dims_c >= 1, f"dim_customer empty: {dims_c}"
assert clicks == expected_events, f"fact_clicks={clicks} != landed={expected_events}"
assert orders >= 0
assert clicks > 0

summary = {
    "events_landed": expected_events,
    "dim_product": dims_p,
    "dim_customer": dims_c,
    "fact_clicks": clicks,
    "fact_orders": orders,
    "gmv": float(gmv),
    "duckdb": str(db),
}
print(json.dumps(summary, indent=2))
print("Pipeline OK — star schema loaded and asserted.")
PY
