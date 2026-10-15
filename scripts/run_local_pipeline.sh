#!/usr/bin/env bash
# Real local stack entrypoint (produce → transform → DuckDB star schema).
# Snowflake remains an optional cloud sink; see .env.example and README.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
exec "$ROOT/scripts/run_pipeline.sh" "$@"
