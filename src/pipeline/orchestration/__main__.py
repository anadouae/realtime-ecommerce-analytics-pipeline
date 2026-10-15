"""CLI: ``python -m pipeline.orchestration.tasks`` style entry via package."""

from __future__ import annotations

import argparse
import json
import logging

from pipeline.orchestration.tasks import land_events, transform_and_load


def main() -> int:
    parser = argparse.ArgumentParser(description="Run land / transform tasks")
    parser.add_argument("step", choices=["land", "transform", "all"], default="all", nargs="?")
    parser.add_argument("--count", type=int, default=200)
    parser.add_argument("--output", default="data/raw/clickstream.ndjson")
    parser.add_argument("--duckdb-path", default="data/warehouse/analytics.duckdb")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)

    result = {}
    if args.step in {"land", "all"}:
        result["land"] = land_events(count=args.count, output=args.output)
    if args.step in {"transform", "all"}:
        result["transform"] = transform_and_load(
            input_path=args.output,
            duckdb_path=args.duckdb_path,
        )
    print(json.dumps(result, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
