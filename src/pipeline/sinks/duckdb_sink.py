"""Local DuckDB star-schema sink — first-class warehouse for the local stack."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable, Optional

from .base import WarehouseAdapter

DIM_DDL = {
    "dim_product": """
        CREATE TABLE IF NOT EXISTS dim_product (
            product_sk BIGINT PRIMARY KEY,
            product_id VARCHAR UNIQUE,
            sku VARCHAR,
            product_name VARCHAR,
            category VARCHAR,
            brand VARCHAR,
            unit_price DOUBLE,
            is_active BOOLEAN,
            effective_from TIMESTAMP,
            effective_to TIMESTAMP
        )
    """,
    "dim_customer": """
        CREATE TABLE IF NOT EXISTS dim_customer (
            customer_sk BIGINT PRIMARY KEY,
            customer_id VARCHAR UNIQUE,
            email VARCHAR,
            country VARCHAR,
            segment VARCHAR,
            is_active BOOLEAN,
            effective_from TIMESTAMP,
            effective_to TIMESTAMP
        )
    """,
}

FACT_DDL = {
    "fact_clicks": """
        CREATE TABLE IF NOT EXISTS fact_clicks (
            click_sk BIGINT,
            event_id VARCHAR,
            event_type VARCHAR,
            event_ts TIMESTAMP,
            session_id VARCHAR,
            customer_sk BIGINT,
            customer_id VARCHAR,
            product_sk BIGINT,
            product_id VARCHAR,
            page_url VARCHAR,
            search_query VARCHAR,
            quantity INTEGER,
            device VARCHAR,
            country VARCHAR,
            referrer VARCHAR
        )
    """,
    "fact_orders": """
        CREATE TABLE IF NOT EXISTS fact_orders (
            order_sk BIGINT,
            order_id VARCHAR,
            event_id VARCHAR,
            order_ts TIMESTAMP,
            customer_sk BIGINT,
            customer_id VARCHAR,
            product_sk BIGINT,
            product_id VARCHAR,
            quantity INTEGER,
            unit_price DOUBLE,
            line_total DOUBLE,
            currency VARCHAR,
            status VARCHAR,
            channel VARCHAR
        )
    """,
}


class DuckDBSink(WarehouseAdapter):
    def __init__(self, path: str = "data/warehouse/analytics.duckdb") -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        import duckdb

        self._con = duckdb.connect(str(self.path))

    def ensure_schema(self) -> None:
        for ddl in DIM_DDL.values():
            self._con.execute(ddl)
        for ddl in FACT_DDL.values():
            self._con.execute(ddl)

    def upsert_dim(self, table: str, rows: Iterable[dict[str, Any]], key: str) -> int:
        row_list = list(rows)
        if not row_list:
            return 0
        # Delete-then-insert SCD1 for matching natural keys (idempotent re-runs).
        keys = [r[key] for r in row_list]
        placeholders = ", ".join(["?"] * len(keys))
        self._con.execute(f"DELETE FROM {table} WHERE {key} IN ({placeholders})", keys)
        cols = list(row_list[0].keys())
        col_sql = ", ".join(cols)
        qmarks = ", ".join(["?"] * len(cols))
        insert = f"INSERT INTO {table} ({col_sql}) VALUES ({qmarks})"
        for row in row_list:
            self._con.execute(insert, [row[c] for c in cols])
        return len(row_list)

    def append_fact(self, table: str, rows: Iterable[dict[str, Any]]) -> int:
        row_list = list(rows)
        if not row_list:
            return 0
        # Deduplicate by event_id so re-runs stay idempotent.
        id_col = "event_id"
        ids = [r[id_col] for r in row_list if id_col in r]
        if ids:
            placeholders = ", ".join(["?"] * len(ids))
            self._con.execute(
                f"DELETE FROM {table} WHERE {id_col} IN ({placeholders})",
                ids,
            )
        cols = list(row_list[0].keys())
        col_sql = ", ".join(cols)
        qmarks = ", ".join(["?"] * len(cols))
        insert = f"INSERT INTO {table} ({col_sql}) VALUES ({qmarks})"
        for row in row_list:
            self._con.execute(insert, [row[c] for c in cols])
        return len(row_list)

    def query(self, sql: str) -> list[tuple]:
        return self._con.execute(sql).fetchall()

    def export_json(self, out_dir: Optional[str] = None) -> Path:
        target = Path(out_dir or self.path.parent / "export")
        target.mkdir(parents=True, exist_ok=True)
        for table in list(DIM_DDL) + list(FACT_DDL):
            rows = self._con.execute(f"SELECT * FROM {table}").fetchdf().to_dict(orient="records")
            (target / f"{table}.json").write_text(json.dumps(rows, default=str, indent=2))
        return target

    def close(self) -> None:
        self._con.close()
