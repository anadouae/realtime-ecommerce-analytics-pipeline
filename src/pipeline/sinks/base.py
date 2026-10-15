"""Warehouse adapter interface — Snowflake and local sinks implement this."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Iterable


class WarehouseAdapter(ABC):
    """Load star-schema tables into an analytical store.

    Local: DuckDB (or Postgres) as the first-class sink.
    Cloud: Snowflake via connector when credentials are provided via env.
    """

    @abstractmethod
    def ensure_schema(self) -> None:
        """Create database / schemas / tables if missing."""

    @abstractmethod
    def upsert_dim(self, table: str, rows: Iterable[dict[str, Any]], key: str) -> int:
        """SCD1 upsert; returns rows written."""

    @abstractmethod
    def append_fact(self, table: str, rows: Iterable[dict[str, Any]]) -> int:
        """Append-only fact load; returns rows written."""

    @abstractmethod
    def close(self) -> None:
        ...

    def load_star(self, star: dict[str, list[dict[str, Any]]]) -> dict[str, int]:
        """Convenience: load all star tables from ``StarSchemaBuilder.transform``."""
        self.ensure_schema()
        counts: dict[str, int] = {}
        if "dim_product" in star:
            counts["dim_product"] = self.upsert_dim("dim_product", star["dim_product"], "product_id")
        if "dim_customer" in star:
            counts["dim_customer"] = self.upsert_dim("dim_customer", star["dim_customer"], "customer_id")
        if "fact_clicks" in star:
            counts["fact_clicks"] = self.append_fact("fact_clicks", star["fact_clicks"])
        if "fact_orders" in star:
            counts["fact_orders"] = self.append_fact("fact_orders", star["fact_orders"])
        return counts
