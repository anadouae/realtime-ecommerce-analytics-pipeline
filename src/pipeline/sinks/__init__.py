from .base import WarehouseAdapter
from .duckdb_sink import DuckDBSink
from .snowflake_stub import SnowflakeAdapter

__all__ = ["WarehouseAdapter", "DuckDBSink", "SnowflakeAdapter", "get_sink"]


def get_sink(backend: str = "duckdb", **kwargs):
    """Factory: ``duckdb`` (local) or ``snowflake`` (offline plan / live connector)."""
    backend = backend.lower()
    if backend == "duckdb":
        return DuckDBSink(**kwargs)
    if backend == "snowflake":
        return SnowflakeAdapter(**kwargs)
    raise ValueError(f"Unknown sink backend: {backend}")
