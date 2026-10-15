"""Snowflake adapter interface with local-safe offline behaviour.

Credentials are read exclusively from environment variables
(``SNOWFLAKE_ACCOUNT``, ``SNOWFLAKE_USER``, ``SNOWFLAKE_PASSWORD``, …)
or constructor args — never hardcoded. When credentials are absent, the
adapter records planned SQL / row counts so CI and the local DuckDB stack
keep working. Secrets are redacted from logs via ``pipeline.security``.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional

from pipeline.security import redact_secrets, safe_repr_mapping

from .base import WarehouseAdapter

logger = logging.getLogger(__name__)


@dataclass
class PlannedWrite:
    operation: str
    table: str
    row_count: int
    sample_keys: list[str] = field(default_factory=list)


class SnowflakeAdapter(WarehouseAdapter):
    """Production-shaped adapter; falls back to offline plan recording."""

    def __init__(
        self,
        account: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        warehouse: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        role: Optional[str] = None,
    ) -> None:
        self.account = account or os.getenv("SNOWFLAKE_ACCOUNT")
        self.user = user or os.getenv("SNOWFLAKE_USER")
        self.password = password or os.getenv("SNOWFLAKE_PASSWORD")
        self.warehouse = warehouse or os.getenv("SNOWFLAKE_WAREHOUSE", "ANALYTICS_WH")
        self.database = database or os.getenv("SNOWFLAKE_DATABASE", "ECOM_ANALYTICS")
        self.schema = schema or os.getenv("SNOWFLAKE_SCHEMA", "MART")
        self.role = role or os.getenv("SNOWFLAKE_ROLE", "TRANSFORMER")
        self._conn = None
        self.plan: list[PlannedWrite] = []
        self.mode = "live" if self._credentials_present() else "offline"

    def __repr__(self) -> str:
        safe = safe_repr_mapping(
            {
                "mode": self.mode,
                "account": self.account,
                "user": self.user,
                "password": self.password,
                "warehouse": self.warehouse,
                "database": self.database,
                "schema": self.schema,
                "role": self.role,
            }
        )
        return f"SnowflakeAdapter({safe})"

    def _credentials_present(self) -> bool:
        return bool(self.account and self.user and self.password)

    def _connect(self):
        if self._conn is not None:
            return self._conn
        if not self._credentials_present():
            return None
        try:
            import snowflake.connector  # type: ignore

            self._conn = snowflake.connector.connect(
                account=self.account,
                user=self.user,
                password=self.password,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema,
                role=self.role,
            )
            logger.info(
                "Connected to Snowflake account=%s db=%s",
                self.account,
                self.database,
            )
            return self._conn
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Snowflake connect failed (%s); switching to offline mode",
                redact_secrets(str(exc)),
            )
            self.mode = "offline"
            return None

    def ensure_schema(self) -> None:
        ddl_path = os.path.join(
            os.path.dirname(__file__),
            "..",
            "..",
            "..",
            "sql",
            "snowflake",
            "ddl.sql",
        )
        self.plan.append(PlannedWrite("ensure_schema", f"{self.database}.{self.schema}", 0))
        conn = self._connect()
        if conn is None:
            logger.info("[offline] would apply DDL from %s", ddl_path)
            return
        if os.path.isfile(ddl_path):
            sql = open(ddl_path, encoding="utf-8").read()
            cur = conn.cursor()
            for stmt in sql.split(";"):
                stmt = stmt.strip()
                if stmt:
                    cur.execute(stmt)
            cur.close()

    def upsert_dim(self, table: str, rows: Iterable[dict[str, Any]], key: str) -> int:
        row_list = list(rows)
        sample = [str(r.get(key)) for r in row_list[:5]]
        self.plan.append(PlannedWrite("upsert_dim", table, len(row_list), sample))
        conn = self._connect()
        if conn is None:
            logger.info("[offline] MERGE INTO %s (%s rows) key=%s", table, len(row_list), key)
            return len(row_list)
        raise NotImplementedError(
            "Live Snowflake MERGE not implemented here; extend with COPY+MERGE or keep offline mode."
        )

    def append_fact(self, table: str, rows: Iterable[dict[str, Any]]) -> int:
        row_list = list(rows)
        sample = [str(r.get("event_id", r.get("order_id"))) for r in row_list[:5]]
        self.plan.append(PlannedWrite("append_fact", table, len(row_list), sample))
        conn = self._connect()
        if conn is None:
            logger.info("[offline] INSERT INTO %s (%s rows)", table, len(row_list))
            return len(row_list)
        raise NotImplementedError(
            "Live Snowflake INSERT not implemented here; extend with COPY INTO or keep offline mode."
        )

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
