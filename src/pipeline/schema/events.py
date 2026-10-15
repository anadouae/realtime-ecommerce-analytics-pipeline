"""Clickstream and order event contracts for the analytics pipeline.

These schemas are the single source of truth shared by the producer,
Spark transforms, and warehouse DDL. Keep field names snake_case and
stable — they map 1:1 to Kafka JSON payloads and star-schema columns.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator


class EventType(str, Enum):
    PAGE_VIEW = "page_view"
    PRODUCT_VIEW = "product_view"
    ADD_TO_CART = "add_to_cart"
    REMOVE_FROM_CART = "remove_from_cart"
    CHECKOUT_START = "checkout_start"
    PURCHASE = "purchase"
    SEARCH = "search"


# Deterministic reference data used by the simulator and unit tests.
PRODUCT_CATALOG: list[dict[str, Any]] = [
    {"product_id": "P1001", "sku": "SKU-TEE-01", "name": "Organic Tee", "category": "Apparel", "brand": "GreenThread", "unit_price": 29.99},
    {"product_id": "P1002", "sku": "SKU-HOOD-02", "name": "Fleece Hoodie", "category": "Apparel", "brand": "GreenThread", "unit_price": 69.99},
    {"product_id": "P1003", "sku": "SKU-MUG-03", "name": "Ceramic Mug", "category": "Home", "brand": "ClayCo", "unit_price": 14.50},
    {"product_id": "P1004", "sku": "SKU-LAMP-04", "name": "Desk Lamp", "category": "Home", "brand": "LuxLite", "unit_price": 45.00},
    {"product_id": "P1005", "sku": "SKU-BUD-05", "name": "Wireless Buds", "category": "Electronics", "brand": "SonicX", "unit_price": 89.99},
    {"product_id": "P1006", "sku": "SKU-KB-06", "name": "Mech Keyboard", "category": "Electronics", "brand": "SonicX", "unit_price": 129.00},
    {"product_id": "P1007", "sku": "SKU-BAG-07", "name": "Tote Bag", "category": "Accessories", "brand": "CarryAll", "unit_price": 24.00},
    {"product_id": "P1008", "sku": "SKU-WATCH-08", "name": "Smart Watch", "category": "Electronics", "brand": "PulseTech", "unit_price": 199.99},
]

CUSTOMER_SEED: list[dict[str, Any]] = [
    {"customer_id": "C001", "email": "alice@example.com", "country": "FR", "segment": "premium"},
    {"customer_id": "C002", "email": "bob@example.com", "country": "US", "segment": "standard"},
    {"customer_id": "C003", "email": "carla@example.com", "country": "DE", "segment": "premium"},
    {"customer_id": "C004", "email": "dan@example.com", "country": "UK", "segment": "standard"},
    {"customer_id": "C005", "email": "eva@example.com", "country": "ES", "segment": "new"},
    {"customer_id": "C006", "email": "frank@example.com", "country": "FR", "segment": "churn_risk"},
    {"customer_id": "C007", "email": "gina@example.com", "country": "IT", "segment": "standard"},
    {"customer_id": "C008", "email": "hugo@example.com", "country": "US", "segment": "premium"},
]


class ClickstreamEvent(BaseModel):
    """Canonical clickstream envelope published to Kafka topic `clickstream.events`."""

    event_id: str = Field(default_factory=lambda: str(uuid4()))
    event_type: EventType
    event_ts: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    session_id: str
    customer_id: Optional[str] = None
    product_id: Optional[str] = None
    page_url: Optional[str] = None
    search_query: Optional[str] = None
    quantity: Optional[int] = None
    device: str = "web"
    country: Optional[str] = None
    referrer: Optional[str] = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("event_ts", mode="before")
    @classmethod
    def _parse_ts(cls, value: Any) -> Any:
        if isinstance(value, str):
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        return value

    def to_kafka_dict(self) -> dict[str, Any]:
        payload = self.model_dump(mode="json")
        # Normalize to ISO-8601 with Z for cross-language consumers.
        ts = self.event_ts
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        payload["event_ts"] = ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        payload["event_type"] = self.event_type.value
        return payload


class OrderEvent(BaseModel):
    """Order fact derived from PURCHASE clickstream events (or OMS feed)."""

    order_id: str = Field(default_factory=lambda: f"O-{uuid4().hex[:12]}")
    event_id: str
    order_ts: datetime
    customer_id: str
    product_id: str
    quantity: int = Field(ge=1)
    unit_price: float = Field(gt=0)
    currency: str = "EUR"
    status: str = "completed"
    channel: str = "web"

    @property
    def line_total(self) -> float:
        return round(self.quantity * self.unit_price, 2)

    def to_dict(self) -> dict[str, Any]:
        data = self.model_dump(mode="json")
        data["line_total"] = self.line_total
        data["order_ts"] = self.order_ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        return data
