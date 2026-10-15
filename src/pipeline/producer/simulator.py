"""Clickstream producer simulator.

Publishes JSON events to Kafka (or writes to a local NDJSON file when
``KAFKA_BOOTSTRAP`` is ``file`` / unreachable). Rate and mix are seeded for
deterministic local and CI runs. Optional SASL settings come from env
(see ``.env.example`` / ``pipeline.security.kafka_security_kwargs``).
"""

from __future__ import annotations

import argparse
import json
import os
import logging
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from uuid import uuid4

from pipeline.schema.events import (
    CUSTOMER_SEED,
    PRODUCT_CATALOG,
    ClickstreamEvent,
    EventType,
)
from pipeline.security import configure_secure_logging, kafka_security_kwargs

logger = logging.getLogger(__name__)

# Funnel-ish weights: more views than purchases.
EVENT_WEIGHTS: list[tuple[EventType, float]] = [
    (EventType.PAGE_VIEW, 0.30),
    (EventType.PRODUCT_VIEW, 0.25),
    (EventType.SEARCH, 0.10),
    (EventType.ADD_TO_CART, 0.15),
    (EventType.REMOVE_FROM_CART, 0.05),
    (EventType.CHECKOUT_START, 0.08),
    (EventType.PURCHASE, 0.07),
]


class ClickstreamSimulator:
    def __init__(
        self,
        seed: int = 42,
        topic: str = "clickstream.events",
        bootstrap: Optional[str] = None,
        output_path: Optional[str] = None,
    ) -> None:
        self.rng = random.Random(seed)
        self.topic = topic
        self.bootstrap = bootstrap or os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
        self.output_path = Path(output_path or os.getenv("PRODUCER_OUTPUT", "data/raw/clickstream.ndjson"))
        self._producer = None
        self._types, self._weights = zip(*EVENT_WEIGHTS)

    def _choose_type(self) -> EventType:
        return self.rng.choices(self._types, weights=self._weights, k=1)[0]

    def generate_event(self) -> ClickstreamEvent:
        """Generate one realistic clickstream event. O(1)."""
        customer = self.rng.choice(CUSTOMER_SEED)
        product = self.rng.choice(PRODUCT_CATALOG)
        et = self._choose_type()
        session_id = f"S-{self.rng.randint(1000, 9999)}"
        qty = self.rng.randint(1, 3) if et in {EventType.ADD_TO_CART, EventType.PURCHASE} else None

        meta: dict[str, Any] = {}
        if et == EventType.PURCHASE:
            meta["order_id"] = f"O-{uuid4().hex[:10]}"

        return ClickstreamEvent(
            event_type=et,
            event_ts=datetime.now(timezone.utc),
            session_id=session_id,
            customer_id=customer["customer_id"],
            product_id=product["product_id"] if et != EventType.PAGE_VIEW else None,
            page_url=f"/products/{product['product_id']}" if et != EventType.SEARCH else "/search",
            search_query="wireless" if et == EventType.SEARCH else None,
            quantity=qty,
            device=self.rng.choice(["web", "ios", "android"]),
            country=customer["country"],
            referrer=self.rng.choice([None, "google", "email", "direct"]),
            metadata=meta,
        )

    def _get_kafka_producer(self):
        if self._producer is not None:
            return self._producer
        # Explicit file-only mode for Airflow land tasks / offline local runs.
        if not self.bootstrap or self.bootstrap in {"file", "none", "__force_file__"}:
            return None
        try:
            from kafka import KafkaProducer  # type: ignore

            kafka_kwargs = {
                "bootstrap_servers": self.bootstrap,
                "value_serializer": lambda v: json.dumps(v).encode("utf-8"),
                "key_serializer": lambda k: k.encode("utf-8") if k else None,
                "acks": "all",
                "retries": 3,
                "request_timeout_ms": 3000,
            }
            kafka_kwargs.update(kafka_security_kwargs())
            self._producer = KafkaProducer(**kafka_kwargs)
            logger.info("Kafka producer connected to %s", self.bootstrap)
            return self._producer
        except Exception as exc:  # noqa: BLE001 — NDJSON fallback for local stack
            logger.warning("Kafka unavailable (%s); writing NDJSON to %s", exc, self.output_path)
            return None

    def publish(self, event: ClickstreamEvent) -> None:
        payload = event.to_kafka_dict()
        producer = self._get_kafka_producer()
        if producer is not None:
            key = event.session_id
            producer.send(self.topic, key=key, value=payload)
            return
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        with self.output_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(payload) + "\n")

    def run(self, count: int = 100, rate_per_sec: float = 10.0, flush: bool = True) -> int:
        """Emit ``count`` events at approximately ``rate_per_sec``. Returns published count."""
        delay = 1.0 / rate_per_sec if rate_per_sec > 0 else 0.0
        published = 0
        for _ in range(count):
            event = self.generate_event()
            self.publish(event)
            published += 1
            if delay:
                time.sleep(delay)
        if flush and self._producer is not None:
            self._producer.flush()
        logger.info("Published %s events", published)
        return published


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Clickstream event simulator")
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--rate", type=float, default=20.0, help="events/sec")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--topic", default="clickstream.events")
    parser.add_argument("--bootstrap", default=None)
    parser.add_argument("--output", default=None, help="NDJSON fallback path")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    configure_secure_logging(logging.DEBUG if args.verbose else logging.INFO)
    sim = ClickstreamSimulator(
        seed=args.seed,
        topic=args.topic,
        bootstrap=args.bootstrap,
        output_path=args.output,
    )
    return 0 if sim.run(count=args.count, rate_per_sec=args.rate) else 1


if __name__ == "__main__":
    raise SystemExit(main())
