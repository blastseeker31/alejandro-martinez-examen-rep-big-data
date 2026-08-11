"""Productor Kafka con confirmación de entrega y configuración idempotente."""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from confluent_kafka import Producer

from shared.serialization import serialize_reading


@dataclass(frozen=True)
class DeliveryResult:
    confirmed: bool
    partition: int | None = None
    offset: int | None = None
    error: str | None = None


def build_producer_config(bootstrap_servers: str) -> dict[str, Any]:
    return {
        "bootstrap.servers": bootstrap_servers,
        "acks": "all",
        "enable.idempotence": True,
        "retries": int(os.getenv("PRODUCER_RETRIES", "8")),
        "retry.backoff.ms": 250,
        "compression.type": os.getenv("PRODUCER_COMPRESSION_TYPE", "lz4"),
        "linger.ms": int(os.getenv("PRODUCER_LINGER_MS", "10")),
        "batch.size": int(os.getenv("PRODUCER_BATCH_SIZE", "131072")),
        "delivery.timeout.ms": int(os.getenv("PRODUCER_DELIVERY_TIMEOUT_MS", "120000")),
    }


class EventProducer:
    def __init__(self, bootstrap_servers: str, client: Any | None = None):
        self.client = client or Producer(build_producer_config(bootstrap_servers))

    @staticmethod
    def _encode(payload: Any) -> bytes:
        if hasattr(payload, "model_dump_json"):
            return serialize_reading(payload)
        return json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")

    def publish_one(self, payload: Any, topic: str = "agro.sensor-readings") -> DeliveryResult:
        results = self.publish_many([payload], topic=topic)
        return results[0]

    def publish_many(
        self,
        payloads: list[Any],
        topic: str = "agro.sensor-readings",
        poll_timeout: float = 0.1,
    ) -> list[DeliveryResult]:
        pending: list[DeliveryResult | None] = [None] * len(payloads)

        def callback(index: int) -> Callable[[Any, Any], None]:
            def on_delivery(error: Any, message: Any) -> None:
                if error is not None:
                    pending[index] = DeliveryResult(confirmed=False, error=str(error))
                    return
                pending[index] = DeliveryResult(
                    confirmed=True,
                    partition=message.partition(),
                    offset=message.offset(),
                )

            return on_delivery

        for index, payload in enumerate(payloads):
            key = None
            if isinstance(payload, dict):
                key = payload.get("parcel_id") or "invalid"
            else:
                key = getattr(payload, "parcel_id", "invalid")
            encoded_key = str(key).encode("utf-8") if key is not None else None
            encoded_value = self._encode(payload)
            while True:
                try:
                    self.client.produce(
                        topic=topic,
                        key=encoded_key,
                        value=encoded_value,
                        callback=callback(index),
                    )
                    self.client.poll(0)
                    break
                except BufferError:
                    self.client.poll(0.5)
        self.client.flush(max(1, int(os.getenv("PRODUCER_FLUSH_TIMEOUT_SECONDS", "120"))))
        deadline_polls = max(1, len(payloads) * 10)
        for _ in range(deadline_polls):
            if all(result is not None for result in pending):
                break
            self.client.poll(poll_timeout)
        return [
            result or DeliveryResult(confirmed=False, error="delivery callback timeout")
            for result in pending
        ]

    def ping(self) -> bool:
        self.client.list_topics(timeout=5)
        return True

    def close(self) -> None:
        self.client.flush(10)
