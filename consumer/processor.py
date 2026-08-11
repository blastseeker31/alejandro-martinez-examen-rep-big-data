"""Lógica determinista de procesamiento de un mensaje Kafka."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import ValidationError

from shared.serialization import deserialize_reading

READINGS_TOPIC = "agro.sensor-readings"
DLQ_TOPIC = "agro.sensor-readings-dlq"
ALERTS_TOPIC = "agro.alerts"


class ProcessOutcome(StrEnum):
    PROCESSED = "processed"
    DUPLICATE = "duplicate"
    INVALID = "invalid"


class SensorEventProcessor:
    """Procesa lecturas con semántica at-least-once.

    La publicación de alertas y la actualización de MongoDB no forman una
    transacción distribuida. Si el proceso cae entre ambas operaciones, una
    alerta puede publicarse más de una vez; nunca se promete exactly-once.
    """

    def __init__(self, store: Any, alert_producer: Any, dlq_producer: Any):
        self.store = store
        self.alert_producer = alert_producer
        self.dlq_producer = dlq_producer

    @staticmethod
    def _metadata(message: Any) -> dict[str, Any]:
        return {
            "source_topic": message.topic(),
            "source_partition": message.partition(),
            "source_offset": message.offset(),
        }

    @staticmethod
    def _publish_checked(producer: Any, payload: dict[str, Any], topic: str) -> None:
        result = producer.publish_one(payload, topic=topic)
        if not result.confirmed:
            raise RuntimeError(result.error or f"No se confirmó publicación en {topic}")

    def _alert_payload(self, reading: dict[str, Any], message: Any) -> dict[str, Any]:
        return {
            **reading,
            "alert_type": reading.get("anomaly_type"),
            "alerted_at": datetime.now(UTC).isoformat(),
            **self._metadata(message),
        }

    def _publish_alert_for_reading(self, reading: dict[str, Any], message: Any) -> None:
        self._publish_checked(
            self.alert_producer,
            self._alert_payload(reading, message),
            ALERTS_TOPIC,
        )
        self.store.mark_alert_published(str(reading["event_id"]))

    def _send_invalid_to_dlq(self, message: Any, exc: ValidationError) -> None:
        raw = message.value() or b""
        dlq_payload = {
            "original_message": raw.decode("utf-8", errors="replace"),
            "error_description": str(exc),
            "error_at": datetime.now(UTC).isoformat(),
            "created_at": datetime.now(UTC),
            "processing_stage": "consumer_validation",
            **self._metadata(message),
        }
        first_observation = self.store.save_processing_error(dlq_payload)
        # Se publica también en cada redelivery para mantener la DLQ disponible
        # si el proceso cayó después del upsert y antes de confirmar Kafka.
        self._publish_checked(self.dlq_producer, dlq_payload, DLQ_TOPIC)
        if first_observation:
            self.store.increment_metric(invalid=1)

    def process(self, message: Any) -> ProcessOutcome:
        try:
            reading = deserialize_reading(message.value() or b"").recalculated_anomaly()
        except ValidationError as exc:
            self._send_invalid_to_dlq(message, exc)
            return ProcessOutcome.INVALID

        document = reading.model_dump(mode="json")
        document.update(
            {
                **self._metadata(message),
                "processed_at": datetime.now(UTC),
                "alert_published": False,
            }
        )
        inserted = self.store.insert_reading(document)
        if not inserted:
            existing = self.store.get_reading(str(reading.event_id))
            if (
                existing
                and existing.get("is_anomaly")
                and not existing.get("alert_published", False)
            ):
                self._publish_alert_for_reading(existing, message)
            self.store.increment_metric(duplicate=1)
            return ProcessOutcome.DUPLICATE

        self.store.increment_aggregate(reading)
        self.store.increment_metric(processed=1, anomaly=int(reading.is_anomaly))
        if reading.is_anomaly:
            self._publish_alert_for_reading(document, message)
        return ProcessOutcome.PROCESSED
