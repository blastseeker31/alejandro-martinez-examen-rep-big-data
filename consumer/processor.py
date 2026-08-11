"""Lógica determinista de procesamiento de un mensaje Kafka."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from shared.serialization import deserialize_reading

READINGS_TOPIC = "agro.sensor-readings"
DLQ_TOPIC = "agro.sensor-readings-dlq"
ALERTS_TOPIC = "agro.alerts"


class ProcessOutcome(StrEnum):
    PROCESSED = "processed"
    DUPLICATE = "duplicate"
    INVALID = "invalid"


class SensorEventProcessor:
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

    def _send_invalid_to_dlq(self, message: Any, exc: Exception) -> None:
        raw = message.value() or b""
        dlq_payload = {
            "original_message": raw.decode("utf-8", errors="replace"),
            "error_description": str(exc),
            "error_at": datetime.now(UTC).isoformat(),
            "processing_stage": "consumer_validation",
            **self._metadata(message),
        }
        self.store.save_processing_error(dlq_payload)
        self._publish_checked(self.dlq_producer, dlq_payload, DLQ_TOPIC)
        self.store.increment_metric(invalid=1)

    def process(self, message: Any) -> ProcessOutcome:
        try:
            reading = deserialize_reading(message.value() or b"").recalculated_anomaly()
        except Exception as exc:
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
            self.store.increment_metric(duplicate=1)
            return ProcessOutcome.DUPLICATE

        self.store.increment_aggregate(reading)
        self.store.increment_metric(processed=1, anomaly=int(reading.is_anomaly))
        if reading.is_anomaly:
            alert_payload = {
                **reading.model_dump(mode="json"),
                "alert_type": reading.anomaly_type,
                "alerted_at": datetime.now(UTC).isoformat(),
                **self._metadata(message),
            }
            self._publish_checked(self.alert_producer, alert_payload, ALERTS_TOPIC)
            self.store.mark_alert_published(str(reading.event_id))
        return ProcessOutcome.PROCESSED
