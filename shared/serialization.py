"""Serialización JSON usada por Kafka."""

from typing import Any

from shared.models import SensorReading


def serialize_reading(reading: SensorReading | dict[str, Any]) -> bytes:
    if not isinstance(reading, SensorReading):
        reading = SensorReading.model_validate(reading)
    return reading.model_dump_json().encode("utf-8")


def deserialize_reading(value: bytes | str) -> SensorReading:
    if isinstance(value, str):
        value = value.encode("utf-8")
    return SensorReading.model_validate_json(value)
