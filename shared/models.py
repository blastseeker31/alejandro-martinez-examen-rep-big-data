"""Modelo validado de las lecturas de AgroStream IoT."""

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class MeasurementType(StrEnum):
    TEMPERATURE = "temperature"
    SOIL_MOISTURE = "soil_moisture"
    AIR_HUMIDITY = "air_humidity"
    PH = "ph"


class AnomalyType(StrEnum):
    BELOW_MINIMUM = "below_minimum"
    ABOVE_MAXIMUM = "above_maximum"


class SourceType(StrEnum):
    WEB_SINGLE = "web_single"
    WEB_BATCH = "web_batch"
    LOAD_TEST = "load_test"


def classify_anomaly(value: float, safe_min: float, safe_max: float) -> AnomalyType | None:
    if value < safe_min:
        return AnomalyType.BELOW_MINIMUM
    if value > safe_max:
        return AnomalyType.ABOVE_MAXIMUM
    return None


class SensorReading(BaseModel):
    """Contrato versionado que viaja como JSON por Kafka."""

    model_config = ConfigDict(extra="forbid", use_enum_values=True)

    event_id: UUID
    batch_id: UUID | None = None
    schema_version: str = "1.0"
    parcel_id: str = Field(min_length=3, max_length=32)
    parcel_name: str = Field(min_length=3, max_length=120)
    sensor_id: str = Field(min_length=3, max_length=64)
    sensor_type: str = Field(default="agricultural_probe", min_length=3, max_length=64)
    measurement_type: MeasurementType
    crop_type: str = Field(min_length=2, max_length=64)
    value: float
    unit: str = Field(min_length=1, max_length=16)
    safe_min: float
    safe_max: float
    is_anomaly: bool = False
    anomaly_type: AnomalyType | None = None
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)
    event_timestamp: datetime
    generated_at: datetime
    source: SourceType

    @field_validator("schema_version")
    @classmethod
    def validate_schema_version(cls, value: str) -> str:
        if value != "1.0":
            raise ValueError("schema_version debe ser 1.0")
        return value

    @field_validator("event_timestamp", "generated_at")
    @classmethod
    def validate_utc_timestamp(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
            raise ValueError("la fecha debe estar expresada en UTC")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_safe_range(self) -> "SensorReading":
        if self.safe_min > self.safe_max:
            raise ValueError("safe_min no puede ser mayor que safe_max")
        return self

    def recalculated_anomaly(self) -> "SensorReading":
        anomaly = classify_anomaly(self.value, self.safe_min, self.safe_max)
        return self.model_copy(update={"is_anomaly": anomaly is not None, "anomaly_type": anomaly})

    def as_kafka_value(self) -> dict[str, Any]:
        return self.model_dump(mode="json")
