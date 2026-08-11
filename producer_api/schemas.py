"""Esquemas HTTP y construcción de eventos individuales."""

from datetime import UTC, datetime
from uuid import uuid4

from pydantic import BaseModel, Field, model_validator

from shared.generator import GenerationRequest, Scenario
from shared.models import MeasurementType, SensorReading, SourceType, classify_anomaly
from shared.parcels import get_parcel


class SingleEventInput(BaseModel):
    parcel_id: str
    sensor_id: str | None = None
    measurement_type: MeasurementType
    value: float
    event_timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    safe_min: float | None = None
    safe_max: float | None = None

    @model_validator(mode="after")
    def validate_timestamp_and_range(self) -> "SingleEventInput":
        if self.event_timestamp.tzinfo is None or self.event_timestamp.utcoffset() != UTC.utcoffset(
            self.event_timestamp
        ):
            raise ValueError("event_timestamp debe estar en UTC")
        if (
            self.safe_min is not None
            and self.safe_max is not None
            and self.safe_min > self.safe_max
        ):
            raise ValueError("safe_min no puede ser mayor que safe_max")
        return self


class BatchEventInput(GenerationRequest):
    pass


class LoadTestInput(BaseModel):
    sizes: list[int] = Field(
        default_factory=lambda: [100, 1000, 5000, 10000], min_length=1, max_length=4
    )
    scenario: Scenario = Scenario.MIXED
    anomaly_percent: float = Field(default=15, ge=0, le=100)
    duplicate_percent: float = Field(default=0, ge=0, le=100)
    malformed_percent: float = Field(default=0, ge=0, le=100)
    seed: int | None = None

    @model_validator(mode="after")
    def validate_sizes(self) -> "LoadTestInput":
        if any(size < 1 or size > 10000 for size in self.sizes):
            raise ValueError("cada tamaño debe estar entre 1 y 10000")
        return self


def build_single_reading(request: SingleEventInput) -> SensorReading:
    parcel = get_parcel(request.parcel_id)
    default_min, default_max = parcel.safe_ranges[request.measurement_type]
    safe_min = request.safe_min if request.safe_min is not None else default_min
    safe_max = request.safe_max if request.safe_max is not None else default_max
    anomaly = classify_anomaly(request.value, safe_min, safe_max)
    return SensorReading(
        event_id=uuid4(),
        parcel_id=parcel.parcel_id,
        parcel_name=parcel.parcel_name,
        sensor_id=request.sensor_id or f"{parcel.parcel_id}-S01",
        measurement_type=request.measurement_type,
        crop_type=parcel.crop_type,
        value=request.value,
        unit={
            MeasurementType.TEMPERATURE: "°C",
            MeasurementType.SOIL_MOISTURE: "%",
            MeasurementType.AIR_HUMIDITY: "%",
            MeasurementType.PH: "pH",
        }[request.measurement_type],
        safe_min=safe_min,
        safe_max=safe_max,
        is_anomaly=anomaly is not None,
        anomaly_type=anomaly,
        latitude=parcel.latitude,
        longitude=parcel.longitude,
        event_timestamp=request.event_timestamp,
        generated_at=datetime.now(UTC),
        source=SourceType.WEB_SINGLE,
    )
