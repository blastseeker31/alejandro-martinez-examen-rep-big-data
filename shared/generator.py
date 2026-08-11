"""Generación reproducible de datos agrícolas con escenarios y errores controlados."""

import random
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from shared.models import MeasurementType, SensorReading, SourceType, classify_anomaly
from shared.parcels import PARCELS, Parcel


class Scenario(StrEnum):
    STABLE = "stable"
    HEAT_WAVE = "heat_wave"
    IRRIGATION_FAILURE = "irrigation_failure"
    HEAVY_RAIN = "heavy_rain"
    MIXED = "mixed"


class GenerationRequest(BaseModel):
    count: int = Field(ge=1, le=10000)
    scenario: Scenario = Scenario.MIXED
    anomaly_percent: float = Field(default=0, ge=0, le=100)
    duplicate_percent: float = Field(default=0, ge=0, le=100)
    malformed_percent: float = Field(default=0, ge=0, le=100)
    seed: int | None = None
    source: SourceType = SourceType.WEB_BATCH


class GenerationStats(BaseModel):
    requested: int
    anomalies_requested: int
    duplicates_requested: int
    malformed_requested: int


class GenerationResult(BaseModel):
    batch_id: UUID
    events: list[SensorReading | dict[str, Any]]
    stats: GenerationStats


def _measurement_meta(measurement: MeasurementType) -> tuple[str, str]:
    return {
        MeasurementType.TEMPERATURE: ("°C", "temperature_probe"),
        MeasurementType.SOIL_MOISTURE: ("%", "soil_probe"),
        MeasurementType.AIR_HUMIDITY: ("%", "humidity_probe"),
        MeasurementType.PH: ("pH", "ph_probe"),
    }[measurement]


def _base_payload(
    parcel: Parcel,
    measurement: MeasurementType,
    rng: random.Random,
    batch_id: UUID,
    source: SourceType,
) -> dict[str, Any]:
    safe_min, safe_max = parcel.safe_ranges[measurement]
    center = (safe_min + safe_max) / 2
    spread = (safe_max - safe_min) / 7
    hour = rng.randrange(0, 24)
    daily_cycle = ((hour - 13) ** 2) / 169
    value = center + rng.gauss(0, spread) - daily_cycle * spread * 0.35
    unit, sensor_type = _measurement_meta(measurement)
    timestamp = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(
        hours=rng.randrange(0, 72)
    )
    event_id = uuid4()
    return {
        "event_id": event_id,
        "batch_id": batch_id,
        "schema_version": "1.0",
        "parcel_id": parcel.parcel_id,
        "parcel_name": parcel.parcel_name,
        "sensor_id": f"{parcel.parcel_id}-S{rng.randint(1, parcel.sensor_count):02d}",
        "sensor_type": sensor_type,
        "measurement_type": measurement,
        "crop_type": parcel.crop_type,
        "value": value,
        "unit": unit,
        "safe_min": safe_min,
        "safe_max": safe_max,
        "is_anomaly": False,
        "anomaly_type": None,
        "latitude": parcel.latitude,
        "longitude": parcel.longitude,
        "event_timestamp": timestamp,
        "generated_at": datetime.now(UTC),
        "source": source,
    }


def _apply_scenario(
    payload: dict[str, Any], parcel: Parcel, scenario: Scenario, rng: random.Random
) -> None:
    measurement = MeasurementType(payload["measurement_type"])
    low, high = parcel.safe_ranges[measurement]
    if scenario is Scenario.HEAT_WAVE and measurement is MeasurementType.TEMPERATURE:
        payload["value"] = high + rng.uniform(1.0, 7.0)
    elif scenario is Scenario.IRRIGATION_FAILURE and measurement is MeasurementType.SOIL_MOISTURE:
        payload["value"] = low - rng.uniform(1.0, 18.0)
    elif scenario is Scenario.HEAVY_RAIN:
        if measurement is MeasurementType.SOIL_MOISTURE:
            payload["value"] = high + rng.uniform(1.0, 15.0)
        elif measurement is MeasurementType.AIR_HUMIDITY:
            payload["value"] = high + rng.uniform(1.0, 6.0)
    elif scenario is Scenario.MIXED and rng.random() < 0.18:
        payload["value"] = (
            high + rng.uniform(1.0, 5.0) if rng.random() < 0.5 else low - rng.uniform(1.0, 5.0)
        )


def _make_valid_event(
    parcel: Parcel,
    batch_id: UUID,
    request: GenerationRequest,
    rng: random.Random,
    force_anomaly: bool,
) -> SensorReading:
    measurement = rng.choice(list(MeasurementType))
    payload = _base_payload(parcel, measurement, rng, batch_id, request.source)
    if request.scenario is not Scenario.STABLE:
        _apply_scenario(payload, parcel, request.scenario, rng)
    if (
        force_anomaly
        and classify_anomaly(payload["value"], payload["safe_min"], payload["safe_max"]) is None
    ):
        if rng.random() < 0.5:
            payload["value"] = payload["safe_min"] - rng.uniform(0.5, 5.0)
        else:
            payload["value"] = payload["safe_max"] + rng.uniform(0.5, 5.0)
    anomaly = classify_anomaly(payload["value"], payload["safe_min"], payload["safe_max"])
    payload["is_anomaly"] = anomaly is not None
    payload["anomaly_type"] = anomaly
    return SensorReading.model_validate(payload)


def generate_malformed_payload(kind: str) -> dict[str, Any]:
    batch_id = uuid4()
    parcel = PARCELS[0]
    request = GenerationRequest(count=1, source=SourceType.WEB_BATCH)
    payload = _make_valid_event(parcel, batch_id, request, random.Random(5), False).model_dump(
        mode="json"
    )
    if kind == "text_value":
        payload["value"] = "not-a-number"
    elif kind == "missing_parcel":
        payload.pop("parcel_id")
    elif kind == "bad_date":
        payload["event_timestamp"] = "fecha-invalida"
    elif kind == "inverted_range":
        payload["safe_min"], payload["safe_max"] = payload["safe_max"], payload["safe_min"]
    elif kind == "unknown_measurement":
        payload["measurement_type"] = "wind_speed"
    else:
        raise ValueError(f"Tipo de malformado no soportado: {kind}")
    return payload


def generate_batch(request: GenerationRequest) -> GenerationResult:
    rng = random.Random(request.seed)
    batch_id = uuid4()
    malformed_count = round(request.count * request.malformed_percent / 100)
    duplicate_count = round((request.count - malformed_count) * request.duplicate_percent / 100)
    valid_count = request.count - malformed_count
    unique_count = max(0, valid_count - duplicate_count)
    anomaly_count = round(unique_count * request.anomaly_percent / 100)
    generated: list[SensorReading | dict[str, Any]] = []
    valid_events: list[SensorReading] = []
    for index in range(unique_count):
        parcel = rng.choices(PARCELS, weights=[p.size_hectares**0.35 for p in PARCELS], k=1)[0]
        valid_events.append(
            _make_valid_event(parcel, batch_id, request, rng, index < anomaly_count)
        )
    generated.extend(valid_events)
    if duplicate_count and valid_events:
        generated.extend(rng.choices(valid_events, k=duplicate_count))
    malformed_kinds = [
        "text_value",
        "missing_parcel",
        "bad_date",
        "inverted_range",
        "unknown_measurement",
    ]
    for index in range(malformed_count):
        generated.append(generate_malformed_payload(malformed_kinds[index % len(malformed_kinds)]))
    rng.shuffle(generated)
    return GenerationResult(
        batch_id=batch_id,
        events=generated,
        stats=GenerationStats(
            requested=request.count,
            anomalies_requested=anomaly_count,
            duplicates_requested=duplicate_count,
            malformed_requested=malformed_count,
        ),
    )
