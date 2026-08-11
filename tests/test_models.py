from datetime import UTC, datetime
from uuid import UUID

import pytest

from shared.models import (
    AnomalyType,
    MeasurementType,
    SensorReading,
    SourceType,
    classify_anomaly,
)


def test_valid_reading_has_required_contract_and_utc_timestamp():
    reading = SensorReading(
        event_id="b5f5c7ef-5a95-4f20-a36f-1f7e0b18fca7",
        parcel_id="HN-ATL-001",
        parcel_name="Finca La Ceiba",
        sensor_id="sensor-001",
        sensor_type="soil_probe",
        measurement_type=MeasurementType.SOIL_MOISTURE,
        crop_type="banano",
        value=42.5,
        unit="%",
        safe_min=25,
        safe_max=70,
        is_anomaly=False,
        anomaly_type=None,
        latitude=15.65,
        longitude=-87.95,
        event_timestamp=datetime.now(UTC),
        generated_at=datetime.now(UTC),
        source=SourceType.WEB_SINGLE,
    )
    assert isinstance(UUID(str(reading.event_id)), UUID)
    assert reading.event_timestamp.tzinfo is not None
    assert reading.schema_version == "1.0"


def test_recalculate_anomaly_classification_is_deterministic():
    assert classify_anomaly(10, 20, 80) == AnomalyType.BELOW_MINIMUM
    assert classify_anomaly(90, 20, 80) == AnomalyType.ABOVE_MAXIMUM
    assert classify_anomaly(50, 20, 80) is None


def test_invalid_safe_range_is_rejected():
    with pytest.raises(ValueError, match="safe_min"):
        SensorReading(
            event_id="b5f5c7ef-5a95-4f20-a36f-1f7e0b18fca7",
            parcel_id="HN-ATL-001",
            parcel_name="Finca La Ceiba",
            sensor_id="sensor-001",
            sensor_type="soil_probe",
            measurement_type="soil_moisture",
            crop_type="banano",
            value=42.5,
            unit="%",
            safe_min=70,
            safe_max=25,
            is_anomaly=False,
            anomaly_type=None,
            latitude=15.65,
            longitude=-87.95,
            event_timestamp=datetime.now(UTC),
            generated_at=datetime.now(UTC),
            source="web_single",
        )


def test_naive_timestamp_is_rejected():
    with pytest.raises(ValueError, match="UTC"):
        SensorReading(
            event_id="b5f5c7ef-5a95-4f20-a36f-1f7e0b18fca7",
            parcel_id="HN-ATL-001",
            parcel_name="Finca La Ceiba",
            sensor_id="sensor-001",
            measurement_type="soil_moisture",
            crop_type="banano",
            value=42.5,
            unit="%",
            safe_min=25,
            safe_max=70,
            latitude=15.65,
            longitude=-87.95,
            event_timestamp=datetime.now(),
            generated_at=datetime.now(UTC),
            source="web_single",
        )
