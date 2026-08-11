import json

from shared.generator import GenerationRequest, generate_batch
from shared.models import SensorReading
from shared.serialization import deserialize_reading, serialize_reading


def test_reading_serialization_round_trip():
    reading = generate_batch(GenerationRequest(count=1, seed=99)).events[0]
    encoded = serialize_reading(reading)
    assert isinstance(encoded, bytes)
    decoded = deserialize_reading(encoded)
    assert isinstance(decoded, SensorReading)
    assert decoded.event_id == reading.event_id
    assert decoded.value == reading.value


def test_invalid_json_is_reported_as_validation_error():
    try:
        deserialize_reading(json.dumps({"value": "bad"}).encode())
    except ValueError as exc:
        assert "event_id" in str(exc)
    else:
        raise AssertionError("Se esperaba un error de validación")
