from datetime import UTC, datetime
from uuid import uuid4

from fastapi.testclient import TestClient

from producer_api.kafka_producer import EventProducer, build_producer_config
from producer_api.main import create_app
from producer_api.schemas import SingleEventInput, build_single_reading
from shared.models import MeasurementType


class FakeKafkaClient:
    def __init__(self):
        self.produced = []
        self.polled = 0

    def produce(self, **kwargs):
        self.produced.append(kwargs)
        callback = kwargs.get("callback")
        if callback:
            callback(
                None, type("Message", (), {"partition": lambda _: 2, "offset": lambda _: 17})()
            )

    def poll(self, timeout):
        self.polled += 1

    def flush(self, timeout):
        return 0


def test_producer_config_enables_idempotence_and_confirmations():
    config = build_producer_config("kafka:29092")
    assert config["acks"] == "all"
    assert config["enable.idempotence"] is True
    assert config["compression.type"] == "lz4"
    assert config["retries"] > 0


def test_single_input_is_enriched_from_parcel_catalog():
    request = SingleEventInput(
        parcel_id="HN-ATL-001",
        sensor_id="HN-ATL-001-S01",
        measurement_type=MeasurementType.SOIL_MOISTURE,
        value=48.0,
        event_timestamp=datetime.now(UTC),
    )
    reading = build_single_reading(request)
    assert reading.parcel_name == "Finca La Ceiba"
    assert reading.crop_type == "banano"
    assert reading.safe_min < reading.safe_max
    assert reading.source == "web_single"


def test_publish_many_returns_delivery_metadata_for_each_message():
    client = FakeKafkaClient()
    producer = EventProducer("kafka:29092", client=client)
    payloads = [
        {"event_id": str(uuid4()), "parcel_id": "HN-ATL-001", "value": 1},
        {"event_id": str(uuid4()), "parcel_id": "HN-ATL-002", "value": 2},
    ]
    results = producer.publish_many(payloads, topic="agro.sensor-readings")
    assert len(results) == 2
    assert all(item.confirmed for item in results)
    assert all(item.partition == 2 and item.offset == 17 for item in results)
    assert client.polled > 0


def test_health_endpoint_works_with_injected_test_dependencies():
    class FakeStore:
        def ping(self):
            return True

        def close(self):
            return None

    class FakeProducer:
        def ping(self):
            return True

        def close(self):
            return None

    app = create_app(producer=FakeProducer(), store=FakeStore())
    response = TestClient(app).get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"
