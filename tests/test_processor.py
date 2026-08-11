
from consumer.processor import ProcessOutcome, SensorEventProcessor
from shared.generator import GenerationRequest, generate_batch


class FakeMessage:
    def __init__(self, value, partition=1, offset=22, topic="agro.sensor-readings"):
        self._value = value
        self._partition = partition
        self._offset = offset
        self._topic = topic

    def value(self):
        return self._value

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def topic(self):
        return self._topic


class FakeStore:
    def __init__(self, duplicate=False):
        self.duplicate = duplicate
        self.readings = []
        self.errors = []
        self.metrics = []
        self.aggregates = []
        self.alert_flags = []

    def insert_reading(self, document):
        if self.duplicate:
            return False
        self.readings.append(document)
        return True

    def save_processing_error(self, document):
        self.errors.append(document)

    def increment_metric(self, **kwargs):
        self.metrics.append(kwargs)

    def increment_aggregate(self, reading):
        self.aggregates.append(reading)

    def mark_alert_published(self, event_id):
        self.alert_flags.append(event_id)

    def get_reading(self, event_id):
        return None


class FakeProducer:
    def __init__(self):
        self.published = []

    def publish_one(self, payload, topic):
        self.published.append((topic, payload))
        return type("Delivery", (), {"confirmed": True, "error": None})()


def test_processor_recalculates_anomaly_and_stores_before_alert():
    event = generate_batch(GenerationRequest(count=1, seed=8)).events[0]
    event.value = event.safe_max + 5
    event.is_anomaly = False
    message = FakeMessage(event.model_dump_json().encode(), partition=4, offset=9)
    store = FakeStore()
    alerts = FakeProducer()
    processor = SensorEventProcessor(
        store=store, alert_producer=alerts, dlq_producer=FakeProducer()
    )

    outcome = processor.process(message)

    assert outcome == ProcessOutcome.PROCESSED
    assert len(store.readings) == 1
    assert store.readings[0]["is_anomaly"] is True
    assert len(store.aggregates) == 1
    assert alerts.published[0][0] == "agro.alerts"


def test_duplicate_does_not_update_aggregate_or_alert():
    event = generate_batch(GenerationRequest(count=1, seed=9)).events[0]
    message = FakeMessage(event.model_dump_json().encode())
    store = FakeStore(duplicate=True)
    alerts = FakeProducer()
    processor = SensorEventProcessor(
        store=store, alert_producer=alerts, dlq_producer=FakeProducer()
    )

    outcome = processor.process(message)

    assert outcome == ProcessOutcome.DUPLICATE
    assert store.aggregates == []
    assert alerts.published == []
    assert store.metrics[-1]["duplicate"] == 1


def test_invalid_message_is_saved_and_sent_to_dlq():
    raw = b'{"event_id":"bad","value":"texto","event_timestamp":"no-fecha"}'
    store = FakeStore()
    dlq = FakeProducer()
    processor = SensorEventProcessor(store=store, alert_producer=FakeProducer(), dlq_producer=dlq)

    outcome = processor.process(FakeMessage(raw, partition=3, offset=44))

    assert outcome == ProcessOutcome.INVALID
    assert len(store.errors) == 1
    assert dlq.published[0][0] == "agro.sensor-readings-dlq"
    dlq_payload = dlq.published[0][1]
    assert dlq_payload["source_partition"] == 3
    assert dlq_payload["source_offset"] == 44
