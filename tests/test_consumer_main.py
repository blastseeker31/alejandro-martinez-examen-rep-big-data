from __future__ import annotations

from pathlib import Path

import consumer.main as consumer_main
from consumer.processor import ProcessOutcome


class FakeMessage:
    def __init__(self, offset: int, partition: int = 2):
        self._offset = offset
        self._partition = partition

    def topic(self) -> str:
        return "agro.sensor-readings"

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def error(self):
        return None


class FakeConsumer:
    def __init__(self, messages: list[FakeMessage]):
        self.messages = iter(messages)
        self.commits: list[int] = []
        self.seeks: list[int] = []

    def subscribe(self, topics):
        self.topics = topics

    def poll(self, _timeout):
        try:
            return next(self.messages)
        except StopIteration:
            return None

    def commit(self, message, asynchronous):
        self.commits.append(message.offset())

    def seek(self, partition):
        self.seeks.append(partition.offset)

    def close(self):
        pass


class FakeDependency:
    def close(self):
        pass


class FlakyProcessor:
    def __init__(self, _store, _alert_producer, _dlq_producer):
        self.attempts: list[int] = []

    def process(self, message):
        self.attempts.append(message.offset())
        if message.offset() == 10 and self.attempts.count(10) == 1:
            raise RuntimeError("MongoDB temporalmente no disponible")
        return ProcessOutcome.PROCESSED


def test_transient_failure_retries_same_message_before_commit():
    message = FakeMessage(offset=10)
    consumer = FakeConsumer([])
    processor = FlakyProcessor(None, None, None)

    outcome = consumer_main.process_message_with_retries(
        consumer,
        processor,
        message,
        is_running=lambda: True,
        wait=lambda _seconds: None,
        retry_base_seconds=0,
        retry_max_seconds=0,
    )

    assert outcome == ProcessOutcome.PROCESSED
    assert processor.attempts == [10, 10]
    assert consumer.seeks == [10]
    assert consumer.commits == [10]


def test_consumer_does_not_commit_later_partition_offset_before_recovery(
    monkeypatch, tmp_path: Path
):
    messages = [FakeMessage(offset=10), FakeMessage(offset=11)]
    consumer = FakeConsumer(messages)
    processor_holder = {}

    class Processor(FlakyProcessor):
        def __init__(self, store, alert_producer, dlq_producer):
            super().__init__(store, alert_producer, dlq_producer)
            processor_holder["processor"] = self

    monkeypatch.setattr(consumer_main, "Consumer", lambda _config: consumer)
    monkeypatch.setattr(consumer_main, "EventProducer", lambda _bootstrap: FakeDependency())
    monkeypatch.setattr(consumer_main, "ProcessingStore", FakeDependency)
    monkeypatch.setattr(consumer_main, "SensorEventProcessor", Processor)
    monkeypatch.setattr(consumer_main, "HEARTBEAT_FILE", tmp_path / "heartbeat")
    monkeypatch.setenv("CONSUMER_RETRY_BACKOFF_SECONDS", "0")
    monkeypatch.setenv("CONSUMER_RETRY_MAX_BACKOFF_SECONDS", "0")

    result = consumer_main.run(max_messages=2)

    assert result["processed"] == 2
    assert consumer.commits == [10, 11]
    assert consumer.seeks == [10]
    assert processor_holder["processor"].attempts == [10, 10, 11]
