"""Proceso Kafka escalable con offsets manuales y apagado ordenado."""

from __future__ import annotations

import json
import logging
import os
import signal
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer

from consumer.mongo_store import ProcessingStore
from consumer.processor import SensorEventProcessor
from producer_api.kafka_producer import EventProducer

READINGS_TOPIC = "agro.sensor-readings"
GROUP_ID = "agro-sensor-processors"
HEARTBEAT_FILE = Path(os.getenv("CONSUMER_HEARTBEAT_FILE", "/tmp/agrostream_consumer_heartbeat"))

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(message)s")
logger = logging.getLogger("agrostream.consumer")


def build_consumer_config(bootstrap_servers: str, group_id: str = GROUP_ID) -> dict[str, Any]:
    return {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "auto.offset.reset": "earliest",
        "session.timeout.ms": 10000,
        "max.poll.interval.ms": 300000,
        "partition.assignment.strategy": "cooperative-sticky",
    }


def _log(event: str, **fields: Any) -> None:
    logger.info(json.dumps({"event": event, **fields}, ensure_ascii=False, default=str))


def run(max_messages: int | None = None) -> dict[str, int]:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    consumer = Consumer(build_consumer_config(bootstrap))
    event_producer = EventProducer(bootstrap)
    dlq_producer = EventProducer(bootstrap)
    store = ProcessingStore()
    processor = SensorEventProcessor(store, event_producer, dlq_producer)
    running = True
    processed = 0
    counts = defaultdict(int)
    failures: dict[tuple[int, int], int] = defaultdict(int)

    def stop_handler(signum: int, _frame: Any) -> None:
        nonlocal running
        running = False
        _log("shutdown_signal", signal=signum)

    signal.signal(signal.SIGTERM, stop_handler)
    signal.signal(signal.SIGINT, stop_handler)
    consumer.subscribe([READINGS_TOPIC])
    _log("consumer_started", group_id=GROUP_ID, consumer_id=os.getenv("HOSTNAME", "local"))
    try:
        while running and (max_messages is None or processed < max_messages):
            message = consumer.poll(float(os.getenv("CONSUMER_POLL_TIMEOUT_SECONDS", "1")))
            HEARTBEAT_FILE.write_text(str(time.time()), encoding="utf-8")
            if message is None:
                continue
            if message.error():
                _log("poll_error", error=str(message.error()))
                continue
            trace = {"partition": message.partition(), "offset": message.offset()}
            try:
                outcome = processor.process(message)
                consumer.commit(message=message, asynchronous=False)
                processed += 1
                counts[outcome.value] += 1
                failures.pop((message.partition(), message.offset()), None)
                _log("message_committed", outcome=outcome.value, **trace)
            except Exception as exc:
                key = (message.partition(), message.offset())
                failures[key] += 1
                _log("processing_error", attempt=failures[key], error=str(exc), **trace)
                if failures[key] >= 3:
                    _log("processing_retry_limit_reached", **trace)
                    # No se confirma: el offset queda pendiente para recuperación al reiniciar.
                    failures.pop(key, None)
                    time.sleep(1)
    finally:
        consumer.close()
        event_producer.close()
        dlq_producer.close()
        store.close()
        _log("consumer_stopped", processed=processed, counts=dict(counts))
    return {"processed": processed, **dict(counts)}


if __name__ == "__main__":
    raise SystemExit(run())
