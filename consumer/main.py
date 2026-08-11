"""Proceso Kafka escalable con offsets manuales y apagado ordenado.

Los fallos de infraestructura se reintentan dentro del mismo mensaje. No se
confirma ni se vuelve a hacer ``poll`` para esa partición hasta que el mensaje
termine correctamente; si el proceso recibe SIGTERM durante los reintentos,
se cierra sin confirmar el offset y Kafka lo redeliverá posteriormente.

``Consumer.commit(message=...)`` confirma el offset siguiente al mensaje
indicado. Por eso un mensaje posterior no puede confirmarse mientras el
actual siga en el ciclo interno de reintentos.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import time
from collections import defaultdict
from collections.abc import Callable
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer, TopicPartition

from consumer.mongo_store import ProcessingStore
from consumer.processor import SensorEventProcessor
from producer_api.kafka_producer import EventProducer

READINGS_TOPIC = "agro.sensor-readings"
GROUP_ID = "agro-sensor-processors"
HEARTBEAT_FILE = Path(os.getenv("CONSUMER_HEARTBEAT_FILE", "/tmp/agrostream_consumer_heartbeat"))

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(message)s")
logger = logging.getLogger("agrostream.consumer")


class ConsumerStopping(RuntimeError):
    """Indica que el proceso debe detenerse sin confirmar el mensaje actual."""


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


def _retry_delay(attempt: int, base_seconds: float, max_seconds: float) -> float:
    return min(base_seconds * (2 ** (attempt - 1)), max_seconds)


def process_message_with_retries(
    consumer: Any,
    processor: Any,
    message: Any,
    *,
    is_running: Callable[[], bool],
    wait: Callable[[float], None] = time.sleep,
    retry_base_seconds: float | None = None,
    retry_max_seconds: float | None = None,
) -> Any:
    """Procesa y confirma un mensaje, sin liberar la partición en caso de error.

    ``processor.process`` ya convierte únicamente errores de validación del
    payload en ``ProcessOutcome.INVALID``. Cualquier excepción que escape se
    considera transitoria o de infraestructura: se reposiciona la partición,
    se aplica backoff y se reintenta el mismo mensaje. No existe un límite que
    descarte el mensaje; al detenerse el proceso el offset permanece pendiente.
    """
    base_seconds = (
        retry_base_seconds
        if retry_base_seconds is not None
        else float(os.getenv("CONSUMER_RETRY_BACKOFF_SECONDS", "1"))
    )
    max_seconds = (
        retry_max_seconds
        if retry_max_seconds is not None
        else float(os.getenv("CONSUMER_RETRY_MAX_BACKOFF_SECONDS", "30"))
    )
    attempt = 0
    trace = {"partition": message.partition(), "offset": message.offset()}

    while is_running():
        try:
            outcome = processor.process(message)
            consumer.commit(message=message, asynchronous=False)
            return outcome
        except Exception as exc:
            attempt += 1
            try:
                consumer.seek(
                    TopicPartition(message.topic(), message.partition(), message.offset())
                )
            except Exception as seek_exc:
                _log("processing_seek_error", error=str(seek_exc), **trace)
            delay = _retry_delay(attempt, base_seconds, max_seconds)
            _log(
                "processing_retry",
                attempt=attempt,
                backoff_seconds=delay,
                error=str(exc),
                **trace,
            )
            wait(delay)

    raise ConsumerStopping(
        f"shutdown durante reintentos de partition={message.partition()} offset={message.offset()}"
    )


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
            try:
                outcome = process_message_with_retries(
                    consumer,
                    processor,
                    message,
                    is_running=lambda: running,
                )
            except ConsumerStopping:
                break
            processed += 1
            counts[outcome.value] += 1
            _log(
                "message_committed",
                outcome=outcome.value,
                partition=message.partition(),
                offset=message.offset(),
            )
    finally:
        consumer.close()
        event_producer.close()
        dlq_producer.close()
        store.close()
        _log("consumer_stopped", processed=processed, counts=dict(counts))
    return {"processed": processed, **dict(counts)}


if __name__ == "__main__":
    raise SystemExit(run())
