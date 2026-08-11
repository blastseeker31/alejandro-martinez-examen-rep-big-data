"""Crea los topics de AgroStream de forma idempotente."""

from __future__ import annotations

import os
import sys
import time

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

TOPICS = {
    "agro.sensor-readings": 6,
    "agro.sensor-readings-dlq": 3,
    "agro.alerts": 3,
}


def main() -> int:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    admin = AdminClient({"bootstrap.servers": bootstrap, "socket.timeout.ms": 5000})
    for attempt in range(1, 31):
        try:
            metadata = admin.list_topics(timeout=5)
            missing = [
                NewTopic(name, num_partitions=partitions, replication_factor=1)
                for name, partitions in TOPICS.items()
                if name not in metadata.topics
            ]
            if missing:
                futures = admin.create_topics(missing)
                for name, future in futures.items():
                    try:
                        future.result()
                        print(f"topic_created={name}")
                    except KafkaException as exc:
                        if "TopicExists" not in str(exc):
                            raise
            print("topics_ready=" + ",".join(sorted(TOPICS)))
            return 0
        except Exception as exc:  # broker puede tardar en iniciar
            print(f"topic_init_attempt={attempt} error={exc}")
            time.sleep(2)
    print("No fue posible conectar con Kafka después de 30 intentos", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
