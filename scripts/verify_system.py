"""Verifica servicios y contratos observables sin marcar resultados no ejecutados."""

from __future__ import annotations

import json
import os
from urllib.request import urlopen

from confluent_kafka.admin import AdminClient
from pymongo import MongoClient

TOPICS = {"agro.sensor-readings": 6, "agro.sensor-readings-dlq": 3, "agro.alerts": 3}


def main() -> int:
    api = os.getenv("API_BASE_URL", "http://localhost:8000").rstrip("/")
    failures = []
    try:
        with urlopen(f"{api}/health", timeout=10) as response:
            health = json.loads(response.read())
        print(f"api_health={health}")
        if health.get("status") != "ok":
            failures.append("api")
    except Exception as exc:
        print(f"api_error={exc}")
        failures.append("api")
    try:
        admin = AdminClient(
            {"bootstrap.servers": os.getenv("KAFKA_EXTERNAL_BOOTSTRAP_SERVERS", "localhost:9092")}
        )
        metadata = admin.list_topics(timeout=10)
        for topic, expected_partitions in TOPICS.items():
            actual = len(metadata.topics[topic].partitions) if topic in metadata.topics else 0
            print(f"topic={topic} partitions={actual}")
            if actual != expected_partitions:
                failures.append(topic)
    except Exception as exc:
        print(f"kafka_error={exc}")
        failures.append("kafka")
    try:
        uri = os.getenv(
            "MONGO_URI_LOCAL",
            "mongodb://agrostream_admin:agrostream_local_only@localhost:27017/agrostream?authSource=admin",
        )
        client = MongoClient(uri, serverSelectionTimeoutMS=3000)
        client.admin.command("ping")
        print("mongodb_health=ok")
        client.close()
    except Exception as exc:
        print(f"mongodb_error={exc}")
        failures.append("mongodb")
    if failures:
        print(f"verify=failed components={sorted(set(failures))}")
        return 1
    print("verify=passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
