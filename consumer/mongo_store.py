"""Repositorio MongoDB del consumidor, con índices y operaciones idempotentes."""

from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

from shared.models import SensorReading


class ProcessingStore:
    def __init__(self, uri: str | None = None, database_name: str | None = None):
        self.client = MongoClient(
            uri
            or os.getenv(
                "MONGO_URI",
                "mongodb://agrostream_admin:agrostream_local_only@mongodb:27017/agrostream?authSource=admin",
            ),
            serverSelectionTimeoutMS=3000,
        )
        self.database = self.client[database_name or os.getenv("MONGO_DATABASE", "agrostream")]
        self.consumer_id = os.getenv("CONSUMER_ID", os.getenv("HOSTNAME", "consumer-local"))

    def ping(self) -> bool:
        self.client.admin.command("ping")
        return True

    def insert_reading(self, document: dict[str, Any]) -> bool:
        try:
            self.database.raw_readings.insert_one(document)
            return True
        except DuplicateKeyError:
            return False

    def get_reading(self, event_id: str) -> dict[str, Any] | None:
        return self.database.raw_readings.find_one({"event_id": event_id}, {"_id": 0})

    def save_processing_error(self, document: dict[str, Any]) -> bool:
        """Registra un inválido una sola vez por origen Kafka.

        Devuelve ``True`` únicamente cuando el upsert insertó el primer
        registro. Un redelivery puede republicarse a la DLQ, pero no duplica
        el documento ni incrementa dos veces la métrica de inválidos.
        """
        key = {
            "source_topic": document["source_topic"],
            "source_partition": document["source_partition"],
            "source_offset": document["source_offset"],
        }
        result = self.database.processing_errors.update_one(
            key,
            {"$setOnInsert": document},
            upsert=True,
        )
        return result.upserted_id is not None

    def increment_metric(self, **counts: int) -> None:
        clean_counts = {key: int(value) for key, value in counts.items() if value}
        if not clean_counts:
            return
        self.database.consumer_metrics.update_one(
            {"consumer_id": self.consumer_id},
            {
                "$inc": clean_counts,
                "$set": {"updated_at": datetime.now(UTC)},
                "$setOnInsert": {"consumer_id": self.consumer_id},
            },
            upsert=True,
        )

    def increment_aggregate(self, reading: SensorReading) -> None:
        is_anomaly = bool(reading.is_anomaly)
        above = int(reading.anomaly_type == "above_maximum")
        below = int(reading.anomaly_type == "below_minimum")
        total = {"$add": [{"$ifNull": ["$total_valid_readings", 0]}, 1]}
        anomalous = {"$add": [{"$ifNull": ["$anomalous_readings", 0]}, int(is_anomaly)]}
        pipeline = [
            {
                "$set": {
                    "parcel_id": reading.parcel_id,
                    "parcel_name": reading.parcel_name,
                    "crop_type": reading.crop_type,
                    "measurement_type": reading.measurement_type,
                    "total_valid_readings": total,
                    "safe_readings": {
                        "$add": [{"$ifNull": ["$safe_readings", 0]}, int(not is_anomaly)]
                    },
                    "anomalous_readings": anomalous,
                    "above_maximum_count": {
                        "$add": [{"$ifNull": ["$above_maximum_count", 0]}, above]
                    },
                    "below_minimum_count": {
                        "$add": [{"$ifNull": ["$below_minimum_count", 0]}, below]
                    },
                    "last_reading_at": reading.event_timestamp,
                    "last_value": reading.value,
                    "safe_min": reading.safe_min,
                    "safe_max": reading.safe_max,
                    "updated_at": datetime.now(UTC),
                }
            },
            {
                "$set": {
                    "alert_rate": {
                        "$multiply": [
                            {"$divide": ["$anomalous_readings", "$total_valid_readings"]},
                            100,
                        ]
                    },
                    "risk_level": {
                        "$switch": {
                            "branches": [
                                {
                                    "case": {
                                        "$gte": [
                                            {
                                                "$multiply": [
                                                    {
                                                        "$divide": [
                                                            "$anomalous_readings",
                                                            "$total_valid_readings",
                                                        ]
                                                    },
                                                    100,
                                                ]
                                            },
                                            float(os.getenv("ALERT_RATE_HIGH_PERCENT", "30")),
                                        ]
                                    },
                                    "then": "ALTO",
                                },
                                {
                                    "case": {
                                        "$gte": [
                                            {
                                                "$multiply": [
                                                    {
                                                        "$divide": [
                                                            "$anomalous_readings",
                                                            "$total_valid_readings",
                                                        ]
                                                    },
                                                    100,
                                                ]
                                            },
                                            float(os.getenv("ALERT_RATE_MEDIUM_PERCENT", "10")),
                                        ]
                                    },
                                    "then": "MEDIO",
                                },
                            ],
                            "default": "BAJO",
                        }
                    },
                }
            },
        ]
        self.database.parcel_aggregates.update_one(
            {"parcel_id": reading.parcel_id, "measurement_type": reading.measurement_type},
            pipeline,
            upsert=True,
        )

    def mark_alert_published(self, event_id: str) -> None:
        self.database.raw_readings.update_one(
            {"event_id": event_id},
            {"$set": {"alert_published": True, "alert_published_at": datetime.now(UTC)}},
        )

    def summary(self) -> dict[str, Any]:
        raw = self.database.raw_readings
        return {
            "raw_readings": raw.count_documents({}),
            "anomalies": raw.count_documents({"is_anomaly": True}),
            "duplicates": sum(
                doc.get("duplicate", 0)
                for doc in self.database.consumer_metrics.find({}, {"duplicate": 1})
            ),
            "invalid": self.database.processing_errors.count_documents({}),
            "consumer_metrics": list(self.database.consumer_metrics.find({}, {"_id": 0})),
        }

    def close(self) -> None:
        self.client.close()
