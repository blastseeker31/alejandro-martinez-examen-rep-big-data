"""Persistencia de ejecuciones de ingesta y resumen de métricas."""

from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from pymongo import MongoClient


class MongoStore:
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

    def ping(self) -> bool:
        self.client.admin.command("ping")
        return True

    def save_run(self, run: dict[str, Any]) -> None:
        run = {**run, "updated_at": datetime.now(UTC)}
        self.database.ingestion_runs.replace_one({"batch_id": run["batch_id"]}, run, upsert=True)

    def get_run(self, batch_id: UUID | str) -> dict[str, Any] | None:
        document = self.database.ingestion_runs.find_one({"batch_id": str(batch_id)}, {"_id": 0})
        return document

    def summary(self) -> dict[str, Any]:
        raw = self.database.raw_readings
        return {
            "raw_readings": raw.count_documents({}),
            "anomalies": raw.count_documents({"is_anomaly": True}),
            "processing_errors": self.database.processing_errors.count_documents({}),
            "consumer_metrics": list(
                self.database.consumer_metrics.find({}, {"_id": 0}).sort("updated_at", -1).limit(20)
            ),
        }

    def get_aggregates(
        self, parcel_id: str, measurement_type: str | None = None
    ) -> list[dict[str, Any]]:
        query: dict[str, Any] = {"parcel_id": parcel_id}
        if measurement_type:
            query["measurement_type"] = measurement_type
        return list(self.database.parcel_aggregates.find(query, {"_id": 0}))

    def get_readings(
        self,
        parcel_id: str,
        measurement_type: str | None = None,
        limit: int = 100,
        start_at: datetime | None = None,
        end_at: datetime | None = None,
    ) -> list[dict[str, Any]]:
        query: dict[str, Any] = {"parcel_id": parcel_id}
        if measurement_type:
            query["measurement_type"] = measurement_type
        if start_at or end_at:
            query["event_timestamp"] = {}
            if start_at:
                query["event_timestamp"]["$gte"] = start_at
            if end_at:
                query["event_timestamp"]["$lte"] = end_at
        return list(
            self.database.raw_readings.find(query, {"_id": 0})
            .sort("event_timestamp", -1)
            .limit(min(limit, 500))
        )

    def close(self) -> None:
        self.client.close()
