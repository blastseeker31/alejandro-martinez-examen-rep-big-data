"""Reconstruye agregaciones desde raw_readings sin reenviar mensajes a Kafka."""

from __future__ import annotations

import os
import sys
import time
from datetime import UTC, datetime
from uuid import uuid4

from pymongo import ASCENDING, MongoClient

from shared.analytics import alert_rate, risk_level


def recompute() -> dict[str, float | int]:
    started = time.monotonic()
    uri = os.getenv(
        "MONGO_URI",
        "mongodb://agrostream_admin:agrostream_local_only@localhost:27017/agrostream?authSource=admin",
    )
    database_name = os.getenv("MONGO_DATABASE", "agrostream")
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    database = client[database_name]
    database.command("ping")
    temporary_name = f"parcel_aggregates_rebuild_{uuid4().hex}"
    pipeline = [
        {"$sort": {"event_timestamp": 1}},
        {
            "$group": {
                "_id": {"parcel_id": "$parcel_id", "measurement_type": "$measurement_type"},
                "parcel_name": {"$last": "$parcel_name"},
                "crop_type": {"$last": "$crop_type"},
                "total_valid_readings": {"$sum": 1},
                "safe_readings": {"$sum": {"$cond": ["$is_anomaly", 0, 1]}},
                "anomalous_readings": {"$sum": {"$cond": ["$is_anomaly", 1, 0]}},
                "above_maximum_count": {
                    "$sum": {"$cond": [{"$eq": ["$anomaly_type", "above_maximum"]}, 1, 0]}
                },
                "below_minimum_count": {
                    "$sum": {"$cond": [{"$eq": ["$anomaly_type", "below_minimum"]}, 1, 0]}
                },
                "last_reading_at": {"$last": "$event_timestamp"},
                "last_value": {"$last": "$value"},
                "safe_min": {"$last": "$safe_min"},
                "safe_max": {"$last": "$safe_max"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "parcel_id": "$_id.parcel_id",
                "measurement_type": "$_id.measurement_type",
                "parcel_name": 1,
                "crop_type": 1,
                "total_valid_readings": 1,
                "safe_readings": 1,
                "anomalous_readings": 1,
                "above_maximum_count": 1,
                "below_minimum_count": 1,
                "last_reading_at": 1,
                "last_value": 1,
                "safe_min": 1,
                "safe_max": 1,
            }
        },
    ]
    grouped = list(database.raw_readings.aggregate(pipeline, allowDiskUse=True))
    documents = []
    for document in grouped:
        rate = alert_rate(document["anomalous_readings"], document["total_valid_readings"])
        documents.append(
            {
                **document,
                "alert_rate": rate,
                "risk_level": risk_level(rate),
                "recomputed_at": datetime.now(UTC),
            }
        )
    database[temporary_name].create_index(
        [("parcel_id", ASCENDING), ("measurement_type", ASCENDING)], unique=True
    )
    if documents:
        database[temporary_name].insert_many(documents, ordered=True)
    database.command(
        "renameCollection",
        f"{database_name}.{temporary_name}",
        to=f"{database_name}.parcel_aggregates",
        dropTarget=True,
    )
    duration = max(time.monotonic() - started, 0.000001)
    processed_readings = database.raw_readings.count_documents({})
    client.close()
    return {
        "processed_readings": processed_readings,
        "aggregate_documents": len(documents),
        "duration_seconds": round(duration, 6),
    }


def main() -> int:
    try:
        result = recompute()
        print(result)
        return 0
    except Exception as exc:
        print(f"recompute_failed={exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
