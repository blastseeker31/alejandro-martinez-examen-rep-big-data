"""Inicializa colecciones e índices de MongoDB sin borrar datos."""

from __future__ import annotations

import os
import sys
import time

from pymongo import ASCENDING, DESCENDING, MongoClient


def main() -> int:
    uri = os.getenv(
        "MONGO_URI",
        "mongodb://agrostream_admin:agrostream_local_only@mongodb:27017/agrostream?authSource=admin",
    )
    database_name = os.getenv("MONGO_DATABASE", "agrostream")
    for attempt in range(1, 31):
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=3000)
            client.admin.command("ping")
            database = client[database_name]
            database.raw_readings.create_index(
                [("event_id", ASCENDING)], unique=True, name="uq_event_id"
            )
            database.raw_readings.create_index(
                [("parcel_id", ASCENDING), ("event_timestamp", DESCENDING)], name="parcel_timestamp"
            )
            database.raw_readings.create_index([("batch_id", ASCENDING)], name="batch_id")
            database.raw_readings.create_index(
                [("event_timestamp", DESCENDING)], name="event_timestamp"
            )
            database.raw_readings.create_index(
                [("source_partition", ASCENDING), ("source_offset", ASCENDING)], name="source_trace"
            )
            database.parcel_aggregates.create_index(
                [("parcel_id", ASCENDING), ("measurement_type", ASCENDING)],
                unique=True,
            )
            database.processing_errors.create_index(
                [("created_at", DESCENDING)], name="error_created_at"
            )
            database.processing_errors.create_index(
                [
                    ("source_topic", ASCENDING),
                    ("source_partition", ASCENDING),
                    ("source_offset", ASCENDING),
                ],
                unique=True,
                name="uq_processing_error_source",
            )
            database.ingestion_runs.create_index(
                [("batch_id", ASCENDING)], unique=True, name="uq_run_batch"
            )
            database.consumer_metrics.create_index(
                [("consumer_id", ASCENDING), ("updated_at", DESCENDING)], name="consumer_updated_at"
            )
            print(f"mongodb_ready=1 database={database_name}")
            client.close()
            return 0
        except Exception as exc:
            print(f"mongo_init_attempt={attempt} error={exc}")
            time.sleep(2)
    print("No fue posible conectar con MongoDB después de 30 intentos", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
