"""API FastAPI para publicar lecturas agrícolas en Kafka."""

from __future__ import annotations

import os
from collections import Counter
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from time import monotonic
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from producer_api.kafka_producer import EventProducer
from producer_api.mongo_store import MongoStore
from producer_api.schemas import (
    BatchEventInput,
    LoadTestInput,
    SingleEventInput,
    build_single_reading,
)
from shared.generator import GenerationRequest, generate_batch
from shared.parcels import PARCELS

READINGS_TOPIC = "agro.sensor-readings"


def _parcel_response(parcel: Any) -> dict[str, Any]:
    return {
        "parcel_id": parcel.parcel_id,
        "parcel_name": parcel.parcel_name,
        "crop_type": parcel.crop_type,
        "size_hectares": parcel.size_hectares,
        "location": parcel.location,
        "latitude": parcel.latitude,
        "longitude": parcel.longitude,
        "sensor_count": parcel.sensor_count,
        "safe_ranges": {
            measurement.value: values for measurement, values in parcel.safe_ranges.items()
        },
    }


def _json_payload(event: Any) -> dict[str, Any]:
    if hasattr(event, "model_dump"):
        return event.model_dump(mode="json")
    return event


def _run_summary(
    batch_id: str, payloads: list[Any], results: list[Any], started: float
) -> dict[str, Any]:
    duration = max(monotonic() - started, 0.000001)
    confirmed = sum(result.confirmed for result in results)
    parcel_counts = Counter()
    measurement_counts = Counter()
    for payload in payloads:
        if isinstance(payload, dict):
            parcel_counts[payload.get("parcel_id", "unknown")] += 1
            measurement_counts[payload.get("measurement_type", "unknown")] += 1
        else:
            parcel_counts[payload.parcel_id] += 1
            measurement_counts[payload.measurement_type] += 1
    return {
        "batch_id": batch_id,
        "events_requested": len(payloads),
        "messages_published": len(results),
        "deliveries_confirmed": confirmed,
        "publication_errors": [result.error for result in results if not result.confirmed],
        "duration_seconds": round(duration, 6),
        "events_per_second": round(confirmed / duration, 2),
        "distribution_by_parcel": dict(sorted(parcel_counts.items())),
        "distribution_by_measurement": dict(sorted(measurement_counts.items())),
        "completed_at": datetime.now(UTC).isoformat(),
    }


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    app.state.producer.close()
    app.state.store.close()


def create_app(producer: Any | None = None, store: Any | None = None) -> FastAPI:
    app = FastAPI(
        title="AgroStream IoT Producer API",
        version="0.1.0",
        description="Publica lecturas agrícolas validadas y lotes de sensores en Kafka.",
        lifespan=lifespan,
    )
    app.state.producer = producer or EventProducer(
        os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    )
    app.state.store = store or MongoStore()

    @app.get("/health", tags=["salud"])
    def health(request: Request) -> dict[str, Any]:
        checks = {}
        for name, dependency in (
            ("kafka", request.app.state.producer),
            ("mongodb", request.app.state.store),
        ):
            try:
                checks[name] = dependency.ping()
            except Exception as exc:
                checks[name] = False
                checks[f"{name}_error"] = str(exc)
        healthy = checks.get("kafka") and checks.get("mongodb")
        return JSONResponse(
            status_code=200 if healthy else 503,
            content={"status": "ok" if healthy else "degraded", "checks": checks},
        )

    @app.get("/ready", tags=["salud"])
    def ready(request: Request) -> dict[str, Any]:
        response = health(request)
        if response.status_code != 200:
            raise HTTPException(status_code=503, detail="Dependencias no listas")
        return {"status": "ready"}

    @app.get("/parcels", tags=["catálogo"])
    def parcels() -> list[dict[str, Any]]:
        return [_parcel_response(parcel) for parcel in PARCELS]

    @app.post("/events/single", tags=["ingesta"])
    def single_event(payload: SingleEventInput, request: Request) -> dict[str, Any]:
        reading = build_single_reading(payload)
        result = request.app.state.producer.publish_one(reading, topic=READINGS_TOPIC)
        if not result.confirmed:
            raise HTTPException(status_code=503, detail=result.error or "Error de entrega")
        return {
            "event_id": str(reading.event_id),
            "delivery_status": "confirmed",
            "partition": result.partition,
            "offset": result.offset,
        }

    @app.post("/events/batch", tags=["ingesta"])
    def batch_events(payload: BatchEventInput, request: Request) -> dict[str, Any]:
        generated = generate_batch(payload)
        payloads = [_json_payload(event) for event in generated.events]
        started = monotonic()
        results = request.app.state.producer.publish_many(payloads, topic=READINGS_TOPIC)
        summary = _run_summary(str(generated.batch_id), payloads, results, started)
        summary["generator_stats"] = generated.stats.model_dump()
        request.app.state.store.save_run(summary)
        return summary

    @app.post("/events/load-test", tags=["ingesta"])
    def load_test(payload: LoadTestInput, request: Request) -> dict[str, Any]:
        runs = []
        for size in payload.sizes:
            generation = GenerationRequest(
                count=size,
                scenario=payload.scenario,
                anomaly_percent=payload.anomaly_percent,
                duplicate_percent=payload.duplicate_percent,
                malformed_percent=payload.malformed_percent,
                seed=payload.seed,
                source="load_test",
            )
            generated = generate_batch(generation)
            payloads = [_json_payload(event) for event in generated.events]
            started = monotonic()
            results = request.app.state.producer.publish_many(payloads, topic=READINGS_TOPIC)
            summary = _run_summary(str(generated.batch_id), payloads, results, started)
            summary["generator_stats"] = generated.stats.model_dump()
            request.app.state.store.save_run(summary)
            runs.append(summary)
        return {"runs": runs}

    @app.get("/runs/{batch_id}", tags=["métricas"])
    def get_run(batch_id: str, request: Request) -> dict[str, Any]:
        run = request.app.state.store.get_run(batch_id)
        if not run:
            raise HTTPException(status_code=404, detail="Lote no encontrado")
        return run

    @app.get("/metrics/summary", tags=["métricas"])
    def metrics_summary(request: Request) -> dict[str, Any]:
        return request.app.state.store.summary()

    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("producer_api.main:app", host="0.0.0.0", port=8000)
