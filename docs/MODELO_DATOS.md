# Modelo de datos

## Evento `SensorReading`

Ejemplo ficticio:

```json
{
  "event_id": "586fdd9e-d225-4e89-90f2-eb712b3cf88b",
  "batch_id": null,
  "schema_version": "1.0",
  "parcel_id": "HN-ATL-001",
  "parcel_name": "Finca La Ceiba",
  "sensor_id": "HN-ATL-001-S01",
  "sensor_type": "agricultural_probe",
  "measurement_type": "soil_moisture",
  "crop_type": "banano",
  "value": 48.5,
  "unit": "%",
  "safe_min": 28.0,
  "safe_max": 72.0,
  "is_anomaly": false,
  "anomaly_type": null,
  "latitude": 15.763,
  "longitude": -86.782,
  "event_timestamp": "2026-08-11T01:00:00Z",
  "generated_at": "2026-08-11T02:02:21.062984Z",
  "source": "web_single"
}
```

Los datos agrícolas y coordenadas son ficticios, pero se mantienen dentro de Honduras. El catálogo contiene 16 parcelas, seis cultivos y cuatro tipos de medición.

## Validaciones

Pydantic valida:

- UUID de `event_id` y `batch_id` opcional.
- `schema_version=1.0`.
- campos obligatorios y longitudes.
- `measurement_type` y `source` enumerados.
- fechas con zona UTC.
- coordenadas dentro de rangos geográficos.
- `safe_min <= safe_max`.

El productor valida la solicitud. El consumidor deserializa y recalcula `is_anomaly` y `anomaly_type`; no confía ciegamente en los flags recibidos.

## Colecciones MongoDB

### `raw_readings`

Lecturas válidas y únicas. Añade `source_topic`, `source_partition`, `source_offset`, `processed_at`, `alert_published` y `alert_published_at` cuando corresponde.

Índices:

- `event_id` único.
- `parcel_id + event_timestamp`.
- `batch_id`.
- `event_timestamp`.
- `source_partition + source_offset`.

### `parcel_aggregates`

Clave lógica única: `parcel_id + measurement_type`. Incluye `total_valid_readings`, `safe_readings`, `anomalous_readings`, conteos above/below, `alert_rate`, `risk_level`, último timestamp, último valor y umbrales.

### `processing_errors`

Conserva el payload original representado como texto seguro, descripción del error, etapa, timestamp, topic original, partición y offset.

### `ingestion_runs`

Registra batch_id, solicitados, publicados, confirmados, errores de publicación, duración, throughput y distribuciones.

### `consumer_metrics`

Agrupa contadores de cada consumidor: procesados, duplicados, inválidos y anomalías, junto con `consumer_id` y `updated_at`.

## Duplicados

El índice único es la última defensa. Si Mongo rechaza el mismo `event_id`, el consumidor registra un duplicado, no actualiza agregados y no publica otra alerta. La semántica at-least-once significa que el redelivery es posible después de un fallo; por eso el efecto durable se comprueba antes de confirmar el offset.

## Alertas y DLQ

Una anomalía válida se publica en `agro.alerts` con la lectura recalculada y metadata de origen. Un mensaje que no puede deserializarse o validarse se registra en `processing_errors` y se publica en `agro.sensor-readings-dlq` con el mensaje original, error, fecha, etapa, topic, partición y offset.

Los fallos transitorios de MongoDB o Kafka no son errores de esquema: se reintentan y no se envían automáticamente a DLQ.
