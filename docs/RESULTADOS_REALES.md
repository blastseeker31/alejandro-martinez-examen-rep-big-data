# Resultados reales

Este archivo no contiene resultados estimados. Cada cifra proviene de una ejecución real en esta computadora con comando, fecha y commit.

## Entorno

- Sistema operativo: Ubuntu 24.04.4 LTS (kernel 7.0.0-28-generic).
- Python: 3.12.3.
- Docker: 29.7.1.
- Docker Compose: v5.1.4.
- CPU: 22 hilos.
- RAM: 15 GiB total.
- Fecha de diagnóstico: 2026-08-11.

## Productor FastAPI → Kafka

Estos números corresponden únicamente a publicación confirmada por Kafka; no son lecturas procesadas ni almacenadas:

| Tamaño | Batch ID | Solicitados | Publicados | Confirmados | Duración API (s) | Throughput API (eventos/s) |
|---:|---|---:|---:|---:|---:|---:|
| 1.000 | `cad70db8-5c59-4e4c-b746-590ec694c182` | 1.000 | 1.000 | 1.000 | 0.049344 | 20.265,89 |
| 10.000 | `eaca1188-1da1-4e33-bbe4-fbaed15b22ea` | 10.000 | 10.000 | 10.000 | 0.300656 | 33.260,59 |
| 10.000 (mixto) | `13140935-8d5a-43a3-addd-7cd5bab38f07` | 10.000 | 10.000 | 10.000 | 0.374253 | 26.719,90 |
| 500 (edge) | `58b22035-8c07-432a-8851-551a00e78b40` | 500 | 500 | 500 | 0.006945 | 71.994,21 |

## Resultados de procesamiento — batch limpio de10,000

Batch ID: `13140935-8d5a-43a3-addd-7cd5bab38f07`
Fecha: 2026-08-11T04:29:41Z
Commit: a347f03
Semilla: 20260811
Escenario: mixed, anomaly_percent=15, duplicate_percent=5, malformed_percent=3

| Métrica | Pre-batch | Post-batch | Delta |
|---|---:|---:|---:|
| raw_readings | 11.182 | 20.397 | +9.215 |
| raw_anomalies | 3.396 | 6.159 | +2.763 |
| processing_errors | 10 | 310 | +300 |

Reconciliación matemática:
- Publicados:10.000
- Lecturas válidas insertadas:9.215
- Duplicados rechazados:485
- Inválidos→DLQ:300
- Suma:9.215+485+300=10.000 ✓

Distribución por parcela: HN-ATL-001=884, HN-ATL-002=482, HN-CHO-001=551, HN-CHO-002=366, HN-COL-001=953, HN-COL-002=729, HN-COM-001=499, HN-COM-002=714, HN-COP-001=488, HN-COR-001=784, HN-COR-002=379, HN-FCO-001=616, HN-FCO-002=452, HN-OLN-001=646, HN-OLN-002=887, HN-VAL-001=510, unknown=60.

Distribución por medición: air_humidity=2592, ph=2378, soil_moisture=2459, temperature=2511, wind_speed=60.

## Resultados de procesamiento — edge cases

Batch ID: `58b22035-8c07-432a-8851-551a00e78b40`
Fecha: 2026-08-11T04:31:18Z
Semilla: 99999
Escenario: stable, anomaly_percent=0, duplicate_percent=20, malformed_percent=10

- Publicados:500
- Inválidos registrados en processing_errors:50
- batch_id propagado a DLQ: sí (campo `batch_id` presente en los documentos de processing_errors)

Evidencia adicional: todos los errores de este batch contienen `batch_id=58b22035-8c07-432a-8851-551a00e78b40`.

## Escalamiento de consumidores

Fecha: 2026-08-10T21:42:55-06:00
Commit: 6404c8d

### Tres consumidores activos

Estado: Stable, 3 miembros, cooperative-sticky.
Asignación: 2 particiones por consumidor.
Lag: 0 en las 6 particiones.

### Rebalanceo con un consumidor detenido

`consumer-2` detenido manualmente: el grupo pasó a 2 miembros, 3 particiones cada uno.
Lag durante interrupción: 0.
Recuperación: `consumer-2` vuelto a levantar, grupo volvió a Stable con 3 miembros.

Evidencia: `artifacts/evidence/consumer_group_rebalance.txt`

## Reprocesamiento batch idempotente

Fecha: 2026-08-11
Commit: a347f03

- Primera ejecución:11.182 lecturas procesadas,64 documentos agregados, SHA canónico=`7de1a45d...`
- Segunda ejecución: mismos conteos, mismo SHA canónico.
- Conclusión: idempotente.

Evidencia: `artifacts/evidence/recompute_aggregates.txt`

## Alta disponibilidad — tres brokers Kafka

Fecha: 2026-08-10T21:26:29-06:00
Commit: 5971a20
Compose: `docker-compose.ha.yml` (override local con proyecto aislado `agrostream-ha`)

- Topics: agro.sensor-readings (6 particiones, RF=3), agro.sensor-readings-dlq (3 particiones, RF=3), agro.alerts (3 particiones, RF=3).
- min.insync.replicas=2.
- ISR completo [1,2,3] con los tres brokers.
- Publicación con `kafka-2` detenido:100/100 confirmadas, errores=0.
- ISR durante caída: [1,3] en las seis particiones.
- Recuperación: `kafka-2` vuelto a healthy, ISR=[1,3,2].

Evidencia: `artifacts/evidence/ha_three_brokers.txt`

## Correcciones de código realizadas

1. **consumer/main.py**: ciclo interno de reintentos con backoff exponencial, `seek` explícito por partición/offset, no avanzar offsets ante fallos transitorios.
2. **consumer/processor.py**: recuperación de alertas pendientes en duplicados, idempotencia de processing_errors (upsert), propagación de `batch_id` a DLQ.
3. **scripts/init_mongodb.py**: idempotencia del índice de parcel_aggregates (no usar nombre fijo que colisione con el existente).
4. **scripts/recompute_aggregates.py**: corrección de `renameCollection` contra `admin`.
5. **scripts/check_consumer_heartbeat.py**: healthcheck con antigüedad configurable.
6. **shared/generator.py**: `generate_malformed_payload` acepta `batch_id` opcional para mantener trazabilidad.
7. **docker-compose.ha.yml**: configuración HA de tres brokers KRaft con RF=3 y min.insync.replicas=2.

## Pruebas automatizadas

26 pruebas pasan (Ruff + Pytest + compileall + Compose config).

Evidencia: ver output de `pytest -q` al final de la auditoría.

## Limitaciones

- Los duplicados incluyen tanto los intencionales del generador como duplicados naturales; no se separan en el conteo actual.
- Reprocesamiento de aggregados es global, no por batch_id.
- HA se probó con brokers aislados en un compose adicional, no en el compose de producción local (recurso único de broker).
