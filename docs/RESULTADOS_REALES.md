# Resultados reales

Este archivo no contiene resultados estimados. Cada cifra debe provenir de una ejecución en esta computadora y conservar comando, fecha y commit.

## Entorno

- Sistema operativo: Ubuntu 24.04.4 LTS.
- Python: 3.12.3.
- Docker: 29.7.1.
- Docker Compose: v5.1.4.
- CPU: 22 hilos.
- RAM: 15 GiB total; 6.9 GiB disponibles durante Fase 0.
- Disco: 72 GiB disponibles durante Fase 0.
- Fecha de diagnóstico: 2026-08-10.

## Resultados disponibles hasta ahora

### Productor FastAPI → Kafka

Estos números son reales y corresponden únicamente a publicación confirmada por Kafka; no son lecturas procesadas ni almacenadas:

| Tamaño | Batch ID | Solicitados | Publicados | Confirmados | Duración API (s) | Throughput API (eventos/s) |
|---:|---|---:|---:|---:|---:|---:|
| 1.000 | `cad70db8-5c59-4e4c-b746-590ec694c182` | 1.000 | 1.000 | 1.000 | 0.049344 | 20.265,89 |
| 10.000 | `eaca1188-1da1-4e33-bbe4-fbaed15b22ea` | 10.000 | 10.000 | 10.000 | 0.300656 | 33.260,59 |

Envío individual:

- `event_id`: `586fdd9e-d225-4e89-90f2-eb712b3cf88b`.
- Estado: confirmado.
- Topic: `agro.sensor-readings`.
- Partición: 2.
- Offset: 0.

## Resultados de procesamiento

Pendientes de completar después de levantar el consumidor y esperar el backlog real:

| Ejecución | Solicitados | Publicados | Confirmados | Válidos únicos | Duplicados | Inválidos | Procesados | DLQ | Alertas | Duración | Throughput consumidor |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Pendiente | — | — | — | — | — | — | — | — | — | — | — |

## Escalabilidad

Pendiente de ejecutar con uno y tres consumidores. No se afirmará mejora hasta medir duración, lag, CPU y memoria.

## Reprocesamiento

Pendiente de ejecutar `scripts/recompute_aggregates.py` y comparar agregados antes/después.

## Alta disponibilidad

Pendiente de evaluar con los recursos de la laptop. El entorno local RF=1 no es HA.

## Conclusiones

Las conclusiones finales se escribirán después de completar las pruebas de integración. Las limitaciones se conservarán aunque el sistema funcione parcialmente.
