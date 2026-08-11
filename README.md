# AgroStream IoT

Plataforma local de sensores IoT agrícolas en tiempo real para el examen de recuperación de Big Data.

Repositorio público: https://github.com/blastseeker31/alejandro-martinez-examen-rep-big-data

## Objetivo

AgroStream genera lecturas agrícolas ficticias pero realistas, las publica en Apache Kafka, las procesa con consumidores Python escalables, persiste lecturas y agregaciones en MongoDB y las presenta mediante una interfaz Streamlit.

El diseño utiliza procesamiento continuo at-least-once, offsets Kafka manuales y deduplicación durable por `event_id`. No se promete exactly-once de extremo a extremo.

## Arquitectura

```text
Streamlit → FastAPI → agro.sensor-readings → Consumer group → MongoDB
                                              ├→ agro.alerts
                                              └→ agro.sensor-readings-dlq
```

Servicios principales:

- `kafka`: Apache Kafka 3.9.1 en modo KRaft.
- `kafka_init`: creación idempotente de topics.
- `mongodb`: MongoDB 7.0 con autenticación local.
- `mongo_init`: colecciones e índices idempotentes.
- `producer_api`: FastAPI y productor confluent-kafka.
- `consumer`: consumidor escalable por Compose.
- `web_ui`: Streamlit y Plotly.
- `kafka_ui`: inspección visual de topics, particiones y grupos.

## Requisitos

- Ubuntu/Linux, Bash.
- Docker y Docker Compose.
- Git y Python 3.12 para pruebas locales.
- Recomendado: 8 GiB de RAM libres y 20 GiB de disco durante las pruebas.

Diagnóstico de esta computadora: Ubuntu 24.04.4 LTS, Python 3.12.3, Docker 29.7.1, Compose v5.1.4, 22 hilos, 15 GiB RAM y 72 GiB libres al iniciar el proyecto.

## Instalación y ejecución

Desde la carpeta del proyecto:

```bash
cp .env.example .env
```

```bash
docker compose up -d
```

Si se necesita volver a crear topics o índices sin borrar datos:

```bash
docker compose run --rm kafka_init
```

```bash
docker compose run --rm mongo_init
```

Verificar servicios:

```bash
docker compose ps
```

Las URLs locales son:

- FastAPI: http://localhost:8000
- Swagger: http://localhost:8000/docs
- Streamlit: http://localhost:8501
- Kafka UI: http://localhost:8081
- MongoDB: localhost:27017, solo para herramientas locales autenticadas

## Uso rápido de la API

Salud:

```bash
curl http://localhost:8000/health
```

Evento individual:

```bash
curl -X POST http://localhost:8000/events/single -H 'Content-Type: application/json' -d '{"parcel_id":"HN-ATL-001","sensor_id":"HN-ATL-001-S01","measurement_type":"soil_moisture","value":48.5,"event_timestamp":"2026-08-11T01:00:00Z"}'
```

Lote:

```bash
curl -X POST http://localhost:8000/events/batch -H 'Content-Type: application/json' -d '{"count":1000,"scenario":"mixed","anomaly_percent":15,"duplicate_percent":0,"malformed_percent":0,"seed":42}'
```

La respuesta distingue eventos solicitados, publicaciones confirmadas y errores. La confirmación del productor no equivale todavía a lectura procesada por el consumidor.

## Pruebas y automatización

Crear el entorno local y ejecutar la suite:

```bash
python3 -m venv .venv
```

```bash
.venv/bin/pip install -e '.[dev]'
```

```bash
.venv/bin/python -m pytest -q
```

Comandos Make disponibles:

- `make check`: compilación y Ruff.
- `make build`: construye imágenes.
- `make up`: levanta la demostración.
- `make down`: detiene servicios sin borrar volúmenes.
- `make logs`: muestra logs.
- `make test`: ejecuta Pytest.
- `make topics`: crea topics idempotentemente.
- `make load-test`: ejecuta el script de carga.
- `make scale-consumers`: intenta tres consumidores en el mismo group.
- `make verify`: ejecuta verificaciones reproducibles.
- `make reset-demo`: solicita escribir `RESET` y luego elimina volúmenes de demostración.

## Topics

| Topic | Particiones | Clave | Propósito |
|---|---:|---|---|
| `agro.sensor-readings` | 6 | `parcel_id` | Lecturas principales |
| `agro.sensor-readings-dlq` | 3 | parcela o `invalid` | Mensajes inválidos |
| `agro.alerts` | 3 | `parcel_id` | Anomalías válidas |

El entorno local usa replication factor 1. Esto permite la laptop, pero no ofrece alta disponibilidad real. La variante HA está documentada en `docker-compose.ha.yml` y `docs/ARQUITECTURA.md`.

## Escalamiento

```bash
docker compose up -d --scale consumer=3
```

Kafka reparte las seis particiones entre miembros del consumer group `agro-sensor-processors`. Más consumidores que particiones no agregan paralelismo útil.

## Solución de problemas

- Si 8000, 8501 o 8081 están ocupados, modifica las variables de puerto en `.env`; 8080 y 8088 ya estaban ocupados en la computadora usada para el proyecto.
- Si Kafka aparece `starting`, espera el healthcheck y revisa `docker compose logs kafka`.
- Si topics no aparecen, ejecuta `docker compose run --rm kafka_init`.
- Si Mongo no tiene índices, ejecuta `docker compose run --rm mongo_init`.
- `docker compose down` conserva datos. `make reset-demo` sí elimina los volúmenes y exige confirmación.
- Nunca publiques `.env`, logs, volúmenes o credenciales.

## Documentación académica

- `docs/ARQUITECTURA.md`: componentes, flujo, Kafka, escalabilidad y fallos.
- `docs/MODELO_DATOS.md`: evento, colecciones e índices.
- `docs/DECISIONES_TECNICAS.md`: alternativas y trade-offs.
- `docs/EVIDENCIAS.md`: capturas y comandos necesarios.
- `docs/RESULTADOS_REALES.md`: solo métricas ejecutadas en esta computadora.
- `docs/GUIA_DEFENSA.md`: preguntas y respuestas.
- `docs/GUION_PRESENTACION.md`: demostración en primera persona.

## Historial inicial

Los avances se publican progresivamente en `main`. El historial actual puede consultarse con `git log --oneline` y en GitHub. Los resultados de rendimiento se mantienen separados entre publicación confirmada, procesamiento y persistencia.
