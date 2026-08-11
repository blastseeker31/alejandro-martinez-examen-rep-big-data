# Guion de presentación individual

## 1. Apertura

Presento AgroStream IoT, una plataforma local de sensores agrícolas. Mi objetivo no es solamente mostrar una gráfica: demuestro una tubería Big Data completa, desde la generación masiva hasta la persistencia, las alertas, la deduplicación y el reprocesamiento.

## 2. Explico la arquitectura

Inicio en Streamlit, donde puedo enviar una lectura individual o configurar un lote. La petición llega a FastAPI. FastAPI valida la entrada y usa un productor Kafka configurado con `acks=all`, idempotencia, compresión, batching y callbacks.

El evento entra a `agro.sensor-readings`, que tiene seis particiones y usa `parcel_id` como clave. Así conservo el orden de una parcela sin crear un topic diferente para cada parcela.

El consumer group `agro-sensor-processors` lee las particiones. Cada consumidor valida con Pydantic, recalcula la anomalía y guarda la lectura única en MongoDB. Si la lectura es anómala publica en `agro.alerts`; si no puede validarse la manda a la DLQ con trazabilidad.

Finalmente Streamlit consulta la API, que centraliza las consultas a MongoDB, y muestra agregados por parcela, gráficas y salud del sistema.

## 3. Demostración ordenada

1. Muestro `docker compose ps` y verifico healthchecks.
2. Abro Kafka UI y enseño los tres topics.
3. Muestro las seis particiones del topic principal.
4. Envío una lectura individual.
5. Enseño la respuesta con `event_id`, partición y offset.
6. Espero al consumidor y consulto la lectura en MongoDB.
7. Ejecuto un lote de 1.000 y muestro solicitados, confirmados y throughput.
8. Ejecuto un lote de 10.000 y guardo el resultado real.
9. Reenvío un `event_id` conocido para demostrar deduplicación.
10. Envío un payload inválido y enseño `processing_errors` y la DLQ.
11. Abro el dashboard, selecciono una parcela y explico tasa de alerta y riesgo.
12. Levanto tres consumidores y enseño miembros y particiones del group.
13. Ejecuto el reprocesamiento batch y comparo agregados.
14. Explico la limitación RF=1 y solo muestro HA si fue ejecutada y medida.

## 4. Decisiones que defiendo

Elegí Kafka porque desacopla captura y procesamiento y permite particiones. Elegí `parcel_id` porque el orden relevante es por parcela. Elegí MongoDB porque el evento es documental y las consultas del dashboard son por parcela y tiempo. Elegí Streamlit porque el objetivo académico es integrar y demostrar Big Data sin introducir un frontend separado.

Elegí at-least-once porque puedo demostrarlo honestamente con offsets manuales e idempotencia. No digo exactly-once porque una caída entre escribir MongoDB y publicar una alerta exige un patrón outbox o transacciones coordinadas.

## 5. Si me piden cambios en vivo

- Si piden más consumidores, explico que el máximo útil depende de las particiones.
- Si piden más volumen, aumento el lote hasta 10.000 sin cambiar silenciosamente los límites.
- Si piden una alerta, muestro cómo el consumidor recalcula el rango.
- Si piden replay, ejecuto el script batch y no reenvío Kafka.
- Si piden alta disponibilidad, muestro el Compose HA solo si la laptop lo soporta y explico que tres brokers en un solo host no son HA completa.
- Si piden exactamente-once, rechazo la afirmación y explico qué outbox/replica set adicional necesitaría.

## 6. Cierre

Cierro separando los resultados: publicados no significa procesados; confirmados por Kafka no significa almacenados; las lecturas válidas únicas no incluyen duplicados; los inválidos se cuentan en DLQ. Todas las cifras que presento vienen de comandos ejecutados y están registradas en `RESULTADOS_REALES.md`.
