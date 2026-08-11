# Decisiones técnicas

## DT-001 — Kafka KRaft

- Decisión: Kafka 3.9.1 en modo KRaft, sin ZooKeeper.
- Alternativa: Kafka con ZooKeeper.
- Ventaja: menos servicios y arquitectura actual de Kafka.
- Desventaja: la configuración de listeners y quorum debe ser explícita.
- Trade-off: simplicidad operativa local frente a la complejidad adicional de un quorum HA.
- Evidencia: `docker compose ps`, logs de Kafka y `kafka-topics --describe`.

## DT-002 — Un topic particionado frente a un topic por parcela

- Decisión: un topic principal con seis particiones y clave `parcel_id`.
- Alternativa: un topic por parcela.
- Ventaja: topología estable, consumer group único y paralelismo administrable.
- Desventaja: una parcela muy activa puede concentrar tráfico en una partición.
- Trade-off: simplicidad y escalabilidad general frente a hotspots.
- Evidencia: distribución real de particiones y group.

## DT-003 — MongoDB frente a PostgreSQL

- Decisión: MongoDB documental.
- Alternativa: PostgreSQL con tablas de lecturas y agregados.
- Ventaja: documento natural para eventos con metadata agrícola y consultas por parcela.
- Desventaja: consistencia entre lectura, agregación y alerta requiere diseño explícito.
- Trade-off: flexibilidad del documento frente a transacciones relacionales más familiares.
- Evidencia: índices creados y consultas de dashboard.

## DT-004 — Streamlit frente a frontend JavaScript separado

- Decisión: Streamlit.
- Alternativa: React/Vue con API separada.
- Ventaja: permite defender el pipeline y visualizar datos con poco código de infraestructura.
- Desventaja: menos control fino sobre estado y experiencia que un frontend dedicado.
- Trade-off: velocidad de desarrollo académico frente a flexibilidad de producto.

## DT-005 — At-least-once frente a exactly-once

- Decisión: at-least-once con auto-commit deshabilitado e idempotencia Mongo.
- Alternativa: exactly-once de extremo a extremo.
- Ventaja: comportamiento explícito, reproducible y defendible con un broker local.
- Desventaja: existe ventana de redelivery entre efectos y commit.
- Trade-off: menos complejidad y costo frente a una garantía que no se puede prometer sin coordinar todos los sistemas.
- Evidencia: logs de commit, índice único y prueba de duplicado.

## DT-006 — Clave parcel_id

- Decisión: `parcel_id` como clave de Kafka.
- Alternativa: `event_id`, round-robin o `sensor_id`.
- Ventaja: orden de una parcela y reparto entre parcelas.
- Desventaja: no garantiza orden global y puede producir hotspot.

## DT-007 — Un consumidor frente a varios

- Decisión: mismo group escalable mediante Compose.
- Alternativa: un proceso monolítico.
- Ventaja: demuestra paralelismo horizontal y recuperación por rebalanceo.
- Desventaja: más conexiones, logs y coordinación.
- Límite: el topic principal tiene seis particiones; más de seis consumidores no aumenta paralelismo útil.

## DT-008 — Replication factor 1 frente a 3

- Desarrollo: RF=1 para cuidar recursos de la laptop.
- HA opcional: tres brokers, RF=3 y min ISR=2.
- Ventaja de RF=1: bajo consumo.
- Desventaja: la caída del broker implica indisponibilidad/pérdida según persistencia.
- Importante: tres brokers en el mismo host no equivalen a HA completa.

## DT-009 — Streaming frente a batch

- Streaming: detección continua de alertas y baja latencia de procesamiento.
- Batch: recomputación idempotente desde `raw_readings`, backfill y auditoría.
- Trade-off: streaming reacciona antes; batch facilita reconstrucción y consistencia derivada.

## DT-010 — Docker Compose

- Decisión: servicios reproducibles con healthchecks y scripts idempotentes.
- Alternativa: instalar Kafka y Mongo directamente en Ubuntu.
- Ventaja: aislamiento y ejecución desde cero.
- Desventaja: consumo de recursos y necesidad de entender redes/volúmenes.

## DT-011 — Riesgo entre efectos y offset

El orden implementado es almacenar lectura, actualizar agregado, publicar alerta si aplica y confirmar offset. Si el proceso cae entre efectos, el redelivery puede encontrar una lectura existente. `alert_published` hace visible el estado, pero una garantía transaccional completa requeriría un outbox o MongoDB replica set. Esta limitación no se oculta ni se presenta como exactly-once.

## DT-012 — Umbrales de riesgo

- BAJO: menos de 10%.
- MEDIO: desde 10% hasta menos de 30%.
- ALTO: 30% o más.

Son decisiones del proyecto para el dashboard, no una norma agrícola universal.
