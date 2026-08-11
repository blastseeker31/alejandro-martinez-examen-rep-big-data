# Seguimiento del proyecto AgroStream IoT

Este archivo es el control operativo del proyecto. Se actualiza después de cada fase con resultados comprobados en esta computadora. Una casilla solo se marca cuando existe evidencia real.

## Estado general

- [x] Fase 0: diagnóstico local completado.
- [ ] Fase 1: base del proyecto.
- [ ] Fase 2: infraestructura Kafka/MongoDB/Kafka UI.
- [ ] Fase 3: productor FastAPI.
- [ ] Fase 4: consumidor y persistencia.
- [ ] Fase 5: interfaz Streamlit.
- [ ] Fase 6: escalabilidad y reprocesamiento.
- [ ] Fase 7: alta disponibilidad.
- [ ] Fase 8: documentación y defensa.
- [ ] Fase 9: auditoría final y publicación.

## Diagnóstico real inicial

- Sistema: Ubuntu 24.04.4 LTS.
- Python: 3.12.3.
- Docker: 29.7.1.
- Docker Compose: v5.1.4.
- Git: 2.43.0.
- GitHub CLI: 2.45.0.
- CPU: 22 hilos.
- Memoria: 15 GiB total; 6.9 GiB disponibles durante el diagnóstico.
- Disco: 72 GiB disponibles en `/` durante el diagnóstico.
- GitHub: sesión autenticada como `blastseeker31`.
- Repositorios objetivo: los dos nombres solicitados no existían durante el diagnóstico.
- Puertos 8080 y 8088: ocupados; no se usarán para AgroStream.

## Contratos que no deben cambiar sin documentarlo

- Topic principal: `agro.sensor-readings`, 6 particiones, clave `parcel_id`.
- Topic DLQ: `agro.sensor-readings-dlq`, 3 particiones.
- Topic alertas: `agro.alerts`, 3 particiones.
- Consumer group: `agro-sensor-processors`.
- API local: puerto 8000.
- Streamlit local: puerto 8501.
- Kafka UI local: puerto 8081.
- MongoDB local: puerto 27017, protegido con usuario y contraseña definidos por entorno.
- Versionado del evento: `schema_version=1.0`.
- Procesamiento: at-least-once, offsets manuales, deduplicación por `event_id`.

## Lista de aceptación

### Datos y productor

- [ ] Lectura individual real llega a Kafka y MongoDB.
- [ ] Lotes de 100, 1.000, 5.000 y 10.000 eventos ejecutados o documentados con evidencia.
- [ ] Escenarios estable, ola de calor, falla de riego, lluvia intensa y mixto.
- [ ] Duplicados conservan `event_id`.
- [ ] Malformados controlados: valor de texto, parcela ausente, fecha inválida, rango invertido y medición desconocida.
- [ ] Throughput calculado desde duración real.

### Kafka e infraestructura

- [ ] Kafka funciona en KRaft sin ZooKeeper.
- [ ] Topics creados idempotentemente con particiones solicitadas.
- [ ] Kafka UI muestra topics y particiones.
- [ ] Consumer group visible.
- [ ] Healthchecks reales y espera de disponibilidad.
- [ ] HA de tres brokers evaluada y probada si los recursos lo permiten.

### Consumidor y almacenamiento

- [ ] Validación Pydantic en consumidor.
- [ ] `is_anomaly` y `anomaly_type` recalculados en consumidor.
- [ ] Índice único `raw_readings.event_id`.
- [ ] Duplicados no se almacenan, no alertan y no actualizan agregados.
- [ ] Inválidos llegan a DLQ y `processing_errors`.
- [ ] Alertas válidas llegan a `agro.alerts`.
- [ ] Offset confirmado después del almacenamiento.
- [ ] SIGTERM, reintentos acotados y cierre ordenado.

### Interfaz y análisis

- [ ] Generador Streamlit individual y masivo.
- [ ] Dashboard con parcela obligatoria, filtros y umbrales.
- [ ] Balance de alertas y nivel configurable.
- [ ] Panel de salud de API, Kafka, MongoDB y consumidores.
- [ ] Reprocesamiento batch idempotente mediante aggregation pipeline.

### Calidad y entrega

- [ ] Pruebas unitarias y de integración reproducibles.
- [ ] README probado desde cero.
- [ ] Documentación académica completa en español.
- [ ] Resultados reales sin números inventados.
- [ ] Revisión de secretos antes de cada push.
- [ ] Historial de commits progresivo y natural.
- [ ] Repositorio público accesible.
- [ ] `git status` limpio al finalizar.

## Registro de fases y evidencia

### Fase 0 — Diagnóstico

- Estado: completada.
- Evidencia: salida de comandos de sistema ejecutados por Hermes el 2026-08-10.
- Limitaciones detectadas: puertos 8080 y 8088 ocupados; se elegirán puertos alternativos.

### Fase 1 — Base

- Estado: en progreso.
- Commits: pendiente.
- Pruebas: pendiente.

### Fase 2 — Infraestructura

- Estado: pendiente.
- Evidencia: pendiente.

### Fase 3 — Productor

- Estado: pendiente.
- Evidencia: pendiente.

### Fase 4 — Procesamiento

- Estado: pendiente.
- Evidencia: pendiente.

### Fase 5 — Interfaz

- Estado: pendiente.
- Evidencia: pendiente.

### Fase 6 — Escalabilidad

- Estado: pendiente.
- Evidencia: pendiente.

### Fase 7 — Alta disponibilidad

- Estado: pendiente.
- Evidencia: pendiente.

### Fase 8 — Documentación

- Estado: pendiente.
- Evidencia: pendiente.

### Fase 9 — Auditoría final

- Estado: pendiente.
- Evidencia: pendiente.

## Registro de delegaciones

Se agregarán aquí el objetivo, archivos revisados, observaciones recibidas y cómo se verificó cada recomendación. Los subagentes no tienen autoridad para publicar cambios.

## Registro de problemas y correcciones

| Fecha | Problema | Causa | Corrección | Prueba posterior |
|---|---|---|---|---|
| 2026-08-10 | Puertos 8080 y 8088 ocupados | Servicios preexistentes en Ubuntu | Reservar 8000, 8501 y 8081 para AgroStream | Pendiente de levantar servicios |

## Regla de cierre

No se declarará cumplido el proyecto por tener código escrito. El cierre exige ejecución real, pruebas, conteos separados, logs revisados, documentación coherente, repositorio público sin secretos y estado Git limpio.
