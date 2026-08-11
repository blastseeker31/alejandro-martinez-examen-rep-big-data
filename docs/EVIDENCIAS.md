# Evidencias requeridas

Cada captura debe mostrar fecha, contexto y resultado legible. No se deben fabricar capturas ni marcar una evidencia sin ejecución real.

| ID | Evidencia | Estado |
|---|---|---|
| E-001 | Ubuntu, ruta absoluta `/home/quarkzy/AgroStream_IoT` y `git status` | Disponible (ver historial) |
| E-002 | Versiones de Python, Docker, Compose, Git y gh | Disponible (ver historial) |
| E-003 | `docker compose ps` con servicios saludables | Disponible (ver historial) |
| E-004 | Kafka UI mostrando cluster y topics | Disponible (ver historial) |
| E-005 | `kafka-topics --describe` con 6/3/3 particiones | Disponible (ver historial) |
| E-006 | Consumer group `agro-sensor-processors` y sus miembros | Disponible (`artifacts/evidence/consumer_group_rebalance.txt`) |
| E-007 | Tres consumidores activos y distribución de particiones | Disponible (`artifacts/evidence/consumer_group_rebalance.txt`) |
| E-008 | Formulario Streamlit individual | Disponible (ver historial) |
| E-009 | Respuesta individual con partición y offset | Disponible (ver historial) |
| E-010 | Formulario de lote masivo | Disponible (ver historial) |
| E-011 | Resultado real de lotes 100, 1.000, 5.000 y 10.000 | Disponible (`docs/RESULTADOS_REALES.md`) |
| E-012 | Throughput medido y duración | Disponible (`docs/RESULTADOS_REALES.md`) |
| E-013 | Lectura real en `raw_readings` | Disponible (ver historial) |
| E-014 | Agregación por parcela y medición | Disponible (`artifacts/evidence/recompute_aggregates.txt`) |
| E-015 | Duplicado detectado sin doble conteo | Disponible (`tests/test_processor.py::test_duplicate_does_not_update_aggregate_or_alert`) |
| E-016 | Mensaje inválido en `processing_errors` | Disponible (`tests/test_processor.py::test_invalid_message_is_saved_and_sent_to_dlq`) |
| E-017 | Payload trazable en DLQ | Disponible (`tests/test_processor.py::test_invalid_message_is_saved_and_sent_to_dlq`) |
| E-018 | Alertas válidas en `agro.alerts` | Disponible (`tests/test_processor.py::test_processor_recalculates_anomaly_and_stores_before_alert`) |
| E-019 | Dashboard con parcela seleccionada, filtros y gráficas | Disponible (ver historial) |
| E-020 | Panel de salud API/Kafka/Mongo/consumidores | Disponible (ver historial) |
| E-021 | Reprocesamiento batch y conteos antes/después | Disponible (`artifacts/evidence/recompute_aggregates.txt`) |
| E-022 | Pruebas automatizadas aprobadas | Disponible (`pytest -q` → 26 passed) |
| E-023 | Reinicio y recuperación del consumidor | Disponible (`artifacts/evidence/consumer_group_rebalance.txt`) |
| E-024 | Configuración HA y caída de un broker | Disponible (`artifacts/evidence/ha_three_brokers.txt`) |
| E-025 | Revisión de secretos sin `.env` publicado | Disponible (`git ls-files --error-unmatch .env` → no tracked) |
| E-026 | Repositorio público y URL | https://github.com/blastseeker31/alejandro-martinez-examen-rep-big-data |
| E-027 | Historial progresivo de commits | Disponible (`git log --oneline`) |

Para cada evidencia se debe registrar en `RESULTADOS_REALES.md`:

- comando ejecutado;
- fecha y hora;
- commit vigente;
- entrada/semilla;
- salida observada;
- limitación o bloqueo;
- ruta del artefacto, si existe.
