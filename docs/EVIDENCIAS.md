# Evidencias requeridas

Cada captura debe mostrar fecha, contexto y resultado legible. No se deben fabricar capturas ni marcar una evidencia sin ejecución real.

| ID | Evidencia |
|---|---|
| E-001 | Ubuntu, ruta absoluta `/home/quarkzy/AgroStream_IoT` y `git status` |
| E-002 | Versiones de Python, Docker, Compose, Git y gh |
| E-003 | `docker compose ps` con servicios saludables |
| E-004 | Kafka UI mostrando cluster y topics |
| E-005 | `kafka-topics --describe` con 6/3/3 particiones |
| E-006 | Consumer group `agro-sensor-processors` y sus miembros |
| E-007 | Tres consumidores activos y distribución de particiones |
| E-008 | Formulario Streamlit individual |
| E-009 | Respuesta individual con partición y offset |
| E-010 | Formulario de lote masivo |
| E-011 | Resultado real de lotes 100, 1.000, 5.000 y 10.000 |
| E-012 | Throughput medido y duración |
| E-013 | Lectura real en `raw_readings` |
| E-014 | Agregación por parcela y medición |
| E-015 | Duplicado detectado sin doble conteo |
| E-016 | Mensaje inválido en `processing_errors` |
| E-017 | Payload trazable en DLQ |
| E-018 | Alertas válidas en `agro.alerts` |
| E-019 | Dashboard con parcela seleccionada, filtros y gráficas |
| E-020 | Panel de salud API/Kafka/Mongo/consumidores |
| E-021 | Reprocesamiento batch y conteos antes/después |
| E-022 | Pruebas automatizadas aprobadas |
| E-023 | Reinicio y recuperación del consumidor |
| E-024 | Configuración HA y caída de un broker, si fue viable |
| E-025 | Revisión de secretos sin `.env` publicado |
| E-026 | Repositorio público y URL |
| E-027 | Historial progresivo de commits |

Para cada evidencia se debe registrar en `RESULTADOS_REALES.md`:

- comando ejecutado;
- fecha y hora;
- commit vigente;
- entrada/semilla;
- salida observada;
- limitación o bloqueo;
- ruta del artefacto, si existe.
