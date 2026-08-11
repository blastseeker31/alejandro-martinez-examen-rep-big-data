# Guía de defensa

## Big Data y 5 V

**¿Por qué es Big Data si corre en una laptop?** Porque el proyecto demuestra conceptos de captura, velocidad, particionado, procesamiento distribuido y escalabilidad. La laptop es un entorno reproducible; no se presenta como infraestructura productiva.

**¿Dónde están las 5 V?** Volumen en lotes y lecturas; velocidad en throughput y lag; variedad en mediciones, cultivos y escenarios; veracidad en Pydantic, UTC, rangos y DLQ; valor en alertas y nivel de riesgo.

## Kafka

**¿Qué es un topic?** Un registro lógico de eventos. Aquí el topic principal contiene lecturas y los topics secundarios separan alertas e inválidos.

**¿Qué es una partición?** Una secuencia ordenada dentro de un topic. Permite que varios consumidores procesen en paralelo.

**¿Por qué `parcel_id` como clave?** Para que las lecturas de una parcela mantengan orden dentro de la misma partición. No existe orden global.

**¿Qué es un offset?** La posición de un mensaje dentro de una partición. El consumidor lo confirma después del efecto durable.

**¿Qué es un consumer group?** Un conjunto de consumidores que cooperan; cada partición activa pertenece a un miembro a la vez.

**¿Qué ocurre con más consumidores que particiones?** Los sobrantes quedan sin particiones activas. El paralelismo útil está limitado por las seis particiones.

**¿Qué es consumer lag?** La diferencia entre el último offset producido y la posición confirmada por el grupo. Un lag creciente significa que el procesamiento no alcanza la llegada.

**¿Qué diferencia hay entre broker y producer?** El broker almacena y sirve eventos; el producer publica eventos al broker.

**¿Qué significa KRaft?** El modo moderno de Kafka donde el quorum de control está integrado; este proyecto no usa ZooKeeper.

## Entrega y consistencia

**¿Es exactly-once?** No. El productor es idempotente y el consumidor at-least-once. Puede existir redelivery después de una caída, pero MongoDB usa `event_id` único para no duplicar lecturas.

**¿Qué hace `acks=all`?** El producer espera la confirmación de las réplicas en ISR según la configuración. En local RF=1 significa confirmación del único broker, no HA.

**¿Qué ocurre si llega un duplicado?** La inserción choca con el índice único; se registra como duplicado, no incrementa agregados y no produce otra alerta.

**¿Qué es una DLQ?** Un topic para mensajes que no se pueden procesar por errores permanentes de contrato. Conserva payload y trazabilidad del mensaje original.

**¿Un error temporal va a DLQ?** No automáticamente. Una caída temporal de MongoDB o Kafka requiere reintento; DLQ es para errores permanentes de validación/serialización.

**¿Por qué recalcular anomalías?** Para que el consumidor sea la autoridad de la regla y no confíe en flags manipulables o incorrectos del productor.

## MongoDB y análisis

**¿Por qué MongoDB?** El evento es documental, contiene metadata heterogénea y se consulta por parcela, medición y tiempo. Los índices sostienen esas consultas.

**¿Cómo se calcula `alert_rate`?** `anomalous_readings / total_valid_readings * 100`. BAJO es menor de 10%, MEDIO desde 10% hasta menor de 30%, y ALTO desde 30%. Son umbrales del proyecto, no una norma agrícola.

**¿Qué diferencia hay entre evento y agregado?** El evento es la lectura cruda única; el agregado resume contadores y último estado para consultas rápidas.

**¿Qué es batch reprocessing?** Leer las lecturas válidas almacenadas, ejecutar una aggregation pipeline y reconstruir agregados sin reenviar Kafka. Sirve para backfill y auditoría.

## Escalabilidad y fallos

**¿Cómo escala?** Kafka divide por particiones y Compose puede levantar varios consumidores del mismo group.

**¿Qué ocurre si cae un consumidor?** Kafka rebalancea las particiones a los miembros restantes; los offsets no confirmados pueden redeliverarse.

**¿Qué ocurre si cae un broker local?** Con RF=1 se pierde disponibilidad del topic durante la caída. La variante HA usa tres brokers, RF=3 y min ISR=2, pero tres brokers en la misma laptop no protegen contra la caída del host.

**¿Qué pasa si se duplica el volumen?** Aumenta lag o tiempo de procesamiento si no se agregan particiones, consumidores o recursos; primero se mide el cuello de botella.

## Defensa del código apoyado por IA

**¿Qué partes apoyó la IA?** La IA ayudó a proponer estructura, escribir código inicial, pruebas y documentación. Yo debo explicar y demostrar cada componente; el repositorio conserva historial progresivo y pruebas reales.

**¿Cómo explico código generado por IA?** Empiezo por el contrato, explico entrada/salida y luego la decisión: por ejemplo, el consumidor desactiva auto-commit, valida, inserta usando índice único, actualiza agregados, publica alerta y confirma offset. Si no puedo explicar una línea, la simplifico o la investigo antes de defenderla.

## Preguntas de demostración

- ¿Qué evidencia muestra que Kafka tiene seis particiones?
- ¿Qué conteos separan publicados, confirmados, válidos, duplicados, inválidos y procesados?
- ¿Qué sucede si se reenvía el mismo UUID?
- ¿Cómo pruebo que un inválido llegó a DLQ?
- ¿Qué comando muestra miembros del consumer group?
- ¿Qué resultado es publicación confirmada y qué resultado es persistencia MongoDB?
- ¿Qué limitación mantiene RF=1?
