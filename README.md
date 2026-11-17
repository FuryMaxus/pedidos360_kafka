# Pedidos360 · Kafka

Streaming de eventos de pedidos con **Apache Kafka** y **Node.js (KafkaJS)**. Todo corre local.

## Qué hace

Un productor publica los cambios de estado de pedidos simulados en el tópico `pedidos.eventos`. Dos consumidores leen **los mismos eventos**, cada uno con su propio consumer group:

- **report**: calcula ventas por hora, tiempo de entrega y pedidos activos por estado.
- **audit**: guarda cada evento para armar el historial de cada pedido.

Los mensajes que fallan van a `pedidos.eventos.dlt` con el motivo. Un servicio **admin** (Express) administra tópicos y grupos y expone los resultados de report y audit para el monitor web.




|###configuracion
| --- | --- |
| 3 particiones | Hasta 3 consumidores por grupo en paralelo |
| 3 réplicas, `min.insync.replicas=2` | No se pierden datos si cae un broker |
| Retención: 7 días (eventos), 30 días (DLT) | Releer la semana para reportes; tiempo para revisar los fallos |
| `key = pedidoId` | Los eventos de un pedido quedan en la misma partición, en orden |
| `acks=-1` + productor idempotente | Confirmación de las réplicas, sin duplicados por reintentos de red |
| Header `correlationId` | Seguir un pedido de punta a punta |
| Validar → 3 intentos (1 s, 2 s) → DLT | Un mensaje malo no bloquea a los demás |
| Consumidores idempotentes por `eventId` | Kafka entrega al menos una vez: un evento repetido no se aplica dos veces |
| SQLite | Base local simple para los resultados de report y audit |

Evento (DTO):

```json
{ "eventId": "uuid", "tipo": "PEDIDO_CREADO", "pedidoId": "PED-001", "clienteId": "C-100", "total": 10990, "fecha": "2026-11-17T06:36:28Z" }
```

## Estructura

```text
docker-compose.yml   Kafka (ZooKeeper, 3 brokers) + Kafka UI
shared/              configuración, cliente Kafka, tópicos, DTO, base de consumidores, SQLite
bridge/producer.js   productor simulado
report/, audit/      consumer.js (mensajería) + handler.js (lógica de negocio)
admin/               API REST de administración y del monitor
scripts/             errores, dlt, kpis, timeline
```

## API del admin

| Método | Ruta | Uso |
| --- | --- | --- |
| POST / GET | `/admin/topics` | Crear / listar tópicos |
| GET / DELETE | `/admin/topics/:topic` | Ver / borrar un tópico |
| PUT | `/admin/topics/:topic/config` | Cambiar retención, cleanup o min.insync |
| PUT | `/admin/topics/:topic/partitions` | Aumentar particiones |
| GET | `/admin/groups` y `/admin/groups/:groupId/lag` | Consumer groups y su lag |
| GET | `/api/monitor/kpis` y `/api/monitor/eventos` | Datos para el monitor |

## Pruebas

| # | Comando | Resultado esperado |
| --- | --- | --- |
| 1 | `npm run producer -- 3 --max 10` | Cada pedido queda siempre en la misma partición (Kafka UI) |
| 2 | Dos terminales con `npm run report` y cerrar una | Se reparten las particiones y luego se reasignan (rebalanceo) |
| 3 | Levantar `audit` después de report | audit recibe todos los eventos |
| 4 | Detener report, publicar 20 eventos y consultar `/admin/groups/report/lag` | Lag 20; vuelve a 0 al reiniciar report |
| 5 | `npm run errores` y luego `npm run dlt` | Los mensajes inválidos quedan en el DLT con su motivo; el resto se procesa |


