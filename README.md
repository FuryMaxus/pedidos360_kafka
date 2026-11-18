# Pedidos360 · Kafka

Streaming de eventos de pedidos con **Apache Kafka** y **Node.js (KafkaJS)**. Todo corre local.

## Qué hace

Un productor publica los cambios de estado de pedidos simulados en el tópico `pedidos.eventos`. Dos consumidores leen **los mismos eventos**, cada uno con su propio consumer group:

- **report**: calcula ventas por hora, tiempo de entrega y pedidos activos por estado.
- **audit**: guarda cada evento para armar el historial de cada pedido.

Los mensajes que fallan van a `pedidos.eventos.dlt` con el motivo. Un servicio **admin** (Express) administra tópicos y grupos y expone los resultados de report y audit para el monitor web.




## Configuración

| Qué | Por qué |
| --- | --- |
| 3 particiones | Hasta 3 consumidores por grupo en paralelo |
| 3 réplicas, `min.insync.replicas=2` | No se pierden datos si cae un broker |
| 3 ZooKeeper (ensamble) | Si cae uno, los otros dos forman mayoría y el clúster sigue eligiendo líderes y creando tópicos |
| Retención: eventos 7 días o 512 MB, DLT 30 días, carga 5 min o 50 MB | Releer la semana para reportes; tiempo para revisar los fallos; los datos de carga son descartables |
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
docker-compose.yml   Kafka (3 ZooKeeper, 3 brokers) + Kafka UI
shared/              configuración, cliente Kafka, tópicos, DTO, base de consumidores, SQLite
bridge/producer.js   productor simulado
report/, audit/      consumer.js (mensajería) + handler.js (lógica de negocio)
admin/               API REST de administración y del monitor
scripts/             errores, dlt, kpis, timeline, lag, generar, particiones, replicas, retencion, carga
```

Tópicos: `pedidos.eventos` (eventos), `pedidos.eventos.dlt` (errores) y `pedidos.carga` (solo pruebas de carga).

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

## Casos prácticos

Requisitos: `docker compose up -d`, `npm run topics` y `npm run admin` y `npm run audit` corriendo. Los ejemplos usan la sintaxis de Git Bash; en PowerShell, las variables se ponen así: `$env:PROCESS_DELAY_MS=100; npm run report`.

### Caso 1: escalar consumidores y ver el lag bajar

```bash
npm run lag -- --grupo=report                  # A) monitor de lag (dejarlo abierto)
PROCESS_DELAY_MS=100 npm run report            # B) consumidor lento (100 ms por mensaje)
npm run generar -- --pedidos=100 --delay=10    # C) producir con ritmo (500 eventos)
PROCESS_DELAY_MS=100 npm run report            # D) repetir en otras terminales: 2.º, 3.º y 4.º consumidor
```

Con 1 consumidor el lag sube. Con 3 se reparten las particiones (`[0]`, `[1]`, `[2]`) y el lag baja a 0. El 4.º queda sin partición (`[]`). Para aumentar particiones: `npm run particiones -- --total=6`. **No se puede deshacer**: para volver a 3 hay que borrar el tópico con `docker compose down -v`.

### Caso 2: se cae un broker y el clúster sigue

```bash
npm run replicas -- --watch      # líder, réplicas e ISR en vivo
docker compose stop kafka2       # en otra terminal: apagar un broker
npm run producer -- 1            # se sigue publicando (acks=all, min.insync=2)
docker compose start kafka2      # vuelve y entra de nuevo al ISR
```

Al apagar `kafka2` sus réplicas salen del ISR y otro broker toma el liderazgo. Con RF=3, `acks=all` y `min.insync.replicas=2` se puede seguir escribiendo con 1 broker caído.

También se puede apagar un ZooKeeper (`docker compose stop zookeeper1`): los otros dos eligen un nuevo líder entre ellos (se ve en `docker compose logs zookeeper3` como `LEADING`) y Kafka sigue funcionando.

### Caso 3: ver cómo Kafka borra los mensajes viejos

```bash
npm run retencion
```

Crea `pedidos.demo-retencion` (retención 20 s, segmentos de 10 s), envía 2 lotes de 100 y muestra `low`/`high` cada 5 s. `high` no cambia y `low` sube cuando Kafka borra. El broker revisa cada 30 s (`KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS` en `docker-compose.yml`).

### Caso 4: tuning y prueba de carga

```bash
npm run carga -- --count=20000 --modo=simple
npm run carga -- --count=20000 --modo=lote
npm run carga -- --count=20000 --modo=lote --lingerMs=0
npm run carga -- --count=20000 --modo=lote --acks=0
npm run carga -- --count=20000 --modo=lote --acks=-1
npm run carga -- --count=20000 --modo=lote --compresion=none
```

`simple` hace un `send()` por mensaje. `lote` agrupa hasta 500 mensajes (`--batch`) y espera hasta 20 ms para llenar el lote (`--lingerMs`), con compresión gzip. Cada resultado se guarda en `data/carga-resultados.csv`.

Resultado de referencia (depende del computador):

| Prueba | msg/s | llamadas | KB enviados |
| --- | --- | --- | --- |
| simple | 310 | 20 000 | 3 505 |
| lote | 20 952 | 40 | 523 |
| lote `lingerMs=0` | 329 | 20 000 | 3 275 |
| lote `acks=0` | 32 413 | 40 | 523 |
| lote `acks=-1` | 30 803 | 40 | 515 |
| lote sin compresión | 8 154 | 40 | 3 505 |

El lote es mucho más rápido que enviar de a uno, porque hace menos llamadas. `lingerMs=0` anula el lote, porque cada mensaje sale solo. `acks=0` es más rápido, pero no confirma nada. La compresión manda menos bytes a cambio de más CPU.


