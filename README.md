# Pedidos360 · Kafka

Streaming de eventos de pedidos con Kafka y Node.js (KafkaJS). **ms-pedidos360-orders** (la API que usa el frontend) publica cada cambio de estado de un pedido en `pedidos.eventos`. Dos servicios los leen **en paralelo y por separado**:

- **report** (consumer group `report`): calcula ventas por hora, tiempo de entrega y pedidos activos por estado.
- **audit** (consumer group `audit`): guarda cada evento para reconstruir el historial (timeline) de un pedido.

Los mensajes que no se pueden procesar van a `pedidos.eventos.dlt` con el motivo. Un servicio administrador REST crea, lista, modifica y elimina tópicos y consulta consumer groups y lag.

```text
Frontend React ─▶ orders/server.js (ms-pedidos360-orders: pedidos + catálogo en SQLite)
                     │  (o bridge/producer.js: simulador para pruebas y carga)
                     ▼
              ──key=pedidoId, acks=-1, idempotente──▶ pedidos.eventos (3 particiones × 3 réplicas, 7 días)
                                                              ├─ group report ─▶ report/handler.js ─▶ data/report.sqlite
                                                              └─ group audit  ─▶ audit/handler.js  ─▶ data/audit.sqlite
            JSON inválido · datos inválidos · 3 intentos fallidos ─▶ pedidos.eventos.dlt (3 × 3, 30 días)
admin/server.js (Express + KafkaJS Admin) ─▶ tópicos, configuración, grupos y lag
```

La demostración es **100 % local, sin AWS ni LocalStack**. En la nube, el frontend usa API Gateway + Lambda + DynamoDB (`backend-pedidos360`). En local, `ms-pedidos360-orders` cumple ese papel con las mismas rutas y reglas, y además es el productor de Kafka, como en el diagrama de arquitectura. Las tareas (correo, cocina, boleta) siguen siendo responsabilidad de SNS/SQS en la nube y no forman parte de esta demo.

## Requisitos

- Docker Desktop (unos 3 GB de RAM libres para 3 brokers).
- Node.js 22.13 o superior (se usa `node:sqlite`).

## Puesta en marcha

```bash
docker compose up -d          # ZooKeeper + kafka1/2/3 + Kafka UI
docker compose ps             # los 3 brokers deben quedar "healthy"
cp .env.example .env          # opcional: los valores por defecto sirven en local
npm install
npm run topics                # crea pedidos.eventos y pedidos.eventos.dlt
```

Kafka UI: <http://localhost:8080>. Brokers desde el computador: `localhost:9092`, `localhost:9093` y `localhost:9094`. Desde la red Docker: `kafkaN:29092`.

Cada servicio corre en su propia terminal:

```bash
npm run admin                 # REST de administración en http://localhost:3000
npm run orders                # ms-pedidos360-orders en http://localhost:4000 (lo usa el frontend)
npm run report
npm run audit
npm run producer -- 4         # opcional: 4 pedidos simulados = 20 eventos
```

### Demo con el frontend

Con `orders`, `report` y `audit` corriendo, en `../fronted-pedidos360`:

```bash
npm install
npm run dev:demo              # http://localhost:5173, usa .env.demo
```

1. **Entrar como Cliente** → Pedidos → Nuevo pedido → Crear. `orders` publica `PEDIDO_CREADO`.
2. **Salir → Entrar como Operador** → abrir el pedido → Aceptar → Pasar a preparación → Despachar → Marcar como entregado. Cada botón publica un evento.
3. En el detalle del pedido, la sección **Auditoría (Kafka)** muestra los eventos que guardó el servicio audit, con partición y offset. Se actualiza sola cada 3 s.
4. Menú **Reportes**: KPIs del servicio report (ventas por hora, tiempo de entrega, activos por estado).
5. En Kafka UI (<http://localhost:8080>) se ven los mismos mensajes en `pedidos.eventos`.

El login de la demo usa **perfiles locales** (Cliente, Operador, Administrador), sin Microsoft Entra ID. El frontend envía el perfil en los headers `X-User-Id` y `X-User-Role`, y `orders` aplica los mismos permisos que la Lambda. No es un mecanismo de seguridad: sirve solo para la demo local.

## Comandos

| Comando | Qué hace |
| --- | --- |
| `npm run topics` | Crea los tópicos del diseño si faltan y muestra su configuración |
| `npm run admin` | Servicio administrador REST (también crea los tópicos al iniciar) |
| `npm run orders` | ms-pedidos360-orders: API de pedidos y catálogo para el frontend; productor de Kafka |
| `npm run producer -- [pedidos] [--max N] [--cancelados]` | Publica pedidos simulados (5 eventos por pedido) |
| `npm run report` / `npm run audit` | Consumidores; se pueden abrir varias instancias |
| `npm run errores` | Publica mensajes problemáticos para probar validación, reintentos, DLT e idempotencia |
| `npm run dlt` | Muestra el contenido de `pedidos.eventos.dlt` |
| `npm run kpis` | Muestra los KPIs calculados por report (solo lectura) |
| `npm run timeline -- [pedidoId]` | Muestra el historial guardado por audit (solo lectura) |

## Estructura

```text
docker-compose.yml        ZooKeeper, 3 brokers y Kafka UI
shared/
  config.js               variables de entorno (KAFKA_BROKERS, particiones, réplicas, reintentos)
  kafka.js                crearKafka(clientId): único punto de configuración del cliente
  topics.js               nombres y configuración de los tópicos (versionada)
  dto.js                  evento de pedido: crearEvento, validar, serializar, deserializar
  consumer-base.js        mensajería común: leer → validar → reintentar → DLT → commit
  db.js                   SQLite local
orders/server.js          ms-pedidos360-orders: rutas REST, roles y publicación en Kafka
orders/handler.js         lógica de pedidos y catálogo (transiciones, stock) en SQLite
orders/catalogo-inicial.json  productos de ejemplo
bridge/producer.js        productor: key, acks, idempotencia y correlationId (+ simulador)
report/consumer.js        group "report" (solo mensajería)
report/handler.js         lógica de negocio: KPIs
audit/consumer.js         group "audit"
audit/handler.js          lógica de negocio: tabla de eventos y timeline
admin/topicos.js          operaciones de KafkaJS Admin
admin/server.js           API REST
admin/setup.js            creación de tópicos por línea de comandos
scripts/                  errores, dlt, kpis y timeline
```

## Decisiones de diseño

### Tópicos

| Tópico | Particiones | Réplicas | min.insync | Retención | Cleanup | Key | Lo leen |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `pedidos.eventos` | 3 | 3 | 2 | 7 días | delete | pedidoId | grupos report y audit |
| `pedidos.eventos.dlt` | 3 | 3 | 2 | 30 días | delete | pedidoId | revisión manual o reproceso |

- **3 particiones:** hasta 3 instancias de cada servicio trabajan en paralelo.
- **Key = pedidoId:** todos los eventos de un pedido caen en la misma partición, y por eso se procesan en orden.
- **3 réplicas y min.insync = 2:** si se cae un broker no se pierden datos y se puede seguir escribiendo con `acks=all`.
- **7 días:** permiten releer la semana para reportería. Los 30 días del DLT dan tiempo para investigar los fallos.
- El DLT usa 3 particiones y 3 réplicas para cumplir el criterio "dos tópicos con 3 particiones y 3 réplicas". La clase sugiere 1 partición. Ambas opciones se configuran por variables de entorno.

### Evento (DTO)

```json
{ "eventId": "uuid", "tipo": "PEDIDO_CREADO", "pedidoId": "PED-…-001", "clienteId": "C-100", "total": 5990, "fecha": "2026-11-17T04:49:38.279Z" }
```

`tipo` admite `PEDIDO_CREADO`, `PEDIDO_ACEPTADO`, `PEDIDO_EN_PREPARACION`, `PEDIDO_DESPACHADO`, `PEDIDO_ENTREGADO` y `PEDIDO_CANCELADO`. Cada mensaje lleva el header `correlationId`, compartido por todos los eventos de un mismo pedido, para rastrearlo de punta a punta.

### Productores

`acks: -1` (all), `idempotent: true` con `maxInFlightRequests: 1`, y validación del DTO antes de enviar.

- **ms-pedidos360-orders** publica un evento por cada acción del frontend: crear pedido y cada cambio de estado, incluida la cancelación. La Lambda publicaba solo ACEPTADO y DESPACHADO, lo que no alcanza para los KPIs. El `correlationId` es el id del pedido. Orden de operaciones: valida → **publica en Kafka** → guarda en SQLite. Si Kafka no confirma, responde 503 y el pedido no cambia. Al aceptar, se verifica y descuenta el stock, igual que la Lambda.
- **bridge/producer.js** (simulador) genera pedidos completos sin pasar por la UI, para los experimentos y la carga. Sus fechas se simulan dentro de las últimas 3 horas, con 5 a 15 minutos entre estados.

### API de ms-pedidos360-orders (puerto 4000)

| Método | Ruta | Rol |
| --- | --- | --- |
| GET | `/api/orders` (`?cliente_id=`) | todos (el cliente solo ve los suyos) |
| GET | `/api/orders/:id` | todos (el cliente solo los suyos) |
| POST | `/api/orders` `{ cliente_id, items: [{ producto_id, cantidad }] }` | Cliente, Operador |
| PATCH | `/api/orders/:id/estado` `{ estado }` | Operador, Administrador |
| GET | `/api/catalog` | público |
| POST / PATCH / DELETE | `/api/catalog[/:id]`, `PATCH /api/catalog/:id/stock` | Administrador |
| GET | `/api/reports/kpis` | Operador, Administrador (lee los resultados de report) |
| GET | `/api/audit/:pedidoId` | todos (el cliente solo sus pedidos; lee los resultados de audit) |

### Consumidores y manejo de errores

1. **JSON inválido o datos inválidos:** el mensaje va directo al DLT, porque reintentar no lo arregla.
2. **Error al procesar:** hasta 3 intentos, esperando 1 s y luego 2 s entre ellos. Si los 3 fallan, va al DLT con `motivo = "agotados los reintentos"`.
3. El DLT guarda el mensaje original, el motivo, el grupo, los intentos, el `correlationId` y el origen (partición y offset).
4. Al terminar sin error, KafkaJS confirma el offset. La entrega es **al menos una vez**, así que los dos servicios son **idempotentes** por `eventId`: un evento repetido no duplica efectos.
5. Para demostrar reintentos sin romper nada, `consumer-base.js` acepta el header de prueba `simularFallo` (`temporal` o `permanente`). Solo lo usa `npm run errores`.

Cada grupo lee todos los mensajes. Por eso un mensaje malo aparece **dos veces** en el DLT, una por grupo (campo `grupo`).

### KPIs de report

- **Ventas por hora:** pedidos `ENTREGADO`, agrupados por la hora UTC de entrega (cantidad y monto).
- **Tiempo de entrega:** minutos entre `PEDIDO_CREADO` y `PEDIDO_ENTREGADO` (promedio, mínimo y máximo).
- **Pedidos activos por estado:** pedidos que no están `ENTREGADO` ni `CANCELADO`.

SQLite reemplaza localmente a la base de KPIs y auditoría del diagrama de clase.

## API del administrador

| Método | Ruta | Cuerpo / query |
| --- | --- | --- |
| POST | `/admin/topics/setup` | crea los tópicos del diseño |
| POST | `/admin/topics` | `{ "topic", "particiones", "replicas", "retencionDias", "cleanupPolicy" }` |
| GET | `/admin/topics` | lista con particiones, réplicas, ISR, retención y mensajes |
| GET | `/admin/topics/:topic` | detalle de un tópico |
| PUT | `/admin/topics/:topic/config` | `{ "retencionDias", "cleanupPolicy", "minInsyncReplicas" }` |
| PUT | `/admin/topics/:topic/partitions` | `{ "total" }` (solo se puede aumentar) |
| DELETE | `/admin/topics/:topic` | elimina el tópico |
| GET | `/admin/groups` | consumer groups, estado y miembros |
| GET | `/admin/groups/:groupId/lag` | lag por partición (`?topic=` opcional) |

```bash
curl -X POST localhost:3000/admin/topics -H "content-type: application/json" -d '{"topic":"prueba","particiones":2,"replicas":3,"retencionDias":1}'
curl -X PUT  localhost:3000/admin/topics/prueba/config -H "content-type: application/json" -d '{"retencionDias":3}'
curl localhost:3000/admin/groups/report/lag
curl -X DELETE localhost:3000/admin/topics/prueba
```

## Experimentos (diapositiva 44)

| # | Pasos | Qué se observa |
| --- | --- | --- |
| 1 | `npm run producer -- 3 --max 10` | En Kafka UI (Topics → pedidos.eventos → Messages) cada `pedidoId` queda siempre en la misma partición. Con pocas keys puede quedar una partición vacía |
| 2 | Abrir `npm run report` en 2 terminales y cerrar una | El log `grupo.asignacion` muestra el reparto (ej. `[0,1]` y `[2]`). Al cerrar una, la otra recibe `[0,1,2]`. Si el proceso muere sin cerrarse, el rebalanceo ocurre tras el session timeout (unos 30 s) |
| 3 | Levantar `npm run audit` después de report | audit recibe todos los eventos desde el inicio (`npm run timeline`) |
| 4 | Detener report, `npm run producer -- 4` y luego `curl localhost:3000/admin/groups/report/lag` | `lagTotal: 20`. Al volver a levantar report, el lag baja a 0 |
| 5 | `npm run errores` y luego `npm run dlt` | JSON roto, evento sin `pedidoId` y falla permanente en el DLT con su motivo. La falla temporal se procesa al segundo intento, el duplicado aparece con `"duplicado":true` y el evento final se procesa |

En Kafka UI, la lista **Consumers** muestra el lag como `N/A`. El valor real aparece al entrar al grupo (`report` o `audit` → *Total lag*).

También se puede usar el CLI de Kafka:

```bash
docker compose exec kafka1 kafka-consumer-groups --bootstrap-server localhost:29092 --describe --group report
```

## De local a EC2

En la EC2 se ejecuta `KAFKA_HOST=<ip-pública-o-privada> docker compose up -d`, y en el computador se apunta `KAFKA_BROKERS=<ip>:9092,<ip>:9093,<ip>:9094` en `.env`. El código no cambia. El security group debe abrir solo para tu IP los puertos 9092-9094 y 8080. Detén la instancia al terminar.

## Detener

```bash
docker compose down           # conserva los datos (volúmenes)
docker compose down -v        # borra también los tópicos y offsets
```

Si se borran los volúmenes, borra también `data/`, porque guarda qué eventos ya se procesaron.
