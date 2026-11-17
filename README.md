# Pedidos360 · Kafka

Proyecto de **DSY1107 – Desarrollo Cloud Native I (EA3 / EP5)**. Agregamos streaming de eventos con **Apache Kafka** a Pedidos360, nuestra plataforma de pedidos de comida.

> **Integrantes:** _(completar)_
> **Repositorios relacionados:** `backend-pedidos360` (serverless AWS), `fronted-pedidos360` (React) y `fronted-pedido360-kafka` (monitor de esta demo).

---

## 1. ¿Qué problema resolvemos?

Pedidos360 ya usaba **SNS + SQS** para tareas como enviar correos, imprimir el ticket de cocina o generar la boleta. Esas colas funcionan bien para **tareas**: el mensaje se procesa y se borra.

Después necesitábamos **reportes** (ventas por hora, tiempo de entrega) y un **historial de todo lo que pasó con cada pedido**. Con una cola eso no sirve: si el mensaje se borra al leerlo, nadie más puede leerlo y no se puede volver a calcular nada con los pedidos de la semana pasada.

Por eso usamos Kafka: es un **log de eventos**. Los mensajes **no se borran al leerlos**, sino cuando se cumple el tiempo de retención, y **varios servicios pueden leer los mismos eventos, cada uno a su ritmo**.

| | Cola (SQS) | Kafka |
| --- | --- | --- |
| Al leer un mensaje | Se borra | Se queda; solo avanza el offset del consumidor |
| ¿Quién lo lee? | Un consumidor por mensaje | Cada consumer group lee **todos** los mensajes |
| ¿Se puede releer? | No | Sí, mientras dure la retención |
| Ideal para | Tareas ("envía este correo") | Hechos ("el pedido 123 fue aceptado") |

---

## 2. ¿Qué construimos?

```text
                         ┌──────────────────────── Kafka (3 brokers) ───────────────────────┐
 bridge/producer.js ───▶ │ pedidos.eventos      (3 particiones · 3 réplicas · 7 días)        │
 (simula los pedidos)    │ pedidos.eventos.dlt  (3 particiones · 3 réplicas · 30 días)       │
                         └──────────────┬─────────────────────────────┬─────────────────────┘
                                        │ group "report"              │ group "audit"
                                        ▼                             ▼
                              report/handler.js              audit/handler.js
                              calcula KPIs                   guarda cada evento
                              (data/report.sqlite)           (data/audit.sqlite)
                                        └──────────────┬──────────────┘
                                                       ▼
                   admin/server.js (Express) ── API REST de administración + /api/monitor/*
                                                       ▲
                               fronted-pedido360-kafka (React) ── monitor sin login
```

- **Productor** (`bridge/producer.js`): publica los cambios de estado de pedidos simulados (CREADO → ACEPTADO → EN_PREPARACION → DESPACHADO → ENTREGADO). La diapositiva 46 permite simular los eventos con un script, así que en local no necesitamos AWS.
- **report**: lee todos los eventos y calcula **ventas por hora**, **tiempo de entrega** y **pedidos activos por estado**.
- **audit**: lee los mismos eventos y guarda cada uno en una tabla para armar el **historial de cada pedido**.
- **DLT** (`pedidos.eventos.dlt`): ahí van los mensajes que no se pudieron procesar, junto con el motivo.
- **admin**: API REST para crear, listar, modificar y borrar tópicos, ver los consumer groups y su lag. También entrega los resultados al monitor.
- **Kafka UI** (<http://localhost:8080>): interfaz web para ver los tópicos, los mensajes y los consumidores.

Todo corre **local**, sin AWS ni LocalStack.

---

## 3. Conceptos de Kafka que aplicamos

| Concepto | Qué es (en simple) | Dónde lo usamos |
| --- | --- | --- |
| **Topic** | El "canal" donde se publican los eventos de un mismo tipo | `pedidos.eventos` y `pedidos.eventos.dlt` |
| **Partición** | Un "carril" del topic. Más carriles permiten más consumidores en paralelo | 3 particiones por topic |
| **Offset** | La posición de un mensaje en su partición: el "marcapáginas" del consumidor | Se ve en los logs, en Kafka UI y en el monitor |
| **Key** | Decide a qué partición va el mensaje. Misma key, misma partición, orden garantizado | `key = pedidoId` |
| **Consumer group** | Un equipo de consumidores. Los del mismo grupo se reparten el trabajo y los de grupos distintos reciben todo | grupos `report` y `audit` |
| **Réplicas** | Copias de cada partición en otros brokers, para no perder datos si uno se cae | 3 réplicas, `min.insync.replicas = 2` |
| **acks = all** | El productor espera que el líder y las réplicas confirmen | `acks: -1` en KafkaJS |
| **Lag** | Cuántos mensajes le faltan por leer a un grupo | `GET /admin/groups/:grupo/lag` |
| **DLT** | El tópico de "objetos perdidos": guarda lo que falló, con su motivo | `pedidos.eventos.dlt` |
| **Idempotencia** | Procesar el mismo mensaje 2 veces tiene el mismo efecto que procesarlo 1 vez | report y audit ignoran un `eventId` repetido |

---

## 4. Requisitos

- **Docker Desktop**, con unos 3 GB de RAM libres porque levantamos 3 brokers.
- **Node.js 22.13 o superior**. Usamos `node:sqlite`, que viene incluido en Node.

---

## 5. Cómo ejecutarlo paso a paso

### Paso 1: levantar Kafka

```bash
docker compose up -d
docker compose ps        # esperar a que kafka1, kafka2 y kafka3 digan "healthy"
```

Esto levanta ZooKeeper, 3 brokers y Kafka UI en <http://localhost:8080>.

### Paso 2: instalar y crear los tópicos

```bash
npm install
cp .env.example .env     # opcional: los valores por defecto ya sirven
npm run topics
```

Debería mostrar:

```text
- pedidos.eventos: 3 particiones, 3 réplicas, 7 días, cleanup=delete, min.insync=2
- pedidos.eventos.dlt: 3 particiones, 3 réplicas, 30 días, cleanup=delete, min.insync=2
```

### Paso 3: levantar los servicios (cada uno en su terminal)

```bash
npm run admin            # API en http://localhost:3000
npm run report           # consumidor del grupo "report"
npm run audit            # consumidor del grupo "audit"
```

### Paso 4: publicar pedidos

```bash
npm run producer -- 4    # 4 pedidos × 5 estados = 20 eventos
```

En las terminales de report y audit aparece un log por cada evento procesado.

### Paso 5: ver los resultados

- **Monitor** (recomendado para la demo), en el repo `fronted-pedido360-kafka`:
  ```bash
  cd ../fronted-pedido360-kafka
  npm install
  npm run dev            # http://localhost:5174
  ```
- **En la terminal:** `npm run kpis` para los reportes y `npm run timeline` para la auditoría.
- **Kafka UI:** Topics → `pedidos.eventos` → Messages.

---

## 6. Guion para la demostración

1. Mostrar `docker compose ps` y, en Kafka UI, los **3 brokers** y los **2 tópicos con 3 particiones y 3 réplicas**.
2. Abrir el **monitor** (<http://localhost:5174>) con `admin`, `report` y `audit` corriendo.
3. Ejecutar `npm run producer -- 2` y mostrar cómo el monitor se actualiza solo: suben los eventos y aparecen en la auditoría.
4. En Kafka UI, abrir un mensaje y mostrar la **key** (`pedidoId`), el **JSON** del evento y el header **`correlationId`**.
5. Hacer los **experimentos** de la sección 7, sobre todo el 2 (rebalanceo), el 4 (lag) y el 5 (DLT).

---

## 7. Experimentos de la clase (diapositiva 44)

| # | Qué hacemos | Qué deberíamos ver |
| --- | --- | --- |
| 1 | `npm run producer -- 3 --max 10` | En Kafka UI, cada `pedidoId` siempre en la **misma partición** |
| 2 | Abrir `npm run report` en **2 terminales** y luego cerrar una | En el log `grupo.asignacion` se reparten las particiones (ej. `[0,1]` y `[2]`). Al cerrar una, la otra toma `[0,1,2]` (**rebalanceo**) |
| 3 | Levantar `audit` después de que report ya leyó | audit recibe **todos** los eventos igual, porque es otro grupo con su propio offset |
| 4 | Detener report, ejecutar `npm run producer -- 4` y consultar `curl localhost:3000/admin/groups/report/lag` | `lagTotal: 20`. Al volver a levantar report, el lag baja a **0** |
| 5 | `npm run errores` y luego `npm run dlt` | JSON roto, evento sin `pedidoId` y falla permanente quedan en el **DLT** con su motivo. La falla temporal se procesa al **segundo intento**, el duplicado **no se procesa dos veces** y el último evento se procesa normal |

> En Kafka UI, la lista **Consumers** muestra el lag como `N/A`. Para ver el valor real hay que entrar al grupo (*Total lag*).

---

## 8. Cómo está organizado el código

```text
docker-compose.yml        ZooKeeper + 3 brokers + Kafka UI
.env.example              variables de entorno (brokers, particiones, réplicas, reintentos)
shared/
  config.js               lee las variables de entorno
  kafka.js                crearKafka(): un solo lugar para configurar el cliente
  topics.js               nombres y configuración de los tópicos
  dto.js                  el evento de pedido: crear, validar, serializar
  consumer-base.js        lo común a los consumidores: validar → reintentar → DLT
  db.js                   conexión a SQLite
bridge/producer.js        productor (simula los pedidos)
report/consumer.js        se conecta a Kafka con el grupo "report"
report/handler.js         lógica de negocio: KPIs
audit/consumer.js         se conecta a Kafka con el grupo "audit"
audit/handler.js          lógica de negocio: guardar eventos e historial
admin/server.js           API REST (Express)
admin/topicos.js          operaciones de administración con KafkaJS Admin
admin/setup.js            crea los tópicos desde la terminal
scripts/                  errores, dlt, kpis y timeline (herramientas de prueba)
```

**Separamos la mensajería de la lógica de negocio.** Cada servicio tiene un `consumer.js`, que solo se conecta a Kafka, y un `handler.js`, con la lógica (calcular KPIs o guardar eventos). La parte repetida (validar, reintentar y enviar al DLT) está una sola vez en `shared/consumer-base.js`.

---

## 9. Decisiones que tomamos (y por qué)

### Tópicos

| Tópico | Particiones | Réplicas | Retención | Key | ¿Por qué? |
| --- | --- | --- | --- | --- | --- |
| `pedidos.eventos` | 3 | 3 | 7 días | pedidoId | 3 carriles para escalar. 7 días permiten releer la semana para reportes |
| `pedidos.eventos.dlt` | 3 | 3 | 30 días | pedidoId | Los fallos se guardan más tiempo para investigarlos |

- **Key = pedidoId:** así los eventos de un pedido llegan **en orden**. Si cayeran en particiones distintas, "ENTREGADO" podría procesarse antes que "ACEPTADO".
- **3 réplicas y `min.insync.replicas = 2`:** si se cae un broker, no se pierde nada y se puede seguir escribiendo.
- La clase sugiere 1 partición para el DLT, pero la evaluación pide **dos tópicos con 3 particiones y 3 réplicas**, así que usamos 3. Se puede cambiar en `.env`.

### El evento (DTO)

```json
{
  "eventId": "27e52397-62de-4744-b23d-0a0a27aaf8cc",
  "tipo": "PEDIDO_ENTREGADO",
  "pedidoId": "PED-MWL2CS60-001",
  "clienteId": "C-100",
  "total": 10990,
  "fecha": "2026-11-17T06:36:28.736Z"
}
```

Cada mensaje lleva además el header `correlationId`, el mismo en todos los eventos de un pedido, para seguirlo de punta a punta.

### Productor

- `acks: -1` (all): espera la confirmación de las réplicas, así no se pierden pedidos.
- `idempotent: true`: si la red falla y reintenta, Kafka no duplica el mensaje.
- Valida el evento **antes** de enviarlo.

### Manejo de errores en los consumidores

1. **El mensaje viene mal** (JSON roto o le falta un campo): va **directo al DLT**, porque reintentar no lo arregla.
2. **Falla al procesar** (por ejemplo, la base de datos ocupada): se reintenta **3 veces**, esperando 1 s y luego 2 s. Si sigue fallando, va al DLT.
3. En el DLT queda el mensaje original, el **motivo**, el grupo, los intentos, el `correlationId`, la partición y el offset.
4. Un mensaje malo **no bloquea** a los siguientes: se manda al DLT y el consumidor sigue.
5. Kafka entrega **al menos una vez**, así que un mensaje puede llegar repetido. Por eso report y audit guardan el `eventId` y **no aplican dos veces** el mismo evento.

> Para mostrar los reintentos sin romper nada, `npm run errores` envía mensajes con el header de prueba `simularFallo`.

### KPIs de report

- **Ventas por hora:** pedidos ENTREGADOS, agrupados por hora UTC, con cantidad y monto.
- **Tiempo de entrega:** minutos entre PEDIDO_CREADO y PEDIDO_ENTREGADO (promedio, mínimo y máximo).
- **Pedidos activos por estado:** los que no están ENTREGADOS ni CANCELADOS.

### ¿Por qué SQLite?

En el diagrama de la clase los KPIs y la auditoría se guardan en Oracle. Para trabajar local y simple usamos **SQLite**, que viene incluido en Node y no necesita instalar nada.

---

## 10. API del administrador (puerto 3000)

| Método | Ruta | Para qué |
| --- | --- | --- |
| POST | `/admin/topics/setup` | Crear los tópicos del diseño |
| POST | `/admin/topics` | Crear un tópico: `{ "topic", "particiones", "replicas", "retencionDias", "cleanupPolicy" }` |
| GET | `/admin/topics` | Listar tópicos con particiones, réplicas, retención y cantidad de mensajes |
| GET | `/admin/topics/:topic` | Detalle de un tópico |
| PUT | `/admin/topics/:topic/config` | Cambiar `retencionDias`, `cleanupPolicy` o `minInsyncReplicas` |
| PUT | `/admin/topics/:topic/partitions` | Aumentar particiones: `{ "total" }` |
| DELETE | `/admin/topics/:topic` | Borrar un tópico |
| GET | `/admin/groups` | Ver consumer groups y sus miembros |
| GET | `/admin/groups/:groupId/lag` | Ver el lag de un grupo por partición |
| GET | `/api/monitor/kpis` | KPIs de report (lo usa el monitor) |
| GET | `/api/monitor/eventos` | Últimos 50 eventos de audit (lo usa el monitor) |

Ejemplos:

```bash
curl -X POST localhost:3000/admin/topics -H "content-type: application/json" -d '{"topic":"prueba","particiones":2,"replicas":3,"retencionDias":1}'
curl -X PUT  localhost:3000/admin/topics/prueba/config -H "content-type: application/json" -d '{"retencionDias":3}'
curl localhost:3000/admin/groups/report/lag
curl -X DELETE localhost:3000/admin/topics/prueba
```

---

## 11. Rúbrica: dónde está cada indicador (diapositiva 45)

| Indicador | Dónde está |
| --- | --- |
| Configuración del cliente Kafka (brokers, particiones, réplicas) | `shared/kafka.js`, `shared/config.js`, `shared/topics.js`, `docker-compose.yml` |
| Productores y consumidores con DTO | `shared/dto.js`, `bridge/producer.js`, `report/` y `audit/` |
| Productores con DTO y serializers | `bridge/producer.js` (JSON, key, headers, acks, idempotencia) |
| Consumidores con lógica separada de la mensajería | `consumer.js` (mensajería) y `handler.js` (negocio) |
| Manejo de errores: logs, validación, reintentos | `shared/consumer-base.js` y los logs JSON de cada servicio |
| Parámetros de tópicos (retención, cleanup, particiones, RF) | `shared/topics.js` y `PUT /admin/topics/:topic/config` |
| Consumer groups | grupos `report` y `audit` (experimentos 2, 3 y 4) |
| Servicio administrador (tópicos y grupos) | `admin/server.js` y `admin/topicos.js` |
| Docker Compose con ZooKeeper, brokers y Kafka UI | `docker-compose.yml` |

---

## 12. Comandos

| Comando | Qué hace |
| --- | --- |
| `npm run topics` | Crea los tópicos si no existen y muestra su configuración |
| `npm run admin` | API de administración y del monitor |
| `npm run report` / `npm run audit` | Consumidores (se pueden abrir varias instancias) |
| `npm run producer -- [pedidos] [--max N] [--cancelados]` | Publica pedidos simulados |
| `npm run errores` | Publica mensajes con errores para probar el DLT y los reintentos |
| `npm run dlt` | Muestra lo que hay en el DLT |
| `npm run kpis` | Muestra los reportes en la terminal |
| `npm run timeline -- [pedidoId]` | Muestra la auditoría en la terminal |

---

## 13. Problemas comunes

| Problema | Solución |
| --- | --- |
| `There is no leader for this topic-partition` o errores de conexión al iniciar | Los brokers todavía están partiendo. Esperar a que `docker compose ps` muestre los 3 *healthy* |
| `The group coordinator is not available` la primera vez | Es normal: Kafka está creando su tópico interno de offsets. Se resuelve solo en unos segundos |
| El monitor dice "No se pudo actualizar" | Falta levantar `npm run admin` |
| El monitor muestra datos viejos | Falta levantar `npm run report` o `npm run audit` |
| Quiero partir de cero | `docker compose down -v` y borrar la carpeta `data/` |

---

## 14. De local a la nube (EC2)

El código no cambia: solo se modifica la variable `KAFKA_BROKERS`.

1. En la EC2: `KAFKA_HOST=<ip-de-la-ec2> docker compose up -d`.
2. En el computador, en `.env`: `KAFKA_BROKERS=<ip>:9092,<ip>:9093,<ip>:9094`.
3. Abrir en el security group los puertos 9092-9094 y 8080 **solo para nuestra IP**.
4. **Apagar la EC2** al terminar, para no gastar créditos del Learner Lab.

No usamos MSK porque cobra por clúster encendido aunque nadie lo esté usando.

---

## 15. Detener todo

```bash
docker compose down        # apaga Kafka y conserva los datos
docker compose down -v     # apaga y borra tópicos y offsets
```

Si se usa `-v`, hay que borrar también la carpeta `data/`, porque ahí está registrado qué eventos ya se procesaron.

---

## 16. Lo que aprendimos

- La diferencia entre **colas** (tareas que se borran) y **logs** (hechos que se pueden releer).
- Que la **key** es lo que mantiene el orden de los eventos de un pedido.
- Que dos **consumer groups** distintos reciben los mismos mensajes, y que dentro de un grupo se reparten las particiones.
- Que el **lag** sirve para saber si un consumidor va atrasado o se cayó.
- Que un buen consumidor **valida, reintenta, manda al DLT y es idempotente**, para que un mensaje malo no bloquee a todos.
