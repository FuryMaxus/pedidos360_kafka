
const { setTimeout: esperar } = require('node:timers/promises');
const { crearKafka, particionador } = require('./kafka');
const { PEDIDOS, DLT } = require('./topics');
const { deserializar, validar } = require('./dto');
const config = require('./config');

function crearLogger(servicio) {
  return (nivel, accion, datos = {}) => {
    const linea = JSON.stringify({ fecha: new Date().toISOString(), nivel, servicio, accion, ...datos });
    (nivel === 'error' ? console.error : console.log)(linea);
  };
}

const header = (headers, nombre) => headers?.[nombre]?.toString('utf8');

function simularFallo(tipo, intento) {
  if (tipo === 'permanente' || (tipo === 'temporal' && intento === 1)) {
    throw new Error(`falla simulada (${tipo}) en el intento ${intento}`);
  }
}

/**
 * @param {object} opciones
 * @param {string} opciones.servicio  nombre para logs y clientId (ej: 'report')
 * @param {string} opciones.groupId   consumer group (report y audit usan grupos distintos)
 * @param {(evento, meta) => any} opciones.procesar  lógica de negocio; debe ser idempotente
 */
async function iniciarConsumidor({ servicio, groupId, procesar }) {
  const log = crearLogger(servicio);
  const kafka = crearKafka(`${servicio}-service`);
  const consumer = kafka.consumer({ groupId, allowAutoTopicCreation: false });
  const dlt = kafka.producer({ createPartitioner: particionador, idempotent: true, maxInFlightRequests: 1, allowAutoTopicCreation: false });

  async function enviarADlt(message, partition, motivo, correlationId, intentos) {
    await dlt.send({
      topic: DLT,
      acks: -1,
      messages: [{
        key: message.key,
        value: JSON.stringify({
          motivo,
          grupo: groupId,
          intentos,
          correlationId,
          origen: { topic: PEDIDOS, particion: partition, offset: message.offset },
          fechaError: new Date().toISOString(),
          mensajeOriginal: message.value?.toString('utf8') ?? null
        }),
        headers: { correlationId, motivo, grupo: groupId }
      }]
    });
    log('warn', 'dlt.enviado', { motivo, correlationId, particion: partition, offset: message.offset });
  }

  async function manejarMensaje({ partition, message }) {
    const correlationId = header(message.headers, 'correlationId') || `${PEDIDOS}:${partition}:${message.offset}`;
    const meta = { particion: partition, offset: message.offset, correlationId };

    let evento;
    try {
      evento = deserializar(message.value);
    } catch {
      return enviarADlt(message, partition, 'JSON invalido', correlationId, 0);
    }
    const errores = validar(evento);
    if (errores.length) return enviarADlt(message, partition, errores.join('; '), correlationId, 0);

    const fallo = header(message.headers, 'simularFallo');
    for (let intento = 1; intento <= config.intentos; intento++) {
      try {
        simularFallo(fallo, intento);
        const resultado = await procesar(evento, meta);
        log('info', 'evento.procesado', { ...meta, eventId: evento.eventId, pedidoId: evento.pedidoId, tipo: evento.tipo, ...resultado });
        return;
      } catch (error) {
        log('warn', 'evento.reintento', { ...meta, eventId: evento.eventId, intento, error: error.message });
        if (intento < config.intentos) await esperar(config.esperaBaseMs * 2 ** (intento - 1));
      }
    }
    await enviarADlt(message, partition, 'agotados los reintentos', correlationId, config.intentos);
  }

  consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => {
    log('info', 'grupo.asignacion', { groupId, particiones: payload.memberAssignment[PEDIDOS] || [] });
  });

  await dlt.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: PEDIDOS, fromBeginning: true });

  await consumer.run({ eachMessage: manejarMensaje });
  log('info', 'consumidor.iniciado', { groupId, topic: PEDIDOS });

  const cerrar = async () => {
    log('info', 'consumidor.deteniendo', { groupId });
    await consumer.disconnect();
    await dlt.disconnect();
    process.exit(0);
  };
  process.once('SIGINT', cerrar);
  process.once('SIGTERM', cerrar);
}

module.exports = { iniciarConsumidor };
