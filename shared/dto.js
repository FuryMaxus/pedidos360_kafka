// Evento de pedido (DTO): estructura, creación, validación y serialización JSON.
const { randomUUID } = require('node:crypto');

const TIPOS = [
  'PEDIDO_CREADO',
  'PEDIDO_ACEPTADO',
  'PEDIDO_EN_PREPARACION',
  'PEDIDO_DESPACHADO',
  'PEDIDO_ENTREGADO',
  'PEDIDO_CANCELADO'
];

/**
 * @typedef {Object} EventoPedido
 * @property {string} eventId   UUID único del evento
 * @property {string} tipo      uno de TIPOS
 * @property {string} pedidoId  también es la key del mensaje en Kafka
 * @property {string} clienteId
 * @property {number} total     monto del pedido en pesos
 * @property {string} fecha     ISO 8601 (UTC)
 */

/** @returns {EventoPedido} */
function crearEvento(d) {
  return {
    eventId: d.eventId || randomUUID(),
    tipo: d.tipo,
    pedidoId: d.pedidoId,
    clienteId: d.clienteId,
    total: d.total,
    fecha: d.fecha || new Date().toISOString()
  };
}

const textoNoVacio = v => typeof v === 'string' && v.trim() !== '';

/** Devuelve la lista de errores; [] significa que el evento es válido. */
function validar(e) {
  if (!e || typeof e !== 'object' || Array.isArray(e)) return ['el evento debe ser un objeto JSON'];
  const err = [];
  if (!textoNoVacio(e.eventId)) err.push('eventId requerido');
  if (!TIPOS.includes(e.tipo)) err.push('tipo invalido');
  if (!textoNoVacio(e.pedidoId)) err.push('pedidoId requerido');
  if (!textoNoVacio(e.clienteId)) err.push('clienteId requerido');
  if (typeof e.total !== 'number' || !Number.isFinite(e.total) || e.total < 0) err.push('total debe ser un numero >= 0');
  if (typeof e.fecha !== 'string' || Number.isNaN(Date.parse(e.fecha))) err.push('fecha invalida');
  return err;
}

const serializar = evento => JSON.stringify(evento);

/** Convierte el value de Kafka en evento. Lanza error si no es JSON. */
function deserializar(buffer) {
  return JSON.parse(buffer.toString('utf8'));
}

module.exports = { TIPOS, crearEvento, validar, serializar, deserializar };
