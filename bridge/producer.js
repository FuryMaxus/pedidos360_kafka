
const { randomUUID } = require('node:crypto');
const { crearKafka, particionador } = require('../shared/kafka');
const { crearEvento, validar, serializar } = require('../shared/dto');
const { PEDIDOS } = require('../shared/topics');

const FLUJO_NORMAL = ['PEDIDO_CREADO', 'PEDIDO_ACEPTADO', 'PEDIDO_EN_PREPARACION', 'PEDIDO_DESPACHADO', 'PEDIDO_ENTREGADO'];
const FLUJO_CANCELADO = ['PEDIDO_CREADO', 'PEDIDO_ACEPTADO', 'PEDIDO_CANCELADO'];
const CLIENTES = ['C-100', 'C-101', 'C-102', 'C-103', 'C-104'];
const PRECIOS = [5990, 8490, 10990, 12490, 15990];

function crearProductor() {
  return crearKafka('pedidos360-bridge').producer({
    createPartitioner: particionador,
    idempotent: true,        
    maxInFlightRequests: 1,
    allowAutoTopicCreation: false
  });
}

async function publicar(producer, evento, correlationId) {
  const errores = validar(evento);
  if (errores.length) throw new Error('Evento inválido: ' + errores.join(', '));
  const [resultado] = await producer.send({
    topic: PEDIDOS,
    acks: -1,                
    messages: [{ key: evento.pedidoId, value: serializar(evento), headers: { correlationId } }]
  });
  return resultado;
}

function generarPedidos(cantidad, { cancelados = false } = {}) {
  const lote = Date.now().toString(36).toUpperCase();
  return Array.from({ length: cantidad }, (_, i) => {
    const pedidoId = `PED-${lote}-${String(i + 1).padStart(3, '0')}`;
    const flujo = cancelados && i % 4 === 3 ? FLUJO_CANCELADO : FLUJO_NORMAL;
    let fecha = Date.now() - Math.floor(Math.random() * 3 * 60) * 60000;   // creado hace 0-3 horas
    const datos = { pedidoId, clienteId: CLIENTES[i % CLIENTES.length], total: PRECIOS[i % PRECIOS.length] * (1 + (i % 3)) };
    const eventos = flujo.map(tipo => {
      const evento = crearEvento({ ...datos, tipo, fecha: new Date(fecha).toISOString() });
      fecha += (5 + Math.floor(Math.random() * 10)) * 60000;           
      return evento;
    });
    return { pedidoId, correlationId: randomUUID(), eventos };
  });
}

/** Intercala los eventos por paso (todos los CREADO, luego todos los ACEPTADO, ...). */
function intercalar(pedidos) {
  const salida = [];
  const pasos = Math.max(...pedidos.map(p => p.eventos.length));
  for (let paso = 0; paso < pasos; paso++) {
    for (const p of pedidos) if (p.eventos[paso]) salida.push({ evento: p.eventos[paso], correlationId: p.correlationId });
  }
  return salida;
}

function leerArgumentos(argv) {
  const cantidad = Number(argv.find(a => /^\d+$/.test(a)) ?? 4);
  const iMax = argv.indexOf('--max');
  const max = iMax >= 0 ? Number(argv[iMax + 1]) : Infinity;
  if (!Number.isInteger(cantidad) || cantidad < 1 || !(max > 0)) throw new Error('Uso: node bridge/producer.js [pedidos] [--max N] [--cancelados]');
  return { cantidad, max, cancelados: argv.includes('--cancelados') };
}

async function main() {
  const { cantidad, max, cancelados } = leerArgumentos(process.argv.slice(2));
  const envios = intercalar(generarPedidos(cantidad, { cancelados })).slice(0, max);
  const producer = crearProductor();
  await producer.connect();
  try {
    for (const { evento, correlationId } of envios) {
      const r = await publicar(producer, evento, correlationId);
      console.log(`publicado ${evento.tipo.padEnd(21)} pedido=${evento.pedidoId} particion=${r.partition} offset=${r.baseOffset} correlationId=${correlationId}`);
    }
    console.log(`Total: ${envios.length} eventos de ${cantidad} pedidos en ${PEDIDOS}`);
  } finally {
    await producer.disconnect();
  }
}

if (require.main === module) main().catch(error => { console.error(error.message); process.exitCode = 1; });

module.exports = { crearProductor, publicar, generarPedidos };
