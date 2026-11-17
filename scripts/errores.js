
const { randomUUID } = require('node:crypto');
const { crearKafka, particionador } = require('../shared/kafka');
const { crearEvento, serializar } = require('../shared/dto');
const { PEDIDOS } = require('../shared/topics');

async function main() {
  const producer = crearKafka('pedidos360-errores').producer({ createPartitioner: particionador, idempotent: true, maxInFlightRequests: 1 });
  await producer.connect();
  const id = 'PED-ERR-' + Date.now().toString(36).toUpperCase();
  const valido = (sufijo, tipo = 'PEDIDO_CREADO') => crearEvento({ pedidoId: `${id}-${sufijo}`, clienteId: 'C-900', total: 9990, tipo });
  const repetido = valido('DUP');
  const sinPedido = valido('SIN'); delete sinPedido.pedidoId;

  const casos = [
    ['JSON roto', { key: `${id}-JSON`, value: '{"tipo": "PEDIDO_CREADO", "pedidoId": ' }],
    ['sin pedidoId', { key: `${id}-SIN`, value: serializar(sinPedido) }],
    ['falla temporal', { key: `${id}-TMP`, value: serializar(valido('TMP')), headers: { simularFallo: 'temporal' } }],
    ['falla permanente', { key: `${id}-PERM`, value: serializar(valido('PERM')), headers: { simularFallo: 'permanente' } }],
    ['duplicado (1a vez)', { key: repetido.pedidoId, value: serializar(repetido) }],
    ['duplicado (2a vez)', { key: repetido.pedidoId, value: serializar(repetido) }],
    ['válido al final', { key: `${id}-OK`, value: serializar(valido('OK')) }]
  ];

  try {
    for (const [nombre, mensaje] of casos) {
      const correlationId = randomUUID();
      const [r] = await producer.send({
        topic: PEDIDOS, acks: -1,
        messages: [{ ...mensaje, headers: { ...mensaje.headers, correlationId } }]
      });
      console.log(`${nombre.padEnd(20)} key=${mensaje.key} particion=${r.partition} offset=${r.baseOffset} correlationId=${correlationId}`);
    }
    console.log('\nRevisa los logs de report y audit, luego: npm run dlt');
  } finally {
    await producer.disconnect();
  }
}

main().catch(error => { console.error(error.message); process.exitCode = 1; });
