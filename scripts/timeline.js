
const { timeline, resumen } = require('../audit/handler');

const pedidoId = process.argv[2];
if (pedidoId) {
  const eventos = timeline(pedidoId);
  console.log(eventos.length ? `Timeline de ${pedidoId}:` : `No hay eventos para ${pedidoId}`);
  if (eventos.length) console.table(eventos);
} else {
  const r = resumen();
  console.log(`Eventos auditados: ${r.eventos} | pedidos: ${r.pedidos}`);
  console.table(r.ultimosPedidos);
  console.log('Para ver uno: npm run timeline -- <pedidoId>');
}
