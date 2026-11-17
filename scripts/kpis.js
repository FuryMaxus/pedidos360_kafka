const { kpis } = require('../report/handler');

const r = kpis();
console.log(`Eventos procesados: ${r.eventosProcesados} | pedidos: ${r.pedidos} | cancelados: ${r.cancelados}\n`);
console.log('Ventas por hora (pedidos entregados, hora UTC):');
console.table(r.ventasPorHora);
console.log('Tiempo de entrega (creado -> entregado):');
console.table([r.tiempoEntrega]);
console.log('Pedidos activos por estado:');
console.table(r.activosPorEstado);
