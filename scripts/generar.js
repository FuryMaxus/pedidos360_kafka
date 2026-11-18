// Caso 1: generador sostenido. Publica pedidos simulados con una pausa entre mensajes,
// para ver cómo sube el lag cuando el consumidor es más lento que el productor.
// Uso: npm run generar -- --pedidos=500 --delay=50
//   --pedidos  cantidad de pedidos (cada uno tiene 5 eventos)   [100]
//   --delay    milisegundos de pausa entre un mensaje y el otro  [50]
const { setTimeout: esperar } = require('node:timers/promises');
const { crearProductor, publicar, generarPedidos, intercalar } = require('../bridge/producer');
const { PEDIDOS } = require('../shared/topics');
const { leerOpciones, numero } = require('../shared/args');

async function main() {
  const opciones = leerOpciones();
  const pedidos = numero(opciones, 'pedidos', 100);
  const delay = numero(opciones, 'delay', 50);
  const envios = intercalar(generarPedidos(pedidos));
  const producer = crearProductor();
  await producer.connect();

  let detener = false;
  process.once('SIGINT', () => { detener = true; });
  console.log(`Generando ${envios.length} eventos (${pedidos} pedidos), uno cada ${delay} ms en ${PEDIDOS}. Ctrl+C para detener.`);

  const inicio = Date.now();
  let enviados = 0;
  try {
    for (const { evento, correlationId } of envios) {
      if (detener) break;
      await publicar(producer, evento, correlationId);
      enviados++;
      if (enviados % 100 === 0) console.log(`  ${enviados} eventos publicados`);
      if (delay) await esperar(delay);
    }
  } finally {
    await producer.disconnect();
  }
  const segundos = (Date.now() - inicio) / 1000;
  console.log(`Listo: ${enviados} eventos en ${segundos.toFixed(1)} s (${(enviados / segundos).toFixed(1)} eventos/s).`);
}

main().catch(error => { console.error(error.message); process.exitCode = 1; });
