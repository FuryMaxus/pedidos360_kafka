
const { crearKafka } = require('../shared/kafka');
const { DLT } = require('../shared/topics');

async function main() {
  const ultimos = Number(process.argv[2] ?? 20);
  const kafka = crearKafka('pedidos360-dlt-viewer');
  const admin = kafka.admin();
  await admin.connect();
  const finales = await admin.fetchTopicOffsets(DLT);
  const pendientes = new Map(finales.filter(p => Number(p.high) > Number(p.low)).map(p => [p.partition, Number(p.high)]));
  if (!pendientes.size) { console.log('El DLT está vacío.'); await admin.disconnect(); return; }

  const groupId = 'dlt-viewer-' + Date.now();
  const consumer = kafka.consumer({ groupId });
  const mensajes = [];
  await consumer.connect();
  await consumer.subscribe({ topic: DLT, fromBeginning: true });
  await new Promise((resolve, reject) => {
    consumer.run({
      eachMessage: async ({ partition, message }) => {
        mensajes.push({ particion: partition, offset: message.offset, timestamp: Number(message.timestamp), ...JSON.parse(message.value.toString()) });
        if (Number(message.offset) + 1 >= pendientes.get(partition)) pendientes.delete(partition);
        if (!pendientes.size) resolve();
      }
    }).catch(reject);
  });
  await consumer.disconnect();
  await admin.deleteGroups([groupId]);   // no dejar grupos temporales en Kafka UI
  await admin.disconnect();

  mensajes.sort((a, b) => a.timestamp - b.timestamp);
  console.log(`${mensajes.length} mensajes en ${DLT}; últimos ${Math.min(ultimos, mensajes.length)}:\n`);
  for (const m of mensajes.slice(-ultimos)) {
    console.log(`[${m.grupo}] ${m.motivo} | intentos=${m.intentos} | origen p${m.origen.particion}@${m.origen.offset} | correlationId=${m.correlationId}`);
    console.log(`    original: ${m.mensajeOriginal}`);
  }
}

main().catch(error => { console.error(error.message); process.exitCode = 1; });
