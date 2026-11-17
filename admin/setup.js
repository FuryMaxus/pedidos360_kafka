// Crea los tópicos del diseño (pedidos.eventos y pedidos.eventos.dlt) y muestra su configuración.
const { crearKafka } = require('../shared/kafka');
const { TOPICOS } = require('../shared/topics');
const { crearTopicosBase, describirTopico } = require('./topicos');

async function main() {
  const admin = crearKafka('pedidos360-admin-setup').admin();
  await admin.connect();
  try {
    const resultado = await crearTopicosBase(admin);
    console.log('Tópicos creados:', resultado.creados.join(', ') || 'ninguno (ya existían)');
    for (const { topic } of TOPICOS) {
      const t = await describirTopico(admin, topic);
      console.log(`- ${t.topic}: ${t.particiones} particiones, ${t.replicas} réplicas, ${t.retencionDias} días, cleanup=${t.cleanupPolicy}, min.insync=${t.minInsyncReplicas}`);
    }
  } finally {
    await admin.disconnect();
  }
}

main().catch(error => { console.error('Error creando tópicos:', error.message); process.exitCode = 1; });
