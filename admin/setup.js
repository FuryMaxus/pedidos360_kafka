// Crea los tópicos del diseño (pedidos.eventos, pedidos.eventos.dlt y pedidos.carga),
// aplica su configuración y la muestra.
const { crearKafka } = require('../shared/kafka');
const { TOPICOS } = require('../shared/topics');
const { crearTopicosBase, describirTopico } = require('./topicos');

function textoRetencion({ retencionMs, retencionBytes }) {
  const tiempo = retencionMs >= 86400000 ? `${retencionMs / 86400000} días` : `${retencionMs / 60000} min`;
  return retencionBytes > 0 ? `${tiempo} o ${retencionBytes / 1024 / 1024} MB` : tiempo;
}

async function main() {
  const admin = crearKafka('pedidos360-admin-setup').admin();
  await admin.connect();
  try {
    const resultado = await crearTopicosBase(admin);
    console.log('Tópicos creados:', resultado.creados.join(', ') || 'ninguno (ya existían)');
    for (const { topic } of TOPICOS) {
      const t = await describirTopico(admin, topic);
      console.log(`- ${t.topic}: ${t.particiones} particiones, ${t.replicas} réplicas, retención ${textoRetencion(t)}, cleanup=${t.cleanupPolicy}, min.insync=${t.minInsyncReplicas}`);
    }
  } finally {
    await admin.disconnect();
  }
}

main().catch(error => { console.error('Error creando tópicos:', error.message); process.exitCode = 1; });
