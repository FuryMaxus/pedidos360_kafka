// Caso 2: ver líder, réplicas e ISR de cada partición (y qué brokers están vivos).
// Uso: npm run replicas                 -> una vez
//      npm run replicas -- --watch      -> se actualiza cada 2 s (útil al apagar un broker)
const { crearKafka } = require('../shared/kafka');
const { PEDIDOS, DLT } = require('../shared/topics');
const { leerOpciones } = require('../shared/args');

const opciones = leerOpciones();
const topics = opciones.topic ? [opciones.topic] : [PEDIDOS, DLT];

async function mostrar(admin) {
  const [{ brokers, controller }, { topics: metadata }] = await Promise.all([
    admin.describeCluster(),
    admin.fetchTopicMetadata({ topics })
  ]);
  const vivos = brokers.map(b => b.nodeId).sort();
  console.log(`\n${new Date().toLocaleTimeString('es-CL')}  brokers vivos: [${vivos.join(', ')}]  controlador: ${controller}`);
  for (const t of metadata) {
    console.log(`  ${t.name}`);
    for (const p of t.partitions.sort((a, b) => a.partitionId - b.partitionId)) {
      const fueraDeIsr = p.replicas.filter(r => !p.isr.includes(r));
      const aviso = fueraDeIsr.length ? `   <- fuera del ISR: [${fueraDeIsr.join(', ')}]` : '';
      console.log(`    partición ${p.partitionId}  líder=${p.leader}  réplicas=[${p.replicas.join(', ')}]  isr=[${p.isr.join(', ')}]${aviso}`);
    }
  }
}

async function main() {
  const admin = crearKafka('pedidos360-replicas').admin();
  await admin.connect();
  await mostrar(admin);
  if (!opciones.watch) return admin.disconnect();
  console.log('\n(se actualiza cada 2 s; Ctrl+C para salir)');
  const intervalo = setInterval(() => mostrar(admin).catch(e => console.error('  error:', e.message)), 2000);
  process.once('SIGINT', async () => { clearInterval(intervalo); await admin.disconnect(); process.exit(0); });
}

main().catch(error => { console.error(error.message); process.exitCode = 1; });
