// Caso 3: ver cómo Kafka borra los mensajes viejos según la retención.
// Uso: npm run retencion
//
// Crea el tópico pedidos.demo-retencion con retention.ms=20 s y segment.ms=10 s,
// envía 100 mensajes, espera 15 s, envía otros 100 y muestra cada 5 s:
//   low  = primer offset disponible (sube cuando Kafka borra)
//   high = próximo offset a escribir (no cambia: los offsets nunca se reutilizan)
// Kafka solo borra segmentos cerrados y revisa cada 30 s (KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS
// en docker-compose.yml), así que el borrado tarda un poco más que los 20 s.
const { setTimeout: esperar } = require('node:timers/promises');
const { crearKafka, particionador } = require('../shared/kafka');
const config = require('../shared/config');

const TOPIC = 'pedidos.demo-retencion';
const DURACION_S = 120;

async function enviar(producer, desde, cantidad) {
  const messages = Array.from({ length: cantidad }, (_, i) => ({
    key: `PED-DEMO-${desde + i}`,
    value: JSON.stringify({ n: desde + i, fecha: new Date().toISOString() })
  }));
  await producer.send({ topic: TOPIC, acks: -1, messages });
}

async function estado(admin) {
  const [p] = await admin.fetchTopicOffsets(TOPIC);
  return { low: Number(p.low), high: Number(p.high) };
}

async function main() {
  const kafka = crearKafka('pedidos360-retencion');
  const admin = kafka.admin();
  const producer = kafka.producer({ createPartitioner: particionador, allowAutoTopicCreation: false });
  await admin.connect();
  await producer.connect();
  try {
    // Se recrea el tópico para partir siempre de cero.
    if ((await admin.listTopics()).includes(TOPIC)) {
      await admin.deleteTopics({ topics: [TOPIC] });
      await esperar(3000);
    }
    await admin.createTopics({
      waitForLeaders: true,
      topics: [{
        topic: TOPIC, numPartitions: 1, replicationFactor: config.replicas,
        configEntries: [
          { name: 'retention.ms', value: '20000' },   // 20 s
          { name: 'segment.ms', value: '10000' },     // cierra un segmento cada 10 s
          { name: 'cleanup.policy', value: 'delete' }
        ]
      }]
    });
    console.log(`Tópico ${TOPIC} creado: retention.ms=20s, segment.ms=10s. Mirando durante ${DURACION_S} s...\n`);
    console.log('segundos   low   high   disponibles');

    const inicio = Date.now();
    await enviar(producer, 0, 100);
    let segundoLote = false;
    for (let s = 0; s <= DURACION_S; s += 5) {
      await esperar(Math.max(0, inicio + s * 1000 - Date.now()));
      if (!segundoLote && s >= 15) { await enviar(producer, 100, 100); segundoLote = true; }
      const { low, high } = await estado(admin);
      const marca = low > 0 ? `   <- Kafka ya borró ${low}` : '';
      console.log(`${String(s).padStart(5)}    ${String(low).padStart(4)}   ${String(high).padStart(4)}   ${String(high - low).padStart(6)}${marca}`);
      if (s > 30 && low === high) break;
    }
    console.log('\nhigh no cambia (los offsets siguen) y low sube: los mensajes viejos ya no se pueden leer.');
    console.log(`Configuración visible en Kafka UI > Topics > ${TOPIC} > Settings.`);
  } finally {
    await producer.disconnect();
    await admin.disconnect();
  }
}

main().catch(error => { console.error(error.message); process.exitCode = 1; });
