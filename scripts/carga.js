// Caso 4: prueba de carga y tuning del productor sobre el tópico pedidos.carga.
// Uso:
//   npm run carga -- --count=20000 --modo=simple
//   npm run carga -- --count=20000 --modo=lote
//   npm run carga -- --count=20000 --modo=lote --lingerMs=0
//   npm run carga -- --count=20000 --modo=lote --acks=0
//   npm run carga -- --count=20000 --modo=lote --acks=-1
//   npm run carga -- --count=20000 --modo=lote --compresion=none
//
// Opciones:
//   --count       cantidad de mensajes                                     [20000]
//   --modo        simple = un send() por mensaje · lote = agrupa mensajes   [lote]
//   --batch       máximo de mensajes por lote (como batch.size)             [500]
//   --lingerMs    cuánto espera para llenar el lote (como linger.ms)       [20]
//   --acks        0, 1 o -1 (all)                                          [-1]
//   --compresion  gzip o none                                              [gzip en lote, none en simple]
//
// Mide: tiempo, mensajes por segundo, llamadas al broker (send) y bytes.
// Cada resultado se agrega a data/carga-resultados.csv para armar la tabla del informe.
const fs = require('node:fs');
const path = require('node:path');
const { gzipSync } = require('node:zlib');
const { randomUUID } = require('node:crypto');
const { CompressionTypes } = require('kafkajs');
const { crearKafka, particionador } = require('../shared/kafka');
const { crearEvento, serializar } = require('../shared/dto');
const { CARGA } = require('../shared/topics');
const { leerOpciones, numero } = require('../shared/args');
const config = require('../shared/config');

function leerConfiguracion() {
  const o = leerOpciones();
  const modo = o.modo || 'lote';
  const acks = o.acks === undefined ? -1 : Number(o.acks);
  const compresion = o.compresion || (modo === 'lote' ? 'gzip' : 'none');
  if (!['simple', 'lote'].includes(modo)) throw new Error('--modo debe ser simple o lote');
  if (![0, 1, -1].includes(acks)) throw new Error('--acks debe ser 0, 1 o -1');
  if (!['gzip', 'none'].includes(compresion)) throw new Error('--compresion debe ser gzip o none');
  return {
    count: numero(o, 'count', 20000),
    modo, acks, compresion,
    batch: modo === 'simple' ? 1 : Math.max(1, numero(o, 'batch', 500)),
    lingerMs: modo === 'simple' ? 0 : numero(o, 'lingerMs', 20)
  };
}

/**
 * Productor por lotes (batch + linger) sobre KafkaJS:
 * junta mensajes y los envía en un solo send() cuando se llena el lote
 * o cuando pasan lingerMs desde el primer mensaje del lote.
 */
function crearLote(producer, { batch, lingerMs, acks, compresion }) {
  let buffer = [];
  let timer = null;
  let pendiente = Promise.resolve();
  const medicion = { llamadas: 0, bytes: 0, bytesEnviados: 0 };

  function enviar() {
    clearTimeout(timer);
    timer = null;
    if (!buffer.length) return pendiente;
    const messages = buffer;
    buffer = [];
    medicion.llamadas++;
    const crudo = Buffer.concat(messages.map(m => Buffer.from(m.value)));
    medicion.bytes += crudo.length;
    medicion.bytesEnviados += compresion === 'gzip' ? gzipSync(crudo).length : crudo.length;
    pendiente = pendiente.then(() => producer.send({
      topic: CARGA, acks,
      compression: compresion === 'gzip' ? CompressionTypes.GZIP : CompressionTypes.None,
      messages
    }));
    return pendiente;
  }

  async function agregar(mensaje) {
    buffer.push(mensaje);
    if (buffer.length >= batch || lingerMs === 0) return enviar();       // lote lleno o sin espera
    if (!timer) timer = setTimeout(enviar, lingerMs);                     // espera a que se llene
  }

  return { agregar, vaciar: enviar, medicion };
}

async function main() {
  const c = leerConfiguracion();
  const producer = crearKafka('pedidos360-carga').producer({
    createPartitioner: particionador,
    allowAutoTopicCreation: false,
    idempotent: false                // acks=0 o 1 no son compatibles con el productor idempotente
  });
  await producer.connect();

  console.log(`Prueba de carga en ${CARGA}: ${c.count} mensajes · modo=${c.modo} · batch=${c.batch} · lingerMs=${c.lingerMs} · acks=${c.acks} · compresion=${c.compresion}`);
  const lote = crearLote(producer, c);
  const corrida = randomUUID().slice(0, 8);
  const inicio = process.hrtime.bigint();
  try {
    for (let i = 0; i < c.count; i++) {
      const evento = crearEvento({ tipo: 'PEDIDO_CREADO', pedidoId: `PED-CARGA-${corrida}-${i}`, clienteId: 'C-100', total: 10990 });
      await lote.agregar({ key: evento.pedidoId, value: serializar(evento) });
    }
    await lote.vaciar();
  } finally {
    await producer.disconnect();
  }
  const segundos = Number(process.hrtime.bigint() - inicio) / 1e9;
  const { llamadas, bytes, bytesEnviados } = lote.medicion;
  const msgPorSeg = Math.round(c.count / segundos);

  console.log('');
  console.log(`  tiempo            ${segundos.toFixed(2)} s`);
  console.log(`  mensajes/s        ${msgPorSeg}`);
  console.log(`  llamadas (send)   ${llamadas}`);
  console.log(`  bytes sin comprimir ${(bytes / 1024).toFixed(0)} KB · enviados aprox. ${(bytesEnviados / 1024).toFixed(0)} KB`);

  fs.mkdirSync(config.dataDir, { recursive: true });
  const archivo = path.join(config.dataDir, 'carga-resultados.csv');
  if (!fs.existsSync(archivo)) fs.writeFileSync(archivo, 'fecha,modo,count,batch,lingerMs,acks,compresion,segundos,msg_por_s,llamadas,kb_sin_comprimir,kb_enviados\n');
  fs.appendFileSync(archivo, [new Date().toISOString(), c.modo, c.count, c.batch, c.lingerMs, c.acks, c.compresion,
    segundos.toFixed(2), msgPorSeg, llamadas, (bytes / 1024).toFixed(0), (bytesEnviados / 1024).toFixed(0)].join(',') + '\n');
  console.log(`\nResultado agregado a ${path.relative(process.cwd(), archivo)}`);
}

main().catch(error => { console.error(error.message); process.exitCode = 1; });
