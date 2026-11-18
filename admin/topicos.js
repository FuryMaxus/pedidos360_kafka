// Operaciones de administración con KafkaJS Admin: tópicos, configuración y consumer groups.
// Las usa el servidor REST (admin/server.js) y el script de inicialización (admin/setup.js).
const { ConfigResourceTypes } = require('kafkajs');
const { TOPICOS, dias } = require('../shared/topics');
const config = require('../shared/config');

const CONFIGS = ['retention.ms', 'retention.bytes', 'cleanup.policy', 'min.insync.replicas'];

/**
 * Crea los tópicos del diseño que falten y, a los que ya existen, les aplica la
 * configuración de shared/topics.js (así un cambio en el código llega a Kafka).
 */
async function crearTopicosBase(admin) {
  const existentes = new Set(await admin.listTopics());
  const faltantes = TOPICOS.filter(t => !existentes.has(t.topic));
  const actualizados = TOPICOS.filter(t => existentes.has(t.topic));
  if (faltantes.length) await admin.createTopics({ topics: faltantes, waitForLeaders: true });
  if (actualizados.length) {
    await admin.alterConfigs({
      validateOnly: false,
      resources: actualizados.map(t => ({ type: ConfigResourceTypes.TOPIC, name: t.topic, configEntries: t.configEntries }))
    });
  }
  return { creados: faltantes.map(t => t.topic), actualizados: actualizados.map(t => t.topic) };
}

async function crearTopico(admin, { topic, particiones = config.particiones, replicas = config.replicas, retencionDias = 7, cleanupPolicy = 'delete' }) {
  if (!/^[a-zA-Z0-9._-]+$/.test(topic || '')) throw new ErrorCliente('nombre de tópico inválido');
  const creado = await admin.createTopics({
    waitForLeaders: true,
    topics: [{
      topic,
      numPartitions: Number(particiones),
      replicationFactor: Number(replicas),
      configEntries: [
        { name: 'retention.ms', value: dias(Number(retencionDias)) },
        { name: 'cleanup.policy', value: cleanupPolicy }
      ]
    }]
  });
  if (!creado) throw new ErrorCliente(`el tópico ${topic} ya existe`);
  return describirTopico(admin, topic);
}

async function listarTopicos(admin) {
  const nombres = (await admin.listTopics()).filter(t => !t.startsWith('__')).sort();
  // allSettled: un tópico recién eliminado puede seguir listado unos segundos; se omite.
  const resultados = await Promise.allSettled(nombres.map(t => describirTopico(admin, t)));
  return resultados.filter(r => r.status === 'fulfilled').map(r => r.value);
}

async function describirTopico(admin, topic) {
  const [{ topics }, configs, offsets] = await Promise.all([
    admin.fetchTopicMetadata({ topics: [topic] }),
    admin.describeConfigs({
      includeSynonyms: false,
      resources: [{ type: ConfigResourceTypes.TOPIC, name: topic, configNames: CONFIGS }]
    }),
    admin.fetchTopicOffsets(topic)
  ]);
  const valores = Object.fromEntries(configs.resources[0].configEntries.map(c => [c.configName, c.configValue]));
  const particiones = topics[0].partitions.sort((a, b) => a.partitionId - b.partitionId);
  return {
    topic,
    particiones: particiones.length,
    replicas: particiones[0]?.replicas.length ?? 0,
    retencionDias: Number((Number(valores['retention.ms']) / 86400000).toFixed(4)),
    retencionMs: Number(valores['retention.ms']),
    retencionBytes: Number(valores['retention.bytes']),          // -1 = sin límite por tamaño
    cleanupPolicy: valores['cleanup.policy'],
    minInsyncReplicas: Number(valores['min.insync.replicas']),
    mensajes: offsets.reduce((suma, p) => suma + Number(p.high) - Number(p.low), 0),
    detalle: particiones.map(p => ({
      particion: p.partitionId, lider: p.leader, replicas: p.replicas, isr: p.isr,
      offsetFinal: offsets.find(o => o.partition === p.partitionId)?.high
    }))
  };
}

/**
 * Cambia retención, política de limpieza o min.insync.replicas de un tópico existente.
 * alterConfigs reemplaza toda la configuración del tópico, así que se parte de la
 * configuración actual y solo se cambian los valores pedidos.
 */
async function modificarConfiguracion(admin, topic, { retencionDias, retencionMB, cleanupPolicy, minInsyncReplicas }) {
  const cambios = {};
  if (retencionDias !== undefined) cambios['retention.ms'] = dias(Number(retencionDias));
  if (retencionMB !== undefined) cambios['retention.bytes'] = String(Math.round(Number(retencionMB) * 1024 * 1024));
  if (cleanupPolicy !== undefined) cambios['cleanup.policy'] = cleanupPolicy;
  if (minInsyncReplicas !== undefined) cambios['min.insync.replicas'] = String(minInsyncReplicas);
  if (!Object.keys(cambios).length) throw new ErrorCliente('indicar retencionDias, retencionMB, cleanupPolicy o minInsyncReplicas');

  const actual = await admin.describeConfigs({
    includeSynonyms: false,
    resources: [{ type: ConfigResourceTypes.TOPIC, name: topic, configNames: CONFIGS }]
  });
  const valores = Object.fromEntries(actual.resources[0].configEntries
    .filter(c => !c.isDefault).map(c => [c.configName, c.configValue]));
  const configEntries = Object.entries({ ...valores, ...cambios }).map(([name, value]) => ({ name, value }));
  await admin.alterConfigs({ validateOnly: false, resources: [{ type: ConfigResourceTypes.TOPIC, name: topic, configEntries }] });
  return describirTopico(admin, topic);
}

/** Aumenta las particiones (Kafka no permite reducirlas). */
async function aumentarParticiones(admin, topic, total) {
  await admin.createPartitions({ topicPartitions: [{ topic, count: Number(total) }] });
  // Kafka tarda un instante en informar las particiones nuevas: se espera hasta verlas.
  for (let intento = 0; intento < 10; intento++) {
    const descripcion = await describirTopico(admin, topic);
    if (descripcion.particiones >= Number(total)) return descripcion;
    await new Promise(resolver => setTimeout(resolver, 500));
  }
  return describirTopico(admin, topic);
}

async function eliminarTopico(admin, topic) {
  await admin.deleteTopics({ topics: [topic] });
  return { eliminado: topic };
}

async function listarGrupos(admin) {
  const { groups } = await admin.listGroups();
  if (!groups.length) return [];
  const { groups: detalle } = await admin.describeGroups(groups.map(g => g.groupId));
  return detalle.map(g => ({
    groupId: g.groupId,
    estado: g.state,
    miembros: g.members.map(m => ({ clientId: m.clientId, host: m.clientHost }))
  }));
}

/** Lag = último offset del tópico - offset confirmado por el grupo, por partición. */
async function lagDelGrupo(admin, groupId, topic) {
  const [finales, confirmados] = await Promise.all([
    admin.fetchTopicOffsets(topic),
    admin.fetchOffsets({ groupId, topics: [topic] })
  ]);
  const porParticion = confirmados.find(t => t.topic === topic)?.partitions || [];
  const particiones = finales.map(f => {
    const confirmado = Number(porParticion.find(p => p.partition === f.partition)?.offset ?? -1);
    const desde = confirmado < 0 ? Number(f.low) : confirmado; // -1 = el grupo aún no confirma nada
    return { particion: f.partition, offsetFinal: Number(f.high), offsetConfirmado: confirmado, lag: Math.max(0, Number(f.high) - desde) };
  }).sort((a, b) => a.particion - b.particion);
  return { groupId, topic, lagTotal: particiones.reduce((s, p) => s + p.lag, 0), particiones };
}

class ErrorCliente extends Error {}

module.exports = {
  crearTopicosBase, crearTopico, listarTopicos, describirTopico, modificarConfiguracion,
  aumentarParticiones, eliminarTopico, listarGrupos, lagDelGrupo, ErrorCliente
};
