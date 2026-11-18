// Nombres y configuración de los tópicos. El servicio admin los crea desde aquí,
// así la configuración queda versionada en Git y es reproducible.
const config = require('./config');

const PEDIDOS = 'pedidos.eventos';
const DLT = 'pedidos.eventos.dlt';
const CARGA = 'pedidos.carga';            // solo para las pruebas de carga (npm run carga)

const GRUPOS = { REPORT: 'report', AUDIT: 'audit' };

const MINUTO = 60 * 1000;
const DIA = 24 * 60 * MINUTO;
const MB = 1024 * 1024;
const dias = n => String(n * DIA);

/**
 * Retención: Kafka borra lo más antiguo cuando se cumple lo que ocurra primero,
 * el tiempo (retention.ms) o el tamaño por partición (retention.bytes).
 */
function definicion(topic, retencionMs, retencionBytes) {
  const configEntries = [
    { name: 'retention.ms', value: String(retencionMs) },
    { name: 'cleanup.policy', value: 'delete' },
    { name: 'min.insync.replicas', value: String(config.minInsync) }
  ];
  if (retencionBytes) configEntries.push({ name: 'retention.bytes', value: String(retencionBytes) });
  return {
    topic,
    numPartitions: config.particiones,      // 3 carriles: hasta 3 consumidores por grupo
    replicationFactor: config.replicas,     // 3 copias: tolera la caída de un broker
    configEntries
  };
}

const TOPICOS = [
  definicion(PEDIDOS, 7 * DIA, 512 * MB),   // eventos: 7 días o 512 MB por partición
  definicion(DLT, 30 * DIA),                // los fallos se guardan más tiempo para investigarlos
  definicion(CARGA, 5 * MINUTO, 50 * MB)    // pruebas de carga: datos descartables, 5 min o 50 MB
];

module.exports = { PEDIDOS, DLT, CARGA, GRUPOS, TOPICOS, dias };
