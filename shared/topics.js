// Nombres y configuración de los tópicos. El servicio admin los crea desde aquí,
// así la configuración queda versionada en Git y es reproducible.
const config = require('./config');

const PEDIDOS = 'pedidos.eventos';
const DLT = 'pedidos.eventos.dlt';

const GRUPOS = { REPORT: 'report', AUDIT: 'audit' };

const dias = n => String(n * 24 * 60 * 60 * 1000);

function definicion(topic, retencionDias) {
  return {
    topic,
    numPartitions: config.particiones,      // 3 carriles: hasta 3 consumidores por grupo
    replicationFactor: config.replicas,     // 3 copias: tolera la caída de un broker
    configEntries: [
      { name: 'retention.ms', value: dias(retencionDias) },
      { name: 'cleanup.policy', value: 'delete' },
      { name: 'min.insync.replicas', value: String(config.minInsync) }
    ]
  };
}

const TOPICOS = [
  definicion(PEDIDOS, 7),   // reportería y auditoría releen una semana de eventos
  definicion(DLT, 30)       // los fallos se guardan más tiempo para investigarlos
];

module.exports = { PEDIDOS, DLT, GRUPOS, TOPICOS, dias };
