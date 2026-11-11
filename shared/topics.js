const PEDIDOS = 'orders.events';
const DLT_REPORT = `${PEDIDOS}.report.dlt`;
const DLT_AUDIT = `${PEDIDOS}.audit.dlt`;
const TIMELINE = 'audit.timeline';

// Retención calculada en milisegundos
const diasEnMs = (dias) => String(dias * 24 * 60 * 60 * 1000);

const TOPICOS = [
  {
    topic: PEDIDOS,
    numPartitions: 3,
    replicationFactor: 2, 
    configEntries: [
      { name: 'retention.ms', value: diasEnMs(7) },
      { name: 'cleanup.policy', value: 'delete' }
    ]
  },
  {
    topic: DLT_REPORT,
    numPartitions: 3,
    replicationFactor: 2,
    configEntries: [
      { name: 'retention.ms', value: diasEnMs(14) },
      { name: 'cleanup.policy', value: 'delete' }
    ]
  },
  {
    topic: DLT_AUDIT,
    numPartitions: 3,
    replicationFactor: 2,
    configEntries: [
      { name: 'retention.ms', value: diasEnMs(14) },
      { name: 'cleanup.policy', value: 'delete' }
    ]
  },
  {
    topic: TIMELINE,
    numPartitions: 3,
    replicationFactor: 2,
    configEntries: [
      { name: 'retention.ms', value: diasEnMs(30) },
      { name: 'cleanup.policy', value: 'compact,delete' }
    ]
  }
];

module.exports = { PEDIDOS, DLT_REPORT, DLT_AUDIT, TIMELINE, TOPICOS };