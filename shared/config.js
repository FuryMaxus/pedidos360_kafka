
const path = require('node:path');

function entero(nombre, porDefecto) {
  const valor = Number(process.env[nombre] ?? porDefecto);
  if (!Number.isInteger(valor) || valor < 0) throw new Error(`${nombre} debe ser un entero positivo`);
  return valor;
}

const config = {
  brokers: (process.env.KAFKA_BROKERS || 'localhost:9092,localhost:9093,localhost:9094')
    .split(',').map(b => b.trim()).filter(Boolean),

  particiones: entero('KAFKA_PARTITIONS', 3),
  replicas: entero('KAFKA_REPLICATION_FACTOR', 3),
  minInsync: entero('KAFKA_MIN_INSYNC_REPLICAS', 2),

  intentos: entero('CONSUMER_MAX_ATTEMPTS', 3),
  esperaBaseMs: entero('CONSUMER_RETRY_BASE_MS', 1000),

  adminPort: entero('ADMIN_PORT', 3000),
  ordersPort: entero('ORDERS_PORT', 4000),
  dataDir: path.resolve(process.env.DATA_DIR || path.join(__dirname, '..', 'data'))
};

module.exports = config;
