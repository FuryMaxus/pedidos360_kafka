// Único punto donde se crea el cliente de Kafka (brokers, clientId, reintentos).
const { Kafka, logLevel, Partitioners } = require('kafkajs');
const config = require('./config');

function crearKafka(clientId) {
  return new Kafka({
    clientId,                 // ej: 'report-service'
    brokers: config.brokers,  // ej: localhost:9092,localhost:9093,localhost:9094
    logLevel: logLevel.ERROR,   // solo errores de KafkaJS; los servicios escriben sus propios logs
    connectionTimeout: 5000,
    retry: {
      initialRetryTime: 300,
      retries: 8
    }
  });
}

// Particionador por defecto de KafkaJS 2 (murmur2, igual que Java): hash(key) % particiones.
const particionador = Partitioners.DefaultPartitioner;

module.exports = { crearKafka, particionador };
