const { Kafka } = require('kafkajs');
const { TOPICOS } = require('../shared/topics');

const kafka = new Kafka({
  clientId: 'admin-pedidos360',
  brokers: ['localhost:9093', 'localhost:9094'],
  connectionTimeout: 10000
});

async function setup() {

    const admin = kafka.admin();
    console.log('Conectando a Kafka Admin...');
    await admin.connect();

    console.log('Creando tópicos con factor de replicación 2...');


    const success = await admin.createTopics({
        waitForLeaders: true,
        timeout: 30000,
        topics: TOPICOS
    });

    if (success) {
        console.log('¡Tópicos creados exitosamente con su configuración avanzada!');
    } else {
        console.log('Los tópicos ya existían.');
    }

    const existingTopics = await admin.listTopics();
    console.log('Tópicos activos:', existingTopics.filter(t => !t.startsWith('_')));

    await admin.disconnect();
}   
setup().catch(console.error);