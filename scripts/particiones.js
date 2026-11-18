// Caso 1: aumentar las particiones de un tópico (Kafka solo permite aumentarlas, nunca bajarlas).
// Uso: npm run particiones -- --total=6                      -> pedidos.eventos a 6 particiones
//      npm run particiones -- --total=6 --topic=otro.topico
//
// ¡No se puede deshacer! Para volver a 3 hay que borrar el tópico (docker compose down -v).
// Además, desde ese momento la misma key puede caer en otra partición
// (hash(key) % particiones); los eventos antiguos no se mueven.
const { crearKafka } = require('../shared/kafka');
const { PEDIDOS } = require('../shared/topics');
const { aumentarParticiones, describirTopico } = require('../admin/topicos');
const { leerOpciones, numero } = require('../shared/args');

async function main() {
  const opciones = leerOpciones();
  const topic = opciones.topic || PEDIDOS;
  const total = numero(opciones, 'total', 0);
  if (!total) {
    console.log('Indica el total: npm run particiones -- --total=6');
    console.log('Ojo: no se puede deshacer (para volver a 3 hay que borrar el tópico con docker compose down -v).');
    return;
  }
  const admin = crearKafka('pedidos360-particiones').admin();
  await admin.connect();
  try {
    const antes = await describirTopico(admin, topic);
    if (total <= antes.particiones) {
      console.log(`${topic} ya tiene ${antes.particiones} particiones. Kafka no permite bajarlas ni dejarlas iguales.`);
      return;
    }
    const despues = await aumentarParticiones(admin, topic, total);
    console.log(`${topic}: ${antes.particiones} -> ${despues.particiones} particiones.`);
    console.log('Los consumidores de cada grupo se rebalancean solos para repartir las nuevas particiones.');
  } finally {
    await admin.disconnect();
  }
}

main().catch(error => { console.error(error.message); process.exitCode = 1; });
