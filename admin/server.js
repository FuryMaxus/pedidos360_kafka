// Servicio administrador de Kafka (REST con Express + KafkaJS Admin)
// y API de solo lectura con los resultados de report y audit (para el monitor).
const express = require('express');
const { crearKafka } = require('../shared/kafka');
const { PEDIDOS } = require('../shared/topics');
const config = require('../shared/config');
const ops = require('./topicos');
const report = require('../report/handler');
const audit = require('../audit/handler');

const admin = crearKafka('pedidos360-admin').admin();
const app = express();
app.use(express.json());

// Envuelve cada ruta para responder errores en JSON sin repetir try/catch.
const ruta = fn => async (req, res) => {
  try {
    res.status(req.method === 'POST' ? 201 : 200).json(await fn(req));
  } catch (error) {
    const status = error instanceof ops.ErrorCliente ? 400 : 500;
    console.error(`${req.method} ${req.path} -> ${status}: ${error.message}`);
    res.status(status).json({ error: error.message });
  }
};

// Tópicos
app.post('/admin/topics/setup', ruta(() => ops.crearTopicosBase(admin)));                 // crea los del diseño
app.post('/admin/topics', ruta(req => ops.crearTopico(admin, req.body)));                 // { topic, particiones, replicas, retencionDias, cleanupPolicy }
app.get('/admin/topics', ruta(() => ops.listarTopicos(admin)));
app.get('/admin/topics/:topic', ruta(req => ops.describirTopico(admin, req.params.topic)));
app.put('/admin/topics/:topic/config', ruta(req => ops.modificarConfiguracion(admin, req.params.topic, req.body))); // { retencionDias, cleanupPolicy, minInsyncReplicas }
app.put('/admin/topics/:topic/partitions', ruta(req => ops.aumentarParticiones(admin, req.params.topic, req.body.total)));
app.delete('/admin/topics/:topic', ruta(req => ops.eliminarTopico(admin, req.params.topic)));

// Consumer groups
app.get('/admin/groups', ruta(() => ops.listarGrupos(admin)));
app.get('/admin/groups/:groupId/lag', ruta(req => ops.lagDelGrupo(admin, req.params.groupId, req.query.topic || PEDIDOS)));

// Rutas de solo lectura para el monitor (repositorio fronted-pedido360-kafka):
// muestran lo que calcularon report y audit a partir de Kafka.
app.get('/api/monitor/kpis', ruta(() => report.kpis()));
app.get('/api/monitor/eventos', ruta(() => audit.ultimosEventos(50)));

async function main() {
  await admin.connect();
  const { creados } = await ops.crearTopicosBase(admin);
  if (creados.length) console.log('Tópicos creados al iniciar:', creados.join(', '));
  const servidor = app.listen(config.adminPort, () => {
    console.log(`Admin Kafka en http://localhost:${config.adminPort}/admin/topics`);
    console.log(`API monitor en http://localhost:${config.adminPort}/api/monitor/kpis`);
  });
  const cerrar = async () => { servidor.close(); await admin.disconnect(); process.exit(0); };
  process.once('SIGINT', cerrar);
  process.once('SIGTERM', cerrar);
}

main().catch(error => { console.error('No se pudo iniciar el admin:', error.message); process.exit(1); });
