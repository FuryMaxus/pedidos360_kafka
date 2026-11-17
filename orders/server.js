
const express = require('express');
const config = require('../shared/config');
const { crearEvento } = require('../shared/dto');
const { crearProductor, publicar } = require('../bridge/producer');
const pedidos = require('./handler');
const report = require('../report/handler');
const audit = require('../audit/handler');

const { ErrorHttp } = pedidos;
const producer = crearProductor();
const app = express();
app.use(express.json());

const ROLES = ['Cliente', 'Operador', 'Administrador'];

function usuario(req) {
  const id = req.get('X-User-Id');
  const rol = req.get('X-User-Role');
  if (!id || !ROLES.includes(rol)) throw new ErrorHttp(401, 'Falta el perfil de usuario (X-User-Id, X-User-Role)');
  return { id, rol };
}

function exigirRol(req, ...roles) {
  const u = usuario(req);
  if (!roles.includes(u.rol)) throw new ErrorHttp(403, 'Acceso denegado');
  return u;
}

async function publicarEvento(pedido) {
  const evento = crearEvento({
    tipo: 'PEDIDO_' + pedido.estado,
    pedidoId: pedido.id,
    clienteId: pedido.cliente_id,
    total: pedidos.totalDe(pedido.items)
  });
  try {
    const r = await publicar(producer, evento, pedido.id);
    console.log(`kafka: ${evento.tipo} pedido=${pedido.id} particion=${r.partition} offset=${r.baseOffset}`);
  } catch (error) {
    console.error(`kafka: no se pudo publicar ${evento.tipo} de ${pedido.id}: ${error.message}`);
    throw new ErrorHttp(503, 'No se pudo registrar el evento en Kafka; el pedido no fue modificado. Intenta de nuevo.');
  }
}

const ruta = (fn, status = 200) => async (req, res) => {
  try {
    res.status(status).json(await fn(req));
  } catch (error) {
    const codigo = error instanceof ErrorHttp ? error.status : 500;
    if (codigo === 500) console.error(error);
    res.status(codigo).json({ error: codigo === 500 ? 'Error interno' : error.message });
  }
};

app.get('/api/orders', ruta(req => {
  const u = exigirRol(req, ...ROLES);
  return pedidos.listarPedidos(u.rol === 'Cliente' ? u.id : req.query.cliente_id);
}));

app.get('/api/orders/:id', ruta(req => {
  const u = exigirRol(req, ...ROLES);
  const pedido = pedidos.obtenerPedido(req.params.id);
  if (u.rol === 'Cliente' && pedido.cliente_id !== u.id) throw new ErrorHttp(403, 'Acceso denegado');
  return pedido;
}));

app.post('/api/orders', ruta(async req => {
  const u = exigirRol(req, 'Cliente', 'Operador');
  const clienteId = u.rol === 'Cliente' ? u.id : (req.body.cliente_id || u.id);
  const pedido = pedidos.prepararPedido(clienteId, req.body.items);
  await publicarEvento(pedido);         
  return pedidos.guardarPedido(pedido);
}, 201));

app.patch('/api/orders/:id/estado', ruta(async req => {
  exigirRol(req, 'Operador', 'Administrador');
  const pedido = pedidos.prepararCambioEstado(req.params.id, req.body.estado);
  await publicarEvento(pedido);
  return pedidos.guardarCambioEstado(pedido);
}));


app.get('/api/catalog', ruta(() => pedidos.listarProductos()));
app.post('/api/catalog', ruta(req => { exigirRol(req, 'Administrador'); return pedidos.crearProducto(req.body); }, 201));
app.patch('/api/catalog/:id', ruta(req => { exigirRol(req, 'Administrador'); return pedidos.actualizarProducto(req.params.id, req.body); }));
app.patch('/api/catalog/:id/stock', ruta(req => { exigirRol(req, 'Administrador'); return pedidos.ajustarStock(req.params.id, req.body.cantidad); }));
app.delete('/api/catalog/:id', ruta(req => { exigirRol(req, 'Administrador'); return pedidos.eliminarProducto(req.params.id); }));


app.get('/api/reports/kpis', ruta(req => { exigirRol(req, 'Operador', 'Administrador'); return report.kpis(); }));
app.get('/api/audit/:pedidoId', ruta(req => {
  const u = exigirRol(req, ...ROLES);
  if (u.rol === 'Cliente') {
    const pedido = pedidos.obtenerPedido(req.params.pedidoId);
    if (pedido.cliente_id !== u.id) throw new ErrorHttp(403, 'Acceso denegado');
  }
  return audit.timeline(req.params.pedidoId);
}));

async function main() {
  await producer.connect();
  const servidor = app.listen(config.ordersPort, () => console.log(`ms-pedidos360-orders en http://localhost:${config.ordersPort}/api/orders`));
  const cerrar = async () => { servidor.close(); await producer.disconnect(); process.exit(0); };
  process.once('SIGINT', cerrar);
  process.once('SIGTERM', cerrar);
}

main().catch(error => { console.error('No se pudo iniciar orders:', error.message); process.exit(1); });
