
const { randomUUID } = require('node:crypto');
const { abrirDb, transaccion } = require('../shared/db');
const catalogoInicial = require('./catalogo-inicial.json');

const TRANSICIONES = {
  CREADO: ['ACEPTADO', 'CANCELADO'],
  ACEPTADO: ['EN_PREPARACION', 'CANCELADO'],
  EN_PREPARACION: ['DESPACHADO', 'CANCELADO'],
  DESPACHADO: ['ENTREGADO'],
  ENTREGADO: [],
  CANCELADO: []
};

class ErrorHttp extends Error {
  constructor(status, mensaje) { super(mensaje); this.status = status; }
}

const db = abrirDb('orders');
db.exec(`
  CREATE TABLE IF NOT EXISTS productos (
    id     TEXT PRIMARY KEY,
    nombre TEXT NOT NULL,
    precio REAL NOT NULL,
    stock  INTEGER NOT NULL
  );
  CREATE TABLE IF NOT EXISTS pedidos (
    id             TEXT PRIMARY KEY,
    cliente_id     TEXT NOT NULL,
    items          TEXT NOT NULL,   -- JSON con producto_id, nombre, cantidad, precio_unitario
    estado         TEXT NOT NULL,
    fecha_creacion TEXT NOT NULL
  );
`);
// Primera ejecución: carga el catálogo de ejemplo.
if (db.prepare('SELECT COUNT(*) AS n FROM productos').get().n === 0) {
  const insertar = db.prepare('INSERT INTO productos (id, nombre, precio, stock) VALUES (?, ?, ?, ?)');
  for (const p of catalogoInicial) insertar.run(p.id, p.nombre, p.precio, p.stock);
}


const aPedido = fila => fila && { ...fila, items: JSON.parse(fila.items) };
const totalDe = items => items.reduce((suma, i) => suma + i.cantidad * i.precio_unitario, 0);

function listarPedidos(clienteId) {
  const filas = clienteId
    ? db.prepare('SELECT * FROM pedidos WHERE cliente_id = ? ORDER BY fecha_creacion DESC').all(clienteId)
    : db.prepare('SELECT * FROM pedidos ORDER BY fecha_creacion DESC').all();
  return filas.map(aPedido);
}

function obtenerPedido(id) {
  const pedido = aPedido(db.prepare('SELECT * FROM pedidos WHERE id = ?').get(id));
  if (!pedido) throw new ErrorHttp(404, 'Pedido no encontrado');
  return pedido;
}

function prepararPedido(clienteId, items) {
  if (!Array.isArray(items) || items.length === 0) throw new ErrorHttp(400, 'Faltan los items del pedido');
  const lineas = items.map(item => {
    const producto = db.prepare('SELECT * FROM productos WHERE id = ?').get(String(item.producto_id));
    const cantidad = Number(item.cantidad);
    if (!producto) throw new ErrorHttp(400, `El producto ${item.producto_id} no existe en el catálogo.`);
    if (!Number.isInteger(cantidad) || cantidad <= 0) throw new ErrorHttp(400, 'La cantidad debe ser un entero positivo');
    return { producto_id: producto.id, nombre: producto.nombre, cantidad, precio_unitario: producto.precio };
  });
  return { id: randomUUID(), cliente_id: clienteId, items: lineas, estado: 'CREADO', fecha_creacion: new Date().toISOString() };
}

function guardarPedido(pedido) {
  db.prepare('INSERT INTO pedidos (id, cliente_id, items, estado, fecha_creacion) VALUES (?, ?, ?, ?, ?)')
    .run(pedido.id, pedido.cliente_id, JSON.stringify(pedido.items), pedido.estado, pedido.fecha_creacion);
  return pedido;
}

function prepararCambioEstado(id, nuevoEstado) {
  const pedido = obtenerPedido(id);
  if (!Object.hasOwn(TRANSICIONES, nuevoEstado)) throw new ErrorHttp(400, 'Estado inválido');
  if (!TRANSICIONES[pedido.estado].includes(nuevoEstado)) {
    throw new ErrorHttp(400, `Transición no permitida. No se puede pasar de '${pedido.estado}' a '${nuevoEstado}'.`);
  }
  if (nuevoEstado === 'ACEPTADO') {
    for (const item of pedido.items) {
      const producto = db.prepare('SELECT stock FROM productos WHERE id = ?').get(item.producto_id);
      if (!producto) throw new ErrorHttp(400, `El producto ${item.producto_id} no existe en el catálogo.`);
      if (producto.stock < item.cantidad) {
        throw new ErrorHttp(400, `Stock insuficiente para la ID ${item.producto_id}. Disponible: ${producto.stock}`);
      }
    }
  }
  return { ...pedido, estado: nuevoEstado, estadoAnterior: pedido.estado };
}

function guardarCambioEstado(pedido) {
  transaccion(db, () => {
    db.prepare('UPDATE pedidos SET estado = ? WHERE id = ?').run(pedido.estado, pedido.id);
    if (pedido.estado === 'ACEPTADO') {
      for (const item of pedido.items) {
        db.prepare('UPDATE productos SET stock = stock - ? WHERE id = ?').run(item.cantidad, item.producto_id);
      }
    }
  });
  const { estadoAnterior, ...guardado } = pedido;
  return guardado;
}


const listarProductos = () => db.prepare('SELECT * FROM productos ORDER BY nombre').all();

function obtenerProducto(id) {
  const producto = db.prepare('SELECT * FROM productos WHERE id = ?').get(id);
  if (!producto) throw new ErrorHttp(404, 'Producto no encontrado');
  return producto;
}

function crearProducto({ nombre, precio, stock = 0 }) {
  if (!nombre || !(Number(precio) > 0)) throw new ErrorHttp(400, 'Faltan datos requeridos (nombre, precio)');
  const producto = { id: 'PRD-' + randomUUID().slice(0, 8).toUpperCase(), nombre, precio: Number(precio), stock: Number(stock) || 0 };
  db.prepare('INSERT INTO productos (id, nombre, precio, stock) VALUES (?, ?, ?, ?)').run(producto.id, producto.nombre, producto.precio, producto.stock);
  return producto;
}

function actualizarProducto(id, { nombre, precio }) {
  const actual = obtenerProducto(id);
  if (nombre === undefined && precio === undefined) throw new ErrorHttp(400, 'No hay campos para actualizar');
  db.prepare('UPDATE productos SET nombre = ?, precio = ? WHERE id = ?').run(nombre ?? actual.nombre, Number(precio ?? actual.precio), id);
  return obtenerProducto(id);
}

function ajustarStock(id, cantidad) {
  if (!Number.isInteger(cantidad) || cantidad === 0) throw new ErrorHttp(400, 'La cantidad debe ser un entero distinto de 0');
  const { stock } = obtenerProducto(id);
  if (stock + cantidad < 0) throw new ErrorHttp(400, 'El stock no puede quedar negativo');
  db.prepare('UPDATE productos SET stock = stock + ? WHERE id = ?').run(cantidad, id);
  return { id, nuevo_stock: stock + cantidad };
}

function eliminarProducto(id) {
  obtenerProducto(id);
  db.prepare('DELETE FROM productos WHERE id = ?').run(id);
  return { mensaje: 'Producto eliminado', id };
}

module.exports = {
  ErrorHttp, totalDe,
  listarPedidos, obtenerPedido, prepararPedido, guardarPedido, prepararCambioEstado, guardarCambioEstado,
  listarProductos, crearProducto, actualizarProducto, ajustarStock, eliminarProducto
};
