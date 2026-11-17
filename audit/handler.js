// Lógica de negocio de auditoría: guarda cada evento para armar el timeline de un pedido.
// Idempotente: eventId es la clave primaria, un evento repetido no se guarda dos veces.
const { abrirDb } = require('../shared/db');

const db = abrirDb('audit');
db.exec(`
  CREATE TABLE IF NOT EXISTS eventos (
    event_id       TEXT PRIMARY KEY,
    pedido_id      TEXT NOT NULL,
    tipo           TEXT NOT NULL,
    cliente_id     TEXT NOT NULL,
    total          REAL NOT NULL,
    fecha          TEXT NOT NULL,
    particion      INTEGER NOT NULL,
    offset         TEXT NOT NULL,
    correlation_id TEXT,
    recibido_en    TEXT NOT NULL
  );
  CREATE INDEX IF NOT EXISTS idx_eventos_pedido ON eventos (pedido_id, fecha);
`);

const insertar = db.prepare(`
  INSERT OR IGNORE INTO eventos
  (event_id, pedido_id, tipo, cliente_id, total, fecha, particion, offset, correlation_id, recibido_en)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);

function procesar(evento, meta) {
  const { changes } = insertar.run(
    evento.eventId, evento.pedidoId, evento.tipo, evento.clienteId, evento.total, evento.fecha,
    meta.particion, meta.offset, meta.correlationId, new Date().toISOString()
  );
  return { duplicado: changes === 0 };
}

/** Consulta de solo lectura: historial ordenado de un pedido. */
function timeline(pedidoId) {
  return db.prepare(`SELECT tipo, fecha, total, particion, offset, event_id AS eventId, correlation_id AS correlationId
                     FROM eventos WHERE pedido_id = ? ORDER BY fecha, CAST(offset AS INTEGER)`).all(pedidoId);
}

function resumen() {
  return {
    eventos: db.prepare('SELECT COUNT(*) AS n FROM eventos').get().n,
    pedidos: db.prepare('SELECT COUNT(DISTINCT pedido_id) AS n FROM eventos').get().n,
    ultimosPedidos: db.prepare('SELECT pedido_id AS pedidoId, COUNT(*) AS eventos FROM eventos GROUP BY pedido_id ORDER BY MAX(recibido_en) DESC LIMIT 5').all()
  };
}

module.exports = { procesar, timeline, resumen };
