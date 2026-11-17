
const { abrirDb, transaccion } = require('../shared/db');

const db = abrirDb('report');
db.exec(`
  CREATE TABLE IF NOT EXISTS eventos_procesados (
    event_id TEXT PRIMARY KEY
  );
  CREATE TABLE IF NOT EXISTS pedidos (
    pedido_id    TEXT PRIMARY KEY,
    cliente_id   TEXT NOT NULL,
    total        REAL NOT NULL,
    estado       TEXT NOT NULL,
    ultima_fecha TEXT NOT NULL,
    creado_en    TEXT,
    entregado_en TEXT
  );
`);

const yaProcesado = db.prepare('SELECT 1 FROM eventos_procesados WHERE event_id = ?');
const marcarProcesado = db.prepare('INSERT INTO eventos_procesados (event_id) VALUES (?)');
const guardarPedido = db.prepare(`
  INSERT INTO pedidos (pedido_id, cliente_id, total, estado, ultima_fecha, creado_en, entregado_en)
  VALUES (?, ?, ?, ?, ?, ?, ?)
  ON CONFLICT (pedido_id) DO UPDATE SET
    total        = excluded.total,
    -- solo un evento más reciente cambia el estado actual
    estado       = CASE WHEN excluded.ultima_fecha >= pedidos.ultima_fecha THEN excluded.estado ELSE pedidos.estado END,
    ultima_fecha = MAX(pedidos.ultima_fecha, excluded.ultima_fecha),
    creado_en    = COALESCE(pedidos.creado_en, excluded.creado_en),
    entregado_en = COALESCE(pedidos.entregado_en, excluded.entregado_en)`);

function procesar(evento) {
  return transaccion(db, () => {
    if (yaProcesado.get(evento.eventId)) return { duplicado: true };
    marcarProcesado.run(evento.eventId);
    const estado = evento.tipo.replace('PEDIDO_', '');
    guardarPedido.run(
      evento.pedidoId, evento.clienteId, evento.total, estado, evento.fecha,
      evento.tipo === 'PEDIDO_CREADO' ? evento.fecha : null,
      evento.tipo === 'PEDIDO_ENTREGADO' ? evento.fecha : null
    );
    return { duplicado: false, estado };
  });
}


function ventasPorHora() {
  return db.prepare(`
    SELECT substr(entregado_en, 1, 13) || ':00Z' AS hora, COUNT(*) AS pedidos, SUM(total) AS monto
    FROM pedidos WHERE entregado_en IS NOT NULL
    GROUP BY hora ORDER BY hora`).all();
}

function tiempoEntrega() {
  return db.prepare(`
    SELECT COUNT(*) AS pedidos,
           ROUND(AVG(minutos), 1) AS promedioMinutos,
           ROUND(MIN(minutos), 1) AS minimoMinutos,
           ROUND(MAX(minutos), 1) AS maximoMinutos
    FROM (SELECT (julianday(entregado_en) - julianday(creado_en)) * 1440 AS minutos
          FROM pedidos WHERE creado_en IS NOT NULL AND entregado_en IS NOT NULL)`).get();
}

function activosPorEstado() {
  return db.prepare(`
    SELECT estado, COUNT(*) AS pedidos FROM pedidos
    WHERE estado NOT IN ('ENTREGADO', 'CANCELADO')
    GROUP BY estado ORDER BY estado`).all();
}

function kpis() {
  return {
    eventosProcesados: db.prepare('SELECT COUNT(*) AS n FROM eventos_procesados').get().n,
    pedidos: db.prepare('SELECT COUNT(*) AS n FROM pedidos').get().n,
    ventasPorHora: ventasPorHora(),
    tiempoEntrega: tiempoEntrega(),
    activosPorEstado: activosPorEstado(),
    cancelados: db.prepare("SELECT COUNT(*) AS n FROM pedidos WHERE estado = 'CANCELADO'").get().n
  };
}

module.exports = { procesar, kpis, ventasPorHora, tiempoEntrega, activosPorEstado };
