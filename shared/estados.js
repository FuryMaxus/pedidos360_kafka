const TRANSICIONES = {
  'PEDIDO_CREADO': ['PEDIDO_ACEPTADO', 'PEDIDO_CANCELADO'],
  'PEDIDO_ACEPTADO': ['PEDIDO_EN_PREPARACION', 'PEDIDO_CANCELADO'],
  'PEDIDO_EN_PREPARACION': ['PEDIDO_DESPACHADO', 'PEDIDO_CANCELADO'],
  'PEDIDO_DESPACHADO': ['PEDIDO_ENTREGADO'],
  'PEDIDO_ENTREGADO': [],
  'PEDIDO_CANCELADO': []
};

function transicionValida(estadoActual, nuevoEstado) {
  if (!estadoActual && nuevoEstado === 'PEDIDO_CREADO') return true;
  if (!estadoActual) return false;
  
  return TRANSICIONES[estadoActual]?.includes(nuevoEstado) || false;
}

module.exports = { transicionValida };