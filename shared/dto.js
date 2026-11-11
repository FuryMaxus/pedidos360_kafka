function crearEvento(datos) {
  return {
    pedidoId: datos.pedidoId,
    tipo: datos.tipo,
    clienteId: datos.clienteId || 'C-DESCONOCIDO',
    version: datos.version || 2,
    actor: datos.actor || { usuario: 'sistema', rol: 'SISTEMA' },
    origen: datos.origen || 'web',
    items: datos.items || [],
    total: datos.total || 0,
    fecha: new Date().toISOString()
  };
}

module.exports = { crearEvento };