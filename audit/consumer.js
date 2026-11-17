// Servicio audit: consumer group "audit" sobre pedidos.eventos.
const { iniciarConsumidor } = require('../shared/consumer-base');
const { GRUPOS } = require('../shared/topics');
const { procesar } = require('./handler');

iniciarConsumidor({ servicio: 'audit', groupId: GRUPOS.AUDIT, procesar })
  .catch(error => { console.error('No se pudo iniciar audit:', error.message); process.exit(1); });
