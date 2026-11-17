
const { iniciarConsumidor } = require('../shared/consumer-base');
const { GRUPOS } = require('../shared/topics');
const { procesar } = require('./handler');

iniciarConsumidor({ servicio: 'report', groupId: GRUPOS.REPORT, procesar })
  .catch(error => { console.error('No se pudo iniciar report:', error.message); process.exit(1); });
