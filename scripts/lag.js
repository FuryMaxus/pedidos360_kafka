// Caso 1: monitor de lag. Muestra cada 2 s cuántos mensajes le faltan por leer a un grupo.
// Uso: npm run lag -- --grupo=report        (por defecto: report y audit)
//      npm run lag -- --grupo=report --una-vez
const { crearKafka } = require('../shared/kafka');
const { PEDIDOS, GRUPOS } = require('../shared/topics');
const { lagDelGrupo } = require('../admin/topicos');
const { leerOpciones, numero } = require('../shared/args');

const opciones = leerOpciones();
const grupos = opciones.grupo ? [opciones.grupo] : [GRUPOS.REPORT, GRUPOS.AUDIT];
const intervaloMs = numero(opciones, 'intervalo', 2) * 1000;

async function mostrar(admin) {
  const hora = new Date().toLocaleTimeString('es-CL');
  for (const grupo of grupos) {
    const { lagTotal, particiones } = await lagDelGrupo(admin, grupo, PEDIDOS);
    const detalle = particiones.map(p => `p${p.particion}=${p.lag}`).join('  ');
    console.log(`${hora}  grupo=${grupo.padEnd(7)} lag total=${String(lagTotal).padStart(5)}   ${detalle}`);
  }
}

async function main() {
  const admin = crearKafka('pedidos360-lag').admin();
  await admin.connect();
  await mostrar(admin);
  if (opciones['una-vez']) return admin.disconnect();
  console.log(`(se actualiza cada ${intervaloMs / 1000} s; Ctrl+C para salir)`);
  const intervalo = setInterval(() => mostrar(admin).catch(e => console.error(e.message)), intervaloMs);
  process.once('SIGINT', async () => { clearInterval(intervalo); await admin.disconnect(); process.exit(0); });
}

main().catch(error => { console.error(error.message); process.exitCode = 1; });
