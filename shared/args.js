// Lee opciones de la línea de comandos con la forma --nombre=valor o --bandera.
// Ej: npm run generar -- --pedidos=500 --delay=50  ->  { pedidos: '500', delay: '50' }
function leerOpciones(argv = process.argv.slice(2)) {
  const opciones = {};
  for (const arg of argv) {
    const coincide = /^--([^=]+)(?:=(.*))?$/.exec(arg);
    if (coincide) opciones[coincide[1]] = coincide[2] ?? true;
  }
  return opciones;
}

/** Devuelve la opción como número entero, o el valor por defecto. */
function numero(opciones, nombre, porDefecto) {
  if (opciones[nombre] === undefined) return porDefecto;
  const valor = Number(opciones[nombre]);
  if (!Number.isInteger(valor) || valor < 0) throw new Error(`--${nombre} debe ser un entero >= 0`);
  return valor;
}

module.exports = { leerOpciones, numero };
