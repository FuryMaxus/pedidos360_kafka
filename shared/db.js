// Base local SQLite (adaptación local de la base de KPIs/auditoría del diagrama).
const fs = require('node:fs');
const path = require('node:path');
const { DatabaseSync } = require('node:sqlite');
const config = require('./config');

function abrirDb(nombre) {
  fs.mkdirSync(config.dataDir, { recursive: true });
  const db = new DatabaseSync(path.join(config.dataDir, nombre + '.sqlite'));
  // WAL + busy_timeout: permite varias instancias del mismo servicio (experimento 2).
  db.exec('PRAGMA journal_mode = WAL; PRAGMA busy_timeout = 10000;');
  return db;
}

/** Ejecuta fn dentro de una transacción: o se guarda todo, o nada. */
function transaccion(db, fn) {
  db.exec('BEGIN IMMEDIATE');
  try {
    const resultado = fn();
    db.exec('COMMIT');
    return resultado;
  } catch (error) {
    db.exec('ROLLBACK');
    throw error;
  }
}

module.exports = { abrirDb, transaccion };
