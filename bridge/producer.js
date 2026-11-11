const { Kafka, Partitioners } = require('kafkajs');
const { PEDIDOS } = require('../shared/topics');
const { crearEvento } = require('../shared/dto');

const kafka = new Kafka({
  clientId: 'orders-stress-producer',
  brokers: ['localhost:9093', 'localhost:9094']
});

const producer = kafka.producer({
  createPartitioner: Partitioners.DefaultPartitioner
});

const CATALOGO = [
  { id: 'PROD-1', precio: 5000 },
  { id: 'PROD-2', precio: 8500 },
  { id: 'PROD-3', precio: 2000 },
  { id: 'PROD-4', precio: 3500 },
  { id: 'PROD-5', precio: 12000 }
];

const CLIENTES = ['C-100', 'C-101', 'C-102', 'C-103', 'C-104'];
const ORIGENES = ['web', 'app-ios', 'app-android', 'kiosco-local'];
const OPERADORES = ['op-carlos', 'op-ana', 'op-luis', 'op-marta'];

const FLUJO_NORMAL = ['PEDIDO_CREADO', 'PEDIDO_ACEPTADO', 'PEDIDO_EN_PREPARACION', 'PEDIDO_DESPACHADO', 'PEDIDO_ENTREGADO'];
const FLUJO_CANCELADO = ['PEDIDO_CREADO', 'PEDIDO_ACEPTADO', 'PEDIDO_CANCELADO'];

function obtenerRandom(array) {
  return array[Math.floor(Math.random() * array.length)];
}

function generarCarritoRandom() {
  const cantidadItems = Math.floor(Math.random() * 3) + 1;
  const items = [];
  let total = 0;
  
  for (let i = 0; i < cantidadItems; i++) {
    const prod = obtenerRandom(CATALOGO);
    if (!items.find(item => item.productoId === prod.id)) {
      const cant = Math.floor(Math.random() * 4) + 1;
      items.push({ productoId: prod.id, cantidad: cant, precio: prod.precio });
      total += (prod.precio * cant);
    }
  }
  return { items, total };
}

async function dispararTrafico() {
    await producer.connect();
    console.log('Productor conectado al clúster Kafka.');
    
    const cantidadPedidos = parseInt(process.argv[2]) || 100;
    console.log(`Generando ráfaga de ${cantidadPedidos} pedidos con datos aleatorios...`);
    
    const startTime = Date.now();
    let mensajesEnviados = 0;

    for (let i = 1; i <= cantidadPedidos; i++) {

        const pedidoId = `P-STRESS-${Date.now().toString().slice(-5)}-${i}`;
        const clienteId = obtenerRandom(CLIENTES);
        const origen = obtenerRandom(ORIGENES);
        const operador = obtenerRandom(OPERADORES);

        const flujo = Math.random() > 0.85 ? FLUJO_CANCELADO : FLUJO_NORMAL;
        const { items, total } = generarCarritoRandom();

        for (const estado of flujo) {
            const esCliente = (estado === 'PEDIDO_CREADO');
            const actor = esCliente 
                ? { usuario: `usr-${clienteId}`, rol: 'CLIENTE' }
                : { usuario: operador, rol: 'OPERADOR' };
            
            const evento = crearEvento({
                tipo: estado,
                pedidoId: pedidoId,
                clienteId: clienteId,
                total: total,
                version: 2,
                origen: origen,
                actor: actor,
                items: estado === 'PEDIDO_CREADO' ? items : undefined
            });

            await producer.send({
                topic: PEDIDOS,
                messages: [{
                    key: pedidoId,
                    value: JSON.stringify(evento)
                }]
            });
            mensajesEnviados++;
        }
    }
    const tiempoTotal = ((Date.now() - startTime) / 1000).toFixed(2);
    console.log(`Ráfaga completada: ${mensajesEnviados} eventos inyectados en ${tiempoTotal}s.`);
    await producer.disconnect();
}
dispararTrafico().catch(console.error);