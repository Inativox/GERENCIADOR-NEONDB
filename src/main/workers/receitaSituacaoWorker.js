const { parentPort, workerData } = require('node:worker_threads');
const { Pool } = require('pg');
const { readOnlyPoolOptions } = require('../flows/postgres');
const { annotateFile } = require('../receitaSituacao');
const controller = new AbortController();
parentPort.on('message', message => { if (message?.type === 'cancel') controller.abort(); });
const pool = new Pool(readOnlyPoolOptions(workerData.connection, { max: 1 }));
pool.on('error', () => {});
annotateFile({ ...workerData, pool, signal: controller.signal, onProgress: counts => parentPort.postMessage({ type: 'progress', counts }) })
    .then(result => parentPort.postMessage({ type: 'result', result }))
    .catch(error => parentPort.postMessage({ type: 'error', message: ['RECEITA_SITUACAO', 'FLOW_VALIDATION'].includes(error.code) ? error.message : 'Não foi possível consultar a lista. Confira o arquivo e o acesso ao banco.' }))
    .finally(async () => { await pool.end(); parentPort.close(); });
