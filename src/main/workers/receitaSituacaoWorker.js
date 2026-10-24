const { parentPort, workerData } = require('node:worker_threads');
const { createBqClient } = require('../flows/bq');
const { annotateFile } = require('../receitaSituacao');
const controller = new AbortController();
parentPort.on('message', message => { if (message?.type === 'cancel') controller.abort(); });
async function run() {
    // Workers cannot share the main-process client; this one reads the same local Google credential.
    const bq = createBqClient({ keyFile: workerData.bq?.keyFile || null, project: workerData.bq?.project || 'mbtech-bronze' });
    try {
        const result = await annotateFile({ ...workerData, bq, signal: controller.signal,
            onProgress: counts => parentPort.postMessage({ type: 'progress', counts }), onStage: message => parentPort.postMessage({ type: 'stage', message }) });
        parentPort.postMessage({ type: 'result', result });
    } catch (error) {
        parentPort.postMessage({ type: 'error', authRequired: error.authRequired === true, message: ['RECEITA_SITUACAO', 'FLOW_VALIDATION'].includes(error.code) ? error.message : 'Não foi possível consultar a lista. Confira o arquivo e o acesso ao BigQuery.' });
    } finally { parentPort.close(); }
}
void run();
