'use strict';
const { parentPort, workerData } = require('node:worker_threads');
const { runFlow } = require('../flows/pipeline');

const controller = new AbortController();
const pending = new Map(); let requestId = 0;
parentPort.on('message', message => {
    if (message?.type === 'cancel') {
        controller.abort();
        for (const request of pending.values()) request.reject(Object.assign(new Error('Execução cancelada.'), { code: 'FLOW_CANCELLED' }));
        pending.clear();
    }
    if (message?.type === 'api-response') {
        const request = pending.get(message.id);
        if (request) { pending.delete(message.id); if (message.error) request.reject(Object.assign(new Error(message.error), { code: 'FLOW_VALIDATION' })); else request.resolve(message); }
    }
});
function apiRequest(action) {
    return new Promise((resolve, reject) => {
        if (controller.signal.aborted) { reject(Object.assign(new Error('Execução cancelada.'), { code: 'FLOW_CANCELLED' })); return; }
        const id = ++requestId; pending.set(id, { resolve, reject });
        parentPort.postMessage({ type: 'api-request', action, id });
    });
}
runFlow({ ...workerData, cachePolicy: { compressed: true, prune: true }, processingPolicy: { batchSize: 10000 }, signal: controller.signal,
    providers: { async acquireApi() {
        const session = await apiRequest('acquire');
        const client = require('../flows/disponibilidadeApi').createApiClient({ credentials: session.credentials });
        return { ...client, nextAllowedAt: session.nextAllowedAt, assert: () => apiRequest('assert'), release: () => controller.signal.aborted ? Promise.resolve() : apiRequest('release') };
    } },
    onUpdate: data => parentPort.postMessage({ type: 'update', data }) })
    .then(data => parentPort.postMessage({ type: 'result', data }))
    .catch(error => parentPort.postMessage({ type: 'error', message: error.message,
        code: ['FLOW_CANCELLED', 'FLOW_DISK_FULL'].includes(error.code) ? error.code : 'FLOW_FAILED' }))
    .finally(() => parentPort.close());
