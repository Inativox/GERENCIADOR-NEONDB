const path = require('node:path');
const { Worker } = require('node:worker_threads');

let running = false;

async function runLimpezaColunas(caminhos, onEvent) {
    if (!Array.isArray(caminhos) || !caminhos.length || caminhos.some(p => typeof p !== 'string' || !p.trim())) {
        throw new Error('Selecione arquivos válidos para iniciar.');
    }
    if (running) throw new Error('Uma limpeza de colunas já está em andamento.');
    running = true;
    try {
        return await new Promise((resolve, reject) => {
            const worker = new Worker(path.join(__dirname, 'workers/limpezaColunasWorker.js'), {
                workerData: { caminhos },
                resourceLimits: { maxOldGenerationSizeMb: 512 },
            });
            let result;
            worker.on('message', event => {
                if (event.type === 'finished') result = event.result;
                else {
                    try { onEvent(event); }
                    catch (error) { worker.terminate(); reject(error); }
                }
            });
            worker.once('error', error => {
                reject(error.code === 'ERR_WORKER_OUT_OF_MEMORY'
                    ? new Error('O arquivo excedeu a memória disponível para limpeza. Divida a planilha em arquivos menores.')
                    : error);
            });
            worker.once('exit', code => {
                if (code === 0 && result) resolve(result);
                else reject(new Error('O processamento foi interrompido antes de concluir o lote.'));
            });
        });
    } finally {
        running = false;
    }
}

module.exports = { runLimpezaColunas, isLimpezaColunasRunning: () => running };
