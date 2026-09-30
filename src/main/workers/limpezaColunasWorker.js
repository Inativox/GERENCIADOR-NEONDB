const { parentPort, workerData } = require('node:worker_threads');
const path = require('node:path');
const { limparArquivo } = require('../limpezaColunasArquivo');

async function run() {
    const { caminhos } = workerData;
    const log = message => parentPort.postMessage({ type: 'log', message });
    const result = { processados: 0, pulados: 0, primeiraSaida: null };
    for (let index = 0; index < caminhos.length; index++) {
        const nome = path.basename(caminhos[index]);
        parentPort.postMessage({ type: 'progress', current: index, total: caminhos.length, fileName: nome });
        log(`Processando ${nome}...`);
        const output = await limparArquivo(caminhos[index]);
        if (output.ok) {
            result.processados++;
            result.primeiraSaida ||= output.caminhoSaida;
            log(`${nome} -> OK (${output.linhas.toLocaleString('pt-BR')} linhas, ${output.ddisRemovidos.toLocaleString('pt-BR')} DDIs removidos)`);
            if (output.truncados) {
                log(`⚠️ ${output.truncados.toLocaleString('pt-BR')} número(s) em notação científica truncada ficaram em branco. Os dígitos perdidos não existem no arquivo; use o XLSX original com as colunas formatadas como Texto antes de exportar.`);
            }
        } else {
            result.pulados++;
            log(`${nome} -> PULADO\n${output.motivo}`);
        }
        parentPort.postMessage({ type: 'progress', current: index + 1, total: caminhos.length, fileName: nome });
    }
    if (result.primeiraSaida) log(`Arquivos salvos em: ${path.dirname(result.primeiraSaida)}`);
    parentPort.postMessage({ type: 'finished', result });
    parentPort.close();
}

run().catch(error => { throw error; });
