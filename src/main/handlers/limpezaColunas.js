/**
 * Handler da aba de Limpeza de Colunas: reduz planilhas a NOME, CPF e FONE1.
 */
const { ipcMain } = require('electron');
const path = require('path');

const state = require('../state');
const { logSystemAction } = require('../database/connection');
const { limparArquivo } = require('../limpezaColunasArquivo');

const isAdmin = () => state.currentUser && state.currentUser.role === 'admin';

/**
 * Número em notação científica que chegou como texto já perdeu dígitos na origem.
 * Vale avisar em vez de entregar telefone terminado em zeros sem explicação.
 */
function avisarTruncados(truncados, log) {
    if (!truncados) return;
    log(`   ⚠️ ${truncados.toLocaleString('pt-BR')} numero(s) vieram em notacao cientifica ja truncada`);
    log('      (ex: "5,52199E+12"). Os digitos perdidos nao existem mais no arquivo,');
    log('      entao ficaram em branco em vez de virar um numero errado.');
    log('      Solucao: use o .xlsx original, ou formate a coluna como Texto antes');
    log('      de exportar o CSV.');
}

async function processarLote(caminhos, log) {
    let processados = 0;
    let pulados = 0;
    let primeiraSaida = null;

    for (const caminho of caminhos) {
        const nome = path.basename(caminho);
        const resultado = await limparArquivo(caminho);

        if (resultado.ok) {
            processados++;
            if (!primeiraSaida) primeiraSaida = resultado.caminhoSaida;
            log(`${nome} -> OK (${resultado.linhas.toLocaleString('pt-BR')} linhas)`);
            avisarTruncados(resultado.truncados, log);
        } else {
            pulados++;
            log(`${nome} -> PULADO`);
            log(`   ${resultado.motivo}`);
        }
    }

    return { processados, pulados, primeiraSaida };
}

function register() {
    ipcMain.on('start-limpeza-colunas', async (event, caminhos) => {
        const log = (mensagem) => event.sender.send('limpeza-colunas-log', mensagem);
        const finalizar = (success, processados, pulados) =>
            event.sender.send('limpeza-colunas-finished', { success, processados, pulados });

        if (!isAdmin()) {
            log('❌ Acesso negado.');
            return finalizar(false, 0, 0);
        }

        if (!Array.isArray(caminhos) || caminhos.length === 0) {
            log('❌ Nenhum arquivo selecionado.');
            return finalizar(false, 0, 0);
        }

        log(`Iniciando limpeza de ${caminhos.length} arquivo(s)...`);
        logSystemAction(state.currentUser.username, 'Limpeza de Colunas', `Limpou ${caminhos.length} arquivos.`);

        try {
            const { processados, pulados, primeiraSaida } = await processarLote(caminhos, log);

            log('');
            log(`Concluido: ${processados} de ${caminhos.length} arquivo(s).`);
            // Sem abrir o explorador: o caminho vai para o log para o usuário
            // continuar no gerenciador e saber onde os arquivos caíram.
            if (primeiraSaida) log(`Arquivos salvos em: ${path.dirname(primeiraSaida)}`);

            finalizar(true, processados, pulados);
        } catch (erro) {
            log(`❌ Erro critico: ${erro.message}`);
            finalizar(false, 0, 0);
        }
    });
}

module.exports = { register };
