/**
 * Handler da aba de Limpeza de Colunas: reduz planilhas a NOME, CPF e FONE1.
 */
const { ipcMain } = require('electron');

const state = require('../state');
const { logSystemAction } = require('../database/connection');
const { runLimpezaColunas, isLimpezaColunasRunning } = require('../limpezaColunasJob');

const isAdmin = () => state.currentUser && state.currentUser.role === 'admin';
let activeSender = null;

function register() {
    ipcMain.on('start-limpeza-colunas', async (event, caminhos) => {
        const send = (channel, payload) => {
            if (!event.sender.isDestroyed()) event.sender.send(channel, payload);
        };
        const log = mensagem => send('limpeza-colunas-log', mensagem);
        const finalizar = (success, processados, pulados, message) =>
            send('limpeza-colunas-finished', { success, processados, pulados, message });

        if (!isAdmin()) {
            log('❌ Acesso negado.');
            return finalizar(false, 0, 0);
        }

        if (!Array.isArray(caminhos) || caminhos.length === 0 || caminhos.some(p => typeof p !== 'string' || !p.trim())) {
            log('❌ Nenhum arquivo selecionado.');
            return finalizar(false, 0, 0);
        }
        if (isLimpezaColunasRunning()) {
            const message = 'Uma limpeza de colunas já está em andamento. Aguarde a conclusão antes de iniciar outro lote.';
            log(`⚠️ ${message}`);
            // A janela nova não receberá a conclusão enviada ao renderer antigo.
            if (activeSender !== event.sender) finalizar(false, 0, 0, message);
            return;
        }

        log(`Iniciando limpeza de ${caminhos.length} arquivo(s)...`);
        logSystemAction(state.currentUser.username, 'Limpeza de Colunas', `Iniciou limpeza de ${caminhos.length} arquivos.`);

        try {
            activeSender = event.sender;
            const { processados, pulados } = await runLimpezaColunas(caminhos, event => {
                if (event.type === 'log') log(event.message);
                if (event.type === 'progress') send('limpeza-colunas-progress', event);
            });
            log(`Concluído: ${processados} de ${caminhos.length} arquivo(s).`);
            finalizar(processados > 0, processados, pulados,
                processados === 0 ? 'Nenhum arquivo foi gerado. Confira os motivos no painel de atividade.' : undefined);
        } catch (erro) {
            log(`❌ Erro critico: ${erro.message}`);
            finalizar(false, 0, 0, erro.message);
        } finally {
            if (activeSender === event.sender) activeSender = null;
        }
    });
}

module.exports = { register };
