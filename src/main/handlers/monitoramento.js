/**
 * Handlers da aba de Monitoramento: relatórios Fastway, Bitrix e download de gravações.
 */
const { ipcMain, dialog } = require('electron');
const fs = require('fs');
const axios = require('axios');

const state = require('../state');

function register() {
    ipcMain.handle('fetch-monitoring-report', async (event, { reportUrl, operatorTimesParams }) => {
        if (!state.currentUser) {
            return { success: false, message: 'Acesso negado. Faça o login.' };
        }

        let mainReportResult;
        try {
            const response = await axios.get(reportUrl, {
                timeout: 4000000,
                headers: { 'User-Agent': 'PostmanRuntime/7.44.1' }
            });
            if (response.status === 200) {
                const data = (typeof response.data === 'string' && response.data.includes("Nenhum registro encontrado"))
                    ? []
                    : response.data;
                mainReportResult = { success: true, data: data, operatorTimesData: null };
            } else {
                return { success: false, message: `A API principal retornou um status inesperado: ${response.status}` };
            }
        } catch (error) {
            console.error("Erro ao buscar relatório de monitoramento:", error.message);
            return { success: false, message: `Falha na comunicação com a API principal: ${error.message}` };
        }

        if (mainReportResult.success && operatorTimesParams) {
            const { data_inicio, data_fim, operador_id, grupo_operador_id } = operatorTimesParams;
            const baseUrl = 'http://mbfinance.fastssl.com.br/api/relatorio/operador_tempos.php';
            const url = `${baseUrl}?data_inicial=${data_inicio}&data_final=${data_fim}&operador_id=${operador_id}&grupo_operador_id=${grupo_operador_id}&servico_id=&operador_ativo=`;
            try {
                const timesResponse = await axios.get(url, { timeout: 30000 });
                if (timesResponse.status === 200) {
                    mainReportResult.operatorTimesData = timesResponse.data;
                } else {
                    console.error(`API de tempos retornou status ${timesResponse.status}`);
                }
            } catch (error) {
                console.error('[DEBUG MAIN] ERRO na chamada da API de tempos:', error.message);
            }
        }

        return mainReportResult;
    });

    // Stub: preload.js expõe este canal mas não há implementação no main.js original
    ipcMain.handle('fetch-bitrix-report', async (event, options) => {
        return { success: false, message: 'Não implementado.' };
    });

    ipcMain.handle('download-recording', async (event, url, fileName) => {
        if (!state.mainWindow) {
            return { success: false, message: 'Janela principal não encontrada.' };
        }
        const { canceled, filePath } = await dialog.showSaveDialog(state.mainWindow, {
            title: 'Salvar Gravação',
            defaultPath: fileName,
            filters: [{ name: 'Áudio MP3', extensions: ['mp3'] }]
        });
        if (canceled || !filePath) {
            return { success: true, message: 'Download cancelado pelo usuário.' };
        }
        try {
            const response = await axios({
                method: 'get',
                url: url,
                responseType: 'stream',
                headers: {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                }
            });
            const writer = fs.createWriteStream(filePath);
            response.data.pipe(writer);
            return new Promise((resolve, reject) => {
                writer.on('finish', () => resolve({ success: true, message: `Gravação salva em: ${filePath}` }));
                writer.on('error', (err) => {
                    console.error("Erro ao salvar o arquivo:", err);
                    reject({ success: false, message: `Falha ao salvar o arquivo: ${err.message}` });
                });
            });
        } catch (error) {
            console.error("Erro no download da gravação:", error);
            let errorMessage = error.message;
            if (error.response && error.response.status === 403) {
                errorMessage = "Acesso negado (403 Forbidden). Verifique a URL ou permissões no servidor.";
            }
            return { success: false, message: `Erro ao baixar a gravação: ${errorMessage}` };
        }
    });
}

module.exports = { register };
