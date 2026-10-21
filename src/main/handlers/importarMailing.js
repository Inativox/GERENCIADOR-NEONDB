/**
 * Handlers da aba Importar Mailing: envio manual de um CSV para um serviço da Fastway.
 * Liberado para qualquer usuário logado; a Situação Receita continua restrita a admin.
 */
const { ipcMain, dialog } = require('electron');
const path = require('node:path');
const { randomUUID } = require('node:crypto');
const state = require('../state');
const privateConfig = require('../runtimeConfig');
const fastway = require('../flows/fastway');

// Only these services can receive a manual import; a new one needs a code change.
const SERVICES = [
    { id: 63, group: 'Santander', label: 'Santander', name: 'SANTANDER' },
    { id: 69, group: 'Santander', label: 'Santander ME', name: 'SANTANDER ME' },
    { id: 75, group: 'Santander', label: 'Santander Cadência', name: 'SANTANDER CADENCIA' },
    { id: 62, group: 'C6 Bank', label: 'Transbordo 98 PT2', name: '[C6 BANK] - NOVO TRANSBORDO 98 PT2' },
    { id: 66, group: 'C6 Bank', label: 'Transbordo 98 PT3', name: '[C6 BANK] - NOVO TRANSBORDO 98 PT3' },
    { id: 123, group: 'C6 Bank', label: 'Transbordo 98 PT4', name: '[C6 BANK] - NOVO TRANSBORDO 98 PT4' },
    { id: 67, group: 'C6 Bank', label: 'Transbordo 98 PT5', name: '[C6 BANK] - NOVO TRANSBORDO 98 PT5' },
    { id: 120, group: 'C6 Bank', label: 'Transbordo 98 LTDA', name: '[C6 BANK] - NOVO TRANSBORDO 98 LTDA' },
    { id: 87, group: 'PAG', label: 'Principal', name: 'PAG PRINCIPAL' },
    { id: 106, group: 'PAG', label: 'Principal 2', name: 'PAG PRINCIPAL 2' },
    { id: 92, group: 'PAG', label: 'Cadência', name: 'PAG CADENCIA' },
    { id: 18, group: 'MP', label: 'Principal', name: 'MP PRINCIPAL' },
    { id: 107, group: 'MP', label: 'Principal 2', name: 'MP PRINCIPAL 2' },
    { id: 52, group: 'MP', label: 'Cadência', name: 'MP CADENCIA' },
    { id: 139, group: 'MP', label: '213 - MP', name: '213 - MP' },
    { id: 51, group: 'URA', label: 'Qualificada Gomes', name: 'URA QUALIFICADA GOMES' },
    { id: 148, group: 'URA', label: 'Qualificada MP', name: 'URA QUALIFICADA MP' },
];
const validation = message => Object.assign(new Error(message), { code: 'FLOW_VALIDATION' });

/** Each user uploads with their own Fastway logon (the `fastway` block of their entry in users.json / access file). */
function ownCredentials(owner) {
    const own = privateConfig.loadUsers()[owner]?.fastway;
    return typeof own?.login === 'string' && own.login && typeof own.password === 'string' && own.password ? { login: own.login, password: own.password } : null;
}

function register() {
    const files = new Map(), latest = new Map();
    const protect = action => async (event, input) => {
        if (!state.currentUser || event.sender !== state.mainWindow?.webContents) return { success: false, message: 'Acesso negado.' };
        const owner = state.currentUser.username;
        try {
            const result = await action(input, owner);
            if (state.currentUser?.username !== owner) return { success: false, message: 'A sessão mudou. Reabra a importação.' };
            return { success: true, ...result };
        } catch (error) { return { success: false, message: error.code === 'FLOW_VALIDATION' ? error.message : 'Não foi possível concluir a importação. Confira o arquivo e tente novamente.' }; }
    };
    ipcMain.handle('mailing-import-state', protect((_input, owner) => ({ services: SERVICES, configured: Boolean(ownCredentials(owner)), admin: state.currentUser.role === 'admin', maxRows: fastway.MAX_ROWS, job: latest.get(owner) || null })));
    ipcMain.handle('mailing-import-file', protect(async (_input, owner) => {
        const selected = await dialog.showOpenDialog(state.mainWindow, { title: 'Mailing para importar na Fastway', properties: ['openFile'], filters: [{ name: 'CSV', extensions: ['csv'] }] });
        if (selected.canceled || !selected.filePaths.length) return { cancelled: true };
        const filename = selected.filePaths[0];
        files.delete(owner);
        if (path.extname(filename).toLowerCase() !== '.csv') throw validation('Selecione um arquivo .csv separado por ponto e vírgula.');
        const { rows } = await fastway.inspectCsv(filename), id = randomUUID();
        files.set(owner, { id, filename, rows });
        return { file: { id, name: path.basename(filename), rows } };
    }));
    ipcMain.handle('mailing-import-send', protect(async (input, owner) => {
        const selected = files.get(owner), service = SERVICES.find(item => item.id === input?.serviceId);
        if (!selected || input?.fileId !== selected.id) throw validation('Selecione o CSV antes de enviar.');
        if (!service) throw validation('Escolha um dos serviços da lista.');
        if (state.mailingUpload) throw validation('Já existe um envio para a Fastway em andamento. Aguarde a confirmação.');
        const credentials = ownCredentials(owner);
        if (!credentials) throw validation('Seu usuário ainda não tem login Fastway cadastrado. Peça a um admin para incluir no seu acesso.');
        const job = { status: 'running', file: path.basename(selected.filename), rows: selected.rows, service, message: 'Enviando para a Fastway…' };
        latest.set(owner, job); state.mailingUpload = job;
        try {
            const result = await fastway.upload({ file: selected.filename, campaignId: String(service.id), credentials });
            Object.assign(job, { status: 'completed', rows: result.rows, message: `Fastway confirmou ${result.rows.toLocaleString('pt-BR')} registros em ${service.id} · ${service.name}.` });
            files.delete(owner);
            return { job };
        } catch (error) {
            Object.assign(job, { status: 'failed', message: error.code === 'FLOW_VALIDATION' ? error.message : 'Não foi possível concluir o envio. Confira o painel da Fastway antes de enviar de novo.' });
            throw validation(job.message);
        } finally { state.mailingUpload = null; }
    }));
}
module.exports = { register, SERVICES };
