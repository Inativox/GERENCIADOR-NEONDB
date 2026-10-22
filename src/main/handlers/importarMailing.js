/**
 * Handlers da aba Importar Mailing: envio manual de um CSV para um serviço da Fastway.
 * Liberado para qualquer usuário logado; a Situação Receita continua restrita a admin.
 */
const { ipcMain, dialog } = require('electron');
const path = require('node:path');
const { randomUUID } = require('node:crypto');
const state = require('../state');
const fastway = require('../flows/fastway');
const logins = require('../runtimeFastwayLogins');

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

/** Login digitado na tela para este envio. Cada pessoa envia com o próprio login, nunca com o do computador. */
function typedCredentials(input) {
    const login = typeof input?.login === 'string' ? input.login.trim() : '';
    const password = typeof input?.password === 'string' ? input.password : '';
    if (!login || !password || login.length > 200 || password.length > 300) throw validation('Digite o login e a senha da Fastway.');
    return { login, password };
}

// Só guarda depois que a Fastway aceitou o envio, para nunca salvar uma senha errada.
function rememberLogin(owner, credentials, job) {
    try { logins.save(owner, credentials); }
    catch { job.message += ' O login não foi salvo neste computador.'; }
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
    ipcMain.handle('mailing-import-state', protect((_input, owner) => ({ services: SERVICES, fastway: logins.summary(owner), admin: state.currentUser.role === 'admin', maxRows: fastway.MAX_ROWS, job: latest.get(owner) || null })));
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
        const typed = input?.fastway === undefined ? null : typedCredentials(input.fastway);
        const credentials = typed || logins.get(owner);
        if (!credentials) throw validation('Digite o login e a senha da Fastway para enviar.');
        const job = { status: 'running', file: path.basename(selected.filename), rows: selected.rows, service, message: 'Enviando para a Fastway…' };
        latest.set(owner, job); state.mailingUpload = job;
        try {
            const result = await fastway.upload({ file: selected.filename, campaignId: String(service.id), credentials });
            Object.assign(job, { status: 'completed', rows: result.rows, message: `Fastway confirmou ${result.rows.toLocaleString('pt-BR')} registros em ${service.id} · ${service.name}.` });
            files.delete(owner);
            if (typed && input.fastway.save === true) rememberLogin(owner, typed, job);
            return { job, fastway: logins.summary(owner) };
        } catch (error) {
            Object.assign(job, { status: 'failed', message: error.code === 'FLOW_VALIDATION' ? error.message : 'Não foi possível concluir o envio. Confira o painel da Fastway antes de enviar de novo.' });
            throw validation(job.message);
        } finally { state.mailingUpload = null; }
    }));
    ipcMain.handle('mailing-import-forget', protect((_input, owner) => {
        logins.forget(owner);
        return { fastway: logins.summary(owner) };
    }));
}
module.exports = { register, SERVICES };
