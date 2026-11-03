/**
 * Handlers da aba Importar Mailing: fila de CSVs enviados um por vez, cada um para o seu serviço da Fastway.
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
const MAX_QUEUE = 50;
const READ_FAILURE = 'Não foi possível ler o arquivo. Tente novamente.';
const SEND_FAILURE = 'Não foi possível concluir o envio. Confira o painel da Fastway antes de enviar de novo.';
const validation = message => Object.assign(new Error(message), { code: 'FLOW_VALIDATION' });
const friendly = (error, fallback) => error.code === 'FLOW_VALIDATION' ? error.message : fallback;
// O Windows não diferencia maiúsculas no caminho: a mesma lista com outra grafia ainda é a mesma.
const samePath = (a, b) => process.platform === 'win32' ? a.toLowerCase() === b.toLowerCase() : a === b;

/** Login digitado na tela para esta fila. Cada pessoa envia com o próprio login, nunca com o do computador. */
function typedCredentials(input) {
    const login = typeof input?.login === 'string' ? input.login.trim() : '';
    const password = typeof input?.password === 'string' ? input.password : '';
    if (!login || !password || login.length > 200 || password.length > 300) throw validation('Digite o login e a senha da Fastway.');
    return { login, password };
}

// Só guarda depois que a Fastway aceitou um envio, para nunca salvar uma senha errada.
function rememberLogin(owner, credentials, item) {
    try { logins.save(owner, credentials); }
    catch { item.message += ' O login não foi salvo neste computador.'; }
}

function findService(serviceId) {
    const service = SERVICES.find(item => item.id === serviceId);
    if (!service) throw validation('Escolha um dos serviços da lista antes de adicionar.');
    return service;
}

// A tela recebe a fila sem o caminho completo dos arquivos.
const view = queue => ({ running: queue.running, items: queue.items.map(({ filename, ...item }) => item) });

/** Confere um CSV escolhido e o coloca no fim da fila, esperando o envio. */
async function enqueue(queue, filename, service) {
    if (path.extname(filename).toLowerCase() !== '.csv') throw validation('Selecione um arquivo .csv separado por ponto e vírgula.');
    if (queue.items.length >= MAX_QUEUE) throw validation(`A fila aceita até ${MAX_QUEUE} listas. Limpe as concluídas para adicionar mais.`);
    const repeated = queue.items.some(item => ['waiting', 'running'].includes(item.status) && item.service.id === service.id && samePath(item.filename, filename));
    if (repeated) throw validation(`Essa lista já está na fila para ${service.id}.`);
    const { rows } = await fastway.inspectCsv(filename);
    // A conferência leva segundos; outra seleção pode ter enchido a fila nesse meio-tempo.
    if (queue.items.length >= MAX_QUEUE) throw validation(`A fila aceita até ${MAX_QUEUE} listas. Limpe as concluídas para adicionar mais.`);
    queue.items.push({ id: randomUUID(), filename, name: path.basename(filename), rows, service, status: 'waiting', message: '' });
}

/** Sobe a lista e marca o resultado nela. Devolve true quando a Fastway recusou o login, para a fila parar. */
async function sendItem(item, credentials) {
    try {
        const result = await fastway.upload({ file: item.filename, campaignId: String(item.service.id), credentials });
        Object.assign(item, { status: 'completed', rows: result.rows, message: `Fastway confirmou ${result.rows.toLocaleString('pt-BR')} registros em ${item.service.id} · ${item.service.name}.` });
        return false;
    } catch (error) {
        // Com o login recusado, as próximas falhariam do mesmo jeito: elas esperam o login certo.
        const stop = error.loginRejected === true;
        Object.assign(item, { status: 'failed', message: friendly(error, SEND_FAILURE) + (stop ? ' A fila parou: corrija o login e envie de novo.' : '') });
        return stop;
    }
}

function register() {
    const queues = new Map();
    const queueOf = owner => { if (!queues.has(owner)) queues.set(owner, { running: false, items: [] }); return queues.get(owner); };
    // Com a janela fechada a fila continua; só a tela deixa de ser avisada.
    const publish = (owner, queue) => {
        const contents = state.mainWindow?.webContents;
        if (state.currentUser?.username !== owner || !contents || contents.isDestroyed?.()) return;
        contents.send('mailing-import-update', { queue: view(queue), fastway: logins.summary(owner) });
    };
    // Uma lista por vez, na ordem; listas adicionadas durante o envio entram no fim e também sobem.
    async function run(owner, queue, credentials, toSave) {
        try {
            for (let item = queue.items.find(next => next.status === 'waiting'); item; item = queue.items.find(next => next.status === 'waiting')) {
                Object.assign(item, { status: 'running', message: 'Enviando para a Fastway…' });
                publish(owner, queue);
                const stop = await sendItem(item, credentials);
                if (item.status === 'completed' && toSave) { rememberLogin(owner, toSave, item); toSave = null; }
                if (stop) break;
                publish(owner, queue);
            }
        } finally {
            queue.running = false;
            if (state.mailingUpload === queue) state.mailingUpload = null;
            publish(owner, queue);
        }
    }
    const protect = action => async (event, input) => {
        if (!state.currentUser || event.sender !== state.mainWindow?.webContents) return { success: false, message: 'Acesso negado.' };
        const owner = state.currentUser.username;
        try {
            const result = await action(input, owner);
            if (state.currentUser?.username !== owner) return { success: false, message: 'A sessão mudou. Reabra a importação.' };
            return { success: true, ...result };
        } catch (error) { return { success: false, message: friendly(error, 'Não foi possível concluir a importação. Confira o arquivo e tente novamente.') }; }
    };
    ipcMain.handle('mailing-import-state', protect((_input, owner) => ({ services: SERVICES, fastway: logins.summary(owner), admin: state.currentUser.role === 'admin', maxRows: fastway.MAX_ROWS, maxQueue: MAX_QUEUE, queue: view(queueOf(owner)) })));
    ipcMain.handle('mailing-import-add', protect(async (input, owner) => {
        const service = findService(input?.serviceId), queue = queueOf(owner);
        const selected = await dialog.showOpenDialog(state.mainWindow, { title: `Mailings para ${service.id} · ${service.name}`, properties: ['openFile', 'multiSelections'], filters: [{ name: 'CSV', extensions: ['csv'] }] });
        if (selected.canceled || !selected.filePaths.length) return { cancelled: true, queue: view(queue) };
        const rejected = [];
        for (const filename of selected.filePaths) {
            try { await enqueue(queue, filename, service); }
            catch (error) { rejected.push({ name: path.basename(filename), message: friendly(error, READ_FAILURE) }); }
        }
        return { queue: view(queue), rejected };
    }));
    ipcMain.handle('mailing-import-remove', protect((input, owner) => {
        const queue = queueOf(owner), index = queue.items.findIndex(item => item.id === input?.itemId);
        if (index === -1) throw validation('Essa lista não está mais na fila.');
        if (queue.items[index].status === 'running') throw validation('Essa lista já está sendo enviada e não pode sair da fila.');
        queue.items.splice(index, 1);
        return { queue: view(queue) };
    }));
    ipcMain.handle('mailing-import-clear', protect((_input, owner) => {
        const queue = queueOf(owner);
        queue.items = queue.items.filter(item => ['waiting', 'running'].includes(item.status));
        return { queue: view(queue) };
    }));
    ipcMain.handle('mailing-import-start', protect((input, owner) => {
        const queue = queueOf(owner);
        if (queue.running || state.mailingUpload) throw validation('A fila já está enviando. Listas adicionadas agora sobem na sequência.');
        if (!queue.items.some(item => item.status === 'waiting')) throw validation('Adicione ao menos uma lista à fila.');
        const typed = input?.fastway === undefined ? null : typedCredentials(input.fastway);
        const credentials = typed || logins.get(owner);
        if (!credentials) throw validation('Digite o login e a senha da Fastway para enviar.');
        queue.running = true; state.mailingUpload = queue;
        void run(owner, queue, credentials, typed && input.fastway.save === true ? typed : null);
        return { queue: view(queue) };
    }));
    ipcMain.handle('mailing-import-forget', protect((_input, owner) => {
        logins.forget(owner);
        return { fastway: logins.summary(owner) };
    }));
}
module.exports = { register, SERVICES };
