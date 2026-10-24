const { app, ipcMain, dialog, shell } = require('electron');
const path = require('node:path');
const { randomUUID } = require('node:crypto');
const { Worker } = require('node:worker_threads');
const { Pool } = require('pg');
const Store = require('electron-store');
const state = require('../state');
const { lookup, lookupCnaes, document, normalizeKeep } = require('../receitaSituacao');
const { readOnlyPoolOptions } = require('../flows/postgres');
function register() {
    const store = new (require('../settingsWriteRetry')(Store))(), files = new Map(), latest = new Map();
    let active = null;
    const connection = () => store.get('receita_connection_string') || process.env.RECEITA_DATABASE_URL;
    // Situação Receita reads the Receita in BigQuery, with the Google access of the Fluxos screen.
    const bqConfigured = () => state.bqReceitaService?.configured() === true;
    const requireBq = () => { if (!bqConfigured()) throw Object.assign(new Error('Entre com sua conta Google na tela de Fluxos (Acessos às fontes) para consultar a Receita no BigQuery.'), { code: 'RECEITA_SITUACAO' }); };
    // O "Manter só estes CNAEs" da Limpeza Local confere o CNAE principal pela mesma conexão de leitura.
    state.receitaCnaeService = {
        configured: () => Boolean(connection()),
        async lookupCnaes(documents, options) {
            const pool = new Pool(readOnlyPoolOptions(connection(), { max: 1, timeout: 60000 }));
            pool.on('error', () => {});
            try { return await lookupCnaes(pool, documents, options); }
            finally { await pool.end(); }
        }
    };
    const protect = action => async (event, input) => {
        if (!state.currentUser || state.currentUser.role !== 'admin' || event.sender !== state.mainWindow?.webContents) return { success: false, message: 'Acesso negado.' };
        const owner = state.currentUser.username;
        try {
            const result = await action(input, owner, event.sender);
            if (state.currentUser?.username !== owner || state.currentUser?.role !== 'admin') return { success: false, message: 'A sessão mudou. Reabra a consulta.' };
            return { success: true, ...result };
        } catch (error) {
            if (error.authRequired) state.bqReceitaService?.loginExpired(owner);
            return { success: false, message: ['RECEITA_SITUACAO', 'FLOW_VALIDATION'].includes(error.code) ? error.message : 'Não foi possível concluir a consulta. Confira a lista e o acesso à Receita.' };
        }
    };
    ipcMain.handle('receita-situacao-state', protect((_input, owner) => ({ configured: bqConfigured(), job: latest.get(owner) || null })));
    ipcMain.handle('receita-situacao-file', protect(async (_input, owner) => {
        const selected = await dialog.showOpenDialog(state.mainWindow, { title: 'Lista para consultar situação cadastral', properties: ['openFile'], filters: [{ name: 'Listas', extensions: ['xlsx', 'csv'] }] });
        if (selected.canceled || !selected.filePaths.length) return { cancelled: true };
        const filename = selected.filePaths[0], id = randomUUID();
        files.set(owner, { id, filename }); return { file: { id, name: path.basename(filename) } };
    }));
    ipcMain.handle('receita-situacao-one', protect(async (input, owner) => {
        if (active) throw Object.assign(new Error('Aguarde a consulta da lista terminar.'), { code: 'RECEITA_SITUACAO' });
        if (typeof input?.cnpj !== 'string' || input.cnpj.length > 30 || !document(input.cnpj)) throw Object.assign(new Error('Informe um CNPJ com 14 posições, com ou sem pontuação.'), { code: 'RECEITA_SITUACAO' });
        requireBq();
        const cnpj = document(input.cnpj), result = (await lookup(state.bqReceitaService.client(), [cnpj])).get(cnpj);
        return { result: result || null, found: Boolean(result), checkedAt: new Date().toISOString() };
    }));
    ipcMain.handle('receita-situacao-start', protect((input, owner, sender) => {
        const selected = files.get(owner);
        if (!selected || input?.fileId !== selected.id) throw Object.assign(new Error('Selecione a lista antes de consultar.'), { code: 'RECEITA_SITUACAO' });
        if (active) throw Object.assign(new Error('Uma consulta de situação já está em andamento.'), { code: 'RECEITA_SITUACAO' });
        requireBq();
        const keep = normalizeKeep(input?.keep), keepList = keep ? [...keep] : null;
        const removeMei = input?.removeMei === true, csv = input?.csv === true;
        const job = { id: randomUUID(), owner, name: path.basename(selected.filename), status: 'running', counts: { processed: 0, found: 0, notFound: 0, invalid: 0, removed: 0, mei: 0 }, message: 'Lendo a lista.', output: null, csvOutput: null };
        const worker = new Worker(path.join(__dirname, '../workers/receitaSituacaoWorker.js'), { workerData: { filename: selected.filename, bq: state.bqReceitaService.access(), keep: keepList, removeMei, csv }, resourceLimits: { maxOldGenerationSizeMb: 256 } });
        const context = { worker, job, finished: false }; active = context; latest.set(owner, job);
        const emit = () => { if (state.currentUser?.username === owner && state.currentUser?.role === 'admin' && state.mainWindow?.webContents === sender && !sender.isDestroyed()) sender.send('receita-situacao-update', { ...job }); };
        const finish = (status, message, result) => {
            if (context.finished) return; context.finished = true;
            Object.assign(job, { status, message }, result ? { counts: { processed: result.processed, found: result.found, notFound: result.notFound, invalid: result.invalid, removed: result.removed, mei: result.mei }, output: result.output, csvOutput: result.csvOutput || null } : {});
            if (active === context) active = null; emit();
        };
        worker.on('message', update => {
            if (context.finished) return;
            if (update.type === 'progress') { job.counts = update.counts; emit(); }
            if (update.type === 'stage' && job.status === 'running') { job.message = update.message; emit(); }
            if (update.type === 'result') finish('completed', `Consulta concluída. ${update.result.csvOutput ? 'O CSV adicional foi salvo junto do resultado. ' : ''}O arquivo original foi preservado.`, update.result);
            if (update.type === 'error') {
                if (update.authRequired) state.bqReceitaService?.loginExpired(owner);
                finish(job.status === 'cancelling' ? 'cancelled' : 'failed', update.message);
            }
        });
        worker.on('error', () => finish('failed', 'O processamento foi interrompido. O arquivo original foi preservado. Tente novamente.'));
        worker.on('exit', () => { if (!context.finished) finish('failed', 'A consulta foi interrompida. Tente novamente.'); });
        return { job };
    }));
    ipcMain.handle('receita-situacao-cancel', protect((_input, owner) => { if (active?.job.owner === owner) { active.job.status = 'cancelling'; active.worker.postMessage({ type: 'cancel' }); } return {}; }));
    ipcMain.handle('receita-situacao-open', protect(async (_input, owner) => {
        const job = latest.get(owner); if (job?.status !== 'completed' || !job.output) throw Object.assign(new Error('Nenhum arquivo concluído para abrir.'), { code: 'RECEITA_SITUACAO' });
        if (await shell.openPath(job.output)) throw new Error('Falha ao abrir'); return {};
    }));
    const cancel = () => active?.worker.postMessage({ type: 'cancel' });
    app.on('before-quit', cancel); ipcMain.on('logout', cancel);
}
module.exports = { register };
