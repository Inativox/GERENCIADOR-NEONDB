const { ipcMain, app, dialog, shell } = require('electron');
const fs = require('fs');
const path = require('path');
const ExcelJS = require('exceljs');
const { parse } = require('csv-parse');
const { Pool } = require('pg');
const Store = require('electron-store');
const state = require('../state');
const { createFlowManager } = require('../flows/manager');
const { createBqClient, restoreCnpj, existingCredentials } = require('../flows/bq');
const { createBqLogin, loginModeFor, gcloudCommand } = require('../flows/bqAuth');
const { defaults, OPERATIONS, MAX_ROWS } = require('../flows/config');
const { listFormats } = require('../flows/formats');
const { listLayoutFields } = require('../flows/layouts');
const { getReceitaMetadata } = require('../flows/receita');
const { readOnlyPoolOptions } = require('../flows/postgres');
const { createReceitaOptions } = require('../flows/receitaOptions');

async function readRootFile(filename, signal) {
    const documents = new Set(); let skipped = 0;
    const add = value => { const result = restoreCnpj(value); if (result) documents.add(result); else if (value) skipped++; };
    const cancelled = () => { if (signal?.aborted) throw new Error('Leitura da raiz cancelada.'); };
    if (/\.csv$/i.test(filename)) {
        const handle = await fs.promises.open(filename, 'r'); const buffer = Buffer.alloc(4096); await handle.read(buffer, 0, buffer.length, 0); await handle.close();
        const firstLine = buffer.toString('utf8').split(/\r?\n/)[0];
        const source = fs.createReadStream(filename);
        const stream = parse({ bom: true, columns: true, delimiter: firstLine.includes(';') ? ';' : ',', skip_empty_lines: true });
        source.on('error', error => stream.destroy(error)); source.pipe(stream);
        let key;
        try { for await (const record of stream) { cancelled(); if (!key) { key = Object.keys(record).find(item => /^(cpf|cnpj)$/i.test(item.trim())); if (!key) throw new Error('A raiz precisa da coluna CNPJ ou CPF.'); } add(record[key]); } }
        finally { source.destroy(); stream.destroy(); }
    } else {
        const workbook = new ExcelJS.stream.xlsx.WorkbookReader(filename, { worksheets: 'emit', sharedStrings: 'cache', styles: 'ignore', hyperlinks: 'ignore' });
        for await (const worksheet of workbook) {
            let index;
            for await (const row of worksheet) {
                cancelled();
                if (!index) { row.eachCell((cell, position) => { if (/^(cnpj|cpf)$/i.test(String(cell.value).trim())) index = position; }); if (!index) throw new Error('A raiz precisa da coluna CNPJ ou CPF.'); }
                else add(row.getCell(index).text);
            }
            break;
        }
    }
    if (!documents.size) throw new Error('Arquivo raiz sem documentos utilizáveis.');
    return { documents: [...documents], info: { source: 'file', count: documents.size, skipped, queriedAt: new Date().toISOString() } };
}
function register() {
    const store = new Store();
    const selectedDirectories = new Map();
    const config = () => ({ receita: store.get('receita_connection_string') || process.env.RECEITA_DATABASE_URL, enrichment: process.env.DATABASE_URL || store.get('db_connection_string') });
    const receitaOptions = createReceitaOptions({ getConnection: () => config().receita, cacheDirectory: path.join(app.getPath('userData'), 'receita-options'), poolFactory: connection => new Pool(readOnlyPoolOptions(connection, { max: 1, timeout: 180000, connectionTimeout: 10000 })) });
    const bq = () => createBqClient({ keyFile: store.get('flow_bq_key_file'), project: process.env.BQ_PROJECT || 'mbtech-bronze' });
    const login = createBqLogin({
        getMode: () => loginModeFor(store.get('flow_bq_key_file')),
        validate: () => bq().test(),
        onUpdate(update) {
            if (state.currentUser?.username === update.owner && state.mainWindow && !state.mainWindow.isDestroyed?.()) state.mainWindow.webContents.send('flow-bq-auth-update', update);
        },
    });
    const queryBq = async (action, signal) => {
        const owner = state.currentUser?.username;
        try { return await action(bq()); }
        catch (error) {
            if (error.code === 'BQ_AUTH_REQUIRED' && !signal?.aborted && store.get('flow_bq_auto_login') !== false && owner === state.currentUser?.username && state.currentUser?.role === 'admin') {
                // The job keeps its checkpoint. Login is independent of processing and never retries a failed export.
                void login.renew(owner, { automatic: true }).catch(() => {});
            }
            throw error;
        }
    };
    const manager = createFlowManager({
        baseDirectory: path.join(app.getPath('userData'), 'flows'), getUser: () => state.currentUser,
        resolveConnections(flow) {
            const connections = config();
            if (!connections.receita) throw new Error('Configure o acesso à base Receita na aba Gerar listas.');
            if ((flow.enrichment.enabled || flow.cleaning.blocklist || (flow.cleaning.enabled && flow.cleaning.invalidPhones)) && !connections.enrichment) throw new Error('Configure o banco do Gerenciador para enriquecimento e filtros de telefone.');
            return connections;
        },
        async resolveRoot(flow, options) {
            const source = flow.cleaning.enabled ? flow.cleaning.rootSource : 'none';
            if (source === 'bq') return queryBq(client => client.loadRoot(flow.pipelines, options), options.signal);
            if (source === 'file') return readRootFile(flow.cleaning.rootFile, options.signal);
            if (source === 'neon') {
                if (!config().enrichment) throw new Error('Configure o banco para consultar a raiz Neon.');
                const pool = new Pool(readOnlyPoolOptions(config().enrichment, { max: 1, timeout: 30000, connectionTimeout: 10000 }));
                try {
                    const unique = new Set(); let cursor = '', total = 0;
                    while (true) {
                        if (options.signal?.aborted) throw new Error('Leitura da raiz Neon cancelada.');
                        const rows = (await pool.query('SELECT cnpj FROM raiz_cnpjs WHERE cnpj > $1 ORDER BY cnpj LIMIT 5000', [cursor])).rows;
                        if (!rows.length) break;
                        total += rows.length;
                        if (total > 1000000) throw new Error('A raiz Neon excede 1 milhão de registros. Revise a fonte antes de executar.');
                        for (const row of rows) { const value = restoreCnpj(row.cnpj); if (value) unique.add(value); }
                        const next = String(rows[rows.length - 1].cnpj);
                        if (next <= cursor) throw new Error('A raiz Neon não permite paginação segura.');
                        cursor = next;
                        if (rows.length < 5000) break;
                    }
                    if (options.signal?.aborted) throw new Error('Leitura da raiz Neon cancelada.');
                    const documents = [...unique];
                    if (!documents.length) throw new Error('A raiz Neon está vazia ou sem documentos utilizáveis.');
                    return { documents, info: { source, count: documents.length, queriedAt: new Date().toISOString() } };
                } catch (error) { if (/^(A raiz|Leitura da raiz)/.test(error.message)) throw error; throw new Error('Não foi possível consultar a raiz Neon. Verifique o banco.'); }
                finally { await pool.end(); }
            }
            return { documents: [], info: { source: 'none', count: 0, queriedAt: new Date().toISOString() } };
        },
        resolveApiSession: username => require('../apiSessions').acquireFlowApiSession(username),
        onUpdate(job) { if (state.currentUser?.username === job.owner && state.mainWindow && !state.mainWindow.isDestroyed()) state.mainWindow.webContents.send('flow-update', job); },
    });
    state.flowManager = manager;
    const protect = action => async (event, argument) => {
        try {
            if (!state.currentUser || state.currentUser.role !== 'admin' || event.sender !== state.mainWindow?.webContents) throw new Error('Acesso negado. Entre com um perfil autorizado nesta janela.');
            return { success: true, ...await action(argument) };
        } catch (error) {
            const bqError = ['BQ_AUTH_REQUIRED', 'BQ_CREDENTIAL_INVALID', 'BQ_FORBIDDEN'].includes(error.code);
            const validationError = error.code === 'FLOW_VALIDATION';
            return { success: false, message: error.code && !bqError && !validationError ? 'Não foi possível concluir. Verifique os arquivos e a configuração de acesso.' : error.message, ...(bqError ? { code: error.code } : {}) };
        }
    };
    ipcMain.handle('flows-bootstrap', protect(() => {
        let bqConfigured = false;
        try { bqConfigured = Boolean(existingCredentials(store.get('flow_bq_key_file'))); } catch { /* UI offers import/test. */ }
        if (!bqConfigured) bqConfigured = process.platform === 'win32' ? Boolean(gcloudCommand()) : true;
        let bqLoginMode = 'unavailable';
        try { bqLoginMode = loginModeFor(store.get('flow_bq_key_file')); } catch { /* Invalid private file offers import instead. */ }
        const bootstrap = manager.bootstrap();
        return { ...bootstrap, defaults: defaults(), operations: OPERATIONS, formats: bootstrap.formats || listFormats(), layoutFields: listLayoutFields(), access: { receitaConfigured: Boolean(config().receita), apiConfigured: Boolean(require('../keyfile').getApiCredentials()?.c6?.clientId && require('../keyfile').getApiCredentials()?.im?.clientId), bqConfigured, bqLoginMode, bqAutoLogin: store.get('flow_bq_auto_login') !== false, bqAuth: login.status(state.currentUser.username) }, limits: { maxRows: MAX_ROWS } };
    }));
    ipcMain.handle('flows-save-layout', protect(input => ({ layout: manager.saveLayout(input) })));
    ipcMain.handle('flows-receita-options', protect(async input => {
        const owner = state.currentUser.username;
        const result = await receitaOptions.load(input);
        if (state.currentUser?.username !== owner || state.currentUser?.role !== 'admin') throw new Error('A sessão mudou. Reabra a geração de listas.');
        return result;
    }));
    ipcMain.handle('flows-delete-layout', protect(id => { manager.deleteLayout(id); return {}; }));
    ipcMain.handle('flows-preview-layout', protect(input => ({ preview: manager.previewLayout(input) })));
    ipcMain.handle('flows-save', protect(input => ({ flow: manager.save(input) })));
    ipcMain.handle('flows-delete', protect(id => { manager.delete(id); return {}; }));
    ipcMain.handle('flows-select-folder', protect(async () => {
        const result = await dialog.showOpenDialog(state.mainWindow, { title: 'Pasta para listas prontas', properties: ['openDirectory', 'createDirectory'] });
        if (result.canceled || !result.filePaths.length) return { cancelled: true };
        selectedDirectories.set(state.currentUser.username, result.filePaths[0]);
        return { path: result.filePaths[0] };
    }));
    ipcMain.handle('flows-start', protect(input => {
        if (login.isBusy()) throw new Error('Conclua o login Google antes de iniciar uma nova execução.');
        if (require('./limpeza').isCleaning()) throw new Error('Aguarde a limpeza local terminar antes de iniciar um fluxo.');
        if (selectedDirectories.get(state.currentUser.username) !== input?.outputDirectory) throw new Error('Selecione a pasta de saída pelo aplicativo.');
        return { job: manager.start(input) };
    }));
    ipcMain.handle('flows-resume', protect(id => { if (login.isBusy()) throw new Error('Conclua o login Google antes de retomar a execução.'); if (require('./limpeza').isCleaning()) throw new Error('Aguarde a limpeza local terminar.'); return { job: manager.resume(id) }; }));
    ipcMain.handle('flows-cancel', protect(id => { manager.cancel(id); return {}; }));
    ipcMain.handle('flows-open-output', protect(async input => { const error = await shell.openPath(manager.output(input.jobId, input.path)); if (error) throw new Error('Não foi possível abrir o arquivo.'); return {}; }));
    ipcMain.handle('flows-configure-receita', protect(async input => {
        if (manager.isBusy()) throw new Error('Aguarde ou cancele o fluxo antes de mudar o acesso.');
        const connectionString = input?.connectionString;
        if (typeof connectionString !== 'string' || connectionString.length > 4096 || !/^postgres(ql)?:\/\//i.test(connectionString)) throw new Error('Informe uma conexão PostgreSQL válida da base Receita.');
        const pool = new Pool(readOnlyPoolOptions(connectionString, { max: 1, timeout: 15000, connectionTimeout: 10000 }));
        try { await getReceitaMetadata(pool); store.set('receita_connection_string', connectionString); return { message: 'Fonte Receita verificada e salva nesta máquina.' }; }
        catch (error) {
            if (error.code === 'FLOW_VALIDATION') throw error;
            throw new Error('Não foi possível validar a fonte Receita. Confira acesso e schema de empresas.');
        }
        finally { await pool.end(); }
    }));
    ipcMain.handle('flows-configure-bq', protect(async () => {
        if (manager.isBusy() || login.isBusy()) throw new Error('Aguarde ou cancele o fluxo e conclua o login Google antes de mudar o acesso.');
        const result = await dialog.showOpenDialog(state.mainWindow, { title: 'Importar credencial BQ privada desta máquina', properties: ['openFile'], filters: [{ name: 'Credencial Google', extensions: ['json'] }] });
        if (result.canceled || !result.filePaths.length) return { cancelled: true };
        const file = result.filePaths[0];
        if (fs.statSync(file).size > 100000) throw new Error('Arquivo de credencial inválido.');
        try { existingCredentials(file); } catch { throw new Error('Use uma credencial Google válida, fora do instalador.'); }
        await createBqClient({ keyFile: file }).test();
        const directory = path.join(app.getPath('userData'), 'private'); await fs.promises.mkdir(directory, { recursive: true });
        const destination = path.join(directory, 'flow-bq-access.json');
        await fs.promises.copyFile(file, destination + '.tmp'); await fs.promises.rename(destination + '.tmp', destination);
        store.set('flow_bq_key_file', destination);
        return { message: 'Credencial BQ importada e validada nesta máquina.' };
    }));
    ipcMain.handle('flows-test-bq', protect(() => queryBq(client => client.test())));
    ipcMain.handle('flows-renew-bq', protect(() => {
        if (manager.isBusy()) throw new Error('Aguarde ou cancele o fluxo antes de renovar o login Google manualmente.');
        return login.renew(state.currentUser.username);
    }));
    ipcMain.handle('flows-bq-auto-login', protect(input => {
        if (typeof input?.enabled !== 'boolean') throw new Error('Informe se a renovação automática deve ficar ativa.');
        store.set('flow_bq_auto_login', input.enabled);
        return { enabled: input.enabled };
    }));
    ipcMain.on('logout', () => { manager.cancelActive(); login.cancel(); });
    app.on('before-quit', () => { manager.cancelActive(); login.cancel(); void receitaOptions.close().catch(() => {}); });
}
module.exports = { register, readRootFile };
