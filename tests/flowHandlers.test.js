const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const vm = require('node:vm');
const { createRequire } = require('node:module');
const ExcelJS = require('exceljs');
function fixture(t, environment = {}) {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'flow-handler-'));
    t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
    const handlers = new Map(), events = new Map(), settings = new Map(), pools = [], dialogPaths = [];
    const sender = { send() {} }, state = { currentUser: { username: 'Davi', role: 'admin' }, mainWindow: { webContents: sender } };
    let options, cleaning = false, bqError = null, renewals = 0;
    let loadOptions = async () => ({ options: [{ value: 'SP', label: 'SP' }], hasMore: false });
    const manager = { bootstrap: () => ({ user: state.currentUser, flows: [], jobs: [] }), isBusy: () => false,
        save: input => input, delete() {}, start: input => ({ id: 'synthetic', input }), resume: id => ({ id }), cancel() {}, cancelActive() {}, output: () => { throw new Error('Arquivo não pertence'); } };
    class Store { get(key) { return settings.get(key); } set(key, value) { settings.set(key, value); } }
    class Pool { constructor(config) { this.config = config; pools.push(this); } async end() { this.ended = true; } }
    const filename = path.resolve('src/main/handlers/fluxos.js'), actual = createRequire(filename), module = { exports: {} };
    const mocks = { electron: { ipcMain: { handle: (name, fn) => handlers.set(name, fn), on: (name, fn) => events.set(name, fn) }, app: { getPath: () => directory, on() {} }, shell: { openPath: async () => '' }, dialog: { showOpenDialog: async () => ({ canceled: !dialogPaths.length, filePaths: dialogPaths.splice(0) }) } },
        'electron-store': Store, pg: { Pool }, '../state': state, './limpeza': { isCleaning: () => cleaning },
        '../flows/manager': { createFlowManager: value => { options = value; return manager; } },
        '../flows/receita': { getReceitaMetadata: async pool => { assert.ok(pool.config.connectionString); return {}; } },
        '../flows/receitaOptions': { createReceitaOptions: () => ({ load: input => loadOptions(input), close: async () => {} }) },
        '../flows/bqAuth': { ...actual('../flows/bqAuth'), loginModeFor: () => 'gcloud', createBqLogin: options => actual('../flows/bqAuth').createBqLogin({ ...options, runner: async () => { renewals++; bqError = null; } }) },
        '../flows/bq': { ...actual('../flows/bq'), existingCredentials: () => null, createBqClient: () => ({ test: async () => { if (bqError) throw bqError; return { success: true }; }, loadRoot: async () => { if (bqError) throw bqError; return { documents: [], info: { source: 'bq' } }; } }) } };
    vm.runInNewContext(fs.readFileSync(filename, 'utf8'), { require: name => Object.hasOwn(mocks, name) ? mocks[name] : actual(name), module, exports: module.exports, process: { platform: process.platform, env: { RECEITA_DATABASE_URL: 'postgresql://synthetic/environment', ...environment } }, Buffer }, { filename });
    module.exports.register();
    return { directory, handlers, settings, pools, sender, state, dialogPaths, exported: module.exports, options: () => options,
        invoke: (name, value, from = sender) => handlers.get(name)({ sender: from }, value), setCleaning: value => { cleaning = value; },
        failBq: code => { bqError = Object.assign(new Error('Acesso BQ precisa ser verificado.'), { code }); }, renewals: () => renewals,
        setOptionsLoader: loader => { loadOptions = loader; } };
}
test('every flow IPC rejects unauthorized role and foreign renderer before action', async t => {
    const f = fixture(t);
    for (const [name] of f.handlers) { const response = await f.invoke(name, {}, {}); assert.equal(response.success, false, name); assert.match(response.message, /Acesso negado/); }
    for (const role of ['limited', 'master']) { f.state.currentUser.role = role; for (const [name] of f.handlers) assert.equal((await f.invoke(name, {})).success, false, name); }
});
test('native folder selection gates start and manual cleaning excludes flow start/resume', async t => {
    const f = fixture(t);
    assert.equal((await f.invoke('flows-start', { flowId: 'x', outputDirectory: f.directory })).success, false);
    f.dialogPaths.push(f.directory); assert.equal((await f.invoke('flows-select-folder')).path, f.directory);
    assert.equal((await f.invoke('flows-start', { flowId: 'x', outputDirectory: f.directory })).success, true);
    f.setCleaning(true); assert.equal((await f.invoke('flows-start', { flowId: 'x', outputDirectory: f.directory })).success, false);
    assert.equal((await f.invoke('flows-resume', 'job')).success, false);
    assert.equal((await f.invoke('flows-configure-bq')).success, false);
    assert.equal((await f.invoke('flows-renew-bq')).success, false);
    assert.equal((await f.invoke('flows-open-output', { jobId: 'x', path: 'else' })).success, false);
});

test('cache folder selection is native, remembered and separate from historical records', async t => {
    const f = fixture(t);
    assert.equal(f.options().getCacheDirectory(), path.join(f.directory, 'flows'));
    assert.equal((await f.invoke('flows-select-cache-folder')).cancelled, true);
    f.dialogPaths.push(f.directory);
    const selected = await f.invoke('flows-select-cache-folder');
    assert.equal(selected.success, true);
    assert.equal(selected.path, path.join(f.directory, 'Gerenciador-cache'));
    assert.equal(f.options().getCacheDirectory(), selected.path);
    assert.ok(f.options().getCacheDirectories().includes(path.join(f.directory, 'flows')));
    assert.equal(f.options().baseDirectory, path.join(f.directory, 'flows'));
});
test('Receita access validates before saving, replaces environment fallback and never returns secret', async t => {
    const f = fixture(t), uri = 'postgresql://synthetic/selected';
    assert.equal((await f.invoke('flows-configure-receita', { connectionString: 'invalid' })).success, false); assert.equal(f.pools.length, 0);
    const result = await f.invoke('flows-configure-receita', { connectionString: uri });
    assert.equal(result.success, true); assert.equal(f.settings.get('receita_connection_string'), uri); assert.equal(f.pools[0].ended, true);
    assert.equal(f.options().resolveConnections({ enrichment: { enabled: false }, cleaning: { blocklist: false, enabled: false } }).receita, uri);
    const boot = await f.invoke('flows-bootstrap'); assert.equal(boot.access.receitaConfigured, true); assert.ok(!JSON.stringify(boot).includes(uri));
});

test('flows use the Neon login connection before environment defaults while Receita stays separate', async t => {
    const f = fixture(t, { DATABASE_URL: 'postgresql://synthetic/environment-neon' });
    f.settings.set('db_connection_string', 'postgresql://synthetic/login-neon');
    const flow = { enrichment: { enabled: true }, cleaning: { blocklist: true, enabled: true, invalidPhones: true } };
    assert.equal(f.options().resolveConnections(flow).enrichment, 'postgresql://synthetic/login-neon');
    assert.equal(f.options().resolveConnections(flow).receita, 'postgresql://synthetic/environment');
    f.state.pool = { options: { connectionString: 'postgresql://synthetic/active-login' } };
    assert.equal(f.options().resolveConnections(flow).enrichment, 'postgresql://synthetic/active-login');
    const bootstrap = await f.invoke('flows-bootstrap');
    assert.equal(bootstrap.access.neonConfigured, true);
    assert.ok(!JSON.stringify(bootstrap).includes('postgresql://'));
});

test('Receita options IPC returns bounded selections and discards results after the session changes', async t => {
    const f = fixture(t);
    const result = await f.invoke('flows-receita-options', { field: 'uf' });
    assert.equal(result.success, true); assert.equal(result.options[0].value, 'SP');
    let finish;
    f.setOptionsLoader(() => new Promise(resolve => { finish = resolve; }));
    const pending = f.invoke('flows-receita-options', { field: 'uf' });
    f.state.currentUser = { username: 'Outro', role: 'admin' };
    finish({ options: [{ value: 'SP', label: 'SP' }] });
    const stale = await pending;
    assert.equal(stale.success, false); assert.equal(stale.options, undefined); assert.match(stale.message, /sessão mudou/);
});
test('CSV root reader preserves leading zeros and validates headers/empty/cancel', async t => {
    const f = fixture(t), file = path.join(f.directory, 'root.csv');
    fs.writeFileSync(file, '\uFEFF CNPJ ;Nome\n04.252.011/0001-10;Sintética\n4252011000110;Repetida\n0000000000000;Inválida');
    assert.deepEqual(Array.from((await f.exported.readRootFile(file)).documents), ['04252011000110']);
    fs.writeFileSync(file, 'Nome;Fone\nSintética;11912345678'); await assert.rejects(f.exported.readRootFile(file), /coluna/);
    fs.writeFileSync(file, 'CNPJ\n00000000000000'); await assert.rejects(f.exported.readRootFile(file), /sem documentos/);
    const controller = new AbortController(); controller.abort(); fs.writeFileSync(file, 'CNPJ\n04252011000110'); await assert.rejects(f.exported.readRootFile(file, controller.signal), /cancelada/);
});
test('XLSX root streams first sheet only and keeps CNPJ text intact', async t => {
    const f = fixture(t), file = path.join(f.directory, 'root.xlsx');
    const workbook = new ExcelJS.Workbook(), sheet = workbook.addWorksheet('Raiz'); sheet.addRow(['CNPJ']); sheet.addRow(['04252011000110']);
    const second = workbook.addWorksheet('Other'); second.addRow(['Name']); await workbook.xlsx.writeFile(file);
    assert.deepEqual(Array.from((await f.exported.readRootFile(file)).documents), ['04252011000110']);
});
test('only authentication expiry auto-opens renewal, preference persists and manual renewal works', async t => {
    const f = fixture(t);
    f.failBq('BQ_FORBIDDEN');
    assert.equal((await f.invoke('flows-test-bq')).code, 'BQ_FORBIDDEN');
    assert.equal(f.renewals(), 0);
    assert.equal((await f.invoke('flows-bq-auto-login', { enabled: 'true' })).success, false);
    await f.invoke('flows-bq-auto-login', { enabled: false });
    assert.equal((await f.invoke('flows-bootstrap')).access.bqAutoLogin, false);
    f.failBq('BQ_AUTH_REQUIRED'); await f.invoke('flows-test-bq');
    assert.equal(f.renewals(), 0);
    await f.invoke('flows-bq-auto-login', { enabled: true });
    const expired = await f.invoke('flows-test-bq');
    assert.equal(expired.success, false); assert.equal(expired.code, 'BQ_AUTH_REQUIRED');
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(f.renewals(), 1);
    assert.equal((await f.invoke('flows-bootstrap')).access.bqAuth.state, 'ready');
    assert.equal((await f.invoke('flows-renew-bq')).success, true);
    assert.equal(f.renewals(), 2);
});
test('cancelled root lookup never launches a new Google login', async t => {
    const f = fixture(t);
    f.failBq('BQ_AUTH_REQUIRED');
    const controller = new AbortController(); controller.abort();
    await assert.rejects(f.options().resolveRoot({ pipelines: [90], cleaning: { enabled: true, rootSource: 'bq' } }, { signal: controller.signal }), error => error.code === 'BQ_AUTH_REQUIRED');
    assert.equal(f.renewals(), 0);
});
