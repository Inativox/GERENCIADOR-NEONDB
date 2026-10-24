const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), os = require('node:os'), path = require('node:path'), vm = require('node:vm');
const { createRequire } = require('node:module'), { EventEmitter } = require('node:events');
function fixture(t) {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'situacao-ipc-')); t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
    const handlers = new Map(), workers = [], opened = [], sent = [];
    const sender = { isDestroyed: () => false, send: (_channel, value) => sent.push(value) }, state = { currentUser: { username: 'Davi', role: 'admin' }, mainWindow: { webContents: sender } };
    // Google access shared by the Fluxos handler: same credential, automatic renewal on an expired login.
    const client = { synthetic: 'bq-client' }, renewals = [];
    state.bqReceitaService = { configured: () => true, client: () => client, access: () => ({ keyFile: 'C:/synthetic/key.json', project: 'mbtech-bronze' }), loginExpired: owner => renewals.push(owner) };
    let lookup = async () => new Map([['12ABC34501DE35', { cnpj: '12ABC34501DE35', situacao_cadastral_cod: '02' }]]);
    let lookupCnaes = async () => new Map();
    const pools = [];
    class Worker extends EventEmitter { constructor(file, options) { super(); this.options = options; workers.push(this); } postMessage(value) { this.lastMessage = value; } }
    class Pool { constructor(options) { this.options = options; pools.push(this); } on() {} async end() { this.ended = true; } }
    const filename = path.resolve('src/main/handlers/receitaSituacao.js'), actual = createRequire(filename), module = { exports: {} };
    const mocks = { electron: { app: { on() {} }, ipcMain: { handle: (name, handler) => handlers.set(name, handler), on() {} }, dialog: { showOpenDialog: async () => ({ canceled: false, filePaths: [path.join(directory, 'lista.xlsx')] }) }, shell: { openPath: async value => { opened.push(value); return ''; } } },
        '../state': state, pg: { Pool }, 'node:worker_threads': { Worker }, 'electron-store': class { get() { return 'postgresql://synthetic/private'; } },
        '../receitaSituacao': { ...actual('../receitaSituacao'), lookup: (...args) => lookup(...args), lookupCnaes: (...args) => lookupCnaes(...args) } };
    vm.runInNewContext(fs.readFileSync(filename, 'utf8'), { require: name => mocks[name] || actual(name), module, __dirname: path.dirname(filename), process: { env: {} } }, { filename });
    module.exports.register();
    return { handlers, workers, opened, sent, state, pools, client, renewals, invoke: (name, input, origin = sender) => handlers.get(name)({ sender: origin }, input), setLookup: value => { lookup = value; }, setLookupCnaes: value => { lookupCnaes = value; } };
}
test('all situation IPCs enforce role and originating renderer before reading data', async t => {
    const f = fixture(t); for (const [name] of f.handlers) assert.equal((await f.invoke(name, {}, {})).success, false);
    f.state.currentUser.role = 'limited'; for (const [name] of f.handlers) assert.equal((await f.invoke(name, {})).success, false);
    assert.equal(f.workers.length, 0);
});
test('file selection authorizes one worker, only confirmed output opens, and state is isolated by owner', async t => {
    const f = fixture(t); assert.equal((await f.invoke('receita-situacao-start', { fileId: 'unselected' })).success, false);
    const selected = await f.invoke('receita-situacao-file');
    assert.equal((await f.invoke('receita-situacao-start', { fileId: selected.file.id })).success, true);
    assert.equal((await f.invoke('receita-situacao-start', { fileId: selected.file.id })).success, false);
    assert.equal((await f.invoke('receita-situacao-open')).success, false);
    assert.equal(f.workers[0].options.resourceLimits.maxOldGenerationSizeMb, 256);
    f.workers[0].emit('message', { type: 'result', result: { processed: 2, found: 1, notFound: 1, invalid: 0, output: 'synthetic.xlsx' } });
    assert.equal((await f.invoke('receita-situacao-open')).success, true); assert.deepEqual(f.opened, ['synthetic.xlsx']);
    f.state.currentUser.username = 'Outro'; assert.equal((await f.invoke('receita-situacao-state')).job, null);
    assert.equal((await f.invoke('receita-situacao-open')).success, false);
});
test('remove MEI and additional CSV reach the worker only when switched on, and the result carries both', async t => {
    const f = fixture(t); let selected = await f.invoke('receita-situacao-file');
    assert.equal((await f.invoke('receita-situacao-start', { fileId: selected.file.id, removeMei: 'sim', csv: 1 })).success, true);
    assert.deepEqual([f.workers[0].options.workerData.removeMei, f.workers[0].options.workerData.csv], [false, false]);
    f.workers[0].emit('message', { type: 'result', result: { processed: 2, found: 2, notFound: 0, invalid: 0, removed: 0, mei: 0, output: 'a.xlsx', csvOutput: null } });
    selected = await f.invoke('receita-situacao-file');
    const started = await f.invoke('receita-situacao-start', { fileId: selected.file.id, removeMei: true, csv: true });
    assert.equal(started.job.counts.mei, 0);
    assert.deepEqual([f.workers[1].options.workerData.removeMei, f.workers[1].options.workerData.csv], [true, true]);
    f.workers[1].emit('message', { type: 'result', result: { processed: 3, found: 3, notFound: 0, invalid: 0, removed: 0, mei: 1, output: 'b.xlsx', csvOutput: 'b.csv' } });
    const { job } = await f.invoke('receita-situacao-state');
    assert.equal(job.counts.mei, 1); assert.equal(job.csvOutput, 'b.csv'); assert.match(job.message, /CSV/);
});
test('manual query never returns results to a session that changed while awaiting SQL', async t => {
    const f = fixture(t); let finish;
    f.setLookup(() => new Promise(resolve => { finish = resolve; }));
    const pending = f.invoke('receita-situacao-one', { cnpj: '12ABC34501DE35' });
    f.state.currentUser.username = 'Outro'; finish(new Map([['12ABC34501DE35', { cnpj: '12ABC34501DE35' }]]));
    assert.equal((await pending).success, false); assert.equal((await pending).result, undefined);
});
test('Limpeza Local consulta CNAEs pela conexão de leitura da Receita e fecha a conexão ao terminar', async t => {
    const f = fixture(t), calls = [], onProgress = () => {};
    f.setLookupCnaes(async (pool, documents, options) => { calls.push({ pool, documents, options }); return new Map([['12345678000199', '4781400']]); });
    assert.equal(f.state.receitaCnaeService.configured(), true);
    const result = await f.state.receitaCnaeService.lookupCnaes(['12345678000199'], { onProgress });
    assert.deepEqual([...result], [['12345678000199', '4781400']]);
    assert.equal(calls[0].pool, f.pools[0]);
    assert.deepEqual(calls[0].documents, ['12345678000199']);
    assert.equal(calls[0].options.onProgress, onProgress);
    assert.match(f.pools[0].options.options, /default_transaction_read_only=on/);
    assert.equal(f.pools[0].ended, true);
});
test('falha na consulta de CNAEs também fecha a conexão da Receita', async t => {
    const f = fixture(t);
    f.setLookupCnaes(async () => { throw new Error('Não foi possível consultar o CNAE no banco da Receita.'); });
    await assert.rejects(f.state.receitaCnaeService.lookupCnaes(['12345678000199']), /consultar o CNAE/);
    assert.equal(f.pools[0].ended, true);
});
test('situação reads the Receita in BigQuery: the worker gets only the Google access, never the Neon connection', async t => {
    const f = fixture(t); const selected = await f.invoke('receita-situacao-file');
    assert.equal((await f.invoke('receita-situacao-state')).configured, true);
    assert.equal((await f.invoke('receita-situacao-start', { fileId: selected.file.id })).success, true);
    const data = f.workers[0].options.workerData;
    assert.deepEqual(data.bq, { keyFile: 'C:/synthetic/key.json', project: 'mbtech-bronze' });
    assert.equal(data.connection, undefined); assert.doesNotMatch(JSON.stringify(data), /postgresql/);
});
test('single lookup goes through the shared BigQuery client and returns the situation found', async t => {
    const f = fixture(t), calls = [];
    f.setLookup(async (bq, documents) => { calls.push({ bq, documents }); return new Map([['12ABC34501DE35', { cnpj: '12ABC34501DE35', situacao_cadastral_cod: '08' }]]); });
    const response = await f.invoke('receita-situacao-one', { cnpj: '12.abc.345/01de-35' });
    assert.equal(response.success, true); assert.equal(response.found, true); assert.equal(response.result.situacao_cadastral_cod, '08');
    assert.equal(calls[0].bq, f.client); assert.deepEqual([...calls[0].documents], ['12ABC34501DE35']);
});
test('without Google access the screen says where to sign in and starts nothing', async t => {
    const f = fixture(t); f.state.bqReceitaService.configured = () => false;
    assert.equal((await f.invoke('receita-situacao-state')).configured, false);
    const selected = await f.invoke('receita-situacao-file');
    const started = await f.invoke('receita-situacao-start', { fileId: selected.file.id });
    assert.equal(started.success, false); assert.match(started.message, /conta Google.*Fluxos/);
    assert.match((await f.invoke('receita-situacao-one', { cnpj: '12ABC34501DE35' })).message, /conta Google/);
    assert.equal(f.workers.length, 0);
});
test('stage messages of the worker reach the screen while the list is consulted', async t => {
    const f = fixture(t); const selected = await f.invoke('receita-situacao-file');
    await f.invoke('receita-situacao-start', { fileId: selected.file.id });
    f.workers[0].emit('message', { type: 'stage', message: 'Enviando a lista ao BigQuery: 50.000 CNPJs.' });
    assert.equal(f.sent.at(-1).message, 'Enviando a lista ao BigQuery: 50.000 CNPJs.');
    assert.equal((await f.invoke('receita-situacao-state')).job.status, 'running');
});
test('an expired Google login, in the list or in the single lookup, asks the Fluxos screen to renew it', async t => {
    const f = fixture(t); const selected = await f.invoke('receita-situacao-file');
    await f.invoke('receita-situacao-start', { fileId: selected.file.id });
    f.workers[0].emit('message', { type: 'error', authRequired: true, message: 'Seu login Google precisa ser renovado.' });
    assert.deepEqual(f.renewals, ['Davi']); assert.equal((await f.invoke('receita-situacao-state')).job.status, 'failed');
    f.setLookup(async () => { throw Object.assign(new Error('Seu login Google precisa ser renovado.'), { code: 'RECEITA_SITUACAO', authRequired: true }); });
    const response = await f.invoke('receita-situacao-one', { cnpj: '12ABC34501DE35' });
    assert.equal(response.success, false); assert.match(response.message, /login Google/); assert.deepEqual(f.renewals, ['Davi', 'Davi']);
});
