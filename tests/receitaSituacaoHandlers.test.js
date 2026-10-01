const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), os = require('node:os'), path = require('node:path'), vm = require('node:vm');
const { createRequire } = require('node:module'), { EventEmitter } = require('node:events');
function fixture(t) {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'situacao-ipc-')); t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
    const handlers = new Map(), workers = [], opened = [], sent = [];
    const sender = { isDestroyed: () => false, send: (_channel, value) => sent.push(value) }, state = { currentUser: { username: 'Davi', role: 'admin' }, mainWindow: { webContents: sender } };
    let lookup = async () => new Map([['12ABC34501DE35', { cnpj: '12ABC34501DE35', situacao_cadastral_cod: '02' }]]);
    class Worker extends EventEmitter { constructor(file, options) { super(); this.options = options; workers.push(this); } postMessage(value) { this.lastMessage = value; } }
    class Pool { on() {} async end() {} }
    const filename = path.resolve('src/main/handlers/receitaSituacao.js'), actual = createRequire(filename), module = { exports: {} };
    const mocks = { electron: { app: { on() {} }, ipcMain: { handle: (name, handler) => handlers.set(name, handler), on() {} }, dialog: { showOpenDialog: async () => ({ canceled: false, filePaths: [path.join(directory, 'lista.xlsx')] }) }, shell: { openPath: async value => { opened.push(value); return ''; } } },
        '../state': state, pg: { Pool }, 'node:worker_threads': { Worker }, 'electron-store': class { get() { return 'postgresql://synthetic/private'; } },
        '../receitaSituacao': { ...actual('../receitaSituacao'), lookup: (...args) => lookup(...args) } };
    vm.runInNewContext(fs.readFileSync(filename, 'utf8'), { require: name => mocks[name] || actual(name), module, __dirname: path.dirname(filename), process: { env: {} } }, { filename });
    module.exports.register();
    return { handlers, workers, opened, sent, state, invoke: (name, input, origin = sender) => handlers.get(name)({ sender: origin }, input), setLookup: value => { lookup = value; } };
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
test('manual query never returns results to a session that changed while awaiting SQL', async t => {
    const f = fixture(t); let finish;
    f.setLookup(() => new Promise(resolve => { finish = resolve; }));
    const pending = f.invoke('receita-situacao-one', { cnpj: '12ABC34501DE35' });
    f.state.currentUser.username = 'Outro'; finish(new Map([['12ABC34501DE35', { cnpj: '12ABC34501DE35' }]]));
    assert.equal((await pending).success, false); assert.equal((await pending).result, undefined);
});
