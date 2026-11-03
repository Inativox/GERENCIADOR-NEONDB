const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), os = require('node:os'), path = require('node:path'), vm = require('node:vm');
const { createRequire } = require('node:module');
const TYPED = { login: 'ana.fastway', password: 'synthetic-own' }, COMPUTER = { login: 'computador', password: 'synthetic-computer' };
const CSV = 'nome;cpf;fone1\nAna;123;11999990000\nBia;456;11999990001\n';
const safeStorage = available => ({
    isEncryptionAvailable: () => available,
    encryptString: text => Buffer.from(`sealed:${Buffer.from(text).toString('hex')}`),
    decryptString: buffer => Buffer.from(buffer.toString().slice(7), 'hex').toString(),
});
function fixture(t, { canSave = true } = {}) {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'mailing-ipc-')); t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
    const handlers = new Map(), uploads = [], updates = [], dialogs = [];
    const sender = { send: (channel, payload) => updates.push({ channel, payload: structuredClone(payload) }) };
    const state = { currentUser: { username: 'Ana', role: 'limited' }, mainWindow: { webContents: sender } };
    const stored = {};
    class Store { get(key, fallback) { return Object.hasOwn(stored, key) ? structuredClone(stored[key]) : fallback; } set(key, value) { stored[key] = structuredClone(value); } }
    const write = (name, content = CSV) => { const file = path.join(directory, name); fs.writeFileSync(file, content); return file; };
    let chosen = [write('mailing.csv')], upload = async input => ({ rows: 2, file: path.basename(input.file) });
    const filename = path.resolve('src/main/handlers/importarMailing.js'), actual = createRequire(filename), module = { exports: {} };
    // O login antigo do arquivo de acesso e o login geral do computador ficam disponíveis para provar que não são usados.
    const logins = actual('../fastwayLogins').createFastwayLogins({ store: new Store(), safeStorage: safeStorage(canSave) });
    const showOpenDialog = async (_window, options) => { dialogs.push(options); return { canceled: !chosen.length, filePaths: chosen }; };
    const mocks = { electron: { ipcMain: { handle: (name, handler) => handlers.set(name, handler) }, dialog: { showOpenDialog } },
        '../runtimeFastwayLogins': logins, '../state': state,
        '../runtimeConfig': { loadUsers: () => ({ Ana: { password: 'x', role: 'limited', fastway: COMPUTER } }), fastwayStatus: () => true, getFastwayCredentials: () => COMPUTER },
        '../flows/fastway': { ...actual('../flows/fastway'), upload: input => { uploads.push(input); return upload(input); } } };
    vm.runInNewContext(fs.readFileSync(filename, 'utf8'), { require: name => mocks[name] || actual(name), module, process, __dirname: path.dirname(filename) }, { filename });
    module.exports.register();
    return { handlers, uploads, updates, dialogs, state, directory, stored, sender,
        invoke: (name, input, origin = sender) => handlers.get(name)({ sender: origin }, input),
        choose: (...files) => { chosen = files.map(([name, content]) => write(name, content)); },
        setUpload: value => { upload = value; },
        settle: async () => { for (let n = 0; n < 200 && state.mailingUpload; n++) await tick(); } };
}
const plain = value => JSON.parse(JSON.stringify(value));
const tick = () => new Promise(setImmediate);
const add = (f, serviceId) => f.invoke('mailing-import-add', { serviceId });
const statuses = queue => plain(queue.items.map(item => [item.name, item.service.id, item.status]));
async function sendAll(f, fastway) {
    const started = await f.invoke('mailing-import-start', fastway === undefined ? {} : { fastway });
    await f.settle();
    return started;
}
// Cada envio fica pendente até o teste responder por ele.
function holdUploads(f) {
    const pending = [];
    f.setUpload(input => new Promise((resolve, reject) => pending.push({ input, resolve, reject })));
    return pending;
}
const refusal = (message, extra = {}) => Object.assign(new Error(message), { code: 'FLOW_VALIDATION', ...extra });

test('importação exige usuário logado na janela principal, mas não exige admin', async t => {
    const f = fixture(t);
    for (const [name] of f.handlers) assert.equal((await f.invoke(name, {}, {})).success, false);
    const state = await f.invoke('mailing-import-state');
    assert.equal(state.success, true); assert.equal(state.admin, false);
    assert.deepEqual(state.fastway, { saved: false, login: null, canSave: true });
    assert.deepEqual(plain(state.queue), { running: false, items: [] });
    assert.ok(state.services.some(service => service.id === 139 && service.name === '213 - MP'));
    f.state.currentUser = null; for (const [name] of f.handlers) assert.equal((await f.invoke(name, {})).success, false);
    assert.equal(f.uploads.length, 0); assert.equal(f.dialogs.length, 0);
});

test('adicionar à fila exige um serviço da lista antes de abrir a seleção de arquivos', async t => {
    const f = fixture(t);
    for (const serviceId of [undefined, 999, '63']) assert.match((await add(f, serviceId)).message, /serviço/);
    assert.equal(f.dialogs.length, 0);
});

test('CSV inválido fica fora da fila com o motivo, e os válidos da mesma seleção entram', async t => {
    const f = fixture(t);
    f.choose(['sem-cpf.csv', 'nome;fone1\nAna;11999990000\n'], ['planilha.xlsx', CSV], ['boa.csv', 'nome;cpf;fone1\nAna;123;11999990000\n']);
    const added = await add(f, 63);
    assert.equal(added.success, true);
    assert.deepEqual(plain(f.dialogs[0].properties), ['openFile', 'multiSelections']);
    assert.deepEqual(plain(added.rejected.map(item => item.name)), ['sem-cpf.csv', 'planilha.xlsx']);
    assert.match(added.rejected[0].message, /cpf/); assert.match(added.rejected[1].message, /\.csv/);
    assert.deepEqual(plain(added.queue.items.map(item => [item.name, item.rows, item.service.id, item.status])), [['boa.csv', 1, 63, 'waiting']]);
    assert.equal(f.uploads.length, 0);
});

test('a fila sobe uma lista por vez, na ordem, cada uma para o seu serviço', async t => {
    const f = fixture(t);
    f.choose(['a.csv']); await add(f, 63);
    f.choose(['b.csv']); await add(f, 87);
    const pending = holdUploads(f);
    const started = await f.invoke('mailing-import-start', { fastway: { ...TYPED, login: '  ana.fastway  ' } });
    assert.equal(started.success, true); assert.equal(started.queue.running, true);
    await tick();
    assert.equal(pending.length, 1, 'a segunda só começa depois da primeira');
    assert.ok(f.state.mailingUpload, 'o app sabe que há envio em andamento');
    assert.deepEqual(statuses((await f.invoke('mailing-import-state')).queue), [['a.csv', 63, 'running'], ['b.csv', 87, 'waiting']]);
    pending[0].resolve({ rows: 2 }); await tick();
    assert.equal(pending.length, 2);
    pending[1].resolve({ rows: 2 }); await f.settle();
    const { queue } = await f.invoke('mailing-import-state');
    assert.deepEqual(statuses(queue), [['a.csv', 63, 'completed'], ['b.csv', 87, 'completed']]);
    assert.equal(queue.running, false); assert.equal(f.state.mailingUpload, null);
    assert.match(queue.items[1].message, /2 registros em 87 · PAG PRINCIPAL/);
    assert.deepEqual(plain(f.uploads.map(({ file, campaignId, credentials }) => [path.basename(file), campaignId, credentials])), [['a.csv', '63', TYPED], ['b.csv', '87', TYPED]]);
});

test('lista adicionada durante o envio entra no fim da fila e sobe na sequência', async t => {
    const f = fixture(t); f.choose(['a.csv']); await add(f, 63);
    const pending = holdUploads(f);
    await f.invoke('mailing-import-start', { fastway: TYPED }); await tick();
    f.choose(['b.csv']);
    assert.deepEqual(statuses((await add(f, 18)).queue), [['a.csv', 63, 'running'], ['b.csv', 18, 'waiting']]);
    pending[0].resolve({ rows: 2 }); await tick();
    assert.equal(pending.length, 2); assert.equal(path.basename(pending[1].input.file), 'b.csv');
    pending[1].resolve({ rows: 2 }); await f.settle();
    assert.equal(f.state.mailingUpload, null);
});

test('falha numa lista fica marcada com o motivo e a fila segue para a próxima', async t => {
    const f = fixture(t);
    for (const [name, service] of [['a.csv', 63], ['b.csv', 87], ['c.csv', 18]]) { f.choose([name]); await add(f, service); }
    f.setUpload(async input => {
        if (input.file.endsWith('a.csv')) throw refusal('A Fastway respondeu HTTP 500. Confirme no painel.');
        if (input.file.endsWith('b.csv')) throw new Error('socket hang up');
        return { rows: 2 };
    });
    await sendAll(f, TYPED);
    const { queue } = await f.invoke('mailing-import-state');
    assert.deepEqual(statuses(queue), [['a.csv', 63, 'failed'], ['b.csv', 87, 'failed'], ['c.csv', 18, 'completed']]);
    assert.match(queue.items[0].message, /HTTP 500/);
    assert.match(queue.items[1].message, /painel da Fastway/);
    assert.equal(f.state.mailingUpload, null);
});

test('login recusado pela Fastway para a fila e deixa as próximas esperando', async t => {
    const f = fixture(t); f.choose(['a.csv']); await add(f, 63); f.choose(['b.csv']); await add(f, 87);
    f.setUpload(async () => { throw refusal('A Fastway recusou o login (HTTP 481). O arquivo não foi importado.', { loginRejected: true }); });
    await sendAll(f, { ...TYPED, save: true });
    const { queue, fastway } = await f.invoke('mailing-import-state');
    assert.deepEqual(statuses(queue), [['a.csv', 63, 'failed'], ['b.csv', 87, 'waiting']]);
    assert.match(queue.items[0].message, /HTTP 481.*a fila parou/i);
    assert.equal(queue.running, false); assert.equal(f.uploads.length, 1);
    assert.equal(fastway.saved, false);
});

test('enviar exige lista esperando na fila e recusa um segundo início durante o envio', async t => {
    const f = fixture(t);
    assert.match((await f.invoke('mailing-import-start', { fastway: TYPED })).message, /Adicione/);
    f.choose(['a.csv']); await add(f, 63);
    const pending = holdUploads(f);
    assert.equal((await f.invoke('mailing-import-start', { fastway: TYPED })).success, true);
    const second = await f.invoke('mailing-import-start', { fastway: TYPED });
    assert.equal(second.success, false); assert.match(second.message, /enviando/);
    pending[0].resolve({ rows: 2 }); await f.settle();
    assert.equal(f.uploads.length, 1);
});

test('tira da fila listas que não estão subindo e limpa as concluídas', async t => {
    const f = fixture(t);
    for (const [name, service] of [['a.csv', 63], ['b.csv', 87], ['c.csv', 18]]) { f.choose([name]); await add(f, service); }
    const pending = holdUploads(f);
    await f.invoke('mailing-import-start', { fastway: TYPED }); await tick();
    const [a, b] = (await f.invoke('mailing-import-state')).queue.items;
    assert.match((await f.invoke('mailing-import-remove', { itemId: a.id })).message, /sendo enviada/);
    assert.deepEqual(statuses((await f.invoke('mailing-import-remove', { itemId: b.id })).queue), [['a.csv', 63, 'running'], ['c.csv', 18, 'waiting']]);
    pending[0].resolve({ rows: 2 }); await tick();
    pending[1].resolve({ rows: 2 }); await f.settle();
    assert.deepEqual(plain(f.uploads.map(upload => path.basename(upload.file))), ['a.csv', 'c.csv']);
    f.choose(['d.csv']); await add(f, 63);
    assert.deepEqual(statuses((await f.invoke('mailing-import-clear')).queue), [['d.csv', 63, 'waiting']]);
});

test('a mesma lista não entra duas vezes para o mesmo serviço', async t => {
    const f = fixture(t); f.choose(['a.csv']);
    await add(f, 63);
    assert.match((await add(f, 63)).rejected[0].message, /já está na fila/);
    assert.deepEqual(statuses((await add(f, 87)).queue), [['a.csv', 63, 'waiting'], ['a.csv', 87, 'waiting']]);
});

test('a fila aceita até 50 listas', async t => {
    const f = fixture(t);
    f.choose(...Array.from({ length: 51 }, (_, index) => [`lista-${index + 1}.csv`]));
    const added = await add(f, 63);
    assert.equal(added.queue.items.length, 50);
    assert.deepEqual(plain(added.rejected.map(item => item.name)), ['lista-51.csv']);
    assert.match(added.rejected[0].message, /50 listas/);
});

test('a tela recebe cada mudança da fila, sem o caminho dos arquivos', async t => {
    const f = fixture(t); f.choose(['a.csv']); await add(f, 63);
    await sendAll(f, TYPED);
    const updates = f.updates.filter(update => update.channel === 'mailing-import-update').map(update => update.payload);
    assert.deepEqual([updates[0].queue.running, updates[0].queue.items[0].status], [true, 'running']);
    assert.deepEqual([updates.at(-1).queue.running, updates.at(-1).queue.items[0].status], [false, 'completed']);
    const escaped = JSON.stringify(f.directory).slice(1, -1);
    assert.equal(JSON.stringify([updates, await f.invoke('mailing-import-state')]).includes(escaped), false);
});

test('janela fechada no meio da fila não interrompe o envio das próximas', async t => {
    const f = fixture(t); f.choose(['a.csv']); await add(f, 63); f.choose(['b.csv']); await add(f, 87);
    const pending = holdUploads(f);
    await f.invoke('mailing-import-start', { fastway: TYPED }); await tick();
    f.sender.isDestroyed = () => true;
    f.sender.send = () => { throw new Error('Object has been destroyed'); };
    pending[0].resolve({ rows: 2 }); await tick();
    pending[1].resolve({ rows: 2 }); await f.settle();
    assert.deepEqual(plain(f.uploads.map(upload => path.basename(upload.file))), ['a.csv', 'b.csv']);
    assert.equal(f.state.mailingUpload, null);
});

test('sem login digitado nem salvo não envia, nem com o login do arquivo de acesso ou do computador', async t => {
    const f = fixture(t); await add(f, 63);
    const sent = await sendAll(f);
    assert.equal(sent.success, false); assert.match(sent.message, /login e a senha da Fastway/);
    assert.equal(f.uploads.length, 0);
    assert.deepEqual(statuses((await f.invoke('mailing-import-state')).queue), [['mailing.csv', 63, 'waiting']]);
});

test('login digitado incompleto é recusado antes de chegar na Fastway', async t => {
    const f = fixture(t); await add(f, 63);
    for (const fastway of [{ login: 'ana.fastway', password: '' }, { login: '   ', password: 'x' }, { login: 'a'.repeat(201), password: 'x' }, 'ana:senha']) {
        const sent = await sendAll(f, fastway);
        assert.equal(sent.success, false); assert.match(sent.message, /login e a senha da Fastway/);
    }
    assert.equal(f.uploads.length, 0);
});

test('sem pedir para salvar, o login vale só para aquela fila', async t => {
    const f = fixture(t); await add(f, 63);
    assert.equal((await sendAll(f, TYPED)).success, true);
    assert.deepEqual((await f.invoke('mailing-import-state')).fastway, { saved: false, login: null, canSave: true });
    assert.deepEqual(f.stored, {});
});

test('salvar só guarda o login depois que a Fastway confirma, e a senha não volta para a tela', async t => {
    const f = fixture(t); await add(f, 63);
    f.setUpload(async () => { throw refusal('A Fastway respondeu HTTP 500.'); });
    await sendAll(f, { ...TYPED, save: true });
    assert.equal((await f.invoke('mailing-import-state')).fastway.saved, false);
    f.setUpload(async () => ({ rows: 2 }));
    await add(f, 87);
    await sendAll(f, { ...TYPED, save: true });
    const state = await f.invoke('mailing-import-state');
    assert.deepEqual(state.fastway, { saved: true, login: 'ana.fastway', canSave: true });
    assert.deepEqual(f.updates.at(-1).payload.fastway, { saved: true, login: 'ana.fastway', canSave: true });
    assert.equal(JSON.stringify([state, f.updates, f.stored]).includes(TYPED.password), false);
});

test('login salvo é usado quando nada é digitado, e o digitado tem prioridade', async t => {
    const f = fixture(t);
    await add(f, 63); await sendAll(f, { ...TYPED, save: true });
    await add(f, 87); assert.equal((await sendAll(f)).success, true);
    const other = { login: 'ana.outro', password: 'synthetic-other' };
    await add(f, 18); assert.equal((await sendAll(f, other)).success, true);
    assert.deepEqual(plain(f.uploads.map(upload => upload.credentials)), [TYPED, TYPED, other]);
    assert.equal((await f.invoke('mailing-import-state')).fastway.login, 'ana.fastway', 'usar outro login sem salvar não troca o salvo');
});

test('esquecer apaga o login salvo, que nunca aparece para outro usuário', async t => {
    const f = fixture(t); await add(f, 63);
    await sendAll(f, { ...TYPED, save: true });
    f.state.currentUser = { username: 'Bia', role: 'admin' };
    assert.deepEqual((await f.invoke('mailing-import-state')).fastway, { saved: false, login: null, canSave: true });
    f.state.currentUser = { username: 'Ana', role: 'limited' };
    const forgot = await f.invoke('mailing-import-forget');
    assert.equal(forgot.success, true); assert.deepEqual(forgot.fastway, { saved: false, login: null, canSave: true });
    assert.equal((await f.invoke('mailing-import-state')).fastway.saved, false);
});

test('sem a proteção do Windows a fila segue, mas avisa que o login não foi salvo', async t => {
    const f = fixture(t, { canSave: false }); await add(f, 63);
    assert.equal((await f.invoke('mailing-import-state')).fastway.canSave, false);
    await sendAll(f, { ...TYPED, save: true });
    const { queue } = await f.invoke('mailing-import-state');
    assert.equal(queue.items[0].status, 'completed'); assert.match(queue.items[0].message, /login não foi salvo/);
    assert.deepEqual(f.stored, {});
});

test('a fila de um usuário não aparece para outro', async t => {
    const f = fixture(t); await add(f, 63);
    f.state.currentUser = { username: 'Bia', role: 'admin' };
    const state = await f.invoke('mailing-import-state');
    assert.deepEqual(plain(state.queue.items), []); assert.equal(state.admin, true);
});
