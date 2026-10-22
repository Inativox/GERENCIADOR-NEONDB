const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), os = require('node:os'), path = require('node:path'), vm = require('node:vm');
const { createRequire } = require('node:module');
const TYPED = { login: 'ana.fastway', password: 'synthetic-own' }, COMPUTER = { login: 'computador', password: 'synthetic-computer' };
const safeStorage = available => ({
    isEncryptionAvailable: () => available,
    encryptString: text => Buffer.from(`sealed:${Buffer.from(text).toString('hex')}`),
    decryptString: buffer => Buffer.from(buffer.toString().slice(7), 'hex').toString(),
});
function fixture(t, { canSave = true } = {}) {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'mailing-ipc-')); t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
    const handlers = new Map(), uploads = [], sender = {}, state = { currentUser: { username: 'Ana', role: 'limited' }, mainWindow: { webContents: sender } };
    const stored = {};
    class Store { get(key, fallback) { return Object.hasOwn(stored, key) ? structuredClone(stored[key]) : fallback; } set(key, value) { stored[key] = structuredClone(value); } }
    let chosen = path.join(directory, 'mailing.csv'), upload = async input => ({ rows: 2, file: path.basename(input.file) });
    fs.writeFileSync(chosen, 'nome;cpf;fone1\nAna;123;11999990000\nBia;456;11999990001\n');
    const filename = path.resolve('src/main/handlers/importarMailing.js'), actual = createRequire(filename), module = { exports: {} };
    // O login antigo do arquivo de acesso e o login geral do computador ficam disponíveis para provar que não são usados.
    const logins = actual('../fastwayLogins').createFastwayLogins({ store: new Store(), safeStorage: safeStorage(canSave) });
    const mocks = { electron: { ipcMain: { handle: (name, handler) => handlers.set(name, handler) }, dialog: { showOpenDialog: async () => ({ canceled: false, filePaths: [chosen] }) } },
        '../runtimeFastwayLogins': logins, '../state': state,
        '../runtimeConfig': { loadUsers: () => ({ Ana: { password: 'x', role: 'limited', fastway: COMPUTER } }), fastwayStatus: () => true, getFastwayCredentials: () => COMPUTER },
        '../flows/fastway': { ...actual('../flows/fastway'), upload: input => { uploads.push(input); return upload(input); } } };
    vm.runInNewContext(fs.readFileSync(filename, 'utf8'), { require: name => mocks[name] || actual(name), module, __dirname: path.dirname(filename) }, { filename });
    module.exports.register();
    return { handlers, uploads, state, directory, stored, invoke: (name, input, origin = sender) => handlers.get(name)({ sender: origin }, input),
        choose: (name, content) => { chosen = path.join(directory, name); fs.writeFileSync(chosen, content); }, setUpload: value => { upload = value; } };
}
const plain = value => JSON.parse(JSON.stringify(value));
test('importação exige usuário logado na janela principal, mas não exige admin', async t => {
    const f = fixture(t);
    for (const [name] of f.handlers) assert.equal((await f.invoke(name, {}, {})).success, false);
    const state = await f.invoke('mailing-import-state');
    assert.equal(state.success, true); assert.equal(state.admin, false);
    assert.deepEqual(state.fastway, { saved: false, login: null, canSave: true });
    assert.ok(state.services.some(service => service.id === 139 && service.name === '213 - MP'));
    f.state.currentUser = null; for (const [name] of f.handlers) assert.equal((await f.invoke(name, {})).success, false);
    assert.equal(f.uploads.length, 0);
});
test('CSV sem as colunas obrigatórias é recusado na seleção e não pode ser enviado', async t => {
    const f = fixture(t); f.choose('sem-cpf.csv', 'nome;fone1\nAna;11999990000\n');
    const selected = await f.invoke('mailing-import-file');
    assert.equal(selected.success, false); assert.match(selected.message, /cpf/);
    assert.equal((await f.invoke('mailing-import-send', { fileId: undefined, serviceId: 63, fastway: TYPED })).success, false);
    f.choose('planilha.xlsx', 'nome;cpf;fone1\nAna;123;1\n');
    assert.match((await f.invoke('mailing-import-file')).message, /\.csv/);
    assert.equal(f.uploads.length, 0);
});
test('envio só aceita serviço da lista, manda o ID e o login digitado e exige nova seleção para reenviar', async t => {
    const f = fixture(t), { file } = await f.invoke('mailing-import-file');
    assert.deepEqual({ name: file.name, rows: file.rows }, { name: 'mailing.csv', rows: 2 });
    assert.equal((await f.invoke('mailing-import-send', { fileId: file.id, serviceId: 999, fastway: TYPED })).success, false);
    assert.equal((await f.invoke('mailing-import-send', { fileId: file.id, serviceId: '63', fastway: TYPED })).success, false);
    assert.equal(f.uploads.length, 0);
    const sent = await f.invoke('mailing-import-send', { fileId: file.id, serviceId: 63, fastway: { ...TYPED, login: '  ana.fastway  ' } });
    assert.equal(sent.success, true); assert.equal(sent.job.status, 'completed'); assert.match(sent.job.message, /2 registros em 63 · SANTANDER/);
    assert.deepEqual(plain(f.uploads.map(({ file: uploaded, campaignId, credentials }) => ({ uploaded, campaignId, credentials }))), [{ uploaded: path.join(f.directory, 'mailing.csv'), campaignId: '63', credentials: TYPED }]);
    assert.equal((await f.invoke('mailing-import-send', { fileId: file.id, serviceId: 63, fastway: TYPED })).success, false);
    assert.equal(f.uploads.length, 1);
    assert.equal((await f.invoke('mailing-import-state')).job.status, 'completed');
});
test('durante o envio o app marca envio manual em andamento e recusa um segundo envio', async t => {
    const f = fixture(t), { file } = await f.invoke('mailing-import-file');
    let finish; f.setUpload(() => new Promise(resolve => { finish = resolve; }));
    const pending = f.invoke('mailing-import-send', { fileId: file.id, serviceId: 87, fastway: TYPED });
    await new Promise(setImmediate);
    assert.equal(f.state.mailingUpload?.service.id, 87);
    assert.equal((await f.invoke('mailing-import-state')).job.status, 'running');
    const second = await f.invoke('mailing-import-send', { fileId: file.id, serviceId: 87, fastway: TYPED });
    assert.equal(second.success, false); assert.match(second.message, /andamento/);
    finish({ rows: 2 }); assert.equal((await pending).success, true);
    assert.equal(f.state.mailingUpload, null); assert.equal(f.uploads.length, 1);
});
test('recusa da Fastway volta com a mensagem dela, libera o app e mantém o arquivo para tentar de novo', async t => {
    const f = fixture(t), { file } = await f.invoke('mailing-import-file');
    f.setUpload(async () => { throw Object.assign(new Error('A Fastway respondeu HTTP 500. Confirme no painel.'), { code: 'FLOW_VALIDATION' }); });
    const failed = await f.invoke('mailing-import-send', { fileId: file.id, serviceId: 18, fastway: TYPED });
    assert.equal(failed.success, false); assert.match(failed.message, /HTTP 500/);
    assert.equal(f.state.mailingUpload, null); assert.equal((await f.invoke('mailing-import-state')).job.status, 'failed');
    f.setUpload(async () => ({ rows: 2 }));
    assert.equal((await f.invoke('mailing-import-send', { fileId: file.id, serviceId: 18, fastway: TYPED })).success, true);
});
test('sem login digitado nem salvo não envia, nem com o login do arquivo de acesso ou do computador', async t => {
    const f = fixture(t), { file } = await f.invoke('mailing-import-file');
    const sent = await f.invoke('mailing-import-send', { fileId: file.id, serviceId: 63 });
    assert.equal(sent.success, false); assert.match(sent.message, /login e a senha da Fastway/);
    assert.equal(f.uploads.length, 0);
});
test('login digitado incompleto é recusado antes de chegar na Fastway', async t => {
    const f = fixture(t), { file } = await f.invoke('mailing-import-file');
    for (const fastway of [{ login: 'ana.fastway', password: '' }, { login: '   ', password: 'x' }, { login: 'a'.repeat(201), password: 'x' }, 'ana:senha']) {
        const sent = await f.invoke('mailing-import-send', { fileId: file.id, serviceId: 63, fastway });
        assert.equal(sent.success, false); assert.match(sent.message, /login e a senha da Fastway/);
    }
    assert.equal(f.uploads.length, 0);
});
test('sem pedir para salvar, o login vale só para aquele envio', async t => {
    const f = fixture(t), { file } = await f.invoke('mailing-import-file');
    const sent = await f.invoke('mailing-import-send', { fileId: file.id, serviceId: 63, fastway: TYPED });
    assert.equal(sent.success, true); assert.deepEqual(sent.fastway, { saved: false, login: null, canSave: true });
    assert.deepEqual(f.stored, {});
});
test('salvar só guarda o login depois que a Fastway confirma, e a senha não volta para a tela', async t => {
    const f = fixture(t), { file } = await f.invoke('mailing-import-file');
    f.setUpload(async () => { throw Object.assign(new Error('A Fastway recusou o login.'), { code: 'FLOW_VALIDATION' }); });
    assert.equal((await f.invoke('mailing-import-send', { fileId: file.id, serviceId: 63, fastway: { ...TYPED, save: true } })).success, false);
    assert.equal((await f.invoke('mailing-import-state')).fastway.saved, false);
    f.setUpload(async () => ({ rows: 2 }));
    const sent = await f.invoke('mailing-import-send', { fileId: file.id, serviceId: 63, fastway: { ...TYPED, save: true } });
    assert.equal(sent.success, true); assert.deepEqual(sent.fastway, { saved: true, login: 'ana.fastway', canSave: true });
    const state = await f.invoke('mailing-import-state');
    assert.deepEqual(state.fastway, { saved: true, login: 'ana.fastway', canSave: true });
    assert.equal(JSON.stringify([sent, state, f.stored]).includes(TYPED.password), false);
});
test('login salvo é usado quando nada é digitado, e o digitado tem prioridade', async t => {
    const f = fixture(t);
    let { file } = await f.invoke('mailing-import-file');
    await f.invoke('mailing-import-send', { fileId: file.id, serviceId: 63, fastway: { ...TYPED, save: true } });
    ({ file } = await f.invoke('mailing-import-file'));
    assert.equal((await f.invoke('mailing-import-send', { fileId: file.id, serviceId: 87 })).success, true);
    ({ file } = await f.invoke('mailing-import-file'));
    const other = { login: 'ana.outro', password: 'synthetic-other' };
    assert.equal((await f.invoke('mailing-import-send', { fileId: file.id, serviceId: 18, fastway: other })).success, true);
    assert.deepEqual(plain(f.uploads.map(upload => upload.credentials)), [TYPED, TYPED, other]);
    assert.equal((await f.invoke('mailing-import-state')).fastway.login, 'ana.fastway', 'usar outro login sem salvar não troca o salvo');
});
test('esquecer apaga o login salvo, que nunca aparece para outro usuário', async t => {
    const f = fixture(t), { file } = await f.invoke('mailing-import-file');
    await f.invoke('mailing-import-send', { fileId: file.id, serviceId: 63, fastway: { ...TYPED, save: true } });
    f.state.currentUser = { username: 'Bia', role: 'admin' };
    assert.deepEqual((await f.invoke('mailing-import-state')).fastway, { saved: false, login: null, canSave: true });
    f.state.currentUser = { username: 'Ana', role: 'limited' };
    const forgot = await f.invoke('mailing-import-forget');
    assert.equal(forgot.success, true); assert.deepEqual(forgot.fastway, { saved: false, login: null, canSave: true });
    assert.equal((await f.invoke('mailing-import-state')).fastway.saved, false);
});
test('sem a proteção do Windows o envio segue, mas avisa que o login não foi salvo', async t => {
    const f = fixture(t, { canSave: false }), { file } = await f.invoke('mailing-import-file');
    assert.equal((await f.invoke('mailing-import-state')).fastway.canSave, false);
    const sent = await f.invoke('mailing-import-send', { fileId: file.id, serviceId: 63, fastway: { ...TYPED, save: true } });
    assert.equal(sent.success, true); assert.match(sent.job.message, /login não foi salvo/);
    assert.deepEqual(f.stored, {});
});
test('resultado de um usuário não aparece para outro', async t => {
    const f = fixture(t), { file } = await f.invoke('mailing-import-file');
    await f.invoke('mailing-import-send', { fileId: file.id, serviceId: 63, fastway: TYPED });
    f.state.currentUser = { username: 'Bia', role: 'admin' };
    const state = await f.invoke('mailing-import-state');
    assert.equal(state.job, null); assert.equal(state.admin, true);
});
