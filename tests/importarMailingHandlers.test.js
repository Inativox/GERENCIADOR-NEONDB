const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), os = require('node:os'), path = require('node:path'), vm = require('node:vm');
const { createRequire } = require('node:module');
const OWN = { login: 'ana.fastway', password: 'synthetic-own' }, COMPUTER = { login: 'computador', password: 'synthetic-computer' };
function fixture(t, { users = { Ana: { password: 'x', role: 'limited', fastway: OWN }, Bia: { password: 'x', role: 'admin' } }, computer = COMPUTER } = {}) {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'mailing-ipc-')); t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
    const handlers = new Map(), uploads = [], sender = {}, state = { currentUser: { username: 'Ana', role: 'limited' }, mainWindow: { webContents: sender } };
    let chosen = path.join(directory, 'mailing.csv'), upload = async input => ({ rows: 2, file: path.basename(input.file) });
    fs.writeFileSync(chosen, 'nome;cpf;fone1\nAna;123;11999990000\nBia;456;11999990001\n');
    const filename = path.resolve('src/main/handlers/importarMailing.js'), actual = createRequire(filename), module = { exports: {} };
    const mocks = { electron: { ipcMain: { handle: (name, handler) => handlers.set(name, handler) }, dialog: { showOpenDialog: async () => ({ canceled: false, filePaths: [chosen] }) } },
        '../state': state, '../runtimeConfig': { loadUsers: () => users, fastwayStatus: () => Boolean(computer), getFastwayCredentials: () => computer },
        '../flows/fastway': { ...actual('../flows/fastway'), upload: input => { uploads.push(input); return upload(input); } } };
    vm.runInNewContext(fs.readFileSync(filename, 'utf8'), { require: name => mocks[name] || actual(name), module, __dirname: path.dirname(filename) }, { filename });
    module.exports.register();
    return { handlers, uploads, state, directory, invoke: (name, input, origin = sender) => handlers.get(name)({ sender: origin }, input),
        choose: (name, content) => { chosen = path.join(directory, name); fs.writeFileSync(chosen, content); }, setUpload: value => { upload = value; } };
}
test('importação exige usuário logado na janela principal, mas não exige admin', async t => {
    const f = fixture(t);
    for (const [name] of f.handlers) assert.equal((await f.invoke(name, {}, {})).success, false);
    const state = await f.invoke('mailing-import-state');
    assert.equal(state.success, true); assert.equal(state.admin, false); assert.equal(state.configured, true);
    assert.ok(state.services.some(service => service.id === 139 && service.name === '213 - MP'));
    f.state.currentUser = null; for (const [name] of f.handlers) assert.equal((await f.invoke(name, {})).success, false);
    assert.equal(f.uploads.length, 0);
});
test('CSV sem as colunas obrigatórias é recusado na seleção e não pode ser enviado', async t => {
    const f = fixture(t); f.choose('sem-cpf.csv', 'nome;fone1\nAna;11999990000\n');
    const selected = await f.invoke('mailing-import-file');
    assert.equal(selected.success, false); assert.match(selected.message, /cpf/);
    assert.equal((await f.invoke('mailing-import-send', { fileId: undefined, serviceId: 63 })).success, false);
    f.choose('planilha.xlsx', 'nome;cpf;fone1\nAna;123;1\n');
    assert.match((await f.invoke('mailing-import-file')).message, /\.csv/);
    assert.equal(f.uploads.length, 0);
});
test('envio só aceita serviço da lista, manda o ID escolhido e exige nova seleção para reenviar', async t => {
    const f = fixture(t), { file } = await f.invoke('mailing-import-file');
    assert.deepEqual({ name: file.name, rows: file.rows }, { name: 'mailing.csv', rows: 2 });
    assert.equal((await f.invoke('mailing-import-send', { fileId: file.id, serviceId: 999 })).success, false);
    assert.equal((await f.invoke('mailing-import-send', { fileId: file.id, serviceId: '63' })).success, false);
    assert.equal(f.uploads.length, 0);
    const sent = await f.invoke('mailing-import-send', { fileId: file.id, serviceId: 63 });
    assert.equal(sent.success, true); assert.equal(sent.job.status, 'completed'); assert.match(sent.job.message, /2 registros em 63 · SANTANDER/);
    assert.deepEqual(JSON.parse(JSON.stringify(f.uploads.map(({ file: uploaded, campaignId, credentials }) => ({ uploaded, campaignId, credentials })))), [{ uploaded: path.join(f.directory, 'mailing.csv'), campaignId: '63', credentials: OWN }]);
    assert.equal((await f.invoke('mailing-import-send', { fileId: file.id, serviceId: 63 })).success, false);
    assert.equal(f.uploads.length, 1);
    assert.equal((await f.invoke('mailing-import-state')).job.status, 'completed');
});
test('durante o envio o app marca envio manual em andamento e recusa um segundo envio', async t => {
    const f = fixture(t), { file } = await f.invoke('mailing-import-file');
    let finish; f.setUpload(() => new Promise(resolve => { finish = resolve; }));
    const pending = f.invoke('mailing-import-send', { fileId: file.id, serviceId: 87 });
    await new Promise(setImmediate);
    assert.equal(f.state.mailingUpload?.service.id, 87);
    assert.equal((await f.invoke('mailing-import-state')).job.status, 'running');
    const second = await f.invoke('mailing-import-send', { fileId: file.id, serviceId: 87 });
    assert.equal(second.success, false); assert.match(second.message, /andamento/);
    finish({ rows: 2 }); assert.equal((await pending).success, true);
    assert.equal(f.state.mailingUpload, null); assert.equal(f.uploads.length, 1);
});
test('recusa da Fastway volta com a mensagem dela, libera o app e mantém o arquivo para tentar de novo', async t => {
    const f = fixture(t), { file } = await f.invoke('mailing-import-file');
    f.setUpload(async () => { throw Object.assign(new Error('A Fastway respondeu HTTP 500. Confirme no painel.'), { code: 'FLOW_VALIDATION' }); });
    const failed = await f.invoke('mailing-import-send', { fileId: file.id, serviceId: 18 });
    assert.equal(failed.success, false); assert.match(failed.message, /HTTP 500/);
    assert.equal(f.state.mailingUpload, null); assert.equal((await f.invoke('mailing-import-state')).job.status, 'failed');
    f.setUpload(async () => ({ rows: 2 }));
    assert.equal((await f.invoke('mailing-import-send', { fileId: file.id, serviceId: 18 })).success, true);
});
test('usuário sem login Fastway próprio não envia, nem com o acesso salvo no computador', async t => {
    const f = fixture(t, { users: { Ana: { password: 'x', role: 'limited' } } }), { file } = await f.invoke('mailing-import-file');
    assert.equal((await f.invoke('mailing-import-state')).configured, false);
    const sent = await f.invoke('mailing-import-send', { fileId: file.id, serviceId: 63 });
    assert.equal(sent.success, false); assert.match(sent.message, /login Fastway/); assert.match(sent.message, /admin/);
    assert.equal(f.uploads.length, 0);
});
test('login Fastway incompleto no usuário conta como não cadastrado', async t => {
    const f = fixture(t, { users: { Ana: { password: 'x', role: 'limited', fastway: { login: 'ana.fastway', password: '' } } } });
    assert.equal((await f.invoke('mailing-import-state')).configured, false);
});
test('resultado de um usuário não aparece para outro', async t => {
    const f = fixture(t), { file } = await f.invoke('mailing-import-file');
    await f.invoke('mailing-import-send', { fileId: file.id, serviceId: 63 });
    f.state.currentUser = { username: 'Bia', role: 'admin' };
    const state = await f.invoke('mailing-import-state');
    assert.equal(state.job, null); assert.equal(state.admin, true);
});
