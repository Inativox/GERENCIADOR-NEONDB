const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { createPrivateConfig } = require('../src/main/privateConfig');

function fixture(t, packaged = true) {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'private-config-'));
    t.after(() => fs.rmSync(root, { recursive: true, force: true }));
    const folders = { userData: path.join(root, 'user'), appData: path.join(root, 'roaming') };
    const project = path.join(root, 'project');
    fs.mkdirSync(project);
    const legacy = path.join(folders.appData, 'MB Finance', 'Gerenciador de Bases', 'legacy-app.asar');
    const env = {};
    const config = createPrivateConfig({ app: { isPackaged: packaged, getPath: key => folders[key] }, projectRoot: project, environment: env });
    return { root, folders, project, legacy, config, env };
}

test('instalação nova abre sem usuários e ignora segredos no diretório do aplicativo', t => {
    const f = fixture(t);
    fs.writeFileSync(path.join(f.project, 'users.json'), JSON.stringify({ Intruso: { password: '123', role: 'admin' } }));
    assert.deepEqual(f.config.loadUsers(), {});
    assert.equal(f.config.hasAccess(), false);
});

test('migra configuração legada local e não sobrescreve configuração já importada', t => {
    const f = fixture(t);
    fs.mkdirSync(f.legacy, { recursive: true });
    fs.writeFileSync(path.join(f.legacy, 'users.json'), JSON.stringify({ Teste: { password: 'senha-teste', role: 'admin' } }));
    fs.writeFileSync(path.join(f.legacy, '.env'), 'SMTP_USER=teste-local\nSMTP_PASS=teste-ficticio');
    fs.writeFileSync(path.join(f.legacy, 'chave-api.mbkey'), '{"v":1}');
    f.config.initialize();
    assert.equal(f.config.loadUsers().Teste.password, 'senha-teste');
    assert.equal(f.env.SMTP_USER, 'teste-local');
    assert.equal(fs.readFileSync(f.config.keyFilePath(), 'utf8'), '{"v":1}');
    f.config.importBundle({ version: 1, users: { Novo: { password: 'nova', role: 'admin' } }, env: {} });
    f.config.initialize();
    assert.deepEqual(Object.keys(f.config.loadUsers()), ['Novo']);
});

test('importação inválida preserva acesso existente e não aceita variáveis de execução', t => {
    const f = fixture(t);
    f.config.importBundle({ version: 1, users: { Teste: { password: 'senha', role: 'admin' } }, env: { SMTP_USER: 'local' } });
    assert.throws(() => f.config.importBundle({ version: 1, users: {}, env: {} }));
    assert.throws(() => f.config.importBundle({ version: 1, users: { Teste: { password: 'senha', role: 'admin' } }, env: { NODE_OPTIONS: '--require injected.js' } }));
    assert.equal(f.config.loadUsers().Teste.password, 'senha');
    f.config.importBundle({ version: 1, users: { Teste: { password: 'senha', role: 'admin' } }, env: { SMTP_USER: 'atualizado' } });
    assert.equal(f.env.SMTP_USER, 'atualizado');
});

test('desenvolvimento preserva os arquivos locais e variáveis do ambiente têm prioridade', t => {
    const f = fixture(t, false);
    fs.writeFileSync(path.join(f.project, 'users.json'), JSON.stringify({ Dev: { password: 'teste', role: 'admin' } }));
    fs.writeFileSync(path.join(f.project, '.env'), 'SMTP_USER=arquivo');
    f.env.SMTP_USER = 'ambiente';
    f.config.initialize();
    assert.equal(f.config.loadUsers().Dev.password, 'teste');
    assert.equal(f.env.SMTP_USER, 'ambiente');
});
