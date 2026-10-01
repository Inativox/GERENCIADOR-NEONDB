const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const loadModule = require('./helpers/loadModule');

function fixture(validate) {
    const handlers = new Map();
    const settings = new Map();
    const sender = {};
    let closed = 0;
    const dependencies = {
        electron: {
            ipcMain: { handle: (name, handler) => handlers.set(name, handler), on() {} },
            app: { getPath: () => path.resolve('test-user-data'), on() {} },
            dialog: {}, shell: {},
        },
        fs: {}, path, exceljs: {}, 'csv-parse': {},
        pg: { Pool: class { async end() { closed++; } } },
        'electron-store': class {
            get(key) { return settings.get(key); }
            set(key, value) { settings.set(key, value); }
        },
        '../state': { currentUser: { username: 'test', role: 'admin' }, mainWindow: { webContents: sender } },
        '../flows/manager': { createFlowManager: () => ({ isBusy: () => false }) },
        '../flows/bq': {},
        '../flows/bqAuth': { createBqLogin: () => ({}) },
        '../flows/config': {}, '../flows/formats': {}, '../flows/layouts': {},
        '../flows/receita': { getReceitaMetadata: validate },
        '../flows/postgres': { readOnlyPoolOptions: connectionString => ({ connectionString }) },
        '../flows/receitaOptions': { createReceitaOptions: () => ({}) },
    };
    loadModule('src/main/handlers/fluxos.js', dependencies).register();
    return {
        configure: () => handlers.get('flows-configure-receita')({ sender }, { connectionString: 'postgresql://user:synthetic@localhost/receita' }),
        settings, closed: () => closed,
    };
}

test('configuração da Receita informa as colunas ausentes e preserva o acesso salvo', async () => {
    const message = 'Esquema Receita incompatível: public.empresas não contém as colunas obrigatórias razao_social, situacao_cadastral_cod.';
    const f = fixture(async () => { throw Object.assign(new Error(message), { code: 'FLOW_VALIDATION' }); });
    f.settings.set('receita_connection_string', 'previous');
    const result = await f.configure();
    assert.equal(result.success, false);
    assert.equal(result.message, message);
    assert.equal(f.settings.get('receita_connection_string'), 'previous');
    assert.equal(f.closed(), 1);
});

test('configuração da Receita salva somente depois de validar o esquema', async () => {
    const f = fixture(async () => ({}));
    const result = await f.configure();
    assert.equal(result.success, true);
    assert.equal(f.settings.get('receita_connection_string'), 'postgresql://user:synthetic@localhost/receita');
    assert.equal(f.closed(), 1);
});

test('falha inesperada na conexão não expõe detalhes privados', async () => {
    const f = fixture(async () => { throw new Error('private connection details'); });
    const result = await f.configure();
    assert.equal(result.success, false);
    assert.equal(result.message, 'Não foi possível validar a fonte Receita. Confira acesso e schema de empresas.');
    assert.equal(f.settings.has('receita_connection_string'), false);
    assert.equal(f.closed(), 1);
});
