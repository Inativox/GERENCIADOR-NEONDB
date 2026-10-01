const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const loadModule = require('./helpers/loadModule');
const { readOnlyPoolOptions } = require('../src/main/flows/postgres');

function fixture(environment = {}) {
    const handlers = new Map(), settings = new Map(), pools = [];
    const sender = {}, state = { loginWindow: { webContents: sender }, pool: null };
    let validate = async () => ({}), neonError = null;
    class Pool { constructor(options) { this.options = options; pools.push(this); } async end() { this.closed = true; } }
    const auth = loadModule('src/main/handlers/auth.js', {
        electron: { ipcMain: { handle: (name, fn) => handlers.set(name, fn), on() {} }, dialog: {} },
        fs: {}, path,
        'electron-store': class { get(key) { return settings.get(key); } set(key, value) { settings.set(key, value); } },
        '../state': state,
        '../database/connection': { initializePool: async uri => { if (neonError) throw neonError; state.pool = { options: { connectionString: uri } }; }, closePool() {} },
        '../database/cache': {}, '../keyfile': {},
        '../runtimeConfig': { loadUsers: () => ({}) },
        pg: { Pool }, '../flows/receita': { getReceitaMetadata: pool => validate(pool) }, '../flows/postgres': { readOnlyPoolOptions },
    }, { process: { env: environment } });
    auth.register();
    return { settings, pools, state, invoke: (name, input, origin = sender) => handlers.get(name)({ sender: origin }, input),
        validate: action => { validate = action; }, failNeon: () => { neonError = new Error('private synthetic failure'); } };
}

test('login database status recognises saved and inherited accesses without returning connection strings', async () => {
    const f = fixture({ RECEITA_DATABASE_URL: 'postgresql://synthetic/environment-receita' });
    f.settings.set('db_connection_string', 'postgresql://synthetic/neon');
    const status = await f.invoke('get-login-database-status');
    assert.equal(status.neonConfigured, true); assert.equal(status.receitaConfigured, true);
    assert.ok(!JSON.stringify(status).includes('postgresql://'));
    assert.equal((await f.invoke('get-login-database-status', null, {})).success, false);
});

test('Neon and Receita save to independent settings; Receita uses read-only schema validation', async () => {
    const f = fixture();
    assert.equal((await f.invoke('save-and-test-db-connection', 'postgresql://synthetic/neon')).success, true);
    f.validate(async pool => {
        assert.equal(pool.options.statement_timeout, 15000);
        assert.match(pool.options.options, /default_transaction_read_only=on/);
        assert.equal(f.settings.has('receita_connection_string'), false);
    });
    assert.equal((await f.invoke('save-and-test-receita-connection', 'postgresql://synthetic/receita')).success, true);
    assert.equal(f.settings.get('db_connection_string'), 'postgresql://synthetic/neon');
    assert.equal(f.settings.get('receita_connection_string'), 'postgresql://synthetic/receita');
    assert.equal(f.state.pool.options.connectionString, 'postgresql://synthetic/neon');
    assert.equal(f.pools[0].closed, true);
});

test('failed validation preserves both previous accesses and never returns private error details', async () => {
    const f = fixture();
    f.settings.set('db_connection_string', 'previous-neon'); f.settings.set('receita_connection_string', 'previous-receita');
    f.failNeon(); f.validate(async () => { throw new Error('private synthetic failure'); });
    for (const name of ['save-and-test-db-connection', 'save-and-test-receita-connection']) {
        const result = await f.invoke(name, 'postgresql://synthetic/invalid');
        assert.equal(result.success, false); assert.ok(!result.message.includes('private'));
    }
    assert.equal(f.settings.get('db_connection_string'), 'previous-neon');
    assert.equal(f.settings.get('receita_connection_string'), 'previous-receita');
    assert.equal(f.pools[0].closed, true);
});

test('foreign windows, invalid URLs and active flows cannot replace the Receita access', async () => {
    const f = fixture();
    for (const name of ['save-and-test-db-connection', 'save-and-test-receita-connection']) {
        assert.equal((await f.invoke(name, 'postgresql://synthetic/private', {})).success, false);
        for (const input of [null, {}, '', 'https://example.test', 'postgresql://' + 'x'.repeat(4096)]) assert.equal((await f.invoke(name, input)).success, false);
    }
    f.state.flowManager = { isBusy: () => true };
    assert.equal((await f.invoke('save-and-test-receita-connection', 'postgresql://synthetic/private')).success, false);
    assert.equal(f.settings.size, 0); assert.equal(f.pools.length, 0);
});
