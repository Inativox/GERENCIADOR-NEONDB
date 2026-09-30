const test = require('node:test');
const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const loadModule = require('./helpers/loadModule');

function setup(query = async () => ({ rows: [] })) {
    const pools = [];
    const state = { pool: null };
    class Pool extends EventEmitter {
        constructor(options) { super(); this.options = options; this.ended = 0; pools.push(this); }
        query(...args) { return query(...args); }
        async end() { this.ended++; }
    }
    const db = loadModule('src/main/database/connection.js', {
        pg: { Pool },
        'electron-store': class { get() { return 'postgres://synthetic-test'; } },
        '../state': state,
    });
    return { db, state, pools };
}

test('erro de cliente ocioso é tratado sem derrubar a aplicação', async () => {
    const { db, state } = setup();
    await db.initializePool('postgres://synthetic-test');
    assert.doesNotThrow(() => state.pool.emit('error', new Error('connection lost')));
    assert.equal((await state.pool.query('SELECT 1')).rows.length, 0);
});

test('falha de inicialização libera o candidato e preserva pool anterior', async () => {
    const { db, state, pools } = setup();
    await db.initializePool('postgres://working');
    const previous = state.pool;
    let failure = true;
    const originalQuery = previous.query;
    Object.getPrototypeOf(previous).query = async (...args) => {
        if (failure) throw new Error('invalid connection');
        return originalQuery(...args);
    };
    await assert.rejects(db.initializePool('postgres://invalid'), /invalid connection/);
    assert.equal(state.pool, previous);
    assert.equal(previous.ended, 0);
    assert.equal(pools[1].ended, 1);
    failure = false;
});

test('retry concorrente reaproveita o pool e não encerra consultas alheias', async () => {
    const { db, state, pools } = setup();
    await db.initializePool('postgres://synthetic-test');
    const activePool = state.pool;
    const attempts = new Map();
    activePool.query = async (sql) => {
        const attempt = (attempts.get(sql) || 0) + 1;
        attempts.set(sql, attempt);
        if (attempt === 1) throw Object.assign(new Error('connection reset'), { code: 'ECONNRESET' });
        return { rows: [{ value: sql }] };
    };
    const results = await Promise.all([db.queryWithRetry('SELECT 1'), db.queryWithRetry('SELECT 2')]);
    assert.equal(state.pool, activePool);
    assert.equal(activePool.ended, 0);
    assert.equal(pools.length, 1);
    assert.deepEqual(results.map(r => r.rows[0].value), ['SELECT 1', 'SELECT 2']);
});

test('erro SQL definitivo não é repetido', async () => {
    const { db, state } = setup();
    let attempts = 0;
    state.pool = { query: async () => { attempts++; throw Object.assign(new Error('invalid SQL'), { code: '42601' }); } };
    await assert.rejects(db.queryWithRetry('bad SQL'), /invalid SQL/);
    assert.equal(attempts, 1);
});

test('erro transitório respeita limite de tentativas sem trocar o pool', async () => {
    const { db, state } = setup();
    let attempts = 0;
    state.pool = { query: async () => { attempts++; throw Object.assign(new Error('connection reset'), { code: 'ECONNRESET' }); } };
    await assert.rejects(db.queryWithRetry('SELECT 1', [], 3), /connection reset/);
    assert.equal(attempts, 3);
});

for (const stage of ['SELECT NOW()', 'CREATE TABLE IF NOT EXISTS system_logs']) {
test(`logout invalida inicialização e fila na etapa ${stage}`, async () => {
    let release;
    let started;
    let block = true;
    const queryStarted = new Promise(resolve => { started = resolve; });
    const { db, state, pools } = setup(async sql => {
        if (block && sql.includes(stage)) {
            started();
            await new Promise(resolve => { release = resolve; });
        }
        return { rows: [] };
    });
    const first = db.initializePool('postgres://first');
    const queued = db.initializePool('postgres://queued');
    const firstRejected = assert.rejects(first, /cancelada/i);
    const queuedRejected = assert.rejects(queued, /cancelada/i);
    await queryStarted;
    await db.closePool();
    block = false;
    release();
    await Promise.all([firstRejected, queuedRejected]);
    assert.equal(state.pool, null);
    assert.equal(pools.length, 1);
    assert.equal(pools[0].ended, 1);
    await db.initializePool('postgres://new-session');
    assert.equal(state.pool, pools[1]);
});
}
