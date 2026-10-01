'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { createApiLocks } = require('../src/main/apiLocks');

function database() {
    const rows = new Map(), calls = [];
    let releasedClients = 0, gate = Promise.resolve();
    async function query(sql, params = []) {
        if (typeof sql === 'object') { params = sql.values; sql = sql.text; }
        calls.push(sql);
        if (sql.startsWith('SELECT') && sql.includes("status = 'Em uso'")) return { rows: [...rows.values()].filter(row => params[0].includes(row.key_name) && row.status === 'Em uso' && !row.expired) };
        if (sql.startsWith('SELECT')) return { rows: [...rows.values()].filter(row => params[0].includes(row.key_name)).map(() => ({ next_allowed_at: 123 })) };
        if (sql.startsWith('INSERT')) { rows.set(params[0], { key_name: params[0], username: params[1], status: 'Em uso', lock_mode: params[3] }); return { rowCount: 1 }; }
        if (sql.startsWith('UPDATE')) {
            let count = 0;
            for (const row of rows.values()) if (params[0].includes(row.key_name) && row.lock_mode === params[1] && (!sql.includes("status = 'Em uso'") || (row.status === 'Em uso' && !row.expired))) {
                if (sql.includes("status = 'Livre'")) row.status = 'Livre'; count++;
            }
            return { rowCount: count };
        }
        return { rows: [] };
    }
    const pool = { query, async connect() {
        let unlock;
        return { async query(sql, params) {
            if (sql.startsWith('LOCK TABLE')) { const previous = gate; gate = new Promise(resolve => { unlock = resolve; }); await previous; }
            if (sql === 'COMMIT' || sql === 'ROLLBACK') { unlock?.(); unlock = null; }
            return query(sql, params);
        }, release() { releasedClients++; unlock?.(); } };
    } };
    return { pool, rows, calls, releasedClients: () => releasedClients };
}
test('API lock acquires both keys atomically, excludes same-user concurrent sessions and releases ownership', async () => {
    const db = database(), locks = createApiLocks({ getPool: () => db.pool });
    const results = await Promise.allSettled([locks.acquire(['c6', 'im'], 'Davi', 'dupla'), locks.acquire(['c6', 'im'], 'Davi', 'dupla')]);
    assert.equal(results.filter(value => value.status === 'fulfilled').length, 1);
    assert.equal(results.filter(value => value.status === 'rejected').length, 1);
    const lease = results.find(value => value.status === 'fulfilled').value;
    try {
        assert.equal(db.rows.size, 2); assert.equal(db.releasedClients(), 2);
        assert.ok(db.calls.includes('LOCK TABLE api_locks IN SHARE ROW EXCLUSIVE MODE'));
        await lease.heartbeat(); await lease.assert(); assert.equal(lease.nextAllowedAt, 0);
    } finally { await lease.release(); }
    assert.ok([...db.rows.values()].every(row => row.status === 'Livre'));
    await assert.rejects(lease.assert(), /perdida/);
});
test('expired lease cannot heartbeat or release a newer session; existing free keys carry cooldown', async () => {
    const db = database(), locks = createApiLocks({ getPool: () => db.pool });
    const old = await locks.acquire(['c6', 'im'], 'Davi', 'dupla');
    for (const row of db.rows.values()) row.expired = true;
    const replacement = await locks.acquire(['c6', 'im'], 'Outro', 'dupla');
    try {
        assert.equal(replacement.nextAllowedAt, 123);
        await assert.rejects(old.heartbeat(), /perdida/);
        await old.release();
        assert.ok([...db.rows.values()].every(row => row.status === 'Em uso' && row.username === 'Outro'));
        await replacement.heartbeat(); await replacement.assert();
    } finally { await old.release(); await replacement.release(); }
});
test('API reservation fails closed when no pool exists or database rejects the transaction', async () => {
    await assert.rejects(createApiLocks({ getPool: () => null }).acquire(['c6'], 'Davi', 'chave1'), /conexão/);
    let release = false, rollback = false;
    const pool = { async connect() { return { async query(sql) { if (sql === 'ROLLBACK') { rollback = true; return; } throw new Error('postgresql://synthetic/private'); }, release() { release = true; } }; } };
    await assert.rejects(createApiLocks({ getPool: () => pool }).acquire(['c6'], 'Davi', 'chave1'), error => !error.message.includes('postgresql://'));
    assert.equal(release, true); assert.equal(rollback, true);
});
test('heartbeat failures stop a lease instead of silently continuing API requests', async () => {
    const db = database(), locks = createApiLocks({ getPool: () => db.pool });
    const lease = await locks.acquire(['c6'], 'Davi', 'chave1');
    const query = db.pool.query; db.pool.query = async () => { throw Error('private DB failure'); };
    await assert.rejects(lease.heartbeat(), /perdida/); await assert.rejects(lease.assert(), /perdida/);
    db.pool.query = query; await lease.release();
});
