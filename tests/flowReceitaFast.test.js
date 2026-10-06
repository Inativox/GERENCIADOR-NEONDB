'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { iterateReceitaFast } = require('../src/main/flows/receita');
function fixture(source, { fail = false } = {}) {
    const calls = []; let released;
    const client = { connection: { stream: { pause() {}, resume() {}, destroy() {} } }, release(value) { released = value; }, query(query) {
        calls.push(query);
        setImmediate(() => { for (const row of source) query.emit('row', row); query.emit(fail ? 'error' : 'end', fail ? new Error('private') : {}); });
    } };
    return { calls, get released() { return released; }, async connect() { return client; }, async query() {
        return { rows: ['cnpj', 'razao_social', 'situacao_cadastral_cod'].map(column_name => ({ column_name, data_type: 'text' })) };
    } };
}
const row = n => ({ cnpj: String(n).padStart(14, '0'), razao_social: 'Sintética', situacao_cadastral_cod: '02' });
test('fast generation accepts unordered rows with one SELECT and 50k saved batches', async () => {
    const source = Array.from({ length: 50002 }, (_, n) => row(50002 - n));
    const pool = fixture(source); const batches = [];
    for await (const batch of iterateReceitaFast({ pool, filters: { limit: 50002 }, batchSize: 50000 })) batches.push(batch);
    assert.deepEqual(batches.map(b => b.rows.length), [50000, 2]);
    assert.deepEqual(batches.flatMap(b => b.rows.map(r => r.cnpj)), source.map(r => r.cnpj));
    assert.equal(pool.calls.length, 1);
    assert.doesNotMatch(pool.calls[0].text, /ORDER BY|OFFSET|"cnpj" >/i);
    assert.equal(pool.calls[0].values.at(-1), 50002);
    assert.equal(pool.released, false);
});
test('fast resume skips saved CNPJs regardless of scan order and keeps the requested remaining limit', async () => {
    const pool = fixture([row(9), row(1), row(7), row(3), row(2)]); const batches = [];
    for await (const batch of iterateReceitaFast({ pool, filters: { limit: 2 }, savedCount: 2, isSaved: cnpj => [row(9).cnpj, row(3).cnpj].includes(cnpj), batchSize: 1 })) batches.push(batch);
    assert.deepEqual(batches.flatMap(b => b.rows.map(r => r.cnpj)), [row(1).cnpj, row(7).cnpj]);
    assert.equal(pool.calls[0].values.at(-1), 4);
});
test('fast failure discards partial unconfirmed rows and hides server details', async () => {
    const pool = fixture([row(2)], { fail: true });
    await assert.rejects(iterateReceitaFast({ pool, batchSize: 50000 }).next(), e => !e.message.includes('private'));
    assert.equal(pool.released, true);
});
test('cancellation after a saved batch closes the query without yielding the next batch', async () => {
    const pool = fixture([row(9), row(1), row(7)]);
    const controller = new AbortController();
    const iterator = iterateReceitaFast({ pool, batchSize: 2, signal: controller.signal });
    assert.equal((await iterator.next()).value.rows.length, 2);
    controller.abort();
    await assert.rejects(iterator.next(), { name: 'AbortError' });
});
