const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { Worker } = require('node:worker_threads');
const { records } = require('../src/main/flows/jsonl');

function fixture(t, contents) {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'flow-jsonl-test-'));
    t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
    const filename = path.join(directory, 'input.jsonl');
    if (contents !== undefined) fs.writeFileSync(filename, contents);
    return filename;
}

test('JSONL mantém UTF-8 entre chunks, linhas vazias e último registro sem newline', async t => {
    const expected = [{ text: 'á😀'.repeat(20000) }, { text: 'fim' }];
    const filename = fixture(t, JSON.stringify(expected[0]) + '\r\n \n' + JSON.stringify(expected[1]));
    assert.deepEqual(await Array.fromAsync(records(filename)), expected);
});

test('JSONL propaga arquivo ausente e registro corrompido', async t => {
    await assert.rejects(Array.fromAsync(records(fixture(t))), { code: 'ENOENT' });
    await assert.rejects(Array.fromAsync(records(fixture(t, '{"ok":1}\ninvalid\n'))), SyntaxError);
});

test('JSONL respeita cancelamento entre registros do mesmo chunk', async t => {
    const controller = new AbortController();
    const iterator = records(fixture(t, '{"n":1}\n{"n":2}\n'), controller.signal);
    assert.deepEqual((await iterator.next()).value, { n: 1 });
    controller.abort();
    await assert.rejects(iterator.next(), { code: 'FLOW_CANCELLED' });
});

test('JSONL processa 120 mil registros com consumidor assíncrono sob heap de 64 MB', { timeout: 30000 }, async t => {
    const filename = fixture(t);
    const count = 120000;
    const block = (JSON.stringify({ cnpj: '00000000000001', text: 'x'.repeat(1000) }) + '\n').repeat(1000);
    const handle = fs.openSync(filename, 'w');
    try { for (let i = 0; i < count / 1000; i++) fs.writeSync(handle, block); }
    finally { fs.closeSync(handle); }
    const processed = await new Promise((resolve, reject) => {
        const worker = new Worker(`
            const { parentPort, workerData } = require('node:worker_threads');
            const { records } = require(workerData.reader);
            (async () => {
                let count = 0;
                for await (const row of records(workerData.filename)) {
                    if (row.cnpj !== '00000000000001') throw new Error('Unexpected row');
                    if (++count % 2000 === 0) await new Promise(resolve => setImmediate(resolve));
                }
                parentPort.postMessage(count);
            })().catch(error => { throw error; });
        `, { eval: true, resourceLimits: { maxOldGenerationSizeMb: 64 }, workerData: { filename, reader: require.resolve('../src/main/flows/jsonl') } });
        t.after(() => worker.terminate());
        worker.once('message', resolve);
        worker.once('error', reject);
        worker.once('exit', code => { if (code) reject(new Error(`Reader worker exited: ${code}`)); });
    });
    assert.equal(processed, count);
});
