'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const ExcelJS = require('exceljs');
const { createApiClient, runApiStage } = require('../src/main/flows/disponibilidadeApi');
const { runFlow } = require('../src/main/flows/pipeline');
const credentials = { c6: { clientId: 'fixture-c6', clientSecret: 'synthetic-only' }, im: { clientId: 'fixture-im', clientSecret: 'synthetic-only' } };
const cnpj = number => String(number).padStart(14, '0');
const row = number => ({ cnpj: cnpj(number), phones: [`1198765${String(number).padStart(4, '0')}`], razao_social: 'Fictícia' });
async function directory(t) { const file = await fs.mkdtemp(path.join(os.tmpdir(), 'flow-api-')); t.after(() => fs.rm(file, { recursive: true, force: true })); return file; }
function clock() { let time = 1000000; const waits = []; return { now: () => time, sleep: async ms => { waits.push(ms); time += ms; }, waits }; }
function client(response) {
    const requests = [];
    return { requests, api: createApiClient({ credentials, http: { async post(url, body, options) {
        requests.push({ url, body, options });
        return url.endsWith('/token') ? { data: { access_token: 'synthetic-token' } } : { data: response };
    } } }) };
}
test('API client uses the selected private key, preserves alphanumeric CNPJ and accepts a valid empty result', async () => {
    // Official 14-character form: first 12 alphanumeric + two numeric digits.
    const alphanumeric = '12ABC34501DE35';
    const f = client({ CNPJ: [cnpj(1), '12.ABC.345/01DE-35'] });
    assert.deepEqual([...await f.api.consult([cnpj(1), alphanumeric], 'im')], [cnpj(1), alphanumeric]);
    assert.match(f.requests[0].body, /client_id=fixture-im/);
    assert.deepEqual(f.requests[1].body, { CNPJ: [cnpj(1), alphanumeric] });
    assert.equal(f.requests[1].options.timeout, 30000);
    assert.equal((await client({ CNPJ: [] }).api.consult([cnpj(1)], 'c6')).size, 0);
});
test('API response errors are never treated as an empty list of available CNPJs', async () => {
    for (const response of [null, [], { error: 'synthetic' }, { CNPJ: 'not-an-array' }, { CNPJ: [cnpj(2)] }, { CNPJ: [true] }, { CNPJ: [], cnpjs: [] }]) {
        await assert.rejects(client(response).api.consult([cnpj(1)], 'c6'), error => error.code === 'FLOW_VALIDATION' && !error.retryable);
    }
});
test('API auth and HTTP failures are friendly and do not expose tokens or response bodies', async () => {
    for (const status of [400, 401, 403, 429, 500, undefined]) {
        const api = createApiClient({ credentials, http: { post: async () => { throw Object.assign(new Error('private-token'), { response: { status, data: { secret: 'private-token' } } }); } } });
        await assert.rejects(api.consult([cnpj(1)], 'c6'), error => !error.message.includes('private-token') && error.retryable === (!status || status === 429 || status >= 500));
    }
    assert.throws(() => createApiClient({ credentials: { c6: credentials.c6 } }), /duas chaves/);
    await assert.rejects(createApiClient({ credentials, http: { post: async () => ({ data: {} }) } }).consult([cnpj(1)], 'c6'), /token válido/);
});
test('API propagates request cancellation without classifying the lot', async () => {
    const controller = new AbortController();
    const api = createApiClient({ credentials, http: { async post(url, body, options) { assert.equal(options.signal, controller.signal); controller.abort(); throw new Error('cancelled'); } } });
    await assert.rejects(api.consult([cnpj(1)], 'c6', controller.signal), { code: 'FLOW_CANCELLED' });
});
async function stageFixture(t, rows, consult, extra = {}) {
    const jobDir = await directory(t), counts = {}, logs = [], emitted = [], timing = clock(); let releases = 0, acquisitions = 0;
    async function* batches(source, signal, size) { for (let i = 0; i < rows.length; i += size) yield rows.slice(i, i + size); }
    const args = { jobDir, source: 'synthetic', batches, emit: async value => emitted.push(value), counts,
        update: data => logs.push(data.log), acquire: async () => { acquisitions++; return { consult, assert: async () => {}, release: async () => { releases++; } }; },
        batchSize: 4, ...timing, ...extra };
    return { args, counts, logs, emitted, timing, releases: () => releases, acquisitions: () => acquisitions };
}
test('API splits rounds across two keys and waits one minute only between new rounds', async t => {
    const calls = [];
    const f = await stageFixture(t, [1, 2, 3, 4, 5, 6].map(row), async (documents, key) => { calls.push({ documents, key }); return new Set(documents.filter(value => Number(value) % 2)); });
    await runApiStage(f.args);
    assert.deepEqual(calls.map(call => [call.key, call.documents.map(Number)]), [['c6', [1, 2]], ['im', [3, 4]], ['c6', [5]], ['im', [6]]]);
    assert.deepEqual(f.timing.waits, [60000]);
    assert.deepEqual(f.emitted.map(value => Number(value.cnpj)), [1, 3, 5]);
    assert.deepEqual(f.counts, { apiConsulted: 6, apiAvailable: 3, apiClients: 3, apiBatches: 2 });
    assert.equal(f.releases(), 1);
});
test('API resumes the successful half, preserving order and cooldown after a permanent failure', async t => {
    const calls = []; let fail = true;
    const f = await stageFixture(t, [1, 2, 3, 4].map(row), async (documents, key) => {
        calls.push(key);
        if (key === 'im' && fail) throw Object.assign(new Error('Licença recusada.'), { code: 'FLOW_VALIDATION' });
        return new Set(documents);
    });
    await assert.rejects(runApiStage(f.args), /Licença recusada/);
    assert.equal(f.emitted.length, 0); assert.equal(f.releases(), 1);
    fail = false; await runApiStage(f.args);
    assert.deepEqual(calls, ['c6', 'im', 'im']);
    assert.deepEqual(f.timing.waits, [60000]);
    assert.deepEqual(f.emitted.map(value => Number(value.cnpj)), [1, 2, 3, 4]);
    assert.equal(f.counts.apiConsulted, 4); assert.equal(f.releases(), 2);
    f.emitted.length = 0; await runApiStage(f.args);
    assert.equal(calls.length, 3); assert.equal(f.acquisitions(), 2); assert.equal(f.counts.apiAvailable, 4);
});
test('transient API errors retry only the failed key, with one minute delay', async t => {
    const calls = []; let failures = 0;
    const f = await stageFixture(t, [1, 2, 3, 4].map(row), async (documents, key) => {
        calls.push(key);
        if (key === 'im' && failures++ === 0) throw Object.assign(new Error('Temporary.'), { retryable: true });
        return new Set(documents);
    });
    await runApiStage(f.args);
    assert.deepEqual(calls, ['c6', 'im', 'im']); assert.deepEqual(f.timing.waits, [60000]);
    assert.equal(f.counts.apiAvailable, 4);
});
test('API has bounded retries and keeps successful results after exhausting the other key', async t => {
    const calls = [];
    const f = await stageFixture(t, [1, 2, 3, 4].map(row), async (documents, key) => {
        calls.push(key); if (key === 'im') throw Object.assign(new Error('Temporary.'), { retryable: true }); return new Set(documents);
    });
    await assert.rejects(runApiStage(f.args), /5 tentativas/);
    assert.equal(calls.filter(key => key === 'c6').length, 1); assert.equal(calls.filter(key => key === 'im').length, 5);
    assert.deepEqual(f.timing.waits, [60000, 60000, 60000, 60000]); assert.equal(f.releases(), 1);
});
test('API cancellation during cooldown releases keys and preserves the confirmed round', async t => {
    const controller = new AbortController(); let calls = 0;
    const f = await stageFixture(t, [1, 2, 3, 4, 5].map(row), async documents => { calls++; return new Set(documents); }, {
        signal: controller.signal, sleep: async () => { controller.abort(); throw Object.assign(new Error('aborted'), { name: 'AbortError' }); },
    });
    await assert.rejects(runApiStage(f.args), { name: 'AbortError' });
    assert.equal(calls, 2); assert.equal(f.releases(), 1);
    assert.equal(f.emitted.length, 4);
});
test('empty API input does not reserve keys or issue requests; corrupt saved data fails closed', async t => {
    const empty = await stageFixture(t, [], () => { throw Error('should not call'); });
    await runApiStage(empty.args); assert.equal(empty.acquisitions(), 0); assert.equal(empty.counts.apiConsulted, 0);
    const f = await stageFixture(t, [row(1)], async documents => new Set(documents));
    await runApiStage(f.args);
    const resultPath = path.join(f.args.jobDir, 'api-results', '00000000-0.json');
    const saved = JSON.parse(await fs.readFile(resultPath, 'utf8')); saved.available = [cnpj(2)]; await fs.writeFile(resultPath, JSON.stringify(saved));
    await assert.rejects(runApiStage(f.args), /não corresponde/);
});
test('pipeline consults before final cleaning and exports identical cleaned available rows to XLSX and CSV', async t => {
    const jobDir = await directory(t), timing = clock(), calls = [], config = {
        id: 'fixture', name: 'Fixture', operation: 'c6', generation: {}, enrichment: { enabled: false }, api: { enabled: true },
        cleaning: { enabled: true, rootSource: 'none', blocklist: false, prohibitedCnaes: [] }, output: { csv: true, rowsPerFile: 100, formatId: 'fixture' },
    };
    const source = [row(1), row(2), row(3), { ...row(4), phones: ['11111111'] }];
    let release = 0;
    const providers = { async *iterateReceita() { yield { rows: source }; }, getFormat: () => ({ colunas: ['cnpj', 'fone1'] }), mapOutputRow: value => [value.cnpj, value.phones[0]],
        apiTiming: timing, acquireApi: async () => ({ consult: async (documents, key) => { calls.push({ documents, key }); return new Set(documents.filter(value => value !== cnpj(2))); }, release: async () => { release++; } }),
    };
    const args = { jobDir, flow: config, user: { username: 'Davi' }, providers };
    const result = await runFlow(args);
    assert.deepEqual(calls.flatMap(call => call.documents), [cnpj(1), cnpj(2), cnpj(3), cnpj(4)]);
    assert.equal(result.counts.cleaned, 2); assert.equal(result.counts.apiClients, 1); assert.equal(result.counts.withoutPhones, 1); assert.equal(result.counts.exported, 2); assert.equal(release, 1);
    const book = new ExcelJS.Workbook(); await book.xlsx.readFile(result.outputs.find(value => value.kind === 'xlsx').path);
    assert.deepEqual(book.worksheets[0].getColumn(1).values.slice(2), [cnpj(1), cnpj(3)]);
    const csv = await fs.readFile(result.outputs.find(value => value.kind === 'csv').path, 'utf8');
    assert.ok(csv.includes(cnpj(1)) && csv.includes(cnpj(3)) && !csv.includes(cnpj(2)));
    const resumed = await runFlow(args); assert.deepEqual(resumed.outputs, result.outputs); assert.equal(calls.length, 2);
    const saved = await fs.readFile(path.join(jobDir, 'checkpoint.json'), 'utf8'); assert.ok(!saved.includes('clientSecret'));
});
test('pipeline exports no files when API rejects all survivors and fails without final files on API errors', async t => {
    for (const fail of [false, true]) {
        const jobDir = await directory(t), config = { id: 'fixture', operation: 'c6', generation: {}, api: { enabled: true }, cleaning: { enabled: false, blocklist: false }, output: { rowsPerFile: 100 } };
        const providers = { async *iterateReceita() { yield { rows: [row(1)] }; }, getFormat: () => ({ colunas: ['cnpj', 'fone1'] }), mapOutputRow: value => [value.cnpj, value.phones[0]],
            acquireApi: async () => ({ consult: async () => { if (fail) throw Object.assign(new Error('Resposta inválida.'), { code: 'FLOW_VALIDATION' }); return new Set(); }, release: async () => {} }), apiTiming: clock() };
        const args = { flow: config, user: { username: 'Davi' }, jobDir, providers };
        if (fail) { await assert.rejects(runFlow(args), /Resposta inválida/); assert.equal(JSON.parse(await fs.readFile(path.join(jobDir, 'checkpoint.json'), 'utf8')).stages.export, undefined); }
        else { const result = await runFlow(args); assert.equal(result.status, 'empty'); assert.deepEqual(result.outputs, []); assert.equal(result.counts.exported, 0); }
    }
});

test('an optional disabled API exports cleaning survivors without reserving keys', async t => {
    const jobDir = await directory(t), flow = { id: 'fixture', operation: 'c6', generation: {}, api: { enabled: false }, cleaning: { enabled: false, blocklist: false }, output: { rowsPerFile: 100 } };
    const result = await runFlow({ flow, jobDir, user: { username: 'Davi' }, providers: {
        async *iterateReceita() { yield { rows: [row(1), row(2)] }; }, getFormat: () => ({ colunas: ['cnpj', 'fone1'] }), mapOutputRow: value => [value.cnpj, value.phones[0]],
        acquireApi: async () => { throw Error('disabled API must not run'); },
    } });
    assert.equal(result.counts.exported, 2); assert.equal(result.counts.apiConsulted, undefined);
});
