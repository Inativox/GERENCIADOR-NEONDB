const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { runFlow } = require('../src/main/flows/pipeline');
const { model } = require('../src/renderer/flowProgress');
const { Worker } = require('node:worker_threads');

function fixture(t) { const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'flow-cache-test-')); t.after(() => fs.rmSync(directory, { recursive: true, force: true })); return directory; }
function flow(directory) { return { name: 'Synthetic', operation: 'c6', generation: {}, enrichment: { enabled: false }, cleaning: { enabled: true, rootSource: 'none', blocklist: true, invalidPhones: false, removeLandlines: true, prohibitedCnaes: [] }, output: { formatId: 'padrao', rowsPerFile: 10000, directory } }; }
function rows(count) { return Array.from({ length: count }, (_, i) => ({ cnpj: String(i + 1).padStart(14, '0'), razao_social: 'Sintética á😀', phones: [`119${12340000 + i}`] })); }
function output(directory, name = 'cleaning') { const text = fs.readFileSync(path.join(directory, `${name}.jsonl`), 'utf8').trim(); return text ? text.split('\n').map(JSON.parse) : []; }

test('blocklist remove somente contatos bloqueados, compacta os próximos e descarta apenas a linha sem contato restante', async t => {
    const directory = fixture(t), selected = flow(directory);
    const source = rows(4);
    source[0].phones = ['2198765432', '3198765432', '4198765432', '5198765432'];
    source[1].phones = ['2198765432', '6198765432'];
    source[2].phones = ['3198765432'];
    source[3].phones = [];
    selected.enrichment = { enabled: true, strategy: 'append' };
    const result = await runFlow({ flow: selected, user: { username: 'Davi' }, jobDir: directory, providers: {
        async *iterateReceita() { yield { rows: source }; },
        async queryEnrichment() { return [{ cnpj: source[3].cnpj, phones: ['7198765432'] }]; },
        async queryPhones() { return ['552198765432', '3198765432']; },
    } });
    assert.deepEqual(output(directory).map(row => row.phones), [['41998765432', '51998765432'], ['61998765432'], ['71998765432']]);
    assert.equal(result.counts.kept, 3);
    assert.equal(result.counts.blockedPhones, 4);
    assert.equal(result.counts.removedBlocklist, 0);
    assert.equal(result.counts.withoutPhonesAfterFilters, 1);
});

test('geração retoma no cursor confirmado e preserva limite, contadores e linhas UTF-8', async t => {
    const directory = fixture(t), selected = flow(directory), source = rows(2001);
    selected.generation.limit = 2001;
    let first = true, resumeCursor;
    const providers = {
        async *iterateReceita({ afterCnpj, filters }) {
            if (first) { yield { rows: source.slice(0, 2000), cursor: source[1999].cnpj }; first = false; throw new Error('Synthetic failure'); }
            resumeCursor = afterCnpj;
            assert.equal(filters.limit, 1);
            yield { rows: source.slice(2000), cursor: source[2000].cnpj };
        },
        async queryPhones() { return []; },
    };
    await assert.rejects(runFlow({ flow: selected, user: { username: 'Davi' }, jobDir: directory, providers }));
    fs.appendFileSync(path.join(directory, 'generation.jsonl.tmp'), '{unconfirmed tail');
    const result = await runFlow({ flow: selected, user: { username: 'Davi' }, jobDir: directory, providers });
    assert.equal(resumeCursor, source[1999].cnpj);
    assert.equal(result.counts.generated, 2001);
    assert.equal(output(directory, 'generation').length, 2001);
    assert.equal(output(directory, 'generation')[1999].razao_social, 'Sintética á😀');
});

test('enriquecimento retoma no lote seguinte, sem repetir consultas ou duplicar contatos', async t => {
    const directory = fixture(t), selected = flow(directory), source = rows(4001);
    selected.enrichment = { enabled: true, strategy: 'append' };
    let queries = 0, fail = true, resumedDocuments;
    const providers = {
        async *iterateReceita() { for (let i = 0; i < source.length; i += 2000) yield { rows: source.slice(i, i + 2000) }; },
        async queryEnrichment(documents) { queries++; if (queries === 3 && fail) throw new Error('Synthetic failure'); if (!fail) resumedDocuments = documents; return []; },
        async queryPhones() { return []; },
    };
    await assert.rejects(runFlow({ flow: selected, user: { username: 'Davi' }, jobDir: directory, providers }));
    fail = false;
    const updates = [];
    const result = await runFlow({ flow: selected, user: { username: 'Davi' }, jobDir: directory, providers, onUpdate: update => updates.push(update) });
    assert.deepEqual(resumedDocuments, [source[4000].cnpj]);
    assert.equal(queries, 4);
    assert.equal(result.counts.kept, 4001);
    assert.ok(updates.some(update => update.progress?.stage === 'enrichment' && update.progress.processed === 4000 && update.progress.total === 4001));
});

test('limpeza retoma com deduplicação persistida e descarta a cauda não confirmada', async t => {
    const directory = fixture(t), selected = flow(directory), source = rows(2002);
    source[2000].phones = source[0].phones;
    let queries = 0, fail = true;
    const providers = {
        async *iterateReceita() { yield { rows: source }; },
        async queryPhones() { queries++; if (queries === 2 && fail) throw new Error('Synthetic failure'); return []; },
    };
    await assert.rejects(runFlow({ flow: selected, user: { username: 'Davi' }, jobDir: directory, providers }));
    fs.appendFileSync(path.join(directory, 'cleaning.jsonl.tmp'), '{unconfirmed tail');
    fail = false;
    const result = await runFlow({ flow: selected, user: { username: 'Davi' }, jobDir: directory, providers });
    assert.equal(queries, 3);
    assert.equal(result.counts.kept, 2001);
    assert.equal(result.counts.repeatedPhones, 1);
    assert.equal(result.counts.withoutPhonesRepeatedOnly, 1);
    assert.equal(output(directory).length, 2001);
    assert.equal(new Set(output(directory).flatMap(row => row.phones)).size, 2001);
});

test('exportação preserva arquivos confirmados e retoma apenas o arquivo incompleto', async t => {
    const directory = fixture(t), selected = flow(directory), source = rows(5);
    selected.output.rowsPerFile = 2;
    let mapped = 0, fail = true;
    const formats = require('../src/main/flows/formats');
    const providers = {
        async *iterateReceita() { yield { rows: source }; },
        async queryPhones() { return []; },
        getFormat: formats.getFormat,
        mapOutputRow(...args) { if (++mapped === 4 && fail) throw new Error('Synthetic failure'); return formats.mapOutputRow(...args); },
    };
    await assert.rejects(runFlow({ flow: selected, user: { username: 'Davi' }, jobDir: directory, providers }));
    const saved = JSON.parse(fs.readFileSync(path.join(directory, 'export-cache.json')));
    assert.equal(saved.outputs.length, 1);
    const preserved = fs.readFileSync(saved.outputs[0].path);
    fail = false;
    const result = await runFlow({ flow: selected, user: { username: 'Davi' }, jobDir: directory, providers });
    assert.equal(result.outputs.length, 3);
    assert.deepEqual(fs.readFileSync(saved.outputs[0].path), preserved);
    assert.equal(result.outputs.reduce((sum, file) => sum + file.rows, 0), 5);
    assert.equal(mapped, 7);
});

test('encerrar o worker abruptamente preserva o último lote e suas reservas de deduplicação', async t => {
    const directory = fixture(t), selected = flow(directory), source = rows(2002);
    source[2000].phones = source[0].phones;
    await new Promise((resolve, reject) => {
        const worker = new Worker(`
            const {workerData,parentPort}=require('node:worker_threads');
            const {runFlow}=require(workerData.pipeline);
            let calls=0;
            runFlow({flow:workerData.flow,user:{username:'Davi'},jobDir:workerData.directory,
                providers:{async *iterateReceita(){yield {rows:workerData.source};},async queryPhones(){if(++calls>1)await new Promise(()=>{});return [];}},
                onUpdate(update){if(update.progress?.stage==='cleaning'&&update.progress.processed===2000)parentPort.postMessage('saved');},
            }).catch(error=>{throw error;});
        `, { eval: true, workerData: { pipeline: require.resolve('../src/main/flows/pipeline'), flow: selected, directory, source } });
        worker.once('message', () => { worker.terminate().then(resolve, reject); });
        worker.once('error', reject);
    });
    let calls = 0;
    const result = await runFlow({ flow: selected, user: { username: 'Davi' }, jobDir: directory, providers: {
        async *iterateReceita() { throw new Error('Generation must remain cached'); },
        async queryPhones() { calls++; return []; },
    } });
    assert.equal(calls, 1);
    assert.equal(result.counts.kept, 2001);
    assert.equal(result.counts.withoutPhonesRepeatedOnly, 1);
    assert.equal(output(directory).length, 2001);
});

test('API retoma lotes confirmados e reaproveita a metade bem sucedida do lote que falhou', async t => {
    const directory = fixture(t), selected = flow(directory), source = rows(5);
    selected.api = { enabled: true };
    let fail = true, clock = 1000000;
    const calls = [];
    const providers = {
        async *iterateReceita() { yield { rows: source }; },
        async queryPhones() { return []; },
        async acquireApi() { return {
            async consult(documents) { calls.push(...documents); if (documents.includes(source[3].cnpj) && fail) throw Object.assign(new Error('Synthetic failure'), { code: 'FLOW_VALIDATION' }); return new Set(documents); },
            async release() {},
        }; },
        apiTiming: { batchSize: 2, now: () => clock, sleep: async ms => { clock += ms; } },
    };
    await assert.rejects(runFlow({ flow: selected, user: { username: 'Davi' }, jobDir: directory, providers }));
    fail = false;
    const result = await runFlow({ flow: selected, user: { username: 'Davi' }, jobDir: directory, providers });
    assert.equal(result.counts.apiConsulted, 5);
    assert.equal(result.counts.apiAvailable, 5);
    assert.equal(result.counts.apiBatches, 3);
    assert.equal(result.counts.kept, 5);
    for (const row of source) assert.equal(calls.filter(id => id === row.cnpj).length, row === source[3] ? 2 : 1);
    assert.equal(output(directory, 'api').length, 5);
});

test('barra mede registros processados, e geração sem total conhecido permanece indeterminada', () => {
    assert.equal(model({ status: 'running', stage: 'cleaning', progress: { stage: 'cleaning', processed: 500, total: 2000 } }).percent, 25);
    assert.equal(model({ status: 'running', stage: 'generation', counts: { generated: 1000 } }).percent, null);
    assert.equal(model({ status: 'failed', stage: 'enrichment', progress: { stage: 'enrichment', processed: 10, total: 100 } }).paused, true);
    assert.equal(model({ status: 'empty', stage: 'export', progress: { stage: 'export', processed: 0, total: 0 } }).percent, 100);
});
