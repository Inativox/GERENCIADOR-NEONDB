const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const { Writable } = require('node:stream');
const { finished } = require('node:stream/promises');
const { runFlow } = require('../src/main/flows/pipeline');
const { createPackedWriter, writeData, writeBuffered } = require('../src/main/flows/packedJsonl');
const { entries, records } = require('../src/main/flows/jsonl');

function fixture(t) {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'flow-packed-test-'));
    t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
    return dir;
}
function source(count) {
    return Array.from({ length: count }, (_, i) => ({ cnpj: String(i + 1).padStart(14, '0'), razao_social: 'Empresa sintética á😀', cidade: 'São Paulo', logradouro: 'Rua de exemplo', phones: [`119${12340000 + i}`] }));
}
function flow(directory) {
    return { name: 'Synthetic', operation: 'c6', generation: {}, enrichment: { enabled: true, strategy: 'append' }, cleaning: { enabled: true, rootSource: 'none', blocklist: true, invalidPhones: false, removeLandlines: true, prohibitedCnaes: [] }, output: { directory, formatId: 'padrao', rowsPerFile: 1000, csv: true } };
}
const policy = { compressed: true, prune: true };
const checkpoint = directory => JSON.parse(fs.readFileSync(path.join(directory, 'checkpoint.json'), 'utf8'));

test('compressed frames preserve UTF-8 and resume inside a frame with much less disk space', async t => {
    const directory = fixture(t), file = path.join(directory, 'input.jsonl.pack'), rows = source(4100);
    const stream = fs.createWriteStream(file); const done = finished(stream);
    const writer = createPackedWriter(stream);
    for (const row of rows) await writer.append(row);
    await writer.flush(); stream.end(); await done;
    const rawBytes = Buffer.byteLength(rows.map(row => JSON.stringify(row) + '\n').join(''));
    assert.ok(fs.statSync(file).size < rawBytes / 4);
    assert.deepEqual(await Array.fromAsync(records(file)), rows);
    let cursor, n = 0;
    for await (const entry of entries(file)) { cursor = entry.offset; if (++n === 2501) break; }
    assert.deepEqual(await Array.fromAsync(records(file, null, cursor)), rows.slice(2501));
    fs.truncateSync(file, fs.statSync(file).size - 1);
    await assert.rejects(Array.fromAsync(records(file)), /incompleto/);
});

test('compressed generation resumes confirmed cursor and truncates a corrupt unconfirmed tail', async t => {
    const directory = fixture(t), selected = flow(directory), rows = source(2001);
    let first = true, after;
    const providers = {
        async *iterateReceita({ afterCnpj }) {
            if (first) { first = false; yield { rows: rows.slice(0, 2000), cursor: rows[1999].cnpj }; throw new Error('Synthetic failure'); }
            after = afterCnpj; yield { rows: rows.slice(2000), cursor: rows[2000].cnpj };
        },
        async queryEnrichment() { return []; }, async queryPhones() { return []; },
    };
    const args = { flow: selected, user: { username: 'Davi' }, jobDir: directory, providers, cachePolicy: policy };
    await assert.rejects(runFlow(args));
    fs.appendFileSync(path.join(directory, 'generation.jsonl.pack.tmp'), 'corrupt tail');
    const result = await runFlow(args);
    assert.equal(after, rows[1999].cnpj); assert.equal(result.counts.generated, 2001); assert.equal(result.counts.exported, 2001);
    assert.equal(fs.existsSync(path.join(directory, 'generation.jsonl.pack')), false);
    assert.equal(fs.existsSync(path.join(directory, 'cleaning.jsonl.pack')), false);
    assert.ok(result.outputs.every(output => fs.existsSync(output.path)));
    assert.ok(Object.values(checkpoint(directory).stages).filter(stage => stage.file).every(stage => stage.retired));
});

test('cleaning failure retains its source and dedup state while consumed generation is released', async t => {
    const directory = fixture(t), selected = flow(directory), rows = source(4001);
    let generationCalls = 0, enrichmentCalls = 0, cleaningCalls = 0, fail = true;
    const providers = {
        async *iterateReceita() { generationCalls++; for (let i = 0; i < rows.length; i += 2000) yield { rows: rows.slice(i, i + 2000) }; },
        async queryEnrichment() { enrichmentCalls++; return []; },
        async queryPhones() { if (++cleaningCalls === 2 && fail) throw new Error('Synthetic cleaning failure'); return []; },
    };
    const args = { flow: selected, user: { username: 'Davi' }, jobDir: directory, providers, cachePolicy: policy };
    await assert.rejects(runFlow(args));
    const before = checkpoint(directory);
    assert.equal(before.stages.generation.retired, true);
    assert.equal(fs.existsSync(before.stages.generation.file), false);
    assert.equal(fs.existsSync(before.stages.enrichment.file), true);
    const enrichmentBefore = enrichmentCalls; fail = false;
    const result = await runFlow(args);
    assert.equal(generationCalls, 1); assert.equal(enrichmentCalls, enrichmentBefore);
    assert.equal(result.counts.kept, 4001); assert.equal(result.counts.repeatedPhones, 0);
    assert.equal(result.counts.exported, 4001);
    assert.equal(fs.existsSync(before.stages.enrichment.file), false);
});

test('export resumes from a row inside a compressed frame and retains confirmed final files', async t => {
    const directory = fixture(t), rows = source(2101), controller = new AbortController();
    let generationCalls = 0;
    const providers = { async *iterateReceita() { generationCalls++; yield { rows }; }, async queryEnrichment() { return []; }, async queryPhones() { return []; } };
    const args = { flow: flow(directory), user: { username: 'Davi' }, jobDir: directory, providers, cachePolicy: policy };
    await assert.rejects(runFlow({ ...args, signal: controller.signal, onUpdate: update => { if (update.stage === 'export' && update.progress?.processed >= 1000) controller.abort(); } }), { code: 'FLOW_CANCELLED' });
    const before = checkpoint(directory), exportCache = JSON.parse(fs.readFileSync(path.join(directory, 'export-cache.json'), 'utf8'));
    assert.equal(exportCache.processed, 1000); assert.equal(exportCache.inputOffset.row, 1000);
    assert.equal(fs.existsSync(before.stages.cleaning.file), true);
    const paths = exportCache.outputs.map(output => output.path);
    const result = await runFlow(args);
    assert.equal(generationCalls, 1); assert.equal(result.counts.exported, 2101);
    assert.ok(paths.every(file => result.outputs.some(output => output.path === file)));
    const csvLines = result.outputs.filter(output => output.kind === 'csv').flatMap(output => fs.readFileSync(output.path, 'utf8').trim().split('\n').slice(1));
    assert.equal(csvLines.length, 2101); assert.equal(new Set(csvLines).size, 2101);
});

test('an already failed disk stream rejects the next write instead of waiting forever', async () => {
    const error = Object.assign(new Error('Synthetic disk full'), { code: 'ENOSPC' });
    const stream = new Writable({ write(chunk, encoding, callback) { setImmediate(() => callback(error)); } });
    const done = finished(stream).catch(() => {});
    await assert.rejects(writeData(stream, 'first'), { code: 'ENOSPC' }); await done;
    await assert.rejects(writeData(stream, 'second'), { code: 'ENOSPC' });
    await assert.rejects(writeBuffered(stream, 'second'), { code: 'ENOSPC' });
});

test('compressed cache disk failure is explicit, bounded and leaves the stage resumable', async t => {
    const directory = fixture(t), selected = flow(directory), create = fs.createWriteStream;
    t.after(() => { fs.createWriteStream = create; });
    fs.createWriteStream = function(file, options) {
        const stream = create.call(this, file, options);
        if (String(file).endsWith('generation.jsonl.pack.tmp')) setImmediate(() => stream.destroy(Object.assign(new Error('Synthetic disk full'), { code: 'ENOSPC' })));
        return stream;
    };
    const providers = { async *iterateReceita() { yield { rows: source(2) }; }, async queryEnrichment() { return []; }, async queryPhones() { return []; } };
    const args = { flow: selected, user: { username: 'Davi' }, jobDir: directory, providers, cachePolicy: policy };
    await assert.rejects(runFlow(args), { code: 'FLOW_DISK_FULL' });
    fs.createWriteStream = create;
    assert.equal((await runFlow(args)).counts.exported, 2);
});

test('an existing uncompressed checkpoint resumes under the new policy without regenerating', async t => {
    const directory = fixture(t), rows = source(3); let generated = 0, fail = true;
    const providers = { async *iterateReceita() { generated++; yield { rows }; }, async queryEnrichment() { if (fail) throw new Error('Synthetic failure'); return []; }, async queryPhones() { return []; } };
    const args = { flow: flow(directory), user: { username: 'Davi' }, jobDir: directory, providers };
    await assert.rejects(runFlow(args));
    assert.ok(fs.existsSync(path.join(directory, 'generation.jsonl')));
    fail = false;
    const result = await runFlow({ ...args, cachePolicy: policy });
    assert.equal(generated, 1); assert.equal(result.counts.exported, 3);
    assert.equal(fs.existsSync(path.join(directory, 'generation.jsonl')), false);
});

test('compressed API resumes inside frames and retains the successful half of a failed round', async t => {
    const directory = fixture(t), selected = flow(directory), rows = source(5);
    selected.api = { enabled: true }; let fail = true, clock = 1000000; const calls = [];
    const providers = {
        async *iterateReceita() { yield { rows }; }, async queryEnrichment() { return []; }, async queryPhones() { return []; },
        async acquireApi() { return {
            async consult(documents) { calls.push(...documents); if (fail && documents.includes(rows[3].cnpj)) throw Object.assign(new Error('Synthetic failure'), { code: 'FLOW_VALIDATION' }); return new Set(documents); }, async release() {},
        }; }, apiTiming: { batchSize: 2, now: () => clock, sleep: async ms => { clock += ms; } },
    };
    const args = { flow: selected, user: { username: 'Davi' }, jobDir: directory, providers, cachePolicy: policy };
    await assert.rejects(runFlow(args)); fail = false;
    const result = await runFlow(args);
    assert.equal(result.counts.apiConsulted, 5); assert.equal(result.counts.exported, 5);
    rows.forEach((row, i) => assert.equal(calls.filter(id => id === row.cnpj).length, i === 3 ? 2 : 1));
    assert.equal(fs.existsSync(path.join(directory, 'api-results')), false);
});

test('automatic cache cleanup preserves exports even if output was selected inside a cache directory', async t => {
    const directory = fixture(t), output = path.join(directory, 'stage-cache', 'cleaning');
    const args = { flow: flow(output), user: { username: 'Davi' }, jobDir: directory, cachePolicy: policy,
        providers: { async *iterateReceita() { yield { rows: source(2) }; }, async queryEnrichment() { return []; }, async queryPhones() { return []; } } };
    const result = await runFlow(args);
    assert.equal(result.counts.exported, 2);
    assert.ok(result.outputs.every(file => fs.existsSync(file.path)));
});

for (const compressed of [false, true]) {
    for (const interruptedStage of ['enrichment', 'cleaning']) {
        test(`${interruptedStage} resumes a ${compressed ? 'compressed' : 'legacy'} 2000-row checkpoint with 50000-row batches`, async t => {
            const directory = fixture(t), selected = flow(directory), rows = source(52001);
            selected.output.rowsPerFile = 60000;
            let generated = 0, calls = 0, fail = true;
            const resumedDocuments = [], progress = [];
            const query = documents => {
                if (fail && ++calls === 2) throw new Error('Synthetic interruption');
                if (!fail) resumedDocuments.push(...documents);
                return [];
            };
            const providers = {
                async *iterateReceita() { generated++; for (let i = 0; i < rows.length; i += 2000) yield { rows: rows.slice(i, i + 2000) }; },
                async queryEnrichment(documents) { return interruptedStage === 'enrichment' ? query(documents) : []; },
                async queryPhones(kind, phones) { return interruptedStage === 'cleaning' ? query(phones) : []; },
            };
            const args = { flow: selected, user: { username: 'Davi' }, jobDir: directory, providers, cachePolicy: { compressed, prune: true } };
            await assert.rejects(runFlow(args));
            fail = false;
            const result = await runFlow({ ...args, processingPolicy: { batchSize: 50000 }, onUpdate(update) {
                if (update.progress?.stage === interruptedStage) progress.push(update.progress.processed);
            } });
            assert.equal(generated, 1);
            assert.ok(progress.includes(2000));
            assert.ok(progress.includes(52000));
            assert.ok(progress.includes(52001));
            if (interruptedStage === 'enrichment') assert.deepEqual(resumedDocuments, rows.slice(2000).map(row => row.cnpj));
            assert.equal(result.counts.kept, rows.length);
            assert.equal(result.counts.exported, rows.length);
            assert.equal(result.counts.repeatedDocuments, 0);
            assert.equal(result.counts.repeatedPhones, 0);
            const lines = result.outputs.filter(output => output.kind === 'csv').flatMap(output => fs.readFileSync(output.path, 'utf8').trim().split('\n').slice(1));
            assert.equal(lines.length, rows.length);
            assert.equal(new Set(lines).size, rows.length);
        });
    }
}
