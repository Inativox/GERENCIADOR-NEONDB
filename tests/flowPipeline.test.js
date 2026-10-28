'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const ExcelJS = require('exceljs');
const { runFlow, queryEnrichment, queryPhones } = require('../src/main/flows/pipeline');

const headers = ['cnpj', 'nome', 'fone1', 'fone2', 'livre6', 'situacao_cadastral'];
const flow = () => ({ id: 'flow', name: 'Fixture', operation: 'c6', generation: { limit: 100, situacoes: ['02'] },
    enrichment: { enabled: false, strategy: 'append', fillCpf: true },
    cleaning: { enabled: true, rootSource: 'none', blocklist: true, invalidPhones: false, removeLandlines: false, prohibitedCnaes: [] },
    output: { formatId: 'fixture', csv: true, rowsPerFile: 2, includeSituacao: true } });
function providers(rows, extra = {}) {
    return { async *iterateReceita({ filters, signal }) {
        assert.deepEqual(filters.situacoes, ['02']);
        for (let start = 0; start < rows.length; start += 2) {
            if (signal?.aborted) throw new Error('aborted');
            yield { rows: rows.slice(start, start + 2), cursor: rows[start]?.cnpj };
        }
    }, queryPhones: async () => [], getFormat: () => ({ colunas: headers }),
    mapOutputRow: row => [row.cnpj, row.nome, row.phones[0], row.phones[1], row.livre6, row.situacao_cadastral], ...extra };
}
async function setup(t) {
    const jobDir = await fs.mkdtemp(path.join(os.tmpdir(), 'flow-pipeline-'));
    t.after(() => fs.rm(jobDir, { force: true, recursive: true }));
    return jobDir;
}
const row = (id, phones, extra = {}) => ({ cnpj: String(id).padStart(14, '0'), nome: 'Empresa', phones, situacao_cadastral: '02', ...extra });
async function readXlsx(file) { const workbook = new ExcelJS.Workbook(); await workbook.xlsx.readFile(file); return workbook.worksheets[0]; }

test('pipeline preserves Receita situation and text documents, splits output, escapes CSV, resumes stages', async t => {
    const jobDir = await setup(t);
    const source = [row(1, ['5511999990001'], { nome: '=HYPERLINK("x";"y")\ntexto' }), row(2, ['11999990002']), row(3, ['11999990003'])];
    let generationCalls = 0;
    const fake = providers(source);
    const iterate = fake.iterateReceita;
    const config = flow(); config.output.fileName = 'lista rca';
    config.generation.availability = 'available';
    fake.iterateReceita = args => { assert.equal(args.filters.availability, 'available'); generationCalls++; return iterate(args); };
    const args = { flow: config, user: { username: 'Operador' }, jobDir, providers: fake };
    const result = await runFlow(args);
    assert.equal(result.status, 'completed'); assert.equal(result.counts.generated, 3); assert.equal(result.counts.kept, 3);
    assert.deepEqual(result.outputs.map(output => output.rows), [2, 2, 1, 1]);
    assert.deepEqual(result.outputs.map(output => path.basename(output.path)), ['lista rca parte1.xlsx', 'lista rca parte1.csv', 'lista rca parte2.xlsx', 'lista rca parte2.csv']);
    const sheet = await readXlsx(result.outputs[0].path);
    assert.equal(sheet.getCell('A2').value, '00000000000001'); assert.equal(sheet.getCell('C2').value, '11999990001');
    assert.equal(sheet.getCell('F2').value, '02'); assert.equal(sheet.getCell('B2').value[0], "'");
    const csv = await fs.readFile(result.outputs[1].path, 'utf8');
    assert.ok(csv.startsWith('\uFEFF"cnpj";')); assert.ok(csv.includes('"\'=HYPERLINK(""x"";""y"")\ntexto"'));
    const before = await fs.readFile(path.join(jobDir, 'generation.jsonl'), 'utf8');
    const resumed = await runFlow(args);
    assert.deepEqual(resumed.outputs, result.outputs); assert.equal(generationCalls, 1);
    assert.equal(await fs.readFile(path.join(jobDir, 'generation.jsonl'), 'utf8'), before);
});
test('custom layout renamed phones reserve only exported contacts and preserve order in XLSX/CSV', async t => {
    const { validateLayout } = require('../src/main/flows/layouts');
    const { getFormat, mapOutputRow } = require('../src/main/flows/formats');
    const layout = validateLayout({ nome: 'Pronto', colunas: [{ header: 'Campanha', campo: 'manual', valor_manual: '=Campanha;A' }, { header: 'Contato principal', campo: 'telefone_1' }, { header: 'Documento', campo: 'cnpj' }, { header: 'Local', campo: 'composto', partes: ['estado', 'cidade'], sep: ' / ' }] });
    const jobDir = await setup(t), config = flow();
    config.output.formatId = layout.id;
    config.output.includeSituacao = false;
    config.output.formatSnapshot = getFormat(layout.id, { includeSituacao: false }, [layout]);
    config.output.rowsPerFile = 100;
    const source = [row(1, ['11999990001', '11999990002'], { estado: 'SP', cidade: 'Campinas' }), row(2, ['11999990002']), row(3, ['11999990001', '11999990003'])];
    const result = await runFlow({ flow: config, user: { username: 'Davi' }, jobDir, providers: providers(source, { getFormat, mapOutputRow }) });
    assert.equal(result.counts.kept, 3);
    const sheet = await readXlsx(result.outputs.find(output => output.kind === 'xlsx').path);
    assert.deepEqual(sheet.getRow(1).values.slice(1), ['Campanha', 'Contato principal', 'Documento', 'Local']);
    assert.deepEqual(sheet.getRow(2).values.slice(1), ["'=Campanha;A", '11999990001', '00000000000001', 'SP / Campinas']);
    assert.equal(sheet.getCell('B3').value, '11999990002');
    assert.equal(sheet.getCell('B4').value, '11999990003');
    const csv = await fs.readFile(result.outputs.find(output => output.kind === 'csv').path, 'utf8');
    assert.ok(csv.startsWith('\uFEFF"Campanha";"Contato principal";"Documento";"Local"'));
    assert.ok(csv.includes('"11999990002";"00000000000002"'));
});

test('root/blocklist/invalid-phone filtering precede reservation across all batches', async t => {
    const jobDir = await setup(t);
    const config = flow(); config.cleaning.rootSource = 'bq'; config.cleaning.invalidPhones = true;
    const blocked = '11999990001'; const invalid = '11999990002'; const kept = '11999990003';
    const source = [row(1, [blocked, kept]), row(2, [kept]), row(1, [invalid, '11999990004']),
        row(3, ['11999990005']), row(4, [kept, '11999990006']), row(2, ['11999990007'])];
    const result = await runFlow({ flow: config, user: { username: 'Operador' }, jobDir,
        rootFile: { documents: ['00000000000003'], info: { source: 'bq' } },
        providers: providers(source, { queryPhones: async kind => kind === 'blocklist' ? [`55${blocked}`] : [invalid] }) });
    assert.equal(result.counts.kept, 3); assert.equal(result.counts.blockedPhones, 1); assert.equal(result.counts.removedBlocklist, 0); assert.equal(result.counts.removedRoot, 1);
    assert.equal(result.counts.invalidPhones, 1); assert.equal(result.counts.repeatedDocuments, 1); assert.equal(result.counts.repeatedPhones, 2);
    assert.equal(result.counts.withoutPhonesRepeatedOnly, 1);
    const clean = (await fs.readFile(path.join(jobDir, 'cleaning.jsonl'), 'utf8')).trim().split('\n').map(JSON.parse);
    assert.deepEqual(clean.map(record => record.cnpj), ['00000000000001', '00000000000004', '00000000000002']);
    assert.deepEqual(clean.map(record => record.phones), [[kept], ['11999990006'], ['11999990007']]);
});

test('mandatory cleaning survives disabled optional rules and only authenticated exact Davi bypasses blocklist', async t => {
    const jobDir = await setup(t); const config = flow();
    Object.assign(config.cleaning, { enabled: false, blocklist: false, invalidPhones: true, removeLandlines: true, rootSource: 'bq', prohibitedCnaes: ['999'] });
    let calls = [];
    const source = [row(1, ['551130001234'], { cnae: '999' }), row(2, ['bad']), row(3, ['11999990002'])];
    const fake = providers(source, { queryPhones: async kind => { calls.push(kind); return ['11999990002']; } });
    const result = await runFlow({ flow: config, user: { username: 'davi' }, jobDir, providers: fake });
    assert.equal(result.counts.kept, 1); assert.equal(result.counts.blockedPhones, 1); assert.equal(result.counts.withoutPhones, 2);
    assert.ok(calls.every(kind => kind === 'blocklist'));
    const other = await fs.mkdtemp(path.join(jobDir, 'davi-')); calls = [];
    const davi = await runFlow({ flow: config, user: { username: 'Davi' }, jobDir: other, providers: fake });
    assert.equal(davi.counts.kept, 2); assert.deepEqual(calls, []);
});

for (const strategy of ['append', 'overwrite', 'ignore']) {
    test(`enrichment ${strategy}, partner CPF and Receita fields`, async t => {
        const jobDir = await setup(t); const config = flow();
        config.enrichment = { enabled: true, strategy, fillCpf: true };
        const source = [row(1, ['11999990001']), row(2, [])];
        const result = await runFlow({ flow: config, user: { username: 'Davi' }, jobDir,
            providers: providers(source, { queryEnrichment: async documents => documents.map((cnpj, index) => ({ cnpj, empresa_phones: [`1199999000${index + 2}`], socio_cpfs: ['1234567890'] })) }) });
        assert.equal(result.counts.kept, 2);
        const rows = (await fs.readFile(path.join(jobDir, 'enrichment.jsonl'), 'utf8')).trim().split('\n').map(JSON.parse);
        assert.deepEqual(rows[0].phones, strategy === 'append' ? ['11999990001', '11999990002'] : strategy === 'overwrite' ? ['11999990002'] : ['11999990001']);
        assert.equal(rows[0].livre6, '01234567890'); assert.equal(rows[0].situacao_cadastral, '02');
    });
}

test('failed enrichment resumes confirmed generation and never exposes database errors', async t => {
    const jobDir = await setup(t); const config = flow(); config.enrichment.enabled = true;
    let generated = 0; let fail = true;
    const fake = providers([row(1, ['11999990001'])], { async *iterateReceita() { generated++; yield { rows: [row(1, ['11999990001'])] }; },
        queryEnrichment: async () => { if (fail) throw new Error('postgres://user:secret@host database detail'); return []; } });
    const args = { flow: config, user: { username: 'Davi' }, jobDir, providers: fake };
    await assert.rejects(runFlow(args), error => error.code === 'FLOW_FAILED' && !error.message.includes('secret'));
    const checkpoint = JSON.parse(await fs.readFile(path.join(jobDir, 'checkpoint.json'), 'utf8'));
    assert.deepEqual(Object.keys(checkpoint.stages), ['generation']);
    const input = await fs.readFile(path.join(jobDir, 'generation.jsonl'), 'utf8');
    fail = false; const result = await runFlow(args);
    assert.equal(result.counts.kept, 1); assert.equal(generated, 1); assert.equal(await fs.readFile(path.join(jobDir, 'generation.jsonl'), 'utf8'), input);
});

test('cancelled cleaning resumes its confirmed batches, empty results create no final workbook', async t => {
    const jobDir = await setup(t); const config = flow(); const controller = new AbortController(); let cancel = true;
    const fake = providers([row(1, ['11999990001']), row(2, ['11999990002'])], { queryPhones: async () => { if (cancel) controller.abort(); return ['11999990001', '11999990002']; } });
    const args = { flow: config, user: { username: 'Operador' }, jobDir, providers: fake };
    await assert.rejects(runFlow({ ...args, signal: controller.signal }), error => error.code === 'FLOW_CANCELLED');
    const checkpoint = JSON.parse(await fs.readFile(path.join(jobDir, 'checkpoint.json'), 'utf8'));
    assert.deepEqual(Object.keys(checkpoint.stages), ['generation', 'enrichment']);
    cancel = false; const result = await runFlow(args);
    assert.equal(result.status, 'empty'); assert.equal(result.counts.blockedPhones, 2); assert.equal(result.counts.withoutPhonesAfterFilters, 2); assert.deepEqual(result.outputs, []);
    assert.deepEqual(await fs.readdir(path.join(jobDir, 'outputs')), []);
});

test('export failure preserves confirmed parts and retries without regenerating or duplicate rows', async t => {
    const jobDir = await setup(t); const config = flow(); config.output.rowsPerFile = 1;
    config.output.fileName = 'lista rca';
    let fail = true; let maps = 0;
    const fake = providers([row(1, ['11999990001']), row(2, ['11999990002'])]);
    const original = fake.mapOutputRow;
    fake.mapOutputRow = (...args) => { maps++; if (fail && maps === 2) throw new Error('fixture write failure'); return original(...args); };
    const args = { flow: config, user: { username: 'Davi' }, jobDir, providers: fake };
    await assert.rejects(runFlow(args), /salvar os arquivos finais/);
    const confirmed = await fs.readdir(path.join(jobDir, 'outputs'));
    assert.equal(confirmed.length, 2); assert.ok(confirmed.every(file => /\.(xlsx|csv)$/.test(file)));
    assert.deepEqual(Object.keys(JSON.parse(await fs.readFile(path.join(jobDir, 'checkpoint.json'), 'utf8')).stages), ['generation', 'enrichment', 'cleaning']);
    fail = false; const result = await runFlow(args);
    assert.equal(result.outputs.length, 4); assert.deepEqual(result.outputs.map(output => output.rows), [1, 1, 1, 1]);
    assert.deepEqual(result.outputs.map(output => path.basename(output.path)), ['lista rca parte1.xlsx', 'lista rca parte1.csv', 'lista rca parte2.xlsx', 'lista rca parte2.csv']);
    assert.equal(maps, 3); assert.ok(confirmed.every(file => result.outputs.some(output => path.basename(output.path) === file)));
});

test('uncertain Fastway upload resumes at the pending file without regenerating or resending confirmed parts', async t => {
    const jobDir = await setup(t); const config = flow(); config.output.rowsPerFile = 1;
    config.output.fileName = 'lista rca'; config.fastway = { enabled: true, campaignId: '120' };
    let generationCalls = 0; let fail = true; const sent = [];
    const fake = providers([row(1, ['11999990001']), row(2, ['11999990002'])]);
    const iterate = fake.iterateReceita;
    fake.iterateReceita = args => { generationCalls++; return iterate(args); };
    fake.uploadMailing = async ({ file }) => {
        if (fail && sent.length === 1) throw new Error('socket hang up');
        sent.push(path.basename(file)); return { rows: 1 };
    };
    const args = { flow: config, user: { username: 'Davi' }, jobDir, providers: fake, cachePolicy: { prune: true } };
    await assert.rejects(runFlow(args));
    assert.deepEqual(sent, ['lista rca parte1.csv']);
    await assert.rejects(runFlow(args), error => error.code === 'FLOW_VALIDATION' && /não confirmou lista rca parte2\.csv/.test(error.message));
    fail = false; const result = await runFlow({ ...args, retryUncertainUpload: true });
    assert.equal(result.status, 'completed'); assert.equal(generationCalls, 1);
    assert.deepEqual(sent, ['lista rca parte1.csv', 'lista rca parte2.csv']);
});
function fastwayFixture(t, uploadMailing, findMailing) {
    const config = flow(); config.output.rowsPerFile = 1;
    config.output.fileName = 'lista rca'; config.fastway = { enabled: true, campaignId: '120' };
    const fake = providers([row(1, ['11999990001']), row(2, ['11999990002'])]);
    const logs = [], waits = [];
    Object.assign(fake, { uploadMailing, findMailing, wait: async ms => { waits.push(ms); } });
    return { logs, waits, args: jobDir => ({ flow: config, user: { username: 'Davi' }, jobDir, providers: fake, onUpdate: data => data.log && logs.push(data.log) }) };
}
test('Fastway answer lost after import: the listed mailing confirms the file without resending it', async t => {
    const jobDir = await setup(t); const sent = [], asked = [];
    const fixture = fastwayFixture(t, async ({ file }) => {
        sent.push(path.basename(file));
        if (sent.length === 2) throw new Error('socket hang up');
        return { rows: 1 };
    }, async ({ file, campaignId, since }) => {
        asked.push({ file: path.basename(file), campaignId, since });
        return [{ id: 33256, rows: 1, status: 'ATIVO' }];
    });
    const result = await runFlow(fixture.args(jobDir));
    assert.equal(result.status, 'completed');
    assert.deepEqual(sent, ['lista rca parte1.csv', 'lista rca parte2.csv']);
    assert.deepEqual(fixture.waits, [300000]);
    assert.equal(asked.length, 1); assert.equal(asked[0].file, 'lista rca parte2.csv'); assert.equal(asked[0].campaignId, '120');
    assert.ok(Number.isFinite(Date.parse(asked[0].since)));
    assert.ok(fixture.logs.some(log => /não respondeu, mas recebeu lista rca parte2\.csv \(mailing 33256\)/.test(log)));
});
test('resume after an unconfirmed upload skips the file the Fastway already lists', async t => {
    const jobDir = await setup(t); const sent = []; let listed = [];
    const fixture = fastwayFixture(t, async ({ file }) => {
        sent.push(path.basename(file));
        if (sent.length === 2) throw new Error('socket hang up');
        return { rows: 1 };
    }, async () => listed);
    await assert.rejects(runFlow(fixture.args(jobDir)));
    listed = [{ id: 40001, rows: 1, status: 'ATIVO' }];
    const result = await runFlow(fixture.args(jobDir));
    assert.equal(result.status, 'completed');
    assert.deepEqual(sent, ['lista rca parte1.csv', 'lista rca parte2.csv']);
    assert.ok(fixture.logs.some(log => /já tinha recebido lista rca parte2\.csv \(mailing 40001\); o arquivo não será reenviado/.test(log)));
    const checkpoint = JSON.parse(await fs.readFile(path.join(jobDir, 'checkpoint.json'), 'utf8'));
    assert.equal(checkpoint.stages.upload.pending, undefined);
});
test('resume after an unconfirmed upload resends the file the Fastway does not list, without asking', async t => {
    const jobDir = await setup(t); const sent = [];
    const fixture = fastwayFixture(t, async ({ file }) => {
        sent.push(path.basename(file));
        if (sent.length === 2) throw new Error('socket hang up');
        return { rows: 1 };
    }, async () => []);
    await assert.rejects(runFlow(fixture.args(jobDir)));
    const result = await runFlow(fixture.args(jobDir));
    assert.equal(result.status, 'completed');
    assert.deepEqual(sent, ['lista rca parte1.csv', 'lista rca parte2.csv', 'lista rca parte2.csv']);
    assert.ok(fixture.logs.some(log => /não recebeu lista rca parte2\.csv; o arquivo será reenviado/.test(log)));
});
test('resume still asks for authorization when the Fastway mailing list cannot be read', async t => {
    const jobDir = await setup(t); const sent = [];
    const fixture = fastwayFixture(t, async ({ file }) => {
        sent.push(path.basename(file));
        if (sent.length === 2) throw new Error('socket hang up');
        return { rows: 1 };
    }, async () => { throw new Error('HTTP 500'); });
    await assert.rejects(runFlow(fixture.args(jobDir)));
    await assert.rejects(runFlow(fixture.args(jobDir)), error => error.code === 'FLOW_VALIDATION' && /não foi possível consultar os mailings recebidos/.test(error.message));
    assert.deepEqual(sent, ['lista rca parte1.csv', 'lista rca parte2.csv']);
    const result = await runFlow({ ...fixture.args(jobDir), retryUncertainUpload: true });
    assert.equal(result.status, 'completed');
    assert.deepEqual(sent, ['lista rca parte1.csv', 'lista rca parte2.csv', 'lista rca parte2.csv']);
});
test('rejected Fastway login waits and resends the same file without stopping the flow', async t => {
    const jobDir = await setup(t); const config = flow(); config.output.rowsPerFile = 1;
    config.output.fileName = 'lista rca'; config.fastway = { enabled: true, campaignId: '120' };
    const attempts = []; const waits = []; const logs = [];
    const fake = providers([row(1, ['11999990001']), row(2, ['11999990002'])]);
    fake.wait = async ms => { waits.push(ms); };
    fake.uploadMailing = async ({ file }) => {
        attempts.push(path.basename(file));
        if (attempts.length === 2) throw Object.assign(new Error('HTTP 481'), { code: 'FLOW_VALIDATION', loginRejected: true });
        return { rows: 1 };
    };
    const result = await runFlow({ flow: config, user: { username: 'Davi' }, jobDir, providers: fake, onUpdate: data => data.log && logs.push(data.log) });
    assert.equal(result.status, 'completed'); assert.deepEqual(waits, [60000]);
    assert.deepEqual(attempts, ['lista rca parte1.csv', 'lista rca parte2.csv', 'lista rca parte2.csv']);
    assert.ok(logs.some(log => /recusou o login ao enviar lista rca parte2\.csv \(tentativa 1\)/.test(log)));
});
test('Fastway login rejected beyond the retry limit stops with the file pending', async t => {
    const jobDir = await setup(t); const config = flow(); config.output.rowsPerFile = 2;
    config.output.fileName = 'lista rca'; config.fastway = { enabled: true, campaignId: '120' };
    let attempts = 0;
    const fake = providers([row(1, ['11999990001'])]);
    fake.wait = async () => {};
    fake.uploadMailing = async () => { attempts++; throw Object.assign(new Error('HTTP 481'), { code: 'FLOW_VALIDATION', loginRejected: true }); };
    await assert.rejects(runFlow({ flow: config, user: { username: 'Davi' }, jobDir, providers: fake }), /HTTP 481/);
    assert.equal(attempts, 6);
    const checkpoint = JSON.parse(await fs.readFile(path.join(jobDir, 'checkpoint.json'), 'utf8'));
    assert.equal(path.basename(checkpoint.stages.upload.pending), 'lista rca parte1.csv');
});
test('generation reads up to 50000 rows without query timeouts and exports the returned company', async t => {
    const jobDir = await setup(t);
    const ConnectionParameters = require('pg/lib/connection-parameters');
    const { iterateReceita } = require('../src/main/flows/receita');
    const fake = providers([], {
        createPool(settings) {
            const effective = new ConnectionParameters(settings);
            assert.ok(!effective.statement_timeout);
            assert.ok(!effective.query_timeout);
            assert.match(effective.options, /statement_timeout=0$/);
            return { on() {}, async end() {}, async query(sql) {
                if (sql.includes('information_schema')) return { rows: [
                    {column_name:'cnpj',data_type:'text'}, {column_name:'razao_social',data_type:'text'},
                    {column_name:'situacao_cadastral_cod',data_type:'text'}, {column_name:'natureza_juridica_cod',data_type:'text'}, {column_name:'telefone_principal',data_type:'text'}
                ] };
                return { rows: [{cnpj:'00000000000001',razao_social:'Sintetica',situacao_cadastral_cod:'02',telefone_principal:'11999990001'}] };
            } };
        },
        async *iterateReceita(args) {
            assert.equal(args.batchSize, 50000);
            yield* iterateReceita(args);
        }
    });
    const result = await runFlow({ flow: flow(), user: {username:'Davi'}, jobDir, providers: fake,
        connections: {receita:'postgresql://fixture@localhost/source?statement_timeout=60000&query_timeout=30000'}, processingPolicy:{batchSize:100000} });
    assert.equal(result.status, 'completed');
    assert.equal(result.counts.generated, 1);
    const sheet = await readXlsx(result.outputs.find(output=>output.kind==='xlsx').path);
    assert.equal(sheet.getCell('A2').value, '00000000000001');
    assert.equal(sheet.getCell('C2').value, '11999990001');
});

for (const extension of ['xlsx', 'csv']) {
    test(`named export preserves an existing ${extension} and can resume after it is moved`, async t => {
        const jobDir = await setup(t), config = flow(); config.output.fileName = 'lista rca';
        const directory = path.join(jobDir, 'outputs'); await fs.mkdir(directory);
        const existing = path.join(directory, `lista rca parte1.${extension}`);
        await fs.writeFile(existing, 'Existing campaign');
        let generated = 0;
        const fake = providers([row(1, ['11999990001'])]);
        const iterate = fake.iterateReceita;
        fake.iterateReceita = args => { generated++; return iterate(args); };
        const args = { flow: config, user: { username: 'Davi' }, jobDir, providers: fake };
        for (let attempt = 0; attempt < 2; attempt++) {
            await assert.rejects(runFlow(args), { code: 'FLOW_VALIDATION', message: /Já existe um arquivo/ });
            assert.equal(await fs.readFile(existing, 'utf8'), 'Existing campaign');
            assert.deepEqual(await fs.readdir(directory), [path.basename(existing)]);
        }
        await fs.rename(existing, path.join(directory, `arquivo anterior.${extension}`));
        const result = await runFlow(args);
        assert.equal(generated, 1); assert.equal(result.counts.exported, 1);
        assert.equal((await readXlsx(result.outputs[0].path)).getCell('A2').value, '00000000000001');
        assert.equal(await fs.readFile(path.join(directory, `arquivo anterior.${extension}`), 'utf8'), 'Existing campaign');
    });
}

test('bounded pg lookups are read-only and parameterized', async () => {
    const calls = []; const pool = { async query(sql, args) { calls.push({ sql, args }); return { rows: [] }; } };
    const documents = ["00000000000001' OR true --"];
    await queryEnrichment(pool, documents); await queryPhones(pool, 'invalid', ['11999990002']);
    // 1.8.26: a blocklist saiu do Postgres; pedir por aqui é erro, nunca consulta ao Neon.
    await assert.rejects(queryPhones(pool, 'blocklist', ['11999990001']), /BigQuery/);
    assert.equal(calls.length, 2);
    assert.ok(calls.every(call => /^SELECT/.test(call.sql) && call.sql.includes('ANY($1::text[])')));
    assert.ok(!calls[0].sql.includes(documents[0])); assert.deepEqual(calls[0].args, [documents]);
    assert.ok(calls[0].sql.includes('socios')); assert.ok(calls[1].sql.includes('telefones_invalidos'));
});

test('owned pools close on failed generation and secrets stay out of checkpoint', async t => {
    const jobDir = await setup(t); const config = flow(); let closed = 0;
    const fake = providers([], { createPool: () => ({ on() {}, end: async () => { closed++; } }),
        async *iterateReceita() { throw new Error('private connection secret'); } });
    await assert.rejects(runFlow({ flow: config, user: { username: 'Davi' }, jobDir, connections: { receita: 'postgres://secret' }, providers: fake }), /base da Receita/);
    assert.equal(closed, 1); assert.ok(!(await fs.readFile(path.join(jobDir, 'checkpoint.json'), 'utf8')).includes('secret'));
});

test('real format snapshot integrates headers and canonical Receita CNAE filter', async t => {
    const jobDir = await setup(t); const config = flow();
    const { getFormat } = require('../src/main/flows/formats');
    config.output.formatId = 'padrao'; config.output.formatSnapshot = getFormat('padrao');
    config.cleaning.prohibitedCnaes = ['9999999'];
    const fake = providers([row(1, ['11999990001'], { atividade_principal_cod: '9999999' }),
        row(2, ['11999990002'], { razao_social: 'Empresa correta', situacao_cadastral_cod: '02', situacao_cadastral: 'Ativa', situacao_cadastral_data: '2026-01-02' })]);
    delete fake.getFormat; delete fake.mapOutputRow;
    const result = await runFlow({ flow: config, user: { username: 'Operador' }, jobDir, providers: fake });
    assert.equal(result.counts.removedCnae, 1); assert.equal(result.counts.kept, 1);
    const sheet = await readXlsx(result.outputs[0].path);
    assert.deepEqual(sheet.getRow(1).values.slice(1), config.output.formatSnapshot.colunas.map(column => column.header));
    const cnpjIndex = config.output.formatSnapshot.colunas.findIndex(column => column.campo.startsWith('cnpj')) + 1;
    assert.equal(sheet.getRow(2).getCell(cnpjIndex).value, '00000000000002');
    const situationIndex = config.output.formatSnapshot.colunas.findIndex(column => column.campo === 'situacao_cadastral') + 1;
    assert.equal(sheet.getRow(2).getCell(situationIndex).value, 'Ativa');
});

test('selected empty root fails explicitly and keeps confirmed stages available for resume', async t => {
    const jobDir = await setup(t); const config = flow(); config.cleaning.rootSource = 'neon';
    const args = { flow: config, user: { username: 'Operador' }, jobDir, rootFile: { documents: [] }, providers: providers([row(1, ['11999990001'])]) };
    await assert.rejects(runFlow(args), /raiz selecionada está vazia/);
    assert.deepEqual(Object.keys(JSON.parse(await fs.readFile(path.join(jobDir, 'checkpoint.json'), 'utf8')).stages), ['generation', 'enrichment']);
});

test('abort during export removes partial output and resumes immutable cleaned input', async t => {
    const jobDir = await setup(t); const config = flow(); const controller = new AbortController();
    const fake = providers([row(1, ['11999990001']), row(2, ['11999990002'])]);
    const map = fake.mapOutputRow; let cancel = true;
    fake.mapOutputRow = (...args) => { if (cancel) controller.abort(); return map(...args); };
    const args = { flow: config, user: { username: 'Operador' }, jobDir, providers: fake };
    await assert.rejects(runFlow({ ...args, signal: controller.signal }), error => error.code === 'FLOW_CANCELLED');
    assert.deepEqual(await fs.readdir(path.join(jobDir, 'outputs')), []);
    cancel = false; const result = await runFlow(args);
    assert.equal(result.counts.kept, 2); assert.deepEqual(result.outputs.map(output => output.rows), [2, 2]);
});

test('cleaning crosses actual 2000-row batches and bounded enrichment preserves first surviving contacts', async t => {
    const jobDir = await setup(t); const config = flow(); config.output.rowsPerFile = 5000;
    config.enrichment.enabled = true;
    const phone = index => `119${String(index).padStart(8, '0')}`;
    const source = Array.from({ length: 2000 }, (_, index) => row(index + 1, [phone(index + 1)]));
    source.push(row(3001, [phone(1), phone(3001)]), row(1, [phone(4001)]));
    const batchLengths = [];
    const fake = providers(source, { queryEnrichment: async documents => { batchLengths.push(documents.length); return []; } });
    const result = await runFlow({ flow: config, user: { username: 'Operador' }, jobDir, providers: fake });
    assert.deepEqual(batchLengths, [2000, 2]);
    assert.equal(result.counts.kept, 2001); assert.equal(result.counts.repeatedPhones, 1); assert.equal(result.counts.repeatedDocuments, 1);
    const clean = (await fs.readFile(path.join(jobDir, 'cleaning.jsonl'), 'utf8')).trim().split('\n').map(JSON.parse);
    assert.deepEqual(clean.at(-1).phones, [phone(3001)]);
});

test('real layout exposes requested partner CPF without modifying original Receita partner data', async t => {
    const jobDir = await setup(t); const config = flow(); config.enrichment.enabled = true;
    const fake = providers([row(1, ['11999990001'], { cpf_socio: '98765432100' })], {
        queryEnrichment: async () => [{ cnpj: '00000000000001', phones: [], cpfs: ['1234567890'] }] });
    delete fake.getFormat; delete fake.mapOutputRow; config.output.formatId = 'padrao';
    const result = await runFlow({ flow: config, user: { username: 'Operador' }, jobDir, providers: fake });
    const sheet = await readXlsx(result.outputs[0].path);
    const cpfIndex = sheet.getRow(1).values.indexOf('livre6');
    assert.ok(cpfIndex > 0); assert.equal(sheet.getCell(2, cpfIndex).value, '01234567890');
    const record = JSON.parse((await fs.readFile(path.join(jobDir, 'enrichment.jsonl'), 'utf8')).trim());
    assert.equal(record.cpf_socio, '98765432100');
});

test('legacy default CNAE values match textual Receita codes with a leading zero', async t => {
    const jobDir = await setup(t); const config = flow(); delete config.cleaning.prohibitedCnaes;
    const result = await runFlow({ flow: config, user: { username: 'Operador' }, jobDir,
        providers: providers([row(1, ['11999990001'], { atividade_principal_cod: '0114800' }), row(2, ['11999990002'], { atividade_principal_cod: '6201501' })]) });
    assert.equal(result.counts.removedCnae, 1); assert.equal(result.counts.kept, 1);
});

test('an asynchronous XLSX disk stream failure cleans partial outputs and remains resumable', async t => {
    const jobDir = await setup(t); const config = flow();
    const nativeFs = require('node:fs'); const create = nativeFs.createWriteStream;
    t.after(() => { nativeFs.createWriteStream = create; });
    nativeFs.createWriteStream = function (file, options) {
        const stream = create.call(this, file, options);
        if (String(file).endsWith('.xlsx.tmp')) setImmediate(() => stream.destroy(Object.assign(new Error('fixture disk full'), { code: 'ENOSPC' })));
        return stream;
    };
    const args = { flow: config, user: { username: 'Operador' }, jobDir, providers: providers([row(1, ['11999990001'])]) };
    await assert.rejects(runFlow(args), { code: 'FLOW_DISK_FULL' });
    assert.deepEqual(await fs.readdir(path.join(jobDir, 'outputs')), []);
    nativeFs.createWriteStream = create;
    const result = await runFlow(args);
    assert.equal(result.counts.kept, 1); assert.equal(result.outputs.length, 2);
});

test('contacts beyond the final layout capacity remain available to the next company', async t => {
    const jobDir = await setup(t); const config = flow();
    const source = [row(1, ['11999990001', '11999990002', '11999990003']), row(2, ['11999990003'])];
    const result = await runFlow({ flow: config, user: { username: 'Operador' }, jobDir, providers: providers(source) });
    assert.equal(result.counts.kept, 2); assert.equal(result.counts.truncatedPhones, 1); assert.equal(result.counts.repeatedPhones, 0);
    const clean = (await fs.readFile(path.join(jobDir, 'cleaning.jsonl'), 'utf8')).trim().split('\n').map(JSON.parse);
    assert.deepEqual(clean.map(record => record.phones), [['11999990001', '11999990002'], ['11999990003']]);
    const sheet = await readXlsx(result.outputs[0].path);
    assert.equal(sheet.getCell('C3').value, '11999990003');
});

for (const address of [{ cidade: '', bairro: '' }, { cidade: 'Campinas', bairro: 'Centro' }]) {
    test(`validated editor generation integrates real Receita iterator: ${address.cidade || 'defaults'}`, async t => {
        const jobDir = await setup(t);
        const { defaults, validateFlow, effectiveFlow } = require('../src/main/flows/config');
        const input = defaults(); input.api.enabled = false; Object.assign(input.generation, address, { source: 'neon' });
        const user = { username: 'Operador' }; const config = effectiveFlow(validateFlow(input), user);
        const columns = { cnpj: 'text', razao_social: 'text', situacao_cadastral_cod: 'text',
            telefone_principal: 'text', telefone_secundario: 'text', cidade: 'text', bairro: 'text', natureza_juridica_cod: 'text' };
        const calls = [];
        const pool = { async connect() {
            return { connection: { stream: { pause() {}, resume() {} } }, release() {}, query(query) {
                calls.push({ sql: query.text, values: query.values });
                setImmediate(() => { query.emit('row', { cnpj: '00000000000001', razao_social: 'Fixture', situacao_cadastral_cod: '02', telefone_principal: '11999990001' }); query.emit('end', {}); });
            } };
        }, async query(sql, values) {
            calls.push({ sql, values });
            if (sql.includes('information_schema.columns')) return { rows: Object.entries(columns).map(([column_name, data_type]) => ({ column_name, data_type })) };
            return { rows: [{ cnpj: '00000000000001', razao_social: 'Fixture', situacao_cadastral_cod: '02', telefone_principal: '11999990001' }] };
        } };
        const result = await runFlow({ flow: config, user, jobDir, connections: { receita: pool },
            rootFile: { documents: ['99999999999999'], info: { source: 'fixture' } }, providers: { queryPhones: async () => [] } });
        assert.equal(result.status, 'completed'); assert.equal(result.counts.kept, 1);
        assert.deepEqual(config.generation.cidade, address.cidade ? [address.cidade] : []); assert.deepEqual(config.generation.bairro, address.bairro ? [address.bairro] : []);
        const read = calls.find(call => !call.sql.includes('information_schema.columns'));
        assert.deepEqual(read.values[0], ['02']);
        assert.doesNotMatch(read.sql, /ORDER BY|"cnpj" >/);
        if (address.cidade) { assert.ok(read.values.some(value => Array.isArray(value) && value[0] === 'CAMPINAS')); assert.ok(read.values.includes('%CENTRO%')); }
        else { assert.ok(!read.values.some(value => Array.isArray(value) && value.length === 0)); }
        const sheet = await readXlsx(result.outputs[0].path);
        assert.ok(sheet.getRow(2).values.includes('00000000000001')); assert.ok(sheet.getRow(2).values.includes('11999990001'));
    });
}

test('unordered generation resumes confirmed files even when the next scan changes order', async t => {
    const jobDir = await setup(t); const config = flow(); config.generation.limit = 4;
    const controller = new AbortController(); let order = [9, 1, 7, 3];
    const pool = { async query() { return { rows: ['cnpj','razao_social','situacao_cadastral_cod','natureza_juridica_cod','telefone_principal'].map(column_name => ({ column_name, data_type: 'text' })) }; }, async connect() {
        return { connection: { stream: { pause() {}, resume() {}, destroy() {} } }, release() {}, query(query) {
            assert.doesNotMatch(query.text, /ORDER BY|OFFSET|"cnpj" >/);
            setImmediate(() => { for (const n of order) query.emit('row', { cnpj: String(n).padStart(14,'0'), razao_social: 'Synthetic', situacao_cadastral_cod: '02', telefone_principal: `1199999000${n}` }); query.emit('end', {}); });
        } };
    } };
    const fake = providers([]); delete fake.iterateReceita;
    const args = { flow: config, user: { username: 'Davi' }, jobDir, connections: { receita: pool }, providers: fake, processingPolicy: { batchSize: 2 } };
    await assert.rejects(runFlow({ ...args, signal: controller.signal, onUpdate(update) {
        if (update.stage === 'generation' && update.progress?.processed === 2) controller.abort();
    } }), { code: 'FLOW_CANCELLED' });
    order = [3, 9, 7, 1];
    const result = await runFlow(args);
    assert.equal(result.counts.generated, 4);
    const rows = (await fs.readFile(path.join(jobDir,'generation.jsonl'),'utf8')).trim().split('\n').map(JSON.parse);
    assert.deepEqual(rows.map(r => r.cnpj), [9,1,3,7].map(n => String(n).padStart(14,'0')));
});

test('append enrichment counts normalized additions that fit the final layout only', async t => {
    const jobDir = await setup(t); const config = flow();
    config.enrichment = { enabled: true, strategy: 'append', fillCpf: false };
    const source = [row(1, ['5511999990011']), row(2, ['11999990021', '11999990022']), row(3, ['11999990031'])];
    const data = [{ cnpj: source[0].cnpj, phones: ['11999990011'] },
        { cnpj: source[1].cnpj, phones: ['11999990023'] }, { cnpj: source[2].cnpj, phones: ['5511999990032'] }];
    const result = await runFlow({ flow: config, user: { username: 'Operador' }, jobDir,
        providers: providers(source, { queryEnrichment: async documents => data.filter(item => documents.includes(item.cnpj)) }) });
    assert.equal(result.counts.enriched, 1); assert.equal(result.counts.kept, 3);
    const enriched = (await fs.readFile(path.join(jobDir, 'enrichment.jsonl'), 'utf8')).trim().split('\n').map(JSON.parse);
    assert.deepEqual(enriched.map(record => record.status), ['Pobre', 'Pobre', 'Enriquecido']);
    const clean = (await fs.readFile(path.join(jobDir, 'cleaning.jsonl'), 'utf8')).trim().split('\n').map(JSON.parse);
    assert.deepEqual(clean.map(record => record.phones), [['11999990011'], ['11999990021', '11999990022'], ['11999990031', '11999990032']]);
});

test('alphanumeric documents remain intact through cleanup, deduplication and XLSX/CSV export', async t => {
    const jobDir = await setup(t), config = flow();
    const alpha = '12ABC34501DE35';
    const rows = [row(1, ['11999990001'], { cnpj: alpha }), row(2, ['11999990002'], { cnpj: '00000123450135' }), row(3, ['11999990003'], { cnpj: alpha })];
    const result = await runFlow({ flow: config, user: { username: 'Davi' }, jobDir, rootFile: { documents: [], info: { source: 'none' } }, providers: providers(rows) });
    assert.equal(result.counts.kept, 2); assert.equal(result.counts.repeatedDocuments, 1);
    const sheet = await readXlsx(result.outputs.find(item => item.kind === 'xlsx').path);
    assert.equal(sheet.getCell('A2').value, alpha); assert.equal(sheet.getCell('A3').value, '00000123450135');
    assert.ok((await fs.readFile(result.outputs.find(item => item.kind === 'csv').path, 'utf8')).includes(alpha));
});

test('Receita source bq reads BigQuery without opening the Receita pool; neon keeps the Postgres path', async t => {
    const jobDir = await setup(t);
    const config = flow(); config.generation.source = 'bq'; config.generation.availability = 'available';
    const pools = [];
    let bqCalls = 0;
    const fake = providers([]); const neon = fake.iterateReceita; delete fake.iterateReceita;
    fake.createPool = settings => { pools.push(settings); throw new Error('Receita pool must not open'); };
    fake.iterateReceitaBq = async function* (args) {
        bqCalls++;
        assert.equal(args.pool, undefined);
        assert.equal(args.filters.availability, 'available');
        yield { rows: [row(1, ['11999990001']), row(2, ['11999990002'])] };
    };
    const result = await runFlow({ flow: config, user: { username: 'Operador' }, jobDir, connections: { receita: 'postgres://synthetic' }, providers: fake });
    assert.equal(result.counts.generated, 2); assert.equal(bqCalls, 1); assert.deepEqual(pools, []);
    // Source is part of the checkpoint identity: a resume never mixes sources.
    const switched = flow(); switched.generation.source = 'neon'; switched.generation.availability = 'available';
    await assert.rejects(runFlow({ flow: switched, user: { username: 'Operador' }, jobDir, providers: { ...fake, iterateReceita: neon } }), /configuração do fluxo mudou/);

    const neonDir = await setup(t);
    const neonFake = providers([row(3, ['11999990003'])]);
    neonFake.iterateReceitaBq = () => { throw new Error('BigQuery must not be used'); };
    const neonResult = await runFlow({ flow: { ...flow(), generation: { ...flow().generation, source: 'neon' } }, user: { username: 'Operador' }, jobDir: neonDir, providers: neonFake });
    assert.equal(neonResult.counts.generated, 1);
});

test('expired Google login during BigQuery generation keeps its code and friendly message', async t => {
    const jobDir = await setup(t);
    const config = flow(); config.generation.source = 'bq';
    const fake = providers([]); delete fake.iterateReceita;
    fake.iterateReceitaBq = async function* () { throw Object.assign(new Error('Seu login Google precisa ser renovado. Entre pelo navegador e depois retome a execução.'), { code: 'BQ_AUTH_REQUIRED' }); };
    await assert.rejects(runFlow({ flow: config, user: { username: 'Operador' }, jobDir, providers: fake }), { code: 'BQ_AUTH_REQUIRED', message: /login Google/ });
});
test('generation from BigQuery asks only for the Receita fields the layout and cleaning use', async t => {
    const { generationFields } = require('../src/main/flows/pipeline');
    const layout = { colunas: [{ header: 'nome', campo: 'razao_social' }, { header: 'cpf', campo: 'cnpj_numerico' }, { header: 'livre1', campo: 'ano_abertura' },
        { header: 'livre3', campo: 'composto', partes: ['atividade_principal_cod_num', 'estado'] }, { header: 'livre7', campo: 'fixo_c6' }, { header: 'fone1', campo: 'telefone_1' }] };
    assert.deepEqual(generationFields(layout).sort(), ['atividade_principal_cod', 'cnpj', 'data_abertura', 'estado', 'razao_social', 'situacao_cadastral', 'situacao_cadastral_cod', 'telefone_principal', 'telefone_secundario']);
    assert.equal(generationFields({ colunas: ['nome', 'fone1'] }), null);
    assert.equal(generationFields({ colunas: [{ header: 'x', campo: 'desconhecido' }] }), null);
    const jobDir = await setup(t); const config = flow(); config.generation.source = 'bq';
    const fake = providers([]); delete fake.iterateReceita; let asked;
    fake.getFormat = () => layout;
    fake.iterateReceitaBq = async function* (args) { asked = args.fields; yield { rows: [row(1, ['11999990001'])] }; };
    await runFlow({ flow: config, user: { username: 'Operador' }, jobDir, providers: fake });
    assert.ok(asked.includes('estado') && asked.includes('data_abertura') && !asked.includes('logradouro'));
});
test('an unexpected failure keeps its cause in the log, without addresses or passwords', async t => {
    const jobDir = await setup(t); const logs = [];
    const fake = providers([row(1, ['11999990001'])]);
    fake.queryPhones = async () => { throw Object.assign(new Error('connection terminated: postgresql://user:synthetic-pass@host/db password=synthetic-pass'), { code: 'ECONNRESET' }); };
    await assert.rejects(runFlow({ flow: flow(), user: { username: 'Operador' }, jobDir, providers: fake, onUpdate: data => data.log && logs.push(data.log) }), /Não foi possível concluir a limpeza/);
    const detail = logs.find(log => log.startsWith('Detalhe do erro (cleaning)'));
    assert.match(detail, /ECONNRESET connection terminated: \[endereço\] password=\*\*\*/);
    assert.doesNotMatch(logs.join('\n'), /synthetic-pass|postgresql:/);
});
