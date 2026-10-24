const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs/promises'), path = require('node:path'), os = require('node:os');
const ExcelJS = require('exceljs');
const { lookup, lookupCnaes, annotateFile, document } = require('../src/main/receitaSituacao');
const { cnpjCheckDigits } = require('../src/main/documentos');
const { normalizarDocumento } = require('../src/main/limpezaTelefones');
const { restoreCnpj } = require('../src/main/flows/bq');
const columns = ['cnpj', 'razao_social', 'situacao_cadastral_cod', 'situacao_cadastral_data', 'situacao_motivo', 'ultima_atualizacao'];
const RECEITA = { '12ABC34501DE35': { razao_social: 'Empresa fictícia', situacao_cadastral_cod: '08', situacao_cadastral_data: '2026-09-01', situacao_motivo: 'Encerramento', ultima_atualizacao: '2026-09-29' } };
// Base com opcao_mei: um MEI (com caixa e espaço variados), um não MEI e um com o campo vazio.
const MEI_RECEITA = {
    '12ABC34501DE35': { razao_social: 'Empresa fictícia', situacao_cadastral_cod: '08', opcao_mei: 'N' },
    '12345678000190': { razao_social: 'Empresa fictícia', situacao_cadastral_cod: '02', opcao_mei: ' s ' },
    '11222333000181': { razao_social: 'Empresa fictícia', situacao_cadastral_cod: '02', opcao_mei: null },
};
// BigQuery falso: a Receita em memória e a sessão com a tabela temporária da lista, página a página.
function bqFixture(receita = RECEITA, { pageRows = 50000, fail = async () => {}, reorder = rows => rows } = {}) {
    const calls = [], aborted = [], sessions = new Map();
    const value = (params, name) => params.find(param => param.name === name)?.parameterValue;
    return { calls, aborted, sessions, async queryPages({ query, params = [], onPage = async () => {}, session, createSession, maxBytesBilled, signal }) {
        calls.push({ query, params, session, createSession, maxBytesBilled });
        if (signal?.aborted) throw new Error('Consulta BQ cancelada.');
        await fail(query);
        const pages = async rows => {
            for (let start = 0; start < rows.length; start += pageRows) { if (signal?.aborted) throw new Error('Consulta BQ cancelada.'); await onPage(rows.slice(start, start + pageRows)); }
            return { rows: rows.length };
        };
        if (createSession) { const id = `sessao-${sessions.size + 1}`; sessions.set(id, []); return { rows: 0, session: id }; }
        if (query.startsWith('INSERT INTO lista')) {
            const base = Number(value(params, 'base').value);
            value(params, 'cnpjs').arrayValues.forEach((item, offset) => sessions.get(session).push({ idx: String(base + offset), cnpj: item.value }));
            return { rows: 0 };
        }
        if (query.includes('FROM lista l LEFT JOIN')) return pages(reorder(sessions.get(session).map(row => ({ ...row, encontrado: receita[row.cnpj] ? 'true' : 'false', ...(receita[row.cnpj] || {}) }))));
        if (query.includes('IN UNNEST(@cnpjs)')) return pages(value(params, 'cnpjs').arrayValues.map(item => item.value).filter(cnpj => receita[cnpj]).map(cnpj => ({ cnpj, ...receita[cnpj] })));
        throw new Error(`Consulta inesperada: ${query}`);
    }, async abortSession(session) { aborted.push(session); } };
}
const MEI_LIST = 'CNPJ;Nome\n12ABC34501DE35;Não MEI\n12345678000190;MEI\n11222333000181;Sem opção\n11444777000161;Fora da base\n12345678901;CPF';
async function listFile(t, content = MEI_LIST) {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'receita-mei-')); t.after(() => fs.rm(dir, { recursive: true, force: true }));
    const file = path.join(dir, 'lista.csv'); await fs.writeFile(file, content); return { dir, file };
}
async function names(output) { const wb = new ExcelJS.Workbook(); await wb.xlsx.readFile(output); return wb.worksheets[0].getColumn(2).values.slice(2); }

test('document normalization retains valid alphanumeric identifiers in cleanup, BQ root and standalone query', () => {
    assert.equal(normalizarDocumento('12.abc.345/01de-35'), '12ABC34501DE35');
    assert.equal(restoreCnpj('12.abc.345/01de-35'), '12ABC34501DE35');
    assert.equal(cnpjCheckDigits('12ABC34501DE35'), true);
    assert.equal(cnpjCheckDigits('12ABC34501DE34'), false);
    assert.equal(normalizarDocumento('CNPJ inválido'), '');
    assert.equal(document('12345678901'), '');
});
test('single lookup sends only valid documents as a parameter, reads the Receita by CNPJ and never filters by situation', async () => {
    const bq = bqFixture(), results = await lookup(bq, ['12.abc.345/01de-35', '12ABC34501DE35', '12345678901']);
    assert.equal(results.size, 1); assert.equal(results.get('12ABC34501DE35').situacao_cadastral, 'Baixada');
    assert.equal(results.get('12ABC34501DE35').situacao_motivo, 'Encerramento');
    assert.deepEqual(bq.calls[0].params[0].parameterValue.arrayValues, [{ value: '12ABC34501DE35' }]);
    assert.match(bq.calls[0].query, /FROM `mbtech-bronze\.receita\.empresas` e WHERE e\.cnpj IN UNNEST\(@cnpjs\)$/);
    assert.doesNotMatch(bq.calls[0].query, /12ABC34501DE35/);
    assert.equal(bq.calls[0].maxBytesBilled, '40000000000'); assert.equal(bq.calls[0].session, undefined);
});
test('BigQuery failures become messages of this screen, and an expired login is flagged for renewal', async () => {
    const failing = code => bqFixture(RECEITA, { fail: async () => { throw Object.assign(new Error('synthetic detail'), code ? { code } : {}); } });
    await assert.rejects(lookup(failing('BQ_AUTH_REQUIRED'), ['12ABC34501DE35']), error => error.code === 'RECEITA_SITUACAO' && error.authRequired === true && /login Google/.test(error.message));
    await assert.rejects(lookup(failing('FLOW_VALIDATION'), ['12ABC34501DE35']), /não tem acesso à Receita no BigQuery/);
    await assert.rejects(lookup(failing('BQ_BYTES_LIMIT'), ['12ABC34501DE35']), /limite de custo/);
    await assert.rejects(lookup(failing(), ['12ABC34501DE35']), error => /Não foi possível consultar a Receita no BigQuery/.test(error.message) && !/synthetic/.test(error.message));
});
test('CSV annotation retains all original rows, flags missing/invalid CNPJ, escapes formulas and never overwrites input', async t => {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'receita-situacao-')); t.after(() => fs.rm(dir, { recursive: true, force: true }));
    const file = path.join(dir, 'lista.csv'), text = '﻿CNPJ;Nome;RECEITA_SITUACAO\n12ABC34501DE35;=1+1;Anterior\n12345678000190;Outra;Original\n12345678901;CPF;Original\n12ABC34501DE35;Repetida;Original';
    await fs.writeFile(file, text);
    const bq = bqFixture(), result = await annotateFile({ filename: file, bq });
    assert.deepEqual([result.processed, result.found, result.notFound, result.invalid], [4, 2, 1, 1]);
    assert.equal(await fs.readFile(file, 'utf8'), text);
    const wb = new ExcelJS.Workbook(); await wb.xlsx.readFile(result.output); const sheet = wb.worksheets[0];
    assert.equal(sheet.rowCount, 5); assert.equal(sheet.getCell('A2').value, '12ABC34501DE35'); assert.equal(sheet.getCell('B2').value, "'=1+1");
    assert.equal(sheet.getCell('C2').value, 'Anterior'); assert.equal(sheet.getCell('E1').value, 'RECEITA_SITUACAO_2'); assert.equal(sheet.getCell('E2').value, 'Baixada');
    assert.equal(sheet.getCell('I3').value, 'Não encontrado na base'); assert.equal(sheet.getCell('I4').value, 'Documento inválido');
    assert.deepEqual(bq.sessions.get('sessao-1').map(row => row.cnpj), ['12ABC34501DE35', '12345678000190', '12ABC34501DE35']);
    assert.deepEqual(bq.aborted, ['sessao-1']);
});
test('a list goes up to the session in parts of the batch and comes back from one query, in list order', async t => {
    const { file } = await listFile(t, 'CNPJ;Nome\n' + ['12ABC34501DE35', '12345678901', '12345678000190', '11222333000181', '12ABC34501DE35'].map((cnpj, index) => `${cnpj};Linha ${index + 1}`).join('\n'));
    const bq = bqFixture(MEI_RECEITA, { pageRows: 2 }), stages = [], progress = [];
    const result = await annotateFile({ filename: file, bq, batchSize: 2, onStage: message => stages.push(message), onProgress: counts => progress.push(counts.processed) });
    assert.deepEqual([result.processed, result.found, result.notFound, result.invalid], [5, 4, 0, 1]);
    assert.deepEqual(await names(result.output), ['Linha 1', 'Linha 2', 'Linha 3', 'Linha 4', 'Linha 5']);
    const inserts = bq.calls.filter(call => call.query.startsWith('INSERT INTO lista'));
    assert.deepEqual(inserts.map(call => [call.params[0].parameterValue.value, call.params[1].parameterValue.arrayValues.length]), [['0', 2], ['2', 2]]);
    assert.equal(bq.calls.filter(call => call.query.includes('LEFT JOIN')).length, 1);
    assert.deepEqual(bq.calls.map(call => Boolean(call.createSession)), [true, false, false, false]);
    assert.ok(bq.calls.slice(1).every(call => call.session === 'sessao-1')); assert.ok(bq.calls.every(call => call.maxBytesBilled === '40000000000'));
    assert.deepEqual(progress, [3, 5]);
    assert.deepEqual(stages, ['Enviando a lista ao BigQuery: 2 CNPJs.', 'Enviando a lista ao BigQuery: 4 CNPJs.', 'Cruzando a lista com a Receita no BigQuery.']);
    assert.deepEqual(bq.aborted, ['sessao-1']);
});
test('a list without valid CNPJs is written without opening a BigQuery session', async t => {
    const { file } = await listFile(t, 'CNPJ;Nome\n12345678901;CPF\nabc;Texto');
    const bq = bqFixture(), result = await annotateFile({ filename: file, bq });
    assert.deepEqual([result.processed, result.invalid], [2, 2]); assert.deepEqual(await names(result.output), ['CPF', 'Texto']);
    assert.equal(bq.calls.length, 0); assert.deepEqual(bq.aborted, []);
});
test('results out of list order stop the lookup, remove the outputs and close the session', async t => {
    const { dir, file } = await listFile(t);
    const bq = bqFixture(MEI_RECEITA, { reorder: rows => [...rows].reverse() });
    await assert.rejects(annotateFile({ filename: file, bq, csv: true }), /A lista mudou durante a consulta/);
    assert.deepEqual(await fs.readdir(dir), ['lista.csv']); assert.deepEqual(bq.aborted, ['sessao-1']);
});
test('an expired login while sending the list leaves no output, closes the session and is flagged for renewal', async t => {
    const { dir, file } = await listFile(t);
    const bq = bqFixture(MEI_RECEITA, { fail: async query => { if (query.startsWith('INSERT')) throw Object.assign(new Error('expired'), { code: 'BQ_AUTH_REQUIRED' }); } });
    await assert.rejects(annotateFile({ filename: file, bq }), error => error.authRequired === true && error.code === 'RECEITA_SITUACAO');
    assert.deepEqual(await fs.readdir(dir), ['lista.csv']); assert.deepEqual(bq.aborted, ['sessao-1']);
});
test('XLSX annotation preserves leading zeroes and cancellation/header failure removes unfinished outputs', async t => {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'receita-situacao-')); t.after(() => fs.rm(dir, { recursive: true, force: true }));
    const file = path.join(dir, 'lista.xlsx'), wb = new ExcelJS.Workbook(), sheet = wb.addWorksheet('Entrada');
    sheet.addRow(['CNPJ', 'Nome']); sheet.addRow(['00000000E08G12', 'Fictícia']); await wb.xlsx.writeFile(file);
    const result = await annotateFile({ filename: file, bq: bqFixture() }); const output = new ExcelJS.Workbook(); await output.xlsx.readFile(result.output);
    assert.equal(output.worksheets[0].getCell('A2').value, '00000000E08G12');
    const controller = new AbortController(); controller.abort(); await assert.rejects(annotateFile({ filename: file, bq: bqFixture(), signal: controller.signal }), /cancelada/);
    const invalid = path.join(dir, 'errado.csv'); await fs.writeFile(invalid, 'Nome;Fone\nOutro;11999990001'); await assert.rejects(annotateFile({ filename: invalid, bq: bqFixture() }), /coluna CNPJ/);
    assert.ok(!(await fs.readdir(dir)).some(name => name.endsWith('.partial')));
});
test('cancel after a completed page deletes the unfinished XLSX and preserves the CSV', async t => {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'receita-cancel-')); t.after(() => fs.rm(dir, { recursive: true, force: true }));
    const filename = path.join(dir, 'source.csv'), text = 'CNPJ;Nome\n' + Array.from({ length: 1001 }, () => '12ABC34501DE35;Fictícia').join('\n');
    await fs.writeFile(filename, text); const controller = new AbortController(), bq = bqFixture(RECEITA, { pageRows: 1000 });
    await assert.rejects(annotateFile({ filename, bq, signal: controller.signal, onProgress: () => controller.abort() }), /cancelada/);
    assert.deepEqual(await fs.readdir(dir), ['source.csv']); assert.equal(await fs.readFile(filename, 'utf8'), text);
    assert.deepEqual(bq.aborted, ['sessao-1']);
});
test('remove MEI drops only CNPJs marked as MEI in the Receita and counts them apart from the totals', async t => {
    const { file } = await listFile(t);
    const result = await annotateFile({ filename: file, bq: bqFixture(MEI_RECEITA), removeMei: true });
    assert.deepEqual([result.processed, result.found, result.notFound, result.invalid, result.removed, result.mei], [5, 3, 1, 1, 0, 1]);
    assert.deepEqual(await names(result.output), ['Não MEI', 'Sem opção', 'Fora da base', 'CPF']);
    const kept = await annotateFile({ filename: file, bq: bqFixture(MEI_RECEITA) });
    assert.equal(kept.mei, 0); assert.deepEqual(await names(kept.output), ['Não MEI', 'MEI', 'Sem opção', 'Fora da base', 'CPF']);
});
test('a row left out by the situation filter is not counted again as MEI', async t => {
    const { file } = await listFile(t);
    const result = await annotateFile({ filename: file, bq: bqFixture(MEI_RECEITA), removeMei: true, keep: ['baixada'] });
    assert.deepEqual([result.removed, result.mei], [4, 0]);
    const active = await annotateFile({ filename: file, bq: bqFixture(MEI_RECEITA), removeMei: true, keep: ['ativa'] });
    assert.deepEqual([active.removed, active.mei], [3, 1]); assert.deepEqual(await names(active.output), ['Sem opção']);
});
test('additional CSV mirrors the XLSX result, MEI already removed, ready for Importar Mailing', async t => {
    const { file } = await listFile(t, MEI_LIST.replace('Sem opção', '=1+1'));
    const result = await annotateFile({ filename: file, bq: bqFixture(MEI_RECEITA), removeMei: true, csv: true });
    assert.equal(path.dirname(result.csvOutput), path.dirname(result.output));
    assert.equal(path.basename(result.csvOutput, '.csv'), path.basename(result.output, '.xlsx'));
    const text = await fs.readFile(result.csvOutput, 'utf8'), lines = text.split('\r\n').filter(Boolean);
    assert.ok(text.startsWith('﻿'));
    const wb = new ExcelJS.Workbook(); await wb.xlsx.readFile(result.output); const sheet = wb.worksheets[0];
    assert.equal(lines.length, sheet.rowCount);
    const quoted = values => Array.from(values, value => `"${String(value ?? '').replace(/"/g, '""')}"`).join(';');
    // Só o cabeçalho muda: a Fastway exige "cpf", então a coluna CNPJ sai com esse nome no CSV.
    assert.equal(sheet.getCell('A1').value, 'CNPJ'); assert.match(lines[0], /^﻿"cpf";"Nome";"RECEITA_CODIGO"/);
    sheet.eachRow((row, number) => { if (number > 1) assert.equal(lines[number - 1], quoted(row.values.slice(1))); });
    assert.ok(!text.includes('"MEI"')); assert.ok(text.includes(`"'=1+1"`));
    assert.equal((await annotateFile({ filename: file, bq: bqFixture(MEI_RECEITA) })).csvOutput, null);
});
test('a list whose document column is CPF holding CNPJs is consulted like a CNPJ list', async t => {
    const { file } = await listFile(t, MEI_LIST.replace('CNPJ;Nome', 'CPF;Nome'));
    const result = await annotateFile({ filename: file, bq: bqFixture(MEI_RECEITA), removeMei: true, csv: true });
    assert.deepEqual([result.processed, result.found, result.notFound, result.invalid, result.mei], [5, 3, 1, 1, 1]);
    assert.deepEqual(await names(result.output), ['Não MEI', 'Sem opção', 'Fora da base', 'CPF']);
    assert.match(await fs.readFile(result.csvOutput, 'utf8'), /^﻿"CPF";"Nome";/);
});
test('with both CNPJ and CPF columns the CNPJ is consulted and the CSV keeps both names, never two cpf columns', async t => {
    const { file } = await listFile(t, 'CPF;CNPJ;Nome\n11122233344;12345678000190;MEI\n55566677788;12ABC34501DE35;Não MEI');
    const result = await annotateFile({ filename: file, bq: bqFixture(MEI_RECEITA), removeMei: true, csv: true });
    assert.deepEqual([result.found, result.mei], [2, 1]);
    assert.match(await fs.readFile(result.csvOutput, 'utf8'), /^﻿"CPF";"CNPJ";"Nome";/);
});
test('cancel deletes the unfinished CSV together with the XLSX', async t => {
    const { dir, file } = await listFile(t, 'CNPJ;Nome\n' + Array.from({ length: 1001 }, () => '12ABC34501DE35;Fictícia').join('\n'));
    const controller = new AbortController();
    await assert.rejects(annotateFile({ filename: file, bq: bqFixture(MEI_RECEITA, { pageRows: 1000 }), csv: true, signal: controller.signal, onProgress: () => controller.abort() }), /cancelada/);
    assert.deepEqual(await fs.readdir(dir), ['lista.csv']);
});
test('keep filter drops unselected situations from the output and counts them, never the totals', async t => {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'receita-keep-')); t.after(() => fs.rm(dir, { recursive: true, force: true }));
    const file = path.join(dir, 'lista.csv'); await fs.writeFile(file, 'CNPJ;Nome\n12ABC34501DE35;Baixada\n12345678000190;Fora\n12345678901;CPF');
    const only = await annotateFile({ filename: file, bq: bqFixture(), keep: ['baixada'] });
    assert.deepEqual([only.processed, only.found, only.notFound, only.invalid, only.removed], [3, 1, 1, 1, 2]);
    const wb = new ExcelJS.Workbook(); await wb.xlsx.readFile(only.output); assert.equal(wb.worksheets[0].rowCount, 2);
    assert.equal(wb.worksheets[0].getCell('B2').value, 'Baixada');
    const active = await annotateFile({ filename: file, bq: bqFixture(), keep: ['ativa', 'nao_encontrado'] });
    assert.equal(active.removed, 2);
    const all = await annotateFile({ filename: file, bq: bqFixture() }); assert.equal(all.removed, 0);
    await assert.rejects(annotateFile({ filename: file, bq: bqFixture(), keep: [] }), /ao menos uma/);
    await assert.rejects(annotateFile({ filename: file, bq: bqFixture(), keep: ['lixo'] }), /ao menos uma/);
});
test('CSV from a CNPJ list passes the Importar Mailing header check (nome, cpf, fone1)', async t => {
    const { inspectCsv } = require('../src/main/flows/fastway');
    const { file } = await listFile(t, 'CNPJ;nome;fone1\n12ABC34501DE35;Não MEI;11999990000\n12345678000190;MEI;11999990001\n11444777000161;Fora da base;11999990002');
    const result = await annotateFile({ filename: file, bq: bqFixture(MEI_RECEITA), removeMei: true, csv: true });
    assert.deepEqual(await inspectCsv(result.csvOutput), { rows: 2 });
});
// CNAE principal por CNPJ, usado pelo filtro "Manter só estes CNAEs" da Limpeza Local.
function cnaePool({ cnpjType = 'text', fail = false } = {}) {
    const calls = [], cnaes = { '04252011000110': '0111301', '12345678000199': '4781400' };
    return { calls, query: async (sql, values) => {
        if (sql.includes('information_schema')) return { rows: [...columns, 'atividade_principal_cod'].map(column_name => ({ column_name, data_type: column_name === 'cnpj' ? cnpjType : 'text' })) };
        calls.push({ sql, values });
        if (fail) throw new Error('connection terminated: postgresql://synthetic/private');
        return { rows: values[0].filter(cnpj => cnaes[cnpj]).map(cnpj => ({ cnpj: `${cnpj}`, cnae: cnaes[cnpj] })) };
    } };
}
test('CNAE lookup returns the principal CNAE of each found CNPJ, querying each document once', async () => {
    const pool = cnaePool();
    const result = await lookupCnaes(pool, [4252011000110, '04.252.011/0001-10', '12345678000199', '11444777000161', '12345678901']);
    assert.deepEqual([...result], [['04252011000110', '0111301'], ['12345678000199', '4781400']]);
    assert.deepEqual(pool.calls[0].values, [['04252011000110', '12345678000199', '11444777000161']]);
    assert.match(pool.calls[0].sql, /"atividade_principal_cod"/);
});
test('CNAE lookup splits long lists in batches of 1.000 and reports progress after each one', async () => {
    const pool = cnaePool({ cnpjType: 'character' }), progress = [];
    const documents = Array.from({ length: 2500 }, (_, index) => String(10000000000000 + index * 100));
    await lookupCnaes(pool, documents, { onProgress: done => progress.push(done) });
    assert.deepEqual(pool.calls.map(call => call.values[0].length), [1000, 1000, 500]);
    assert.match(pool.calls[0].sql, /= ANY\(\$1::bpchar\[\]\)/);
    assert.deepEqual(progress, [1000, 2000, 2500]);
});
test('CNAE lookup failure gives a friendly message without the connection details', async () => {
    await assert.rejects(lookupCnaes(cnaePool({ fail: true }), ['12345678000199']), error => {
        assert.match(error.message, /CNAE.*Receita/);
        assert.doesNotMatch(error.message, /postgresql|synthetic/);
        return true;
    });
});
test('CNAE lookup stops when the Receita base has no principal CNAE column', async () => {
    const pool = { query: async () => ({ rows: columns.map(column_name => ({ column_name, data_type: 'text' })) }) };
    await assert.rejects(lookupCnaes(pool, ['12345678000199']), /atividade_principal_cod/);
});
