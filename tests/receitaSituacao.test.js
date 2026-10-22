const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs/promises'), path = require('node:path'), os = require('node:os');
const ExcelJS = require('exceljs');
const { lookup, annotateFile, document } = require('../src/main/receitaSituacao');
const { cnpjCheckDigits } = require('../src/main/documentos');
const { normalizarDocumento } = require('../src/main/limpezaTelefones');
const { restoreCnpj } = require('../src/main/flows/bq');
const columns = ['cnpj', 'razao_social', 'situacao_cadastral_cod', 'situacao_cadastral_data', 'situacao_motivo', 'ultima_atualizacao'];
function poolFixture() {
    const calls = [];
    return { calls, query: async (sql, values) => {
        if (sql.includes('information_schema')) return { rows: columns.map(column_name => ({ column_name, data_type: 'text' })) };
        calls.push({ sql, values });
        return { rows: values[0].filter(cnpj => cnpj === '12ABC34501DE35').map(cnpj => ({ cnpj, razao_social: 'Empresa fictícia', situacao_cadastral_cod: '08', situacao_cadastral_data: '2026-09-01', situacao_motivo: 'Encerramento', ultima_atualizacao: '2026-09-29' })) };
    } };
}
test('document normalization retains valid alphanumeric identifiers in cleanup, BQ root and standalone query', () => {
    assert.equal(normalizarDocumento('12.abc.345/01de-35'), '12ABC34501DE35');
    assert.equal(restoreCnpj('12.abc.345/01de-35'), '12ABC34501DE35');
    assert.equal(cnpjCheckDigits('12ABC34501DE35'), true);
    assert.equal(cnpjCheckDigits('12ABC34501DE34'), false);
    assert.equal(normalizarDocumento('CNPJ inválido'), '');
    assert.equal(document('12345678901'), '');
});
test('standalone SQL lookup uses only supplied documents, retains situation/date/reason and never filters by active state', async () => {
    const pool = poolFixture(), results = await lookup(pool, ['12.abc.345/01de-35', '12ABC34501DE35', '12345678901']);
    assert.equal(results.size, 1); assert.equal(results.get('12ABC34501DE35').situacao_cadastral, 'Baixada');
    assert.deepEqual(pool.calls[0].values, [['12ABC34501DE35']]);
    assert.doesNotMatch(pool.calls[0].sql, /12ABC34501DE35|situacao_cadastral_cod.*WHERE.*02/);
    assert.equal(results.get('12ABC34501DE35').situacao_motivo, 'Encerramento');
});
test('CSV annotation retains all original rows, flags missing/invalid CNPJ, escapes formulas and never overwrites input', async t => {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'receita-situacao-')); t.after(() => fs.rm(dir, { recursive: true, force: true }));
    const file = path.join(dir, 'lista.csv'), text = '\uFEFFCNPJ;Nome;RECEITA_SITUACAO\n12ABC34501DE35;=1+1;Anterior\n12345678000190;Outra;Original\n12345678901;CPF;Original\n12ABC34501DE35;Repetida;Original';
    await fs.writeFile(file, text);
    const result = await annotateFile({ filename: file, pool: poolFixture() });
    assert.deepEqual([result.processed, result.found, result.notFound, result.invalid], [4, 2, 1, 1]);
    assert.equal(await fs.readFile(file, 'utf8'), text);
    const wb = new ExcelJS.Workbook(); await wb.xlsx.readFile(result.output); const sheet = wb.worksheets[0];
    assert.equal(sheet.rowCount, 5); assert.equal(sheet.getCell('A2').value, '12ABC34501DE35'); assert.equal(sheet.getCell('B2').value, "'=1+1");
    assert.equal(sheet.getCell('C2').value, 'Anterior'); assert.equal(sheet.getCell('E1').value, 'RECEITA_SITUACAO_2'); assert.equal(sheet.getCell('E2').value, 'Baixada');
    assert.equal(sheet.getCell('I3').value, 'Não encontrado na base'); assert.equal(sheet.getCell('I4').value, 'Documento inválido');
});
test('XLSX annotation preserves leading zeroes and cancellation/header failure removes unfinished outputs', async t => {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'receita-situacao-')); t.after(() => fs.rm(dir, { recursive: true, force: true }));
    const file = path.join(dir, 'lista.xlsx'), wb = new ExcelJS.Workbook(), sheet = wb.addWorksheet('Entrada');
    sheet.addRow(['CNPJ', 'Nome']); sheet.addRow(['00000000E08G12', 'Fictícia']); await wb.xlsx.writeFile(file);
    const result = await annotateFile({ filename: file, pool: poolFixture() }); const output = new ExcelJS.Workbook(); await output.xlsx.readFile(result.output);
    assert.equal(output.worksheets[0].getCell('A2').value, '00000000E08G12');
    const controller = new AbortController(); controller.abort(); await assert.rejects(annotateFile({ filename: file, pool: poolFixture(), signal: controller.signal }), /cancelada/);
    const invalid = path.join(dir, 'errado.csv'); await fs.writeFile(invalid, 'Nome;Fone\nOutro;11999990001'); await assert.rejects(annotateFile({ filename: invalid, pool: poolFixture() }), /coluna CNPJ/);
    assert.ok(!(await fs.readdir(dir)).some(name => name.endsWith('.partial')));
});

test('cancel after a completed lookup batch deletes the unfinished XLSX and preserves the CSV', async t => {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'receita-cancel-')); t.after(() => fs.rm(dir, { recursive: true, force: true }));
    const filename = path.join(dir, 'source.csv'), text = 'CNPJ;Nome\n' + Array.from({ length: 1001 }, () => '12ABC34501DE35;Fictícia').join('\n');
    await fs.writeFile(filename, text); const controller = new AbortController();
    await assert.rejects(annotateFile({ filename, pool: poolFixture(), signal: controller.signal, onProgress: () => controller.abort() }), /cancelada/);
    assert.deepEqual(await fs.readdir(dir), ['source.csv']); assert.equal(await fs.readFile(filename, 'utf8'), text);
});

test('char(14) cnpj column is compared as bpchar[] so the primary key index is used', async () => {
    const calls = [], pool = { query: async (sql, values) => {
        if (sql.includes('information_schema')) return { rows: columns.map(column_name => ({ column_name, data_type: column_name === 'cnpj' ? 'character' : 'text' })) };
        calls.push(sql); return { rows: [] };
    } };
    await lookup(pool, ['12ABC34501DE35']);
    assert.match(calls[0], /= ANY\(\$1::bpchar\[\]\)/);
});
// Base com opcao_mei: um MEI (com caixa e espaço variados), um não MEI e um com o campo vazio.
function meiPool({ withMeiColumn = true } = {}) {
    const rows = { '12ABC34501DE35': ['08', 'N'], '12345678000190': ['02', ' s '], '11222333000181': ['02', null] };
    return { query: async (sql, values) => {
        if (sql.includes('information_schema')) return { rows: [...columns, ...(withMeiColumn ? ['opcao_mei'] : [])].map(column_name => ({ column_name, data_type: 'text' })) };
        return { rows: values[0].filter(cnpj => rows[cnpj]).map(cnpj => ({ cnpj, razao_social: 'Empresa fictícia', situacao_cadastral_cod: rows[cnpj][0], opcao_mei: withMeiColumn ? rows[cnpj][1] : null })) };
    } };
}
const MEI_LIST = 'CNPJ;Nome\n12ABC34501DE35;Não MEI\n12345678000190;MEI\n11222333000181;Sem opção\n11444777000161;Fora da base\n12345678901;CPF';
async function listFile(t, content = MEI_LIST) {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'receita-mei-')); t.after(() => fs.rm(dir, { recursive: true, force: true }));
    const file = path.join(dir, 'lista.csv'); await fs.writeFile(file, content); return { dir, file };
}
async function names(output) { const wb = new ExcelJS.Workbook(); await wb.xlsx.readFile(output); return wb.worksheets[0].getColumn(2).values.slice(2); }
test('remove MEI drops only CNPJs marked as MEI in the Receita and counts them apart from the totals', async t => {
    const { file } = await listFile(t);
    const result = await annotateFile({ filename: file, pool: meiPool(), removeMei: true });
    assert.deepEqual([result.processed, result.found, result.notFound, result.invalid, result.removed, result.mei], [5, 3, 1, 1, 0, 1]);
    assert.deepEqual(await names(result.output), ['Não MEI', 'Sem opção', 'Fora da base', 'CPF']);
    const kept = await annotateFile({ filename: file, pool: meiPool() });
    assert.equal(kept.mei, 0); assert.deepEqual(await names(kept.output), ['Não MEI', 'MEI', 'Sem opção', 'Fora da base', 'CPF']);
});
test('a row left out by the situation filter is not counted again as MEI', async t => {
    const { file } = await listFile(t);
    const result = await annotateFile({ filename: file, pool: meiPool(), removeMei: true, keep: ['baixada'] });
    assert.deepEqual([result.removed, result.mei], [4, 0]);
    const active = await annotateFile({ filename: file, pool: meiPool(), removeMei: true, keep: ['ativa'] });
    assert.deepEqual([active.removed, active.mei], [3, 1]); assert.deepEqual(await names(active.output), ['Sem opção']);
});
test('remove MEI stops before writing when the Receita base has no opcao_mei column', async t => {
    const { dir, file } = await listFile(t);
    await assert.rejects(annotateFile({ filename: file, pool: meiPool({ withMeiColumn: false }), removeMei: true }), /opcao_mei/);
    assert.deepEqual(await fs.readdir(dir), ['lista.csv']);
    assert.equal((await annotateFile({ filename: file, pool: meiPool({ withMeiColumn: false }) })).mei, 0);
});
test('additional CSV mirrors the XLSX result, MEI already removed, ready for Importar Mailing', async t => {
    const { file } = await listFile(t, MEI_LIST.replace('Sem opção', '=1+1'));
    const result = await annotateFile({ filename: file, pool: meiPool(), removeMei: true, csv: true });
    assert.equal(path.dirname(result.csvOutput), path.dirname(result.output));
    assert.equal(path.basename(result.csvOutput, '.csv'), path.basename(result.output, '.xlsx'));
    const text = await fs.readFile(result.csvOutput, 'utf8'), lines = text.split('\r\n').filter(Boolean);
    assert.ok(text.startsWith('\uFEFF'));
    const wb = new ExcelJS.Workbook(); await wb.xlsx.readFile(result.output); const sheet = wb.worksheets[0];
    assert.equal(lines.length, sheet.rowCount);
    const quoted = values => Array.from(values, value => `"${String(value ?? '').replace(/"/g, '""')}"`).join(';');
    // S\u00F3 o cabe\u00E7alho muda: a Fastway exige "cpf", ent\u00E3o a coluna CNPJ sai com esse nome no CSV.
    assert.equal(sheet.getCell('A1').value, 'CNPJ'); assert.match(lines[0], /^\uFEFF"cpf";"Nome";"RECEITA_CODIGO"/);
    sheet.eachRow((row, number) => { if (number > 1) assert.equal(lines[number - 1], quoted(row.values.slice(1))); });
    assert.ok(!text.includes('"MEI"')); assert.ok(text.includes(`"'=1+1"`));
    assert.equal((await annotateFile({ filename: file, pool: meiPool() })).csvOutput, null);
});
test('a list whose document column is CPF holding CNPJs is consulted like a CNPJ list', async t => {
    const { file } = await listFile(t, MEI_LIST.replace('CNPJ;Nome', 'CPF;Nome'));
    const result = await annotateFile({ filename: file, pool: meiPool(), removeMei: true, csv: true });
    assert.deepEqual([result.processed, result.found, result.notFound, result.invalid, result.mei], [5, 3, 1, 1, 1]);
    assert.deepEqual(await names(result.output), ['N\u00E3o MEI', 'Sem op\u00E7\u00E3o', 'Fora da base', 'CPF']);
    assert.match(await fs.readFile(result.csvOutput, 'utf8'), /^\uFEFF"CPF";"Nome";/);
});
test('with both CNPJ and CPF columns the CNPJ is consulted and the CSV keeps both names, never two cpf columns', async t => {
    const { file } = await listFile(t, 'CPF;CNPJ;Nome\n11122233344;12345678000190;MEI\n55566677788;12ABC34501DE35;N\u00E3o MEI');
    const result = await annotateFile({ filename: file, pool: meiPool(), removeMei: true, csv: true });
    assert.deepEqual([result.found, result.mei], [2, 1]);
    assert.match(await fs.readFile(result.csvOutput, 'utf8'), /^\uFEFF"CPF";"CNPJ";"Nome";/);
});
test('cancel deletes the unfinished CSV together with the XLSX', async t => {
    const { dir, file } = await listFile(t, 'CNPJ;Nome\n' + Array.from({ length: 1001 }, () => '12ABC34501DE35;Fictícia').join('\n'));
    const controller = new AbortController();
    await assert.rejects(annotateFile({ filename: file, pool: meiPool(), csv: true, signal: controller.signal, onProgress: () => controller.abort() }), /cancelada/);
    assert.deepEqual(await fs.readdir(dir), ['lista.csv']);
});
test('keep filter drops unselected situations from the output and counts them, never the totals', async t => {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'receita-keep-')); t.after(() => fs.rm(dir, { recursive: true, force: true }));
    const file = path.join(dir, 'lista.csv'); await fs.writeFile(file, 'CNPJ;Nome\n12ABC34501DE35;Baixada\n12345678000190;Fora\n12345678901;CPF');
    const only = await annotateFile({ filename: file, pool: poolFixture(), keep: ['baixada'] });
    assert.deepEqual([only.processed, only.found, only.notFound, only.invalid, only.removed], [3, 1, 1, 1, 2]);
    const wb = new ExcelJS.Workbook(); await wb.xlsx.readFile(only.output); assert.equal(wb.worksheets[0].rowCount, 2);
    assert.equal(wb.worksheets[0].getCell('B2').value, 'Baixada');
    const active = await annotateFile({ filename: file, pool: poolFixture(), keep: ['ativa', 'nao_encontrado'] });
    assert.equal(active.removed, 2);
    const all = await annotateFile({ filename: file, pool: poolFixture() }); assert.equal(all.removed, 0);
    await assert.rejects(annotateFile({ filename: file, pool: poolFixture(), keep: [] }), /ao menos uma/);
    await assert.rejects(annotateFile({ filename: file, pool: poolFixture(), keep: ['lixo'] }), /ao menos uma/);
});
test('CSV from a CNPJ list passes the Importar Mailing header check (nome, cpf, fone1)', async t => {
    const { inspectCsv } = require('../src/main/flows/fastway');
    const { file } = await listFile(t, 'CNPJ;nome;fone1\n12ABC34501DE35;Não MEI;11999990000\n12345678000190;MEI;11999990001\n11444777000161;Fora da base;11999990002');
    const result = await annotateFile({ filename: file, pool: meiPool(), removeMei: true, csv: true });
    assert.deepEqual(await inspectCsv(result.csvOutput), { rows: 2 });
});
