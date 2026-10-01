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
