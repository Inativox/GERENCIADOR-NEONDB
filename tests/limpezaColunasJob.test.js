const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const ExcelJS = require('exceljs');

test('worker mantém saída e original, reporta progresso e recusa lote concorrente', async t => {
    const { runLimpezaColunas } = require('../src/main/limpezaColunasJob');
    const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'columns-worker-'));
    t.after(() => fs.rm(directory, { recursive: true, force: true }));
    const input = path.join(directory, 'base.xlsx');
    const workbook = new ExcelJS.Workbook();
    const sheet = workbook.addWorksheet('Base');
    sheet.addRow(['Nome do Negócio', 'CNPJ', 'Telefone Celular']);
    sheet.addRow(['Empresa Sol', '04.252.011/0001-10', '5521998364849']);
    await workbook.xlsx.writeFile(input);
    const original = await fs.readFile(input);
    const messages = [];
    let timerRan = false;
    const job = runLimpezaColunas([input, path.join(directory, 'missing.xlsx')], event => messages.push(event));
    const timer = setTimeout(() => { timerRan = true; }, 0);
    t.after(() => clearTimeout(timer));
    await assert.rejects(runLimpezaColunas([input], () => {}), /andamento/i);
    const result = await job;
    assert.equal(timerRan, true);
    assert.equal(result.processados, 1);
    assert.equal(result.pulados, 1);
    assert.deepEqual(await fs.readFile(input), original);
    const output = new ExcelJS.Workbook();
    await output.xlsx.readFile(result.primeiraSaida);
    assert.deepEqual(output.worksheets[0].getRow(2).values.slice(1), ['Empresa Sol', 4252011000110, 21998364849]);
    assert.equal(output.worksheets[0].getCell('B2').numFmt, '00000000000000');
    assert.ok(messages.some(event => event.type === 'log' && event.message.includes('PULADO')));
    assert.ok(messages.some(event => event.type === 'progress' && event.current === 2 && event.total === 2));
    const next = await runLimpezaColunas([input], () => {});
    assert.equal(next.processados, 1);
    assert.notEqual(next.primeiraSaida, result.primeiraSaida);
});

test('worker rejeita entrada inválida antes de alocar processamento', async () => {
    const { runLimpezaColunas } = require('../src/main/limpezaColunasJob');
    await assert.rejects(runLimpezaColunas([], () => {}), /arquivo/i);
    await assert.rejects(runLimpezaColunas([null], () => {}), /arquivo/i);
});
