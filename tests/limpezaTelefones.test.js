const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const XLSX = require('xlsx');
const { processFile } = require('../src/main/handlers/limpeza');

function fixture(t, name, data) {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'telefones-limpeza-'));
    t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
    const file = path.join(directory, name);
    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(workbook, XLSX.utils.aoa_to_sheet(data), 'Base');
    XLSX.writeFile(workbook, file);
    return file;
}
function read(file) {
    const wb = XLSX.readFile(file);
    return XLSX.utils.sheet_to_json(wb.Sheets.Base, { header: 1, defval: null });
}
const event = { sender: { send() {} } };
const options = { isAutoRoot: true, checkBlocklist: false, autoAdjust: false };
const context = () => ({ cnpjs: new Set(), telefones: new Set() });

function isolatedProcessor(query, write = (wb, file) => XLSX.writeFile(wb, file)) {
    return require('./helpers/loadModule')('src/main/handlers/limpeza.js', {
        electron: {}, path, fs, xlsx: XLSX, exceljs: {}, '../state': {},
        '../flows/config': require('../src/main/flows/config'),
        '../database/connection': { PROHIBITED_CNAES: new Set(), queryWithRetry: query },
        './files': { readSpreadsheet: async file => XLSX.readFile(file), writeSpreadsheet: write },
        '../limpezaTelefones': require('../src/main/limpezaTelefones')
    }, { setImmediate }).processFile;
}

test('ajuste obrigatório compacta fones por número da coluna e elimina linhas sem contato', async t => {
    const file = fixture(t, 'ajuste.xlsx', [
        ['cnpj', 'fone3', 'fone1', 'fone2', 'nome'],
        ['11111111000191', '5521998364849', null, null, 'Empresa A'],
        ['22222222000191', null, '11111111', '9999999', 'Empresa B'],
        ['33333333000191', '1132345678', null, '21987654321', 'Empresa C'],
        ['44444444000191', null, null, null, 'Empresa D']
    ]);
    await processFile({ path: file, id: 'one' }, new Set(), options, event, context());
    assert.deepEqual(read(file), [
        ['cnpj', 'fone3', 'fone1', 'fone2', 'nome'],
        ['11111111000191', null, 21998364849, null, 'Empresa A'],
        ['33333333000191', null, 21987654321, 1132345678, 'Empresa C']
    ]);
});

test('cruza listas por CNPJ e telefone normalizados e mantém os outros contatos da linha', async t => {
    const batch = context();
    const first = fixture(t, 'primeira.xlsx', [
        ['cnpj', 'fone1', 'fone2'],
        ['04.252.011/0001-10', '55 (21) 99836-4849', '1132345678'],
        ['22222222000191', '11987654321', null]
    ]);
    const second = fixture(t, 'segunda.xlsx', [
        ['cnpj', 'fone1', 'fone2', 'fone3'],
        [4252011000110, '21981234567', null, null],
        ['33333333000191', '21998364849', '21987654321', null],
        ['44444444000191', '1132345678', '11987654321', null],
        ['55555555000191', '21987654321', null, '31987654321']
    ]);
    await processFile({ path: first, id: 'first' }, new Set(), options, event, batch);
    const result = await processFile({ path: second, id: 'second' }, new Set(), options, event, batch);
    assert.deepEqual(read(second), [
        ['cnpj', 'fone1', 'fone2', 'fone3'],
        ['33333333000191', 21987654321, null, null],
        ['55555555000191', 31987654321, null, null]
    ]);
    assert.ok(result.logs.some(log => log.includes('CNPJs repetidos')));
    assert.ok(result.logs.some(log => log.includes('Telefones repetidos')));
});

test('remove dígitos repetidos também com DDD e preserva DDD 55 e números científicos completos', async t => {
    const file = fixture(t, 'sujos.xlsx', [
        ['cnpj', 'fone1', 'fone2', 'fone3'],
        ['11111111000191', '21999999999', '1188888888', '00000000000'],
        ['22222222000191', '55998765432', '5.521998364849E+12', '5.52199E+12'],
        ['33333333000191', '11111111', '11987654321,00', null]
    ]);
    await processFile({ path: file, id: 'phones' }, new Set(), { ...options, isAutoRoot: false }, event, context());
    assert.deepEqual(read(file), [
        ['cnpj', 'fone1', 'fone2', 'fone3'],
        ['22222222000191', 55998765432, 21998364849, null],
        ['33333333000191', 11987654321, null, null]
    ]);
});

test('linha sem telefone e CNPJ filtrado pela raiz não reservam chaves do lote', async t => {
    const batch = context();
    const first = fixture(t, 'filtrada.xlsx', [
        ['cnpj', 'fone1', 'fone2'],
        ['11111111000191', '21998364849', null],
        ['22222222000191', '11111111', null]
    ]);
    const second = fixture(t, 'mantida.xlsx', [
        ['cnpj', 'fone1', 'fone2'],
        ['11111111000191', '21998364849', null],
        ['22222222000191', '11987654321', null]
    ]);
    await processFile({ path: first, id: 'a' }, new Set(['11111111000191']), options, event, batch);
    await processFile({ path: second, id: 'b' }, new Set(), options, event, batch);
    assert.equal(read(second).length, 3);
});

test('evita telefones/CNPJs repetidos no próprio arquivo e não persiste cruzamento entre lotes', async t => {
    const file = fixture(t, 'repetidos.xlsx', [
        ['cnpj', 'fone1', 'fone2'],
        ['11111111000191', '21998364849', '21998364849'],
        ['11111111000191', '11987654321', null],
        ['22222222000191', '21998364849', '11987654321']
    ]);
    await processFile({ path: file, id: 'a' }, new Set(), options, event, context());
    const expected = read(file);
    assert.deepEqual(expected, [
        ['cnpj', 'fone1', 'fone2'],
        ['11111111000191', 21998364849, null],
        ['22222222000191', 11987654321, null]
    ]);
    await processFile({ path: file, id: 'b' }, new Set(), options, event, context());
    assert.deepEqual(read(file), expected);
});

test('dez listas compartilham o cruzamento na ordem selecionada', async t => {
    const batch = context();
    const allDocuments = [], allPhones = [];
    for (let i = 0; i < 10; i++) {
        const file = fixture(t, `lista-${i}.xlsx`, [
            ['cnpj', 'fone1', 'fone2'],
            ['04252011000110', '21998364849', null],
            [String(22345678000100 + i), '21998364849', String(11987654320 + i)]
        ]);
        await processFile({ path: file, id: String(i) }, new Set(), options, event, batch);
        const rows = read(file).slice(1);
        assert.equal(rows.length, i === 0 ? 2 : 1);
        assert.equal(rows.at(-1)[1], 11987654320 + i);
        rows.forEach(row => {
            allDocuments.push(row[0]);
            allPhones.push(...row.slice(1).filter(Boolean));
        });
    }
    assert.equal(new Set(allDocuments).size, 11);
    assert.equal(new Set(allPhones).size, allPhones.length);
});

test('filtros do banco precedem cruzamento e comparam telefones com e sem DDI', async t => {
    const calls = [];
    const run = isolatedProcessor(async (sql, parameters) => {
        calls.push(parameters[0]);
        return { rows: [{ telefone: sql.includes('blocklist') ? '5521998364849' : '5511987654321' }] };
    });
    const batch = context();
    const first = fixture(t, 'banco.xlsx', [
        ['cnpj', 'fone1', 'fone2'],
        ['11111111000191', '21998364849', '31987654321'],
        ['22222222000191', '11987654321', null],
        ['33333333000191', '11987654321', '41987654321']
    ]);
    await run({ path: first, id: 'a' }, new Set(), { checkBlocklist: true, checkNumerosInvalidos: true }, event, batch);
    assert.deepEqual(read(first).slice(1), [['33333333000191', 41987654321, null]]);
    assert.ok(calls.every(phones => phones.includes('21998364849') && phones.includes('5521998364849')));
    const second = fixture(t, 'depois-banco.xlsx', [
        ['cnpj', 'fone1', 'fone2'],
        ['11111111000191', '31987654321', null],
        ['22222222000191', '51987654321', null]
    ]);
    await run({ path: second, id: 'b' }, new Set(), { checkBlocklist: true, checkNumerosInvalidos: true }, event, batch);
    assert.equal(read(second).length, 3);
});

test('falha de gravação não reserva CNPJs nem telefones para as próximas listas', async t => {
    const run = isolatedProcessor(async () => { throw new Error('Consulta inesperada'); }, () => { throw new Error('Arquivo em uso'); });
    const batch = context();
    const file = fixture(t, 'falha.xlsx', [['cnpj', 'fone1'], ['11111111000191', '21998364849']]);
    await assert.rejects(run({ path: file, id: 'a' }, new Set(), options, event, batch), /Arquivo em uso/);
    assert.equal(batch.cnpjs.size, 0);
    assert.equal(batch.telefones.size, 0);
    assert.equal(read(file).length, 2);
    await processFile({ path: file, id: 'b' }, new Set(), options, event, batch);
    assert.equal(batch.telefones.size, 1);
});
