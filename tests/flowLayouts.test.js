const test = require('node:test');
const assert = require('node:assert/strict');
const { listLayoutFields, validateLayout } = require('../src/main/flows/layouts');
const { getFormat, listFormats, mapOutputRow } = require('../src/main/flows/formats');
const valid = () => ({ nome: 'Meu layout', colunas: [{ header: 'Documento', campo: 'cnpj' }, { header: 'Celular principal', campo: 'telefone_1' }] });

test('layout catalog exposes known Receita fields, phone slots and compositions without arbitrary fields/costs', () => {
    const fields = listLayoutFields();
    assert.equal(new Set(fields.map(field => field.id)).size, fields.length);
    for (const id of ['cnpj', 'telefone_10', 'situacao_cadastral', 'nome_socio', 'manual', 'composto']) assert.ok(fields.some(field => field.id === id));
    assert.ok(!fields.some(field => /custo|password|connection/i.test(field.id)));
});
test('custom validation rejects duplicate headers, unknown fields, gaps and inconsistent phone bindings', () => {
    assert.throws(() => validateLayout({ ...valid(), colunas: [{ header: 'fone1', campo: 'email' }] }), /reservado/);
    assert.throws(() => validateLayout({ ...valid(), colunas: [{ header: 'fone1', campo: 'telefone_2' }] }), /Telefone 1/);
    assert.throws(() => validateLayout({ ...valid(), colunas: [{ header: 'Celular', campo: 'telefone_2' }] }), /sequência/);
    assert.throws(() => validateLayout({ ...valid(), colunas: [{ header: 'a', campo: 'telefone_1' }, { header: 'b', campo: 'telefone_1' }] }), /única/);
    assert.throws(() => validateLayout({ ...valid(), colunas: [{ header: 'a', campo: 'telefone_11' }] }), /origem/);
    assert.throws(() => validateLayout({ ...valid(), colunas: [{ header: 'a', campo: 'cnpj' }] }), /Telefone 1/);
    assert.throws(() => validateLayout({ ...valid(), colunas: [{ header: 'Nome', campo: 'cnpj' }, { header: ' nome ', campo: 'telefone_1' }] }), /repetido/);
    assert.throws(() => validateLayout({ ...valid(), colunas: [{ header: 'h', campo: '__proto__' }] }), /origem/);
    assert.throws(() => validateLayout({ ...valid(), colunas: [{ header: 'h\nnewline', campo: 'telefone_1' }] }), /quebras/);
    assert.throws(() => validateLayout({ ...valid(), colunas: Array(61).fill({ header: 'h', campo: 'telefone_1' }) }), /60/);
});
test('all built-in layouts can be customized, preserving inherited combinations and phone capacity', () => {
    for (const source of listFormats()) {
        const saved = validateLayout({ ...source, id: '' });
        const context = { includeSituacao: false, operation: 'c6' };
        assert.deepEqual(mapOutputRow({ phones: ['11987654321', '21987654321'], cnpj: '04252011000110', estado: 'SP', cidade: 'Campinas' }, saved, context), mapOutputRow({ phones: ['11987654321', '21987654321'], cnpj: '04252011000110', estado: 'SP', cidade: 'Campinas' }, source, context));
        assert.ok(saved.colunas.some(column => column.campo === 'telefone_1'));
    }
});
test('custom output respects renamed phones, manual literals, ordered compositions and situation additions', () => {
    const source = validateLayout({ nome: 'Novo', colunas: [...valid().colunas, { header: 'Campanha', campo: 'manual', valor_manual: '=1+1;<script>' }, { header: 'Local', campo: 'composto', partes: ['estado', 'cidade'], sep: ' / ' }, { header: 'situacao_cadastral', campo: 'manual', valor_manual: 'Texto próprio' }] });
    const frozen = getFormat(source.id, {}, [source]);
    const values = mapOutputRow({ cnpj: '04252011000110', phones: ['11987654321'], estado: 'SP', cidade: 'Campinas', situacao_cadastral: 'Ativa' }, frozen);
    assert.deepEqual(values.slice(0, 5), ['04252011000110', '11987654321', '=1+1;<script>', 'SP / Campinas', 'Texto próprio']);
    assert.equal(new Set(frozen.colunas.map(column => column.header)).size, frozen.colunas.length);
    assert.ok(frozen.colunas.some(column => column.header === 'situacao_cadastral_2'));
    assert.equal(getFormat(source.id, { includeSituacao: false }, [source]).colunas.length, source.colunas.length);
    frozen.colunas[0].header = 'mutated'; assert.equal(source.colunas[0].header, 'Documento');
});
test('composition validation does not accept phone slots, nesting, missing fields or oversized literals', () => {
    for (const partes of [['estado'], ['estado', 'missing'], ['estado', 'telefone_1'], ['manual', 'cidade'], Array(9).fill('cidade')]) {
        assert.throws(() => validateLayout({ ...valid(), colunas: [...valid().colunas, { header: 'c', campo: 'composto', partes }] }), /combine/);
    }
    assert.throws(() => validateLayout({ ...valid(), colunas: [...valid().colunas, { header: 'm', campo: 'manual', valor_manual: 'a'.repeat(1001) }] }), /1000/);
});
