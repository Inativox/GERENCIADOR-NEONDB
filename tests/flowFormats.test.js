'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { listFormats, getFormat, mapOutputRow } = require('../src/main/flows/formats');

const record = {
    cnpj: '00123456000190', razao_social: 'Acme Comércio 12345678901',
    data_abertura: '2025-01-02', email: 'contato@acme.test', atividade_principal_cod: '0111301',
    atividade_principal: 'Cultivo', estado: 'SP', cidade: 'Campinas',
    situacao_cadastral_cod: '02', situacao_cadastral: 'Ativa', situacao_cadastral_data: '2026-09-30',
    situacao_motivo: 'Sem motivo', phones: ['11987654321', '21987654321', '31987654321']
};
function asObject(values, format) { return Object.fromEntries(format.colunas.map((column, index) => [column.header, values[index]])); }

test('formats expose all inherited layouts without cost fields or shared mutable state', () => {
    const formats = listFormats();
    assert.deepEqual(formats.map(item => item.id), ['bernardo', 'olos', 'empresaaqui', 'plano_porto', 'padrao']);
    assert.ok(formats.every(item => item.nome && item.colunas.length));
    assert.doesNotMatch(JSON.stringify(formats), /custo|situacao_cadastral/);
    formats[0].colunas[0].header = 'mutated';
    assert.equal(listFormats()[0].colunas[0].header, 'nome');
    const first = getFormat('padrao');
    first.colunas[0].campo = 'mutated';
    assert.equal(getFormat('padrao').colunas[0].campo, 'razao_social');
    assert.throws(() => getFormat('__proto__'), /desconhecido/);
});

test('padrao maps textual documents, dates, operation, compositions and situation in column order', () => {
    const format = getFormat('padrao');
    const output = mapOutputRow(record, format, { operation: 'santander' });
    assert.equal(output.length, format.colunas.length);
    assert.deepEqual(output, ['Acme Comércio', '00123456000190', '02/01/2025', 'contato@acme.test', '0111301 - Cultivo', 'SP - Campinas', '', 'SANTANDER', '11987654321', '21987654321', '31987654321', '02', 'Ativa', '30/09/2026', 'Sem motivo']);
    assert.ok(output.every(value => typeof value === 'string'));
});

test('situation can be omitted explicitly; included columns never duplicate', () => {
    const format = getFormat('padrao', { includeSituacao: false });
    assert.equal(format.colunas.length, 11);
    assert.equal(mapOutputRow(record, format, { includeSituacao: false }).length, 11);
    assert.equal(getFormat('padrao').colunas.filter(column => column.campo === 'situacao_cadastral_cod').length, 1);
});

test('OLOS manual phone slots retain enriched contacts in fone3 through fone10', () => {
    const format = getFormat('olos', { includeSituacao: false });
    const enriched = { ...record, phones: Array.from({ length: 10 }, (_, index) => `119876543${String(index).padStart(2, '0')}`) };
    const output = asObject(mapOutputRow(enriched, format, { operation: 'pagbank' }), format);
    assert.equal(output.livre5, 'OLOS MB');
    assert.equal(output.livre7, 'PAGBANK');
    assert.equal(output.livre1, '2025');
    for (let index = 1; index <= 10; index++) assert.equal(output[`fone${index}`], enriched.phones[index - 1]);
    assert.deepEqual(enriched.phones, Array.from({ length: 10 }, (_, index) => `119876543${String(index).padStart(2, '0')}`));
});

test('EmpresaAqui alternate phone headers and standard manual slots use authoritative cleaned phones', () => {
    for (const id of ['empresaaqui', 'bernardo', 'padrao']) {
        const format = getFormat(id);
        const output = asObject(mapOutputRow({ ...record, telefone_principal: '11912345678', phones: [] }, format), format);
        assert.equal(output.fone1 ?? output['telefone 1'], '');
        assert.equal(output.fone2 ?? output['telefone 2'], '');
    }
    const format = getFormat('empresaaqui');
    const output = asObject(mapOutputRow(record, format), format);
    assert.equal(output['telefone 1'], record.phones[0]);
    assert.equal(output['telefone 2'], record.phones[1]);
});

test('livre5 uses job name and frozen date only when enabled and otherwise preserves layout manual value', () => {
    const format = getFormat('olos');
    const context = { fillLivre5: true, jobName: 'Campanha setembro.xlsx', date: '2026-09-30' };
    assert.equal(asObject(mapOutputRow(record, format, context), format).livre5, 'Campanha setembro | 30/09/2026');
    assert.equal(asObject(mapOutputRow(record, format, { ...context, fillLivre5: false }), format).livre5, 'OLOS MB');
});

test('CPF enrichment adds livre6 when requested and preserves the document as text', () => {
    const format = getFormat('padrao', { fillCpf: true });
    const values = asObject(mapOutputRow({ ...record, livre6: '01234567890' }, format), format);
    assert.equal(values.livre6, '01234567890');
    assert.equal(format.colunas.filter(column => column.header === 'livre6').length, 1);
    assert.equal(getFormat('padrao').colunas.some(column => column.header === 'livre6'), false);
    assert.equal(asObject(mapOutputRow(record, format), format).livre6, '');
});

test('masked Receita CPF fallback stays literal while full enrichment CPF takes precedence', () => {
    const format = getFormat('padrao', { fillCpf: true });
    for (const cpf_socio of ['***123456**', 'XXX.123.456-XX', '•••.123.456-••']) {
        assert.equal(asObject(mapOutputRow({ ...record, cpf_socio }, format), format).livre6, cpf_socio);
        assert.equal(asObject(mapOutputRow({ ...record, cpf_socio, livre6: '01234567890' }, format), format).livre6, '01234567890');
    }
    assert.equal(asObject(mapOutputRow({ ...record, cpf_socio: '012.345.678-90' }, format), format).livre6, '01234567890');
});

test('compositions collapse missing parts and values stay literal for writer escaping', () => {
    const format = { colunas: [
        { header: 'composite', campo: 'composto', partes: ['estado', 'cidade'], sep: ' - ' },
        { header: 'email', campo: 'email' }, { header: 'manual', campo: 'manual', valor_manual: '=1+1;"quoted"\nnext' },
        { header: 'cpf_socio', campo: 'cpf_socio' }, { header: 'cep', campo: 'cep' }
    ] };
    const output = mapOutputRow({ estado: 'SP', email: 'a;"b"\nc', cpf_socio: '01234567890', cep: '00123000' }, format);
    assert.deepEqual(output, ['SP', 'a;"b"\nc', '=1+1;"quoted"\nnext', '01234567890', '00123000']);
    assert.deepEqual(mapOutputRow({}, format).slice(0, 2), ['', '']);
});

test('raw fallback phones use existing normalization and drop invalid contacts', () => {
    const format = getFormat('bernardo', { includeSituacao: false });
    const output = asObject(mapOutputRow({ telefone_principal: '+55 (11) 98765-4321', telefone_secundario: 'abc' }, format), format);
    assert.equal(output.fone1, '11987654321');
    assert.equal(output.fone2, '');
});

test('PLANO DE SAUDE layout keeps geographic and address identifiers as text', () => {
    const format = getFormat('plano_porto', { includeSituacao: false });
    const output = asObject(mapOutputRow({ ...record, cep: '00123000', logradouro: 'Rua Um', bairro: 'Centro' }, format), format);
    assert.equal(output.CNPJ, '00123456000190');
    assert.equal(output.CEP, '00123000');
    assert.equal(output['ANO EMPRESA'], '2025');
    assert.equal(output.LOGRADOURO, 'Rua Um');
});
