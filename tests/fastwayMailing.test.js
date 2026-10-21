const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), os = require('node:os'), path = require('node:path');
const { inspectCsv } = require('../src/main/flows/fastway');

function csv(t, content) {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'fastway-csv-')); t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
    const file = path.join(directory, 'mailing.csv'); fs.writeFileSync(file, content); return file;
}
const rejects = (file, pattern) => assert.rejects(inspectCsv(file), error => error.code === 'FLOW_VALIDATION' && pattern.test(error.message));

test('CSV com nome, cpf e fone1 em qualquer caixa e com outras colunas conta só as linhas de dados', async t => {
    const file = csv(t, '﻿ NOME ;Cpf;email;FONE1\r\nAna;123;ana@x.com;11999990000\r\nBia;456;;11999990001\r\n\r\nCaio;789;;11999990002\r\n');
    assert.deepEqual(await inspectCsv(file), { rows: 3 });
});
test('CSV sem uma das colunas obrigatórias é recusado dizendo qual falta', async t => {
    await rejects(csv(t, 'nome;fone1;email\nAna;11999990000;a@x.com\n'), /cpf/);
    await rejects(csv(t, 'nome;cpf\nAna;123\n'), /fone1/);
});
test('CSV separado por vírgula é recusado pedindo ponto e vírgula', async t => {
    await rejects(csv(t, 'nome,cpf,fone1\nAna,123,11999990000\n'), /ponto e vírgula/);
});
test('CSV vazio ou só com cabeçalho é recusado', async t => {
    await rejects(csv(t, ''), /vazio/);
    await rejects(csv(t, 'nome;cpf;fone1\r\n'), /vazio/);
});
test('linha com número de colunas diferente do cabeçalho é recusada antes do envio', async t => {
    await rejects(csv(t, 'nome;cpf;fone1\nAna;123;11999990000\nBia;456\n'), /linha 3/);
});
test('aceita exatamente 250.000 registros e recusa o 250.001º', async t => {
    assert.deepEqual(await inspectCsv(csv(t, 'nome;cpf;fone1\n' + 'Ana;123;11999990000\n'.repeat(250000))), { rows: 250000 });
    await rejects(csv(t, 'nome;cpf;fone1\n' + 'Ana;123;11999990000\n'.repeat(250001)), /250\.000/);
});

const loadModule = require('./helpers/loadModule');
function fastwayAnswering(post) {
    return loadModule('src/main/flows/fastway.js', { 'node:fs': fs, 'node:os': os, 'node:path': path, axios: { post }, 'form-data': require('form-data'), 'csv-parse': require('csv-parse'), 'node:stream/promises': require('node:stream/promises') });
}
test('erro HTTP da Fastway mostra o que ela respondeu, sem HTML e sem a senha', async t => {
    const file = csv(t, 'nome;cpf;fone1\nAna;123;11999990000\n');
    const fastway = fastwayAnswering(async () => { throw Object.assign(new Error('Request failed'), { response: { status: 481, data: '<html><b>Logon   synthetic-user</b> sem permissão (senha synthetic-pass)</html>' } }); });
    await assert.rejects(fastway.upload({ file, campaignId: '63', credentials: { login: 'synthetic-user', password: 'synthetic-pass' } }), error => {
        assert.equal(error.code, 'FLOW_VALIDATION');
        assert.match(error.message, /HTTP 481/); assert.match(error.message, /Logon synthetic-user sem permissão/);
        assert.doesNotMatch(error.message, /synthetic-pass|<b>/);
        return true;
    });
});
test('erro HTTP sem corpo continua com a mensagem de conferir o painel', async t => {
    const file = csv(t, 'nome;cpf;fone1\nAna;123;11999990000\n');
    const fastway = fastwayAnswering(async () => { throw Object.assign(new Error('Request failed'), { response: { status: 502, data: '' } }); });
    await assert.rejects(fastway.upload({ file, campaignId: '63', credentials: { login: 'synthetic-user', password: 'synthetic-pass' } }), /^Error: A Fastway respondeu HTTP 502\. Confirme no painel/);
});
