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
        assert.equal(error.loginRejected, true);
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
test('consulta de mailings recebidos filtra serviço, nome do arquivo e horário do envio (relógio de Brasília)', async () => {
    let asked;
    const fastway = loadModule('src/main/flows/fastway.js', { 'node:fs': fs, 'node:os': os, 'node:path': path, axios: { post() {}, async get(url, options) { asked = { url, options }; return { data: { Status: '10', Mailings: [
        { id: 1, campanha_id: 120, arquivo: 'RCAN parte3.csv', registros: 250000, status: 'ATIVO', data_importacao: '2026-10-24 09:55:00.1' },
        { id: 2, campanha_id: 120, arquivo: 'RCAN parte3.csv', registros: 250000, status: 'FINALIZADO', data_importacao: '2026-10-22 10:00:00' },
        { id: 3, campanha_id: 62, arquivo: 'RCAN parte3.csv', registros: 250000, status: 'ATIVO', data_importacao: '2026-10-24 10:01:00' },
        { id: 4, campanha_id: 120, arquivo: 'RCAN parte4.csv', registros: 250000, status: 'ATIVO', data_importacao: '2026-10-24 10:01:00' },
        { id: 5, campanha_id: 120, arquivo: 'RCAN parte3.csv', registros: 249000, status: 'FINALIZADO', data_importacao: '2026-10-24 10:02:30.123456' }
    ] } }; } }, 'form-data': require('form-data'), 'csv-parse': require('csv-parse'), 'node:stream/promises': require('node:stream/promises') });
    const found = await fastway.findReceived({ file: path.join(os.tmpdir(), 'RCAN parte3.csv'), campaignId: '120', since: '2026-10-24T13:00:00.000Z', credentials: { login: 'synthetic-user', password: 'synthetic-pass' } });
    assert.deepEqual(JSON.parse(JSON.stringify(found)), [{ id: 1, rows: 250000, status: 'ATIVO' }, { id: 5, rows: 249000, status: 'FINALIZADO' }]);
    assert.match(asked.url, /\/api\/fmi\/mailing\/index\.php$/);
    assert.deepEqual({ ...asked.options.params }, { api_logon: 'synthetic-user', api_senha: 'synthetic-pass', api_comando: 'Listar' });
});
test('consulta de mailings sem lista na resposta falha sem expor a senha', async () => {
    const fastway = loadModule('src/main/flows/fastway.js', { 'node:fs': fs, 'node:os': os, 'node:path': path, axios: { post() {}, async get() { return { data: { Status: '00', mensagem: 'Usuario nao encontrado!' } }; } }, 'form-data': require('form-data'), 'csv-parse': require('csv-parse'), 'node:stream/promises': require('node:stream/promises') });
    await assert.rejects(fastway.findReceived({ file: 'x.csv', campaignId: '120', since: new Date().toISOString(), credentials: { login: 'u', password: 'synthetic-pass' } }), error => error.code === 'FLOW_VALIDATION' && !/synthetic-pass/.test(error.message));
});
