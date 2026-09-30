const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const XLSX = require('xlsx');

const { processFile } = require('../src/main/handlers/limpeza');

function criarPlanilha(t, nome, dados) {
    const pasta = fs.mkdtempSync(path.join(os.tmpdir(), 'limpeza-livre5-'));
    t.after(() => fs.rmSync(pasta, { recursive: true, force: true }));

    const caminho = path.join(pasta, nome);
    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(workbook, XLSX.utils.aoa_to_sheet(dados), 'Base');
    XLSX.writeFile(workbook, caminho);
    return caminho;
}

function lerDados(caminho) {
    const workbook = XLSX.readFile(caminho);
    return XLSX.utils.sheet_to_json(workbook.Sheets[workbook.SheetNames[0]], { header: 1 });
}

function opcoes(overrides = {}) {
    return {
        backup: false,
        checkDb: false,
        saveToDb: false,
        checkBlocklist: false,
        removeLandlines: false,
        checkNumerosInvalidos: false,
        fillLivre5: false,
        cleaningDate: '10/09/2026',
        isAutoRoot: true,
        ...overrides
    };
}

const event = { sender: { send() {} } };

test('preenche livre5 com nome do arquivo e data da limpeza', async (t) => {
    const caminho = criarPlanilha(t, 'clientes_sp.xlsx', [
        ['cnpj', ' Livre5 '],
        ['12345678000199', 'valor antigo'],
        ['98765432000188', '']
    ]);

    const resultado = await processFile(
        { path: caminho, id: 'arquivo-1' },
        new Set(),
        opcoes({ fillLivre5: true }),
        event
    );

    const dados = lerDados(caminho);
    assert.equal(dados[1][1], 'clientes_sp | 10/09/2026');
    assert.equal(dados[2][1], 'clientes_sp | 10/09/2026');
    assert.ok(resultado.logs.some(log => log.includes('clientes_sp | 10/09/2026')));
});

test('mantem livre5 intacto quando a opcao esta desligada', async (t) => {
    const caminho = criarPlanilha(t, 'clientes.xlsx', [
        ['cnpj', 'livre5'],
        ['12345678000199', 'valor original']
    ]);

    await processFile(
        { path: caminho, id: 'arquivo-2' },
        new Set(),
        opcoes(),
        event
    );

    assert.equal(lerDados(caminho)[1][1], 'valor original');
});

test('avisa e continua a limpeza quando livre5 nao existe', async (t) => {
    const caminho = criarPlanilha(t, 'sem_livre5.xlsx', [
        ['cnpj', 'nome'],
        ['12345678000199', '123 Empresa 456']
    ]);

    const resultado = await processFile(
        { path: caminho, id: 'arquivo-3' },
        new Set(),
        opcoes({ fillLivre5: true }),
        event
    );

    assert.equal(lerDados(caminho)[1][1], 'Empresa');
    assert.ok(resultado.logs.some(log => log.includes('não possui a coluna "livre5"')));
});
