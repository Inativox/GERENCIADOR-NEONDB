const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const XLSX = require('xlsx');

const { processFile } = require('../src/main/handlers/limpeza');
const { inspectCsv } = require('../src/main/flows/fastway');

function criarPlanilha(t, nome, dados) {
    const pasta = fs.mkdtempSync(path.join(os.tmpdir(), 'limpeza-csv-'));
    t.after(() => fs.rmSync(pasta, { recursive: true, force: true }));

    const caminho = path.join(pasta, nome);
    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(workbook, XLSX.utils.aoa_to_sheet(dados), 'Base');
    XLSX.writeFile(workbook, caminho);
    return caminho;
}

function opcoes(overrides = {}) {
    return {
        backup: false,
        checkBlocklist: false,
        removeLandlines: false,
        checkNumerosInvalidos: false,
        fillLivre5: false,
        generateCsv: false,
        cleaningDate: '22/10/2026',
        isAutoRoot: true,
        ...overrides
    };
}

const event = { sender: { send() {} } };
const caminhoCsv = caminho => path.join(path.dirname(caminho), `${path.parse(caminho).name}.csv`);

test('gera o csv com as linhas ja limpas, ao lado do xlsx', async (t) => {
    const caminho = criarPlanilha(t, 'mailing_sp.xlsx', [
        ['cpf', 'nome', 'fone1', 'fone2'],
        ['01234567000199', '123 Empresa A', '11999998888', ''],
        ['98765432000188', 'Empresa na raiz', '11977776666', '']
    ]);

    const resultado = await processFile(
        { path: caminho, id: 'arquivo-1' },
        new Set(['98765432000188']),
        opcoes({ generateCsv: true }),
        event
    );

    const csv = fs.readFileSync(caminhoCsv(caminho), 'utf8');
    assert.ok(csv.startsWith('\uFEFF'), 'CSV precisa de BOM para o Excel abrir acentos');
    assert.equal(csv.slice(1), '"cpf";"nome";"fone1";"fone2"\r\n"01234567000199";"Empresa A";"11999998888";""\r\n');
    assert.ok(resultado.processed);
    assert.ok(resultado.logs.some(log => log.includes('mailing_sp.csv')));
});

test('o csv gerado passa na validacao do Importar Mailing', async (t) => {
    const caminho = criarPlanilha(t, 'mailing.xlsx', [
        ['cpf', 'nome', 'fone1', 'fone2', 'livre5'],
        ['01234567000199', 'Empresa; Ltda', '11999998888'],
        ['11222333000144', 'Empresa "B"', '11977776666', '1133334444', 'obs']
    ]);

    await processFile({ path: caminho, id: 'arquivo-2' }, new Set(), opcoes({ generateCsv: true }), event);

    assert.deepEqual(await inspectCsv(caminhoCsv(caminho)), { rows: 2 });
});

test('nao gera csv quando a opcao esta desligada', async (t) => {
    const caminho = criarPlanilha(t, 'sem_csv.xlsx', [
        ['cpf', 'nome', 'fone1'],
        ['01234567000199', 'Empresa A', '11999998888']
    ]);

    await processFile({ path: caminho, id: 'arquivo-3' }, new Set(), opcoes(), event);

    assert.equal(fs.existsSync(caminhoCsv(caminho)), false);
});

test('lista que ja e csv nao ganha um segundo csv', async (t) => {
    const caminho = criarPlanilha(t, 'ja_csv.csv', [
        ['cpf', 'nome', 'fone1'],
        ['01234567000199', 'Empresa A', '11999998888']
    ]);

    const resultado = await processFile({ path: caminho, id: 'arquivo-5' }, new Set(), opcoes({ generateCsv: true }), event);

    assert.deepEqual(fs.readdirSync(path.dirname(caminho)), ['ja_csv.csv']);
    assert.ok(resultado.logs.some(log => log.includes('já é CSV')));
});

test('falha ao gravar o csv nao desfaz a limpeza do xlsx', async (t) => {
    const caminho = criarPlanilha(t, 'bloqueado.xlsx', [
        ['cpf', 'nome', 'fone1'],
        ['01234567000199', '123 Empresa A', '11999998888']
    ]);
    // Uma pasta com o nome do CSV simula o arquivo travado (ex.: aberto no Excel).
    fs.mkdirSync(caminhoCsv(caminho));

    const resultado = await processFile({ path: caminho, id: 'arquivo-4' }, new Set(), opcoes({ generateCsv: true }), event);

    assert.ok(resultado.processed);
    const dados = XLSX.utils.sheet_to_json(XLSX.readFile(caminho).Sheets.Base, { header: 1 });
    assert.equal(dados[1][1], 'Empresa A');
    assert.ok(resultado.logs.some(log => log.includes('Não foi possível gravar o CSV')));
});
