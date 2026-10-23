const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const XLSX = require('xlsx');

const { processFile, parseKeepCnaes } = require('../src/main/handlers/limpeza');

function criarPlanilha(t, nome, dados) {
    const pasta = fs.mkdtempSync(path.join(os.tmpdir(), 'limpeza-cnaes-'));
    t.after(() => fs.rmSync(pasta, { recursive: true, force: true }));

    const caminho = path.join(pasta, nome);
    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(workbook, XLSX.utils.aoa_to_sheet(dados), 'Base');
    XLSX.writeFile(workbook, caminho);
    return caminho;
}

function lerNomes(caminho) {
    const workbook = XLSX.readFile(caminho);
    const dados = XLSX.utils.sheet_to_json(workbook.Sheets[workbook.SheetNames[0]], { header: 1 });
    return dados.slice(1).map(linha => linha[1]);
}

function opcoes(overrides = {}) {
    return {
        backup: false,
        checkBlocklist: false,
        removeLandlines: false,
        checkNumerosInvalidos: false,
        fillLivre5: false,
        cleaningDate: '23/10/2026',
        isAutoRoot: true,
        ...overrides
    };
}

const event = { sender: { send() {} } };

const BASE_COM_CNAES = [
    ['cnpj', 'nome', 'fone1', 'cnae'],
    ['11111111000111', 'Mantida formatada', '11999990001', '4781-4/00'],
    ['22222222000122', 'Mantida numerica', '11999990002', 5611201],
    ['33333333000133', 'Outro CNAE', '11999990003', '4711302'],
    ['44444444000144', 'Sem CNAE', '11999990004', ''],
    ['55555555000155', 'Zero perdido no Excel', '11999990005', 111301]
];

test('mantem so as linhas com CNAE da lista e conta as removidas no resumo', async (t) => {
    const caminho = criarPlanilha(t, 'cnaes.xlsx', BASE_COM_CNAES);

    const resultado = await processFile(
        { path: caminho, id: 'arquivo-1' },
        new Set(),
        opcoes({ keepCnaes: new Set(['4781400', '5611201', '0111301']) }),
        event
    );

    assert.ok(resultado.processed);
    assert.deepEqual(lerNomes(caminho), ['Mantida formatada', 'Mantida numerica', 'Zero perdido no Excel']);
    assert.ok(resultado.logs.some(log => log.includes('Fora da lista de CNAEs: 2')));
});

test('sem a lista de CNAEs, nenhuma linha sai por CNAE permitido', async (t) => {
    const caminho = criarPlanilha(t, 'sem_filtro.xlsx', BASE_COM_CNAES);

    await processFile({ path: caminho, id: 'arquivo-2' }, new Set(), opcoes(), event);

    assert.deepEqual(lerNomes(caminho), ['Mantida formatada', 'Mantida numerica', 'Outro CNAE', 'Sem CNAE', 'Zero perdido no Excel']);
});

test('CNAE proibido sai mesmo quando esta na lista colada', async (t) => {
    const caminho = criarPlanilha(t, 'proibido.xlsx', [
        ['cnpj', 'nome', 'fone1', 'cnae'],
        ['11111111000111', 'Proibida', '11999990001', '4731800'],
        ['22222222000122', 'Permitida', '11999990002', '4781400']
    ]);

    await processFile(
        { path: caminho, id: 'arquivo-3' },
        new Set(),
        opcoes({ keepCnaes: new Set(['4731800', '4781400']) }),
        event
    );

    assert.deepEqual(lerNomes(caminho), ['Permitida']);
});

test('planilha sem coluna de CNAE e pulada intacta quando o filtro esta ativo', async (t) => {
    const caminho = criarPlanilha(t, 'sem_coluna.xlsx', [
        ['cnpj', 'nome', 'fone1'],
        ['11111111000111', '123 Empresa A', '11999990001']
    ]);
    const original = fs.readFileSync(caminho);

    const resultado = await processFile(
        { path: caminho, id: 'arquivo-4' },
        new Set(),
        opcoes({ keepCnaes: new Set(['4781400']) }),
        event
    );

    assert.equal(resultado.processed, false);
    assert.deepEqual(fs.readFileSync(caminho), original);
    assert.deepEqual(fs.readdirSync(path.dirname(caminho)), ['sem_coluna.xlsx']);
    assert.ok(resultado.logs.some(log => log.includes('sem_coluna.xlsx') && log.includes('cnae')));
});

test('le CNAEs colados com qualquer separador, com ou sem pontuacao', () => {
    const codigos = parseKeepCnaes(' 4781400\n5611-2/01, 0111301;4711302\t9609-2/04 4781400 ');

    assert.deepEqual([...codigos].sort(), ['0111301', '4711302', '4781400', '5611201', '9609204']);
});

test('recusa a lista com codigos invalidos e mostra quais sao', () => {
    assert.throws(() => parseKeepCnaes('4781400 47 abc 47814000'), /CNAEs inválidos: 47, abc, 47814000/);
});

test('recusa a lista vazia', () => {
    assert.throws(() => parseKeepCnaes('  \n ; , '), /Cole ao menos um CNAE/);
});
