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

// CNAE principal na Receita, por CNPJ. 55555555000155 está na base sem CNAE preenchido.
const RECEITA = new Map([
    ['11111111000111', '4781400'],
    ['22222222000122', '5611201'],
    ['33333333000133', '4711302'],
    ['04252011000110', '0111301'],
    ['55555555000155', '']
]);
function receita(consultas = []) {
    return async documentos => {
        consultas.push(documentos);
        return new Map(documentos.filter(cnpj => RECEITA.has(cnpj)).map(cnpj => [cnpj, RECEITA.get(cnpj)]));
    };
}

const LISTA_SEM_CNAE = [
    ['cnpj', 'nome', 'fone1'],
    ['11.111.111/0001-11', 'Mantida formatada', '11999990001'],
    [22222222000122, 'Mantida numerica', '11999990002'],
    ['33333333000133', 'Outro CNAE', '11999990003'],
    ['44444444000144', 'Fora da Receita', '11999990004'],
    [4252011000110, 'Zero perdido no Excel', '11999990005'],
    ['55555555000155', 'Sem CNAE na Receita', '11999990006'],
    ['123.456.789-01', 'CPF', '11999990007']
];

test('mantem so os CNPJs cujo CNAE principal na Receita esta na lista', async (t) => {
    const caminho = criarPlanilha(t, 'pagbank.xlsx', LISTA_SEM_CNAE);
    const consultas = [];

    const resultado = await processFile(
        { path: caminho, id: 'arquivo-1' },
        new Set(),
        opcoes({ keepCnaes: new Set(['4781400', '5611201', '0111301']), findCnaes: receita(consultas) }),
        event
    );

    assert.ok(resultado.processed);
    assert.deepEqual(lerNomes(caminho), ['Mantida formatada', 'Mantida numerica', 'Zero perdido no Excel']);
    assert.deepEqual(consultas, [['11111111000111', '22222222000122', '33333333000133', '44444444000144', '04252011000110', '55555555000155']]);
    assert.equal(resultado.removedOutsideCnaes, 4);
    assert.ok(resultado.logs.some(log => log.includes('Fora da lista de CNAEs: 4 (sem CNAE na Receita: 3)')));
});

test('o CNAE da Receita vale mesmo quando a planilha tem coluna de CNAE', async (t) => {
    const caminho = criarPlanilha(t, 'com_coluna.xlsx', [
        ['cnpj', 'nome', 'fone1', 'cnae'],
        ['33333333000133', 'Coluna desatualizada', '11999990003', '4781400'],
        ['11111111000111', 'Receita confirma', '11999990001', '4711302']
    ]);

    await processFile(
        { path: caminho, id: 'arquivo-2' },
        new Set(),
        opcoes({ keepCnaes: new Set(['4781400']), findCnaes: receita() }),
        event
    );

    assert.deepEqual(lerNomes(caminho), ['Receita confirma']);
});

test('sem a lista de CNAEs, a Receita nao e consultada e nenhuma linha sai por CNAE', async (t) => {
    const caminho = criarPlanilha(t, 'sem_filtro.xlsx', LISTA_SEM_CNAE);

    await processFile(
        { path: caminho, id: 'arquivo-3' },
        new Set(),
        opcoes({ findCnaes: async () => { throw new Error('A Receita não deveria ser consultada'); } }),
        event
    );

    assert.deepEqual(lerNomes(caminho), LISTA_SEM_CNAE.slice(1).map(linha => linha[1]));
});

test('CNAE proibido na coluna da planilha continua saindo com o filtro ligado', async (t) => {
    const caminho = criarPlanilha(t, 'proibido.xlsx', [
        ['cnpj', 'nome', 'fone1', 'cnae'],
        ['11111111000111', 'Proibida na planilha', '11999990001', '4731800'],
        ['22222222000122', 'Permitida', '11999990002', '5611201']
    ]);

    await processFile(
        { path: caminho, id: 'arquivo-4' },
        new Set(),
        opcoes({ keepCnaes: new Set(['4781400', '5611201']), findCnaes: receita() }),
        event
    );

    assert.deepEqual(lerNomes(caminho), ['Permitida']);
});

test('falha ao consultar a Receita interrompe sem alterar a planilha', async (t) => {
    const caminho = criarPlanilha(t, 'falha.xlsx', LISTA_SEM_CNAE);
    const original = fs.readFileSync(caminho);

    await assert.rejects(processFile(
        { path: caminho, id: 'arquivo-5' },
        new Set(),
        opcoes({ keepCnaes: new Set(['4781400']), findCnaes: async () => { throw new Error('Não foi possível consultar o CNAE no banco da Receita.'); } }),
        event
    ), /consultar o CNAE/);

    assert.deepEqual(fs.readFileSync(caminho), original);
    assert.deepEqual(fs.readdirSync(path.dirname(caminho)), ['falha.xlsx']);
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
