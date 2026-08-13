const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const ExcelJS = require('exceljs');

const { limparArquivo, caminhoDisponivel } = require('../src/main/limpezaColunasArquivo');

/** Monta uma planilha de fixture numa pasta temporária e devolve o caminho. */
async function criarFixture(nome, cabecalhos, linhas) {
    const pasta = fs.mkdtempSync(path.join(os.tmpdir(), 'limpcol-'));
    const caminho = path.join(pasta, nome);
    const workbook = new ExcelJS.Workbook();
    const aba = workbook.addWorksheet('Origem');
    aba.addRow(cabecalhos);
    linhas.forEach(linha => aba.addRow(linha));
    await workbook.xlsx.writeFile(caminho);
    return caminho;
}

/** Escreve um CSV cru numa pasta temporária e devolve o caminho. */
function criarFixtureCsv(nome, linhas, prefixo = '') {
    const pasta = fs.mkdtempSync(path.join(os.tmpdir(), 'limpcol-'));
    const caminho = path.join(pasta, nome);
    fs.writeFileSync(caminho, prefixo + linhas.join('\r\n'), 'utf8');
    return caminho;
}

/** Lê a saída gerada e devolve as linhas como array de arrays. */
async function lerSaida(caminho) {
    const workbook = new ExcelJS.Workbook();
    await workbook.xlsx.readFile(caminho);
    const aba = workbook.worksheets[0];
    const linhas = [];
    aba.eachRow((linha) => {
        linhas.push([linha.getCell(1).value, linha.getCell(2).value, linha.getCell(3).value]);
    });
    return { aba, linhas };
}

test('limparArquivo mantem so as tres colunas e renomeia os cabecalhos', async () => {
    const entrada = await criarFixture(
        'base.xlsx',
        ['ID', 'Nome do Negócio', 'Lixo', 'CNPJ', 'Telefone Celular', 'Mais Lixo'],
        [[1, 'Padaria Sol', 'x', '04.252.011/0001-10', '5521998364849,00', 'y']]
    );

    const resultado = await limparArquivo(entrada);

    assert.strictEqual(resultado.ok, true);
    assert.strictEqual(resultado.linhas, 1);

    const { linhas } = await lerSaida(resultado.caminhoSaida);
    assert.deepStrictEqual(linhas[0], ['NOME', 'CPF', 'FONE1']);
    assert.deepStrictEqual(linhas[1], ['Padaria Sol', 4252011000110, 5521998364849]);
});

test('limparArquivo grava CPF e FONE1 como numero com formato', async () => {
    const entrada = await criarFixture(
        'formato.xlsx',
        ['Nome do Negócio', 'CNPJ', 'Telefone Celular'],
        [['Mercado Lua', '04.252.011/0001-10', 5521998364849]]
    );

    const resultado = await limparArquivo(entrada);
    const { aba, linhas } = await lerSaida(resultado.caminhoSaida);

    assert.strictEqual(typeof linhas[1][1], 'number');
    assert.strictEqual(typeof linhas[1][2], 'number');
    // o formato precisa estar na célula de dados, não só na definição da coluna
    assert.strictEqual(aba.getCell('B2').numFmt, '00000000000000');
    assert.strictEqual(aba.getCell('C2').numFmt, '0');
});

test('limparArquivo pula o arquivo quando falta uma coluna', async () => {
    const entrada = await criarFixture(
        'faltando.xlsx',
        ['Nome do Negócio', 'CNPJ'],
        [['Padaria Sol', '12345678000199']]
    );

    const resultado = await limparArquivo(entrada);

    assert.strictEqual(resultado.ok, false);
    assert.match(resultado.motivo, /Telefone Celular/);
});

test('limparArquivo recusa .xls antigo', async () => {
    const entrada = await criarFixture('velho.xlsx', ['Nome do Negócio', 'CNPJ', 'Telefone Celular'], []);
    const comoXls = entrada.replace(/\.xlsx$/, '.xls');
    fs.renameSync(entrada, comoXls);

    const resultado = await limparArquivo(comoXls);

    assert.strictEqual(resultado.ok, false);
    assert.match(resultado.motivo, /\.xls/);
});

test('limparArquivo gera so o cabecalho quando nao ha linhas de dados', async () => {
    const entrada = await criarFixture('vazio.xlsx', ['Nome do Negócio', 'CNPJ', 'Telefone Celular'], []);

    const resultado = await limparArquivo(entrada);

    assert.strictEqual(resultado.ok, true);
    assert.strictEqual(resultado.linhas, 0);
    const { linhas } = await lerSaida(resultado.caminhoSaida);
    assert.strictEqual(linhas.length, 1);
});

test('limparArquivo descarta a linha em que as tres colunas estao vazias', async () => {
    const entrada = await criarFixture(
        'buraco.xlsx',
        ['Nome do Negócio', 'CNPJ', 'Telefone Celular'],
        [['Padaria Sol', '12345678000199', '5521998364849'], [null, null, null]]
    );

    const resultado = await limparArquivo(entrada);

    assert.strictEqual(resultado.linhas, 1);
});

test('caminhoDisponivel nunca sobrescreve arquivo existente', async () => {
    const entrada = await criarFixture('lote.xlsx', ['Nome do Negócio', 'CNPJ', 'Telefone Celular'], []);

    const primeiro = caminhoDisponivel(entrada);
    assert.strictEqual(path.basename(primeiro), 'lote_LIMPO.xlsx');

    fs.writeFileSync(primeiro, 'ocupado');
    const segundo = caminhoDisponivel(entrada);
    assert.strictEqual(path.basename(segundo), 'lote_LIMPO_1.xlsx');
});

test('celula exibida como cientifico vira numero inteiro', async () => {
    // No .xlsx o valor é numérico e "5,52199E+12" é só a máscara de exibição.
    // O valor real está guardado, então a conversão recupera todos os dígitos.
    const pasta = fs.mkdtempSync(path.join(os.tmpdir(), 'limpcol-'));
    const entrada = path.join(pasta, 'cientifico.xlsx');
    const workbook = new ExcelJS.Workbook();
    const aba = workbook.addWorksheet('Origem');
    aba.addRow(['Nome do Negócio', 'CNPJ', 'Telefone Celular']);
    aba.addRow(['Padaria Sol', 4252011000110, 5521998364849]);
    aba.getCell('C2').numFmt = '0.00E+00';   // exibe 5,52E+12
    aba.getCell('B2').numFmt = '0.00';       // exibe 4252011000110,00
    await workbook.xlsx.writeFile(entrada);

    const resultado = await limparArquivo(entrada);
    const { linhas } = await lerSaida(resultado.caminhoSaida);

    assert.strictEqual(linhas[1][2], 5521998364849);
    assert.strictEqual(linhas[1][1], 4252011000110);
    assert.strictEqual(resultado.truncados, 0);
});

test('texto cientifico truncado dentro do xlsx fica em branco e e contado', async () => {
    // coluna formatada como Texto guarda a string exibida — aí os dígitos já foram
    const entrada = await criarFixture(
        'truncado.xlsx',
        ['Nome do Negócio', 'CNPJ', 'Telefone Celular'],
        [
            ['Padaria Sol', '12345678000199', '5,52199E+12'],
            ['Mercado Lua', '12345678000199', '5,52199E+12'],
            ['Oficina Zé', '12345678000199', '5521998364849'],
        ]
    );

    const resultado = await limparArquivo(entrada);
    const { linhas } = await lerSaida(resultado.caminhoSaida);

    assert.strictEqual(resultado.truncados, 2);
    assert.strictEqual(linhas[1][2], null);
    assert.strictEqual(linhas[2][2], null);
    assert.strictEqual(linhas[3][2], 5521998364849);
});

test('arquivo sem cientifico truncado reporta zero', async () => {
    const entrada = await criarFixture(
        'limpo.xlsx',
        ['Nome do Negócio', 'CNPJ', 'Telefone Celular'],
        [['Padaria Sol', '12345678000199', 5521998364849]]
    );

    const resultado = await limparArquivo(entrada);

    assert.strictEqual(resultado.truncados, 0);
});

test('telefone vazio gera celula em branco, nao zero', async () => {
    const entrada = await criarFixture(
        'semfone.xlsx',
        ['Nome do Negócio', 'CNPJ', 'Telefone Celular'],
        [['Loja Vazia', '12345678000199', '']]
    );

    const resultado = await limparArquivo(entrada);
    const { linhas } = await lerSaida(resultado.caminhoSaida);

    assert.strictEqual(linhas[1][0], 'Loja Vazia');
    assert.strictEqual(linhas[1][1], 12345678000199);
    assert.strictEqual(linhas[1][2], null);
});

test('limparArquivo recusa CSV explicando o motivo', async () => {
    const entrada = criarFixtureCsv('base.csv', [
        'Nome do Negócio;CNPJ;Telefone Celular',
        'Padaria Sol;12345678000199;5521998364849',
    ]);

    const resultado = await limparArquivo(entrada);

    assert.strictEqual(resultado.ok, false);
    assert.match(resultado.motivo, /csv/i);
    assert.match(resultado.motivo, /xlsx/i);
});

test('XLSX continua saindo como XLSX', async () => {
    const entrada = await criarFixture(
        'planilha.xlsx',
        ['Nome do Negócio', 'CNPJ', 'Telefone Celular'],
        [['Padaria Sol', '12345678000199', '5521998364849']]
    );

    const resultado = await limparArquivo(entrada);

    assert.strictEqual(path.extname(resultado.caminhoSaida), '.xlsx');
});
