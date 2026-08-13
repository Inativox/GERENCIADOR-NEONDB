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

test('CSV separado por ponto e virgula entra e sai como XLSX', async () => {
    const entrada = criarFixtureCsv('base.csv', [
        'ID;Nome do Negócio;CNPJ;Telefone Celular;Lixo',
        '1;Padaria Sol;04.252.011/0001-10;5521998364849,00;x',
        '2;Mercado Lua;12.345.678/0001-99;(21) 99836-4849;y',
    ]);

    const resultado = await limparArquivo(entrada);

    assert.strictEqual(resultado.ok, true);
    assert.strictEqual(path.extname(resultado.caminhoSaida), '.xlsx');
    assert.strictEqual(path.basename(resultado.caminhoSaida), 'base_LIMPO.xlsx');
    assert.strictEqual(resultado.linhas, 2);

    const { linhas } = await lerSaida(resultado.caminhoSaida);
    assert.deepStrictEqual(linhas[0], ['NOME', 'CPF', 'FONE1']);
    assert.deepStrictEqual(linhas[1], ['Padaria Sol', 4252011000110, 5521998364849]);
    assert.deepStrictEqual(linhas[2], ['Mercado Lua', 12345678000199, 21998364849]);
});

test('CSV gera numero de verdade com a mascara do CNPJ', async () => {
    const entrada = criarFixtureCsv('zero.csv', [
        'Nome do Negócio;CNPJ;Telefone Celular',
        'Padaria Sol;04252011000110;5,521998364849E+12',
    ]);

    const resultado = await limparArquivo(entrada);
    const { aba, linhas } = await lerSaida(resultado.caminhoSaida);

    // o ExcelJS converteria "04252011000110" para número já na leitura do CSV;
    // com o map identidade o zero à esquerda chega inteiro nas regras
    assert.strictEqual(linhas[1][1], 4252011000110);
    assert.strictEqual(aba.getCell('B2').numFmt, '00000000000000');
    // científico vindo do CSV também vira número limpo
    assert.strictEqual(linhas[1][2], 5521998364849);
    assert.strictEqual(aba.getCell('C2').numFmt, '0');
});

test('CSV separado por virgula tambem e lido', async () => {
    const entrada = criarFixtureCsv('virgula.csv', [
        'Nome do Negócio,CNPJ,Telefone Celular',
        'Padaria Sol,12345678000199,5521998364849',
    ]);

    const resultado = await limparArquivo(entrada);

    assert.strictEqual(resultado.ok, true);
    assert.strictEqual(resultado.linhas, 1);
    const { linhas } = await lerSaida(resultado.caminhoSaida);
    assert.deepStrictEqual(linhas[1], ['Padaria Sol', 12345678000199, 5521998364849]);
});

test('CSV com BOM no cabecalho e lido normalmente', async () => {
    const entrada = criarFixtureCsv('bom.csv', [
        'Nome do Negócio;CNPJ;Telefone Celular',
        'Padaria Sol;12345678000199;5521998364849',
    ], '﻿');

    const resultado = await limparArquivo(entrada);

    assert.strictEqual(resultado.ok, true);
    assert.strictEqual(resultado.linhas, 1);
});

test('CSV com telefone vazio gera celula em branco, nao zero', async () => {
    const entrada = criarFixtureCsv('semfone.csv', [
        'Nome do Negócio;CNPJ;Telefone Celular',
        'Loja Vazia;12345678000199;',
    ]);

    const resultado = await limparArquivo(entrada);
    const { linhas } = await lerSaida(resultado.caminhoSaida);

    assert.strictEqual(linhas[1][0], 'Loja Vazia');
    assert.strictEqual(linhas[1][1], 12345678000199);
    assert.strictEqual(linhas[1][2], null);
});

test('CSV com cientifico truncado deixa em branco e conta o estrago', async () => {
    const entrada = criarFixtureCsv('truncado.csv', [
        'Nome do Negócio;CNPJ;Telefone Celular',
        'Padaria Sol;12345678000199;5,52199E+12',
        'Mercado Lua;12345678000199;5,52199E+12',
        'Oficina Zé;12345678000199;5521998364849',
    ]);

    const resultado = await limparArquivo(entrada);
    const { linhas } = await lerSaida(resultado.caminhoSaida);

    assert.strictEqual(resultado.ok, true);
    assert.strictEqual(resultado.truncados, 2);
    // em branco, nunca um telefone inventado terminado em zeros
    assert.strictEqual(linhas[1][2], null);
    assert.strictEqual(linhas[2][2], null);
    // o que estava inteiro passa normal
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

test('XLSX continua saindo como XLSX', async () => {
    const entrada = await criarFixture(
        'planilha.xlsx',
        ['Nome do Negócio', 'CNPJ', 'Telefone Celular'],
        [['Padaria Sol', '12345678000199', '5521998364849']]
    );

    const resultado = await limparArquivo(entrada);

    assert.strictEqual(path.extname(resultado.caminhoSaida), '.xlsx');
});
