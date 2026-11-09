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
    // nome, cpf e fone1 do layout C6 (colunas 1, 2 e 9)
    aba.eachRow((linha) => {
        linhas.push([linha.getCell(1).value, linha.getCell(2).value, linha.getCell(9).value]);
    });
    return { aba, linhas };
}

/** Lê o CSV adicional como array de arrays de texto. */
function lerCsv(caminho) {
    const texto = fs.readFileSync(caminho, 'utf8');
    assert.ok(texto.startsWith('﻿'), 'CSV com BOM para o Excel abrir acentuado');
    return texto.slice(1).trim().split('\r\n').map(linha => linha.slice(1, -1).split('";"'));
}

test('limparArquivo gera o layout C6 de 8 telefones a partir do cabecalho antigo', async () => {
    const entrada = await criarFixture(
        'base.xlsx',
        ['ID', 'Nome do Negócio', 'Lixo', 'CNPJ', 'Telefone Celular', 'Mais Lixo'],
        [[1, 'Padaria Sol', 'x', '04.252.011/0001-10', '5521998364849,00', 'y']]
    );

    const resultado = await limparArquivo(entrada);

    assert.strictEqual(resultado.ok, true);
    assert.strictEqual(resultado.linhas, 1);

    const { aba, linhas } = await lerSaida(resultado.caminhoSaida);
    assert.deepStrictEqual(aba.getRow(1).values.slice(1), ['nome', 'cpf', 'livre1', 'chave', 'livre3', 'livre4', 'livre5', 'livre7',
        'fone1', 'fone2', 'fone3', 'fone4', 'fone5', 'fone6', 'fone7', 'fone8']);
    // o DDI 55 sai; o CNPJ mantém os 14 dígitos
    assert.deepStrictEqual(linhas[1], ['Padaria Sol', 4252011000110, 21998364849]);
    assert.strictEqual(aba.getCell('H2').value, 'C6', 'livre7 leva o nome da operação, como no fluxo C6');
});

test('limparArquivo aceita qualquer cabecalho e junta todos os telefones em fone1 a fone8', async () => {
    const entrada = await criarFixture(
        'qualquer.xlsx',
        ['Razão Social', 'Documento', 'Whatsapp', 'Tel. Fixo', 'Celular 2', 'E-mail', 'UF', 'Cidade', 'Obs'],
        [['J.R. Comércio (Filial) - Auto/Peças, LTDA.', '04.252.011/0001-10', '(21) 99836-4849', '(21) 3333-4444', '5521998364849', 'a@b.com', 'RJ', 'Rio de Janeiro', 'x']]
    );

    const resultado = await limparArquivo(entrada);

    assert.strictEqual(resultado.ok, true);
    assert.match(resultado.colunas, /nome ← "Razão Social" · cpf ← "Documento"/);
    assert.match(resultado.colunas, /fones ← "Whatsapp", "Tel\. Fixo", "Celular 2"/);
    const { aba } = await lerSaida(resultado.caminhoSaida);
    // . , ( ) - / saem do nome; o celular repetido (com DDI) entra uma vez só
    assert.deepStrictEqual(aba.getRow(2).values.slice(1), ['JR Comércio Filial Auto Peças LTDA', 4252011000110, , 'a@b.com', , 'RJ - Rio de Janeiro', , 'C6', 21998364849, 2133334444]);
});

test('limparArquivo reconhece as colunas pelo conteudo quando o cabecalho nao diz nada', async () => {
    const entrada = await criarFixture(
        'semnome.xlsx',
        ['A', 'B', 'C', 'D'],
        [['Padaria Sol', '12345678000199', '11987654321', 'id-1'], ['Mercado Lua', '04252011000110', '1133334444', 'id-2']]
    );

    const resultado = await limparArquivo(entrada);

    assert.strictEqual(resultado.ok, true);
    assert.strictEqual(resultado.colunas, 'nome ← "A" (pelo conteúdo) · cpf ← "B" (pelo conteúdo) · fones ← "C" (pelo conteúdo)');
    const { linhas } = await lerSaida(resultado.caminhoSaida);
    assert.deepStrictEqual(linhas.slice(1), [['Padaria Sol', 12345678000199, 11987654321], ['Mercado Lua', 4252011000110, 1133334444]]);
});

test('limparArquivo gera o CSV adicional com o mesmo layout e o CNPJ com zero a esquerda', async () => {
    const entrada = await criarFixture(
        'csv.xlsx',
        ['Nome do Negócio', 'CNPJ', 'Telefone Celular'],
        [['=Padaria Sol', 4252011000110, 5521998364849]]
    );

    const resultado = await limparArquivo(entrada);

    assert.strictEqual(resultado.caminhoCsv, resultado.caminhoSaida.replace(/\.xlsx$/, '.csv'));
    const linhas = lerCsv(resultado.caminhoCsv);
    assert.strictEqual(linhas[0].join(';'), 'nome;cpf;livre1;chave;livre3;livre4;livre5;livre7;fone1;fone2;fone3;fone4;fone5;fone6;fone7;fone8');
    // começa com = vira texto (o Excel não executa); o CNPJ guarda o zero que no número só existe pela máscara
    assert.deepStrictEqual(linhas[1], ["'=Padaria Sol", '04252011000110', '', '', '', '', '', 'C6', '21998364849', '', '', '', '', '', '', '']);
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
    assert.strictEqual(aba.getCell('I2').numFmt, '0');
});

test('limparArquivo pula o arquivo quando nao acha telefone nem CNPJ', async () => {
    const semFone = await criarFixture('faltando.xlsx', ['Nome do Negócio', 'CNPJ'], [['Padaria Sol', '12345678000199']]);
    const semDocumento = await criarFixture('semdoc.xlsx', ['Nome', 'Celular'], [['Padaria Sol', '11987654321']]);

    const [fone, documento] = [await limparArquivo(semFone), await limparArquivo(semDocumento)];

    assert.strictEqual(fone.ok, false);
    assert.match(fone.motivo, /telefone/);
    assert.strictEqual(documento.ok, false);
    assert.match(documento.motivo, /CNPJ/);
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

    // o .csv de mesmo nome também conta: nenhum dos dois é sobrescrito
    fs.writeFileSync(segundo.replace(/\.xlsx$/, '.csv'), 'ocupado');
    assert.strictEqual(path.basename(caminhoDisponivel(entrada)), 'lote_LIMPO_2.xlsx');
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

    assert.strictEqual(linhas[1][2], 21998364849);
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
    assert.strictEqual(linhas[3][2], 21998364849);
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

test('remove o DDI do FONE1 e conta quantos sairam', async () => {
    const entrada = await criarFixture(
        'ddi.xlsx',
        ['Nome do Negócio', 'CNPJ', 'Telefone Celular'],
        [
            ['Padaria Sol', '12345678000199', 5521998364849],   // com DDI
            ['Mercado Lua', '12345678000199', 5555999887766],   // DDI + DDD 55
            ['Oficina Zé', '12345678000199', '5599887766'],     // DDD 55 sem DDI
            ['Bar do Zé', '12345678000199', '21998364849'],     // já sem DDI
        ]
    );

    const resultado = await limparArquivo(entrada);
    const { linhas } = await lerSaida(resultado.caminhoSaida);

    assert.strictEqual(linhas[1][2], 21998364849);
    assert.strictEqual(linhas[2][2], 55999887766);  // o DDD 55 sobreviveu
    assert.strictEqual(linhas[3][2], 5599887766);   // intacto, não era DDI
    assert.strictEqual(linhas[4][2], 21998364849);
    assert.strictEqual(resultado.ddisRemovidos, 2);
});

test('o DDI nunca e removido do CPF', async () => {
    // CNPJ que por acaso começa com 55 não pode perder os dois dígitos
    const entrada = await criarFixture(
        'cnpj55.xlsx',
        ['Nome do Negócio', 'CNPJ', 'Telefone Celular'],
        [['Padaria Sol', '55123456000199', '21998364849']]
    );

    const resultado = await limparArquivo(entrada);
    const { linhas } = await lerSaida(resultado.caminhoSaida);

    assert.strictEqual(linhas[1][1], 55123456000199);
    assert.strictEqual(resultado.ddisRemovidos, 0);
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
