const test = require('node:test');
const assert = require('node:assert');

const {
    desembrulhar,
    textoDe,
    normalizarCabecalho,
    acharCabecalho,
    normalizarDigitos,
    ehCientificoTruncado,
    removerDdi,
    ALVOS,
} = require('../src/main/regrasLimpezaColunas');

test('desembrulhar tira o embrulho do ExcelJS', () => {
    assert.strictEqual(desembrulhar(null), null);
    assert.strictEqual(desembrulhar(undefined), null);
    assert.strictEqual(desembrulhar('cru'), 'cru');
    assert.strictEqual(desembrulhar(42), 42);
    assert.strictEqual(desembrulhar({ formula: 'A1', result: 99 }), 99);
    assert.strictEqual(desembrulhar({ text: 'oi', hyperlink: 'http://x' }), 'oi');
    assert.strictEqual(desembrulhar({ richText: [{ text: 'Pa' }, { text: 'daria' }] }), 'Padaria');
});

test('normalizarCabecalho tira acento, caixa e espaco sobrando', () => {
    assert.strictEqual(normalizarCabecalho('Nome do Negócio '), 'NOME DO NEGOCIO');
    assert.strictEqual(normalizarCabecalho('  TELEFONE   CELULAR'), 'TELEFONE CELULAR');
    assert.strictEqual(normalizarCabecalho('cnpj'), 'CNPJ');
    assert.strictEqual(normalizarCabecalho(null), '');
    // CSV do Excel vem com BOM grudado na primeira célula
    assert.strictEqual(normalizarCabecalho('﻿Nome do Negócio'), 'NOME DO NEGOCIO');
});

test('acharCabecalho casa por exato mesmo com BOM na primeira celula', () => {
    assert.strictEqual(acharCabecalho(['﻿Nome do Negócio', 'X'], ALVOS.NOME), 0);
});

test('acharCabecalho acha por exato, por especifico e por generico', () => {
    assert.strictEqual(acharCabecalho(['ID', 'Nome do Negócio', 'CNPJ'], ALVOS.NOME), 1);
    assert.strictEqual(acharCabecalho(['ID', 'Negócio Principal'], ALVOS.NOME), 1);
    assert.strictEqual(acharCabecalho(['ID', 'Nome Fantasia'], ALVOS.NOME), 1);
    assert.strictEqual(acharCabecalho(['Telefone Fixo', 'Celular do Contato'], ALVOS.FONE1), 1);
    assert.strictEqual(acharCabecalho(['A', 'B'], ALVOS.CPF), -1);
});

test('acharCabecalho prefere o match exato ao generico', () => {
    const cabecalhos = ['Nome Fantasia', 'Nome do Negócio'];
    assert.strictEqual(acharCabecalho(cabecalhos, ALVOS.NOME), 1);
});

test('acharCabecalho ignora celulas vazias do cabecalho', () => {
    assert.strictEqual(acharCabecalho([null, '', 'CNPJ'], ALVOS.CPF), 2);
});

test('normalizarDigitos limpa telefone em varios formatos', () => {
    assert.strictEqual(normalizarDigitos(5521998364849), '5521998364849');
    assert.strictEqual(normalizarDigitos(5521998364849.0), '5521998364849');
    assert.strictEqual(normalizarDigitos('5521998364849,00'), '5521998364849');
    assert.strictEqual(normalizarDigitos('5521998364849.00'), '5521998364849');
    assert.strictEqual(normalizarDigitos('5.521998364849E+12'), '5521998364849');
    assert.strictEqual(normalizarDigitos('(21) 99836-4849'), '21998364849');
    assert.strictEqual(normalizarDigitos('55 21 99836.4849'), '5521998364849');
});

test('normalizarDigitos corta o decimal antes de tirar o separador', () => {
    // sem a ordem correta isso viraria 552199836484900
    assert.notStrictEqual(normalizarDigitos('5521998364849,00'), '552199836484900');
});

test('normalizarDigitos recusa cientifico ja truncado em vez de inventar digito', () => {
    // CSV exportado do Excel guarda o texto EXIBIDO: 6 significativos de 13.
    // Expandir daria 5521990000000 — plausível e errado.
    assert.strictEqual(normalizarDigitos('5,52199E+12'), '');
    assert.strictEqual(normalizarDigitos('5.52199E+12'), '');
    assert.strictEqual(normalizarDigitos('5,5219E+12'), '');
});

test('normalizarDigitos aceita cientifico que preservou todos os digitos', () => {
    assert.strictEqual(normalizarDigitos('5,521998364849E+12'), '5521998364849');
    assert.strictEqual(normalizarDigitos('5.521998364849E+12'), '5521998364849');
    // valor numérico de verdade nunca passa pelo caminho do científico
    assert.strictEqual(normalizarDigitos(5521998364849), '5521998364849');
});

test('ehCientificoTruncado identifica so o texto cientifico sem volta', () => {
    assert.strictEqual(ehCientificoTruncado('5,52199E+12'), true);
    assert.strictEqual(ehCientificoTruncado('5,521998364849E+12'), false);
    assert.strictEqual(ehCientificoTruncado('5521998364849'), false);
    assert.strictEqual(ehCientificoTruncado(5521998364849), false);
    assert.strictEqual(ehCientificoTruncado(''), false);
    assert.strictEqual(ehCientificoTruncado(null), false);
});

test('removerDdi tira o 55 quando ele e mesmo o DDI', () => {
    // 13 dígitos: 55 + DDD 21 + celular de 9
    assert.strictEqual(removerDdi('5521998364849'), '21998364849');
    // 12 dígitos: 55 + DDD 21 + fixo de 8
    assert.strictEqual(removerDdi('552199836484'), '2199836484');
    // DDI 55 + DDD 55: sai o DDI, fica o DDD
    assert.strictEqual(removerDdi('5555999887766'), '55999887766');
});

test('removerDdi nao toca no 55 que e DDD', () => {
    // 10 dígitos: DDD 55 + fixo de 8, sem DDI. Cortar deixaria 8 dígitos.
    assert.strictEqual(removerDdi('5599887766'), '5599887766');
    // 11 dígitos: DDD 55 + celular de 9, sem DDI
    assert.strictEqual(removerDdi('55999887766'), '55999887766');
});

test('removerDdi ignora numero que nao comeca com 55', () => {
    assert.strictEqual(removerDdi('21998364849'), '21998364849');
    assert.strictEqual(removerDdi('11987654321'), '11987654321');
    assert.strictEqual(removerDdi(''), '');
});

test('normalizarDigitos nunca remove o 55 do numero', () => {
    // DDI 55 com DDD 55 (Santa Maria/RS) é o caso que mais parece duplicado
    assert.strictEqual(normalizarDigitos('5555999887766'), '5555999887766');
    assert.strictEqual(normalizarDigitos('55 55 99988-7766'), '5555999887766');
    assert.strictEqual(normalizarDigitos('(55) 99988-7766'), '55999887766');
    assert.strictEqual(normalizarDigitos(5555999887766), '5555999887766');
    assert.strictEqual(normalizarDigitos('5555999887766,00'), '5555999887766');
    assert.strictEqual(normalizarDigitos('5,555999887766E+12'), '5555999887766');
    // 55 no começo, no meio e no fim continua inteiro
    assert.strictEqual(normalizarDigitos('555599988776655'), '555599988776655');
});

test('normalizarDigitos nao confunde separador de milhar com decimal', () => {
    // o ponto aparece 4 vezes: é agrupamento, não decimal — os 14 dígitos ficam
    assert.strictEqual(normalizarDigitos('04.252.011.0001.10'), '04252011000110');
    assert.strictEqual(normalizarDigitos('55.55.99988.7766'), '5555999887766');
    // com um separador só, é decimal mesmo e sai
    assert.strictEqual(normalizarDigitos('5.521.998.364.849,00'), '5521998364849');
    assert.strictEqual(normalizarDigitos('5521998364849,0'), '5521998364849');
});

test('normalizarDigitos preserva o zero a esquerda do CNPJ', () => {
    assert.strictEqual(normalizarDigitos('04.252.011/0001-10'), '04252011000110');
    assert.strictEqual(normalizarDigitos('12.345.678/0001-99'), '12345678000199');
});

test('normalizarDigitos devolve vazio no que nao presta', () => {
    assert.strictEqual(normalizarDigitos(''), '');
    assert.strictEqual(normalizarDigitos(null), '');
    assert.strictEqual(normalizarDigitos(undefined), '');
    assert.strictEqual(normalizarDigitos('   '), '');
    assert.strictEqual(normalizarDigitos('sem numero aqui'), '');
    assert.strictEqual(normalizarDigitos(0), '');
    assert.strictEqual(normalizarDigitos('000'), '');
    assert.strictEqual(normalizarDigitos(NaN), '');
});

test('textoDe apara e converte', () => {
    assert.strictEqual(textoDe('  Padaria Sol  '), 'Padaria Sol');
    assert.strictEqual(textoDe(null), '');
    assert.strictEqual(textoDe(123), '123');
});
