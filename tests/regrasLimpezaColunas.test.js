const test = require('node:test');
const assert = require('node:assert');

const {
    desembrulhar,
    textoDe,
    normalizarCabecalho,
    acharCabecalho,
    normalizarDigitos,
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
