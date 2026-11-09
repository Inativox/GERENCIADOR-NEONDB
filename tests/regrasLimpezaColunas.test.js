const test = require('node:test');
const assert = require('node:assert');

const {
    desembrulhar,
    textoDe,
    normalizarCabecalho,
    normalizarDigitos,
    ehCientificoTruncado,
    removerDdi,
    mapearColunas,
    linhaC6,
    limparNome,
} = require('../src/main/regrasLimpezaColunas');

const indices = mapa => Object.fromEntries(Object.entries(mapa).map(([campo, v]) => [campo, Array.isArray(v) ? v.map(f => f.indice) : v.indice]));

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

test('mapearColunas casa pelo cabecalho mesmo com BOM, acento e caixa diferentes', () => {
    assert.deepStrictEqual(indices(mapearColunas(['﻿Nome do Negócio', 'cnpj', 'TELEFONE CELULAR'])), { fones: [2], cpf: 1, nome: 0 });
    assert.deepStrictEqual(indices(mapearColunas(['nome', 'cpf', 'livre1', 'chave', 'livre3', 'livre4', 'livre5', 'livre7', 'fone1', 'fone2'])),
        { fones: [8, 9], cpf: 1, nome: 0, chave: 3, livre1: 2, livre3: 4, livre4: 5 }, 'planilha já no padrão C6 passa direto');
});

test('mapearColunas prefere o cabecalho mais confiavel e ignora o CPF do socio', () => {
    const mapa = mapearColunas(['Nome Fantasia', 'CPF do Sócio', 'Razão Social', 'CNPJ', 'Celular']);
    assert.deepStrictEqual(indices(mapa), { fones: [4], cpf: 3, nome: 2 });
});

test('mapearColunas acha pelo conteudo o que o cabecalho nao resolve', () => {
    const amostra = [['x1', 'Padaria Sol', '04.252.011/0001-10', '(21) 99836-4849'], ['x2', 'Bar do Zé', '12345678000199', '5521998364849'], ['x3', 'Loja', '', '']];
    const mapa = mapearColunas(['Código', 'Coluna 1', 'Coluna 2', 'Coluna 3'], amostra);
    assert.deepStrictEqual(indices(mapa), { fones: [3], cpf: 2, nome: 1 });
    assert.strictEqual(mapa.cpf.por, 'conteúdo');
});

test('mapearColunas recusa planilha sem documento ou sem telefone', () => {
    assert.throws(() => mapearColunas(['Nome', 'Celular'], [['Padaria', '11987654321']]), /CNPJ/);
    assert.throws(() => mapearColunas(['Nome', 'CNPJ'], [['Padaria', '04252011000110']]), /telefone/);
});

test('limparNome tira . , ( ) - / e espaco sobrando', () => {
    assert.strictEqual(limparNome('J.R. Comércio (Filial) - Auto/Peças, LTDA.'), 'JR Comércio Filial Auto Peças LTDA');
    assert.strictEqual(limparNome('  Padaria   Sol  '), 'Padaria Sol');
    assert.strictEqual(limparNome(null), '');
});

test('linhaC6 tira DDI, repete telefone uma vez so e corta no oitavo', () => {
    const fones = ['5521998364849', '21998364849', ...Array.from({ length: 8 }, (_, i) => `2133334${String(i).padStart(3, '0')}`)];
    const mapa = { nome: { indice: 0 }, cpf: { indice: 1 }, fones: fones.map((_, i) => ({ indice: i + 2 })) };
    const linha = linhaC6(['Padaria Sol', '04.252.011/0001-10', ...fones], mapa);
    assert.strictEqual(linha.cpf, '04252011000110');
    assert.deepStrictEqual(linha.fones, ['21998364849', ...Array.from({ length: 7 }, (_, i) => `2133334${String(i).padStart(3, '0')}`)]);
    assert.strictEqual(linha.sobrando, 1);
    assert.strictEqual(linha.ddis, 1);
    assert.strictEqual(linha.livres.livre7, 'C6');
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
