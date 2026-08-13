# Limpeza de Colunas — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Uma aba nova no app que recebe várias planilhas e devolve, para cada uma, um arquivo com apenas as colunas `NOME`, `CPF` e `FONE1`, já normalizadas.

**Architecture:** Três módulos com responsabilidades separadas. As regras de texto/dígito são funções puras num módulo sem dependência de Electron nem de disco, o que as torna testáveis com `node --test`. O processamento de arquivo (ExcelJS + fs) fica num segundo módulo, também sem Electron, testável por integração. O handler IPC é uma casca fina que só faz o wiring com o Electron. A UI segue o sistema de abas existente.

**Tech Stack:** Electron 28, Node 24, ExcelJS 4.4, `node:test` (runner nativo, sem dependência nova).

## Global Constraints

- JavaScript puro. Sem TypeScript. Sem framework de UI (React, Vue, etc).
- `async/await` para operações assíncronas. Nunca `.then()` em código novo.
- Variáveis e funções em `camelCase`. Constantes em `UPPER_SNAKE_CASE`.
- Funções com responsabilidade única. Passou de 40 linhas, divide.
- `ExcelJS` para leitura e escrita de planilha. `xlsx` só quando necessário — aqui não é.
- **Nenhuma dependência nova.** O bundle já está pesado.
- Todo acesso a arquivo e diálogo nativo fica no processo main. O renderer só chama `window.electronAPI.*`.
- Nunca cores hardcoded no JS. Usar as variáveis CSS de `:root`.
- Não commitar `.env`, `users.json`, `*.py`, `node_modules/`, `dist/`.

## File Structure

| Arquivo | Responsabilidade |
|---|---|
| `src/main/regrasLimpezaColunas.js` | **novo** — funções puras: normalização de dígitos e busca de cabeçalho. Zero I/O, zero Electron. |
| `src/main/limpezaColunasArquivo.js` | **novo** — lê uma planilha, aplica as regras, escreve o arquivo limpo. ExcelJS + fs, zero Electron. |
| `src/main/handlers/limpezaColunas.js` | **novo** — handler IPC. Só wiring: permissão, loop no lote, logs, `shell.showItemInFolder`. |
| `tests/regrasLimpezaColunas.test.js` | **novo** — testes unitários das funções puras. |
| `tests/limpezaColunasArquivo.test.js` | **novo** — teste de integração com planilha de fixture em `os.tmpdir()`. |
| `src/main/index.js` | modificar — `require` + `.register()` |
| `preload.js` | modificar — três funções novas na bridge |
| `index.html` | modificar — item na sidebar + `page-content` novo |
| `src/renderer/index.js` | modificar — `gridConfigs`, `tabInfo`, listeners |
| `package.json` | modificar — script `test` |

---

### Task 1: Regras puras de normalização

**Files:**
- Create: `src/main/regrasLimpezaColunas.js`
- Test: `tests/regrasLimpezaColunas.test.js`
- Modify: `package.json` (bloco `scripts`)

**Interfaces:**
- Consumes: nada.
- Produces:
  - `desembrulhar(valor: any) -> any|null` — tira o embrulho do ExcelJS (richText, formula, hyperlink, Date)
  - `textoDe(valor: any) -> string` — texto aparado, `''` se vazio
  - `normalizarCabecalho(valor: any) -> string` — maiúscula, sem acento, espaços colapsados
  - `acharCabecalho(cabecalhos: any[], alvo: object) -> number` — índice 0-based, `-1` se não achou
  - `normalizarDigitos(valor: any) -> string` — só dígitos, `''` se vazio ou só zeros
  - `ALVOS: { NOME, CPF, FONE1 }` — cada um `{ rotulo, exato, especifico, generico }`

- [ ] **Step 1: Adicionar o script de teste no `package.json`**

No bloco `"scripts"`, adicione a linha `test` (as outras três continuam iguais):

```json
  "scripts": {
    "start": "electron . --max-old-space-size=8192",
    "test": "node --test tests/",
    "dist": "electron-builder",
    "publish": "electron-builder --publish always"
  },
```

- [ ] **Step 2: Escrever o teste que falha**

Crie `tests/regrasLimpezaColunas.test.js`:

```js
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
```

- [ ] **Step 3: Rodar o teste e confirmar que falha**

Run: `npm test`
Expected: FAIL — `Cannot find module '../src/main/regrasLimpezaColunas'`

- [ ] **Step 4: Escrever a implementação**

Crie `src/main/regrasLimpezaColunas.js`:

```js
/**
 * Regras puras de limpeza de colunas: normalização de dígitos e busca de cabeçalho.
 * Sem I/O e sem Electron — de propósito, para poder testar com `node --test`.
 */

const ALVOS = {
    NOME: { rotulo: 'Nome do Negócio', exato: 'NOME DO NEGOCIO', especifico: 'NEGOCIO', generico: 'NOME' },
    CPF: { rotulo: 'CNPJ', exato: 'CNPJ', especifico: 'CNPJ', generico: null },
    FONE1: { rotulo: 'Telefone Celular', exato: 'TELEFONE CELULAR', especifico: 'CELULAR', generico: 'TELEFONE' },
};

/**
 * Uma célula do ExcelJS pode vir como valor cru, fórmula, richText, link ou Date.
 * Aqui a gente tira o embrulho e devolve só o valor de dentro.
 */
function desembrulhar(valor) {
    if (valor === null || valor === undefined) return null;
    if (typeof valor !== 'object') return valor;
    if (valor instanceof Date) return valor.toISOString();
    if (Array.isArray(valor.richText)) return valor.richText.map(parte => parte.text).join('');
    if ('result' in valor) return valor.result;
    if ('text' in valor) return valor.text;
    return valor;
}

function textoDe(valor) {
    const bruto = desembrulhar(valor);
    if (bruto === null || bruto === undefined) return '';
    return String(bruto).trim();
}

function normalizarCabecalho(valor) {
    return textoDe(valor)
        .normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '')
        .replace(/\s+/g, ' ')
        .trim()
        .toUpperCase();
}

/**
 * Procura o cabeçalho em três níveis, do mais específico para o mais frouxo.
 * Devolve o índice 0-based ou -1.
 */
function acharCabecalho(cabecalhos, alvo) {
    const normalizados = cabecalhos.map(normalizarCabecalho);
    const buscas = [
        (texto) => texto === alvo.exato,
        (texto) => texto.includes(alvo.especifico),
        (texto) => alvo.generico !== null && texto.includes(alvo.generico),
    ];

    for (const casa of buscas) {
        const indice = normalizados.findIndex(texto => texto !== '' && casa(texto));
        if (indice !== -1) return indice;
    }
    return -1;
}

/** Converte número em texto sem cair em notação científica. */
function numeroParaTexto(numero) {
    return Math.round(numero).toLocaleString('fullwide', { useGrouping: false });
}

/**
 * Reduz o valor a uma sequência de dígitos.
 * A ordem importa: o sufixo decimal (,00) é cortado ANTES de remover os
 * separadores, senão "5521998364849,00" viraria "552199836484900".
 */
function normalizarDigitos(valor) {
    const bruto = desembrulhar(valor);
    if (bruto === null || bruto === undefined) return '';

    let texto;
    if (typeof bruto === 'number') {
        if (!Number.isFinite(bruto)) return '';
        texto = numeroParaTexto(bruto);
    } else {
        texto = String(bruto).trim();
        if (/e/i.test(texto)) {
            const numero = Number(texto.replace(',', '.'));
            texto = Number.isFinite(numero) ? numeroParaTexto(numero) : texto;
        } else {
            texto = texto.replace(/[.,]\d{1,2}$/, '');
        }
    }

    const digitos = texto.replace(/\D/g, '');
    return /^0*$/.test(digitos) ? '' : digitos;
}

module.exports = {
    ALVOS,
    desembrulhar,
    textoDe,
    normalizarCabecalho,
    acharCabecalho,
    normalizarDigitos,
    numeroParaTexto,
};
```

- [ ] **Step 5: Rodar o teste e confirmar que passa**

Run: `npm test`
Expected: PASS — 10 testes, 0 falhas.

- [ ] **Step 6: Commit**

```bash
git add package.json src/main/regrasLimpezaColunas.js tests/regrasLimpezaColunas.test.js
git commit -m "feat: add pure normalization rules for limpeza de colunas"
```

---

### Task 2: Processamento de arquivo

**Files:**
- Create: `src/main/limpezaColunasArquivo.js`
- Test: `tests/limpezaColunasArquivo.test.js`

**Interfaces:**
- Consumes: de `src/main/regrasLimpezaColunas.js` — `ALVOS`, `textoDe`, `acharCabecalho`, `normalizarDigitos`
- Produces:
  - `caminhoDisponivel(caminhoEntrada: string) -> string` — `<base>_LIMPO.xlsx`, com contador se já existir
  - `limparArquivo(caminhoEntrada: string) -> Promise<{ ok: true, caminhoSaida: string, linhas: number } | { ok: false, motivo: string }>`

- [ ] **Step 1: Escrever o teste que falha**

Crie `tests/limpezaColunasArquivo.test.js`:

```js
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
```

- [ ] **Step 2: Rodar o teste e confirmar que falha**

Run: `npm test`
Expected: FAIL — `Cannot find module '../src/main/limpezaColunasArquivo'`

- [ ] **Step 3: Escrever a implementação**

Crie `src/main/limpezaColunasArquivo.js`:

```js
/**
 * Processamento de arquivo da Limpeza de Colunas: lê uma planilha, aplica as
 * regras e escreve a versão limpa ao lado do original.
 * Sem Electron — de propósito, para poder testar sem subir o app.
 */
const fs = require('fs');
const path = require('path');
const ExcelJS = require('exceljs');

const { ALVOS, textoDe, acharCabecalho, normalizarDigitos } = require('./regrasLimpezaColunas');

const FORMATO_CPF = '00000000000000';
const FORMATO_FONE = '0';
const NOME_ABA_SAIDA = 'Lista';

/** Devolve o primeiro `<base>_LIMPO*.xlsx` que ainda não existe na pasta do original. */
function caminhoDisponivel(caminhoEntrada) {
    const pasta = path.dirname(caminhoEntrada);
    const base = path.basename(caminhoEntrada, path.extname(caminhoEntrada));

    let candidato = path.join(pasta, `${base}_LIMPO.xlsx`);
    let contador = 1;
    while (fs.existsSync(candidato)) {
        candidato = path.join(pasta, `${base}_LIMPO_${contador}.xlsx`);
        contador++;
    }
    return candidato;
}

async function lerPrimeiraAba(caminhoEntrada) {
    const extensao = path.extname(caminhoEntrada).toLowerCase();
    if (extensao === '.xls') {
        throw new Error('formato .xls antigo nao e suportado, converta para .xlsx');
    }

    const workbook = new ExcelJS.Workbook();
    if (extensao === '.csv') {
        await workbook.csv.readFile(caminhoEntrada);
    } else {
        await workbook.xlsx.readFile(caminhoEntrada);
    }

    const aba = workbook.worksheets[0];
    if (!aba) throw new Error('o arquivo nao possui nenhuma aba');
    return aba;
}

/** Lê a linha 1 e devolve, para cada alvo, o número da coluna (1-based). */
function localizarColunas(aba) {
    const cabecalhos = [];
    aba.getRow(1).eachCell({ includeEmpty: true }, (celula, coluna) => {
        cabecalhos[coluna - 1] = celula.value;
    });

    const colunas = {};
    for (const [saida, alvo] of Object.entries(ALVOS)) {
        const indice = acharCabecalho(cabecalhos, alvo);
        if (indice === -1) {
            throw new Error(`coluna "${alvo.rotulo}" nao encontrada`);
        }
        colunas[saida] = indice + 1;
    }
    return colunas;
}

function montarAbaDestino(workbook) {
    const aba = workbook.addWorksheet(NOME_ABA_SAIDA);
    aba.addRow(['NOME', 'CPF', 'FONE1']);
    aba.getColumn(1).width = 40;
    aba.getColumn(2).width = 20;
    aba.getColumn(3).width = 18;
    return aba;
}

/**
 * Precisa rodar DEPOIS das linhas entrarem: no ExcelJS, atribuir `numFmt` a uma
 * coluna propaga o estilo para as células que já existem, não para as futuras.
 */
function aplicarFormatos(aba) {
    aba.getColumn(2).numFmt = FORMATO_CPF;
    aba.getColumn(3).numFmt = FORMATO_FONE;
}

function copiarLinhas(origem, destino, colunas) {
    let linhas = 0;
    for (let numero = 2; numero <= origem.rowCount; numero++) {
        const linha = origem.getRow(numero);
        const nome = textoDe(linha.getCell(colunas.NOME).value);
        const cpf = normalizarDigitos(linha.getCell(colunas.CPF).value);
        const fone = normalizarDigitos(linha.getCell(colunas.FONE1).value);

        if (nome === '' && cpf === '' && fone === '') continue;

        destino.addRow([nome || null, cpf ? Number(cpf) : null, fone ? Number(fone) : null]);
        linhas++;
    }
    return linhas;
}

/**
 * Limpa uma planilha. Nunca lança: devolve `{ ok: false, motivo }` para que um
 * arquivo problemático não derrube o lote inteiro.
 */
async function limparArquivo(caminhoEntrada) {
    try {
        const origem = await lerPrimeiraAba(caminhoEntrada);
        const colunas = localizarColunas(origem);

        const workbook = new ExcelJS.Workbook();
        const destino = montarAbaDestino(workbook);
        const linhas = copiarLinhas(origem, destino, colunas);
        aplicarFormatos(destino);

        const caminhoSaida = caminhoDisponivel(caminhoEntrada);
        await workbook.xlsx.writeFile(caminhoSaida);

        return { ok: true, caminhoSaida, linhas };
    } catch (erro) {
        return { ok: false, motivo: erro.message };
    }
}

module.exports = { limparArquivo, caminhoDisponivel };
```

- [ ] **Step 4: Rodar o teste e confirmar que passa**

Run: `npm test`
Expected: PASS — todos os testes das duas suítes.

- [ ] **Step 5: Commit**

```bash
git add src/main/limpezaColunasArquivo.js tests/limpezaColunasArquivo.test.js
git commit -m "feat: add file processing for limpeza de colunas"
```

---

### Task 3: Handler IPC

**Files:**
- Create: `src/main/handlers/limpezaColunas.js`
- Modify: `src/main/index.js` (bloco de `require` das linhas 16-23 e bloco de `.register()` das linhas 57-65)
- Modify: `preload.js` (bloco de Relacionamento, e bloco de listeners)

**Interfaces:**
- Consumes: de `src/main/limpezaColunasArquivo.js` — `limparArquivo`
- Produces:
  - canal `start-limpeza-colunas` (renderer → main, `send`), payload `string[]`
  - canal `limpeza-colunas-log` (main → renderer), payload `string`
  - canal `limpeza-colunas-finished` (main → renderer), payload `{ success: boolean, processados: number, pulados: number }`
  - bridge: `window.electronAPI.startLimpezaColunas(caminhos)`, `onLimpezaColunasLog(cb)`, `onLimpezaColunasFinished(cb)`

- [ ] **Step 1: Criar o handler**

Crie `src/main/handlers/limpezaColunas.js`:

```js
/**
 * Handler da aba de Limpeza de Colunas: reduz planilhas a NOME, CPF e FONE1.
 */
const { ipcMain, shell } = require('electron');
const path = require('path');

const state = require('../state');
const { logSystemAction } = require('../database/connection');
const { limparArquivo } = require('../limpezaColunasArquivo');

const isAdmin = () => state.currentUser && state.currentUser.role === 'admin';

async function processarLote(caminhos, log) {
    let processados = 0;
    let pulados = 0;
    let primeiraSaida = null;

    for (const caminho of caminhos) {
        const nome = path.basename(caminho);
        const resultado = await limparArquivo(caminho);

        if (resultado.ok) {
            processados++;
            if (!primeiraSaida) primeiraSaida = resultado.caminhoSaida;
            log(`${nome} -> OK (${resultado.linhas.toLocaleString('pt-BR')} linhas)`);
        } else {
            pulados++;
            log(`${nome} -> PULADO`);
            log(`   ${resultado.motivo}`);
        }
    }

    return { processados, pulados, primeiraSaida };
}

function register() {
    ipcMain.on('start-limpeza-colunas', async (event, caminhos) => {
        const log = (mensagem) => event.sender.send('limpeza-colunas-log', mensagem);
        const finalizar = (success, processados, pulados) =>
            event.sender.send('limpeza-colunas-finished', { success, processados, pulados });

        if (!isAdmin()) {
            log('❌ Acesso negado.');
            return finalizar(false, 0, 0);
        }

        if (!Array.isArray(caminhos) || caminhos.length === 0) {
            log('❌ Nenhum arquivo selecionado.');
            return finalizar(false, 0, 0);
        }

        log(`Iniciando limpeza de ${caminhos.length} arquivo(s)...`);
        logSystemAction(state.currentUser.username, 'Limpeza de Colunas', `Limpou ${caminhos.length} arquivos.`);

        try {
            const { processados, pulados, primeiraSaida } = await processarLote(caminhos, log);

            log('');
            log(`Concluido: ${processados} de ${caminhos.length} arquivo(s).`);
            if (primeiraSaida) shell.showItemInFolder(primeiraSaida);

            finalizar(true, processados, pulados);
        } catch (erro) {
            log(`❌ Erro critico: ${erro.message}`);
            finalizar(false, 0, 0);
        }
    });
}

module.exports = { register };
```

- [ ] **Step 2: Registrar o handler no `src/main/index.js`**

No bloco de `require` dos handlers, depois de `relacionamento` (linha 23), adicione:

```js
const limpezaColunas = require('./handlers/limpezaColunas');
```

E no bloco de registro, depois de `relacionamento.register();` (linha 64), adicione:

```js
limpezaColunas.register();
```

- [ ] **Step 3: Expor a bridge no `preload.js`**

Logo depois do bloco `// --- FIM DA MODIFICAÇÃO ---` do Relacionamento (linha 80), adicione:

```js
    // --- Funções de Limpeza de Colunas ---
    startLimpezaColunas: (caminhos) => ipcRenderer.send('start-limpeza-colunas', caminhos),
```

E junto dos listeners de Relacionamento (depois da linha 106), adicione:

```js
    onLimpezaColunasLog: (callback) => ipcRenderer.on("limpeza-colunas-log", (event, ...args) => callback(...args)),
    onLimpezaColunasFinished: (callback) => ipcRenderer.on("limpeza-colunas-finished", (event, ...args) => callback(...args)),
```

- [ ] **Step 4: Confirmar que os testes continuam passando**

Run: `npm test`
Expected: PASS — nada quebrou. (O handler em si não tem teste automatizado: ele só faz wiring com o Electron, e a lógica que importa já está coberta nas Tasks 1 e 2.)

- [ ] **Step 5: Confirmar que o app sobe sem erro de sintaxe**

Run: `node --check src/main/handlers/limpezaColunas.js && node --check src/main/index.js && node --check preload.js`
Expected: sem saída, exit 0.

- [ ] **Step 6: Commit**

```bash
git add src/main/handlers/limpezaColunas.js src/main/index.js preload.js
git commit -m "feat: wire limpeza de colunas IPC handler"
```

---

### Task 4: Aba na HUD

**Files:**
- Modify: `index.html` (sidebar-nav, depois do `<li>` de Relacionamento na linha 72; e novo `page-content` depois do bloco `#relacionamento`)
- Modify: `src/renderer/index.js` (`gridConfigs` na linha 527, `tabInfo` na linha 723, listeners novos)

**Interfaces:**
- Consumes: `window.electronAPI.selectFile`, `startLimpezaColunas`, `onLimpezaColunasLog`, `onLimpezaColunasFinished`
- Produces: IDs de DOM — `limpezaColunas` (page), `limpezaColunasGrid`, `limpezaColunasSelectBtn`, `limpezaColunasStartBtn`, `limpezaColunasFilePaths`, `limpezaColunasLog`

- [ ] **Step 1: Adicionar o item na sidebar**

Em `index.html`, logo depois do `</li>` do item Relacionamento (linha 72) e antes do `</ul>` (linha 73):

```html
                    <li class="nav-item"><a href="#" class="tab-button" data-tab-name="limpezaColunas"><svg
                                xmlns="http://www.w3.org/2000/svg" width="24" height="24" fill="currentColor"
                                viewBox="0 0 16 16">
                                <path
                                    d="M6 10.5a.5.5 0 0 1 .5-.5h3a.5.5 0 0 1 0 1h-3a.5.5 0 0 1-.5-.5zm-2-3a.5.5 0 0 1 .5-.5h7a.5.5 0 0 1 0 1h-7a.5.5 0 0 1-.5-.5zm-2-3a.5.5 0 0 1 .5-.5h11a.5.5 0 0 1 0 1h-11a.5.5 0 0 1-.5-.5z" />
                            </svg><span>Limpeza de Colunas</span></a></li>
```

- [ ] **Step 2: Adicionar a página da aba**

Em `index.html`, logo depois do `</div>` que fecha `<div class="page-content" id="relacionamento">`, adicione:

```html
            <div class="page-content" id="limpezaColunas">
                <div class="grid" id="limpezaColunasGrid" style="gap: 1rem;">

                    <div class="section" data-section-id="config-limpeza-colunas">
                        <div class="section-header">
                            <h2>Limpeza de Colunas</h2>
                            <div class="section-controls">
                                <button class="section-control-btn drag-handle" title="Arrastar"><svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" fill="currentColor" viewBox="0 0 16 16"><path d="M7 2a1 1 0 1 1-2 0 1 1 0 0 1 2 0zm3 0a1 1 0 1 1-2 0 1 1 0 0 1 2 0zM7 5a1 1 0 1 1-2 0 1 1 0 0 1 2 0zm3 0a1 1 0 1 1-2 0 1 1 0 0 1 2 0zM7 8a1 1 0 1 1-2 0 1 1 0 0 1 2 0zm3 0a1 1 0 1 1-2 0 1 1 0 0 1 2 0zm-3 3a1 1 0 1 1-2 0 1 1 0 0 1 2 0zm3 0a1 1 0 1 1-2 0 1 1 0 0 1 2 0zm-3 3a1 1 0 1 1-2 0 1 1 0 0 1 2 0zm3 0a1 1 0 1 1-2 0 1 1 0 0 1 2 0z"/></svg></button>
                                <button class="section-control-btn hide-section" title="Ocultar"><svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" fill="currentColor" viewBox="0 0 16 16"><path d="M16 8s-3-5.5-8-5.5S0 8 0 8s3 5.5 8 5.5S16 8 16 8zM1.173 8a13.133 13.133 0 0 1 1.66-2.043C4.12 4.668 5.88 3.5 8 3.5c2.12 0 3.879 1.168 5.168 2.457A13.133 13.133 0 0 1 14.828 8c-.058.087-.122.183-.195.288-.335.48-.83 1.12-1.465 1.755C11.879 11.332 10.119 12.5 8 12.5c-2.12 0-3.879-1.168-5.168-2.457A13.134 13.134 0 0 1 1.172 8z"/><path d="M8 5.5a2.5 2.5 0 1 0 0 5 2.5 2.5 0 0 0 0-5zM4.5 8a3.5 3.5 0 1 1 7 0 3.5 3.5 0 0 1-7 0z"/></svg></button>
                            </div>
                        </div>

                        <p style="font-size: 13px; color: var(--text-secondary); margin-bottom: 1rem;">
                            Reduz cada planilha a três colunas — <strong>NOME</strong>, <strong>CPF</strong> e
                            <strong>FONE1</strong> — já com os números normalizados. Os arquivos originais não são
                            alterados: cada um gera um <code>_LIMPO.xlsx</code> na mesma pasta.
                        </p>

                        <div class="tool-group">
                            <label>Selecione as planilhas:</label>
                            <button id="limpezaColunasSelectBtn">Selecionar Arquivos</button>
                            <div id="limpezaColunasFilePaths" class="files"></div>
                        </div>

                        <div class="tool-group">
                            <button id="limpezaColunasStartBtn" style="width: 100%;">
                                <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor"
                                    viewBox="0 0 16 16">
                                    <path
                                        d="M11.596 8.697l-6.363 3.692c-.54.313-1.233-.066-1.233-.697V4.308c0-.63.692-1.01 1.233-.696l6.363 3.692a.802.802 0 0 1 0 1.393z" />
                                </svg>
                                Iniciar Limpeza
                            </button>
                        </div>
                    </div>

                    <div class="section full-width" data-section-id="logs-limpeza-colunas">
                        <div class="section-header">
                            <h2>Logs do Processo</h2>
                            <div class="section-controls">
                                <button class="section-control-btn drag-handle" title="Arrastar"><svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" fill="currentColor" viewBox="0 0 16 16"><path d="M7 2a1 1 0 1 1-2 0 1 1 0 0 1 2 0zm3 0a1 1 0 1 1-2 0 1 1 0 0 1 2 0zM7 5a1 1 0 1 1-2 0 1 1 0 0 1 2 0zm3 0a1 1 0 1 1-2 0 1 1 0 0 1 2 0zM7 8a1 1 0 1 1-2 0 1 1 0 0 1 2 0zm3 0a1 1 0 1 1-2 0 1 1 0 0 1 2 0zm-3 3a1 1 0 1 1-2 0 1 1 0 0 1 2 0zm3 0a1 1 0 1 1-2 0 1 1 0 0 1 2 0zm-3 3a1 1 0 1 1-2 0 1 1 0 0 1 2 0zm3 0a1 1 0 1 1-2 0 1 1 0 0 1 2 0z"/></svg></button>
                                <button class="section-control-btn hide-section" title="Ocultar"><svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" fill="currentColor" viewBox="0 0 16 16"><path d="M16 8s-3-5.5-8-5.5S0 8 0 8s3 5.5 8 5.5S16 8 16 8zM1.173 8a13.133 13.133 0 0 1 1.66-2.043C4.12 4.668 5.88 3.5 8 3.5c2.12 0 3.879 1.168 5.168 2.457A13.133 13.133 0 0 1 14.828 8c-.058.087-.122.183-.195.288-.335.48-.83 1.12-1.465 1.755C11.879 11.332 10.119 12.5 8 12.5c-2.12 0-3.879-1.168-5.168-2.457A13.134 13.134 0 0 1 1.172 8z"/><path d="M8 5.5a2.5 2.5 0 1 0 0 5 2.5 2.5 0 0 0 0-5zM4.5 8a3.5 3.5 0 1 1 7 0 3.5 3.5 0 0 1-7 0z"/></svg></button>
                            </div>
                        </div>
                        <div id="limpezaColunasLog" class="logs custom-scrollbar">Aguardando arquivos...</div>
                    </div>

                </div>
            </div>
```

- [ ] **Step 3: Registrar a grid e o título da aba**

Em `src/renderer/index.js`, no objeto `gridConfigs` (linha 527), depois da linha do `relacionamentoGrid`:

```js
        'limpezaColunasGrid': 'limpeza-colunas-sections',
```

E no objeto `tabInfo` (linha 723), depois do bloco `'Relacionamento'` — atenção à vírgula que precisa ser adicionada no fim do bloco anterior:

```js
        'Limpeza de Colunas': {
            title: 'Limpeza de Colunas',
            description: 'Reduz suas planilhas a NOME, CPF e FONE1, com os números já normalizados e sem notação científica. Os arquivos originais permanecem intactos.'
        }
```

- [ ] **Step 4: Adicionar os listeners da aba**

Em `src/renderer/index.js`, imediatamente antes de `const gridConfigs = {` (linha 527), adicione o bloco:

```js
    // --- Limpeza de Colunas ---
    const limpezaColunasSelectBtn = document.getElementById('limpezaColunasSelectBtn');
    const limpezaColunasStartBtn = document.getElementById('limpezaColunasStartBtn');
    const limpezaColunasFilePaths = document.getElementById('limpezaColunasFilePaths');
    const limpezaColunasLog = document.getElementById('limpezaColunasLog');
    let limpezaColunasFiles = [];

    const appendLimpezaColunasLog = (mensagem) => {
        if (!limpezaColunasLog) return;
        if (limpezaColunasLog.textContent === 'Aguardando arquivos...') {
            limpezaColunasLog.textContent = '';
        }
        limpezaColunasLog.textContent += `${mensagem}\n`;
        limpezaColunasLog.scrollTop = limpezaColunasLog.scrollHeight;
    };

    if (limpezaColunasSelectBtn) {
        limpezaColunasSelectBtn.addEventListener('click', async () => {
            const arquivos = await window.electronAPI.selectFile({
                title: 'Selecione as planilhas para limpar',
                multi: true
            });
            if (!arquivos || arquivos.length === 0) {
                appendLimpezaColunasLog('Nenhum arquivo selecionado.');
                return;
            }
            limpezaColunasFiles = arquivos;
            limpezaColunasFilePaths.innerHTML = arquivos.map(p => `<div>${getBasename(p)}</div>`).join('');
            appendLimpezaColunasLog(`${arquivos.length} arquivo(s) selecionado(s).`);
        });
    }

    if (limpezaColunasStartBtn) {
        limpezaColunasStartBtn.addEventListener('click', () => {
            if (limpezaColunasFiles.length === 0) {
                appendLimpezaColunasLog('❌ Selecione pelo menos um arquivo antes de iniciar.');
                return;
            }
            limpezaColunasStartBtn.disabled = true;
            window.electronAPI.startLimpezaColunas(limpezaColunasFiles);
        });
    }

    window.electronAPI.onLimpezaColunasLog(appendLimpezaColunasLog);
    window.electronAPI.onLimpezaColunasFinished(({ success, processados, pulados }) => {
        if (limpezaColunasStartBtn) limpezaColunasStartBtn.disabled = false;
        if (success) {
            appendLimpezaColunasLog(`🎉 Finalizado. ${processados} gerado(s), ${pulados} pulado(s).`);
        } else {
            appendLimpezaColunasLog('❌ Processo finalizado com erro.');
        }
    });
```

- [ ] **Step 5: Confirmar que nada quebrou**

Run: `npm test && node --check src/renderer/index.js`
Expected: testes PASS, `node --check` sem saída e exit 0.

- [ ] **Step 6: Validar no app com uma planilha real**

Run: `npm start`

Confirmar, na ordem:
1. "Limpeza de Colunas" aparece na sidebar logo abaixo de "Relacionamento".
2. Clicar na aba mostra o título e a descrição no header da página.
3. "Selecionar Arquivos" permite escolher mais de um arquivo e lista os nomes.
4. "Iniciar Limpeza" gera `<nome>_LIMPO.xlsx` na pasta do original e revela o arquivo no explorador.
5. Abrindo a saída no Excel: cabeçalhos `NOME`, `CPF`, `FONE1`; o FONE1 aparece como `5521998364849` e não como `5,52199E+12`; o CNPJ com zero à esquerda aparece com os 14 dígitos.
6. Rodar de novo no mesmo arquivo gera `_LIMPO_1.xlsx` sem sobrescrever o anterior.
7. Um arquivo sem a coluna de telefone é reportado como `PULADO` no log e os demais do lote continuam.

- [ ] **Step 7: Commit**

```bash
git add index.html src/renderer/index.js
git commit -m "feat: add Limpeza de Colunas tab to the HUD"
```

---

## Cobertura do spec

| Requisito do spec | Task |
|---|---|
| Handler novo em `handlers/limpezaColunas.js` | 3 |
| Canais IPC e bridge no preload | 3 |
| Gate `isAdmin()` + `logSystemAction` | 3 |
| ExcelJS para leitura e escrita | 2 |
| Entrada `.xlsx` / `.csv`, `.xls` recusado | 2 |
| Três colunas de saída na ordem NOME, CPF, FONE1 | 2 |
| `numFmt` `00000000000000` no CPF e `0` no FONE1 | 2 |
| Busca de cabeçalho em três níveis | 1 |
| Normalização de dígitos nos cinco passos, na ordem certa | 1 |
| Nenhum registro removido, exceto linha totalmente vazia | 2 |
| Saída `_LIMPO.xlsx` na pasta de origem, sem sobrescrever | 2 |
| Revela o primeiro arquivo gerado no explorador | 3 |
| Erro por arquivo, lote continua | 2 (retorno) + 3 (loop) |
| Aba própria na sidebar após Relacionamento | 4 |
| Funções puras exportadas e testadas em `node` puro | 1 |
