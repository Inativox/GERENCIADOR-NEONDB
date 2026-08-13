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
const DELIMITADOR_SAIDA = ';';
const BYTES_AMOSTRA = 8192;

/**
 * A saída espelha a entrada. No XLSX o CPF e o FONE1 são números com máscara;
 * no CSV não existe formatação de célula, então vão como texto de dígitos —
 * é o único jeito de o zero à esquerda do CNPJ sobreviver.
 */
const FORMATOS = {
    XLSX: { extensao: '.xlsx', numerico: true },
    CSV: { extensao: '.csv', numerico: false },
};

function formatoDeSaida(caminhoEntrada) {
    return path.extname(caminhoEntrada).toLowerCase() === '.csv' ? FORMATOS.CSV : FORMATOS.XLSX;
}

/** Devolve o primeiro `<base>_LIMPO*<extensao>` que ainda não existe na pasta do original. */
function caminhoDisponivel(caminhoEntrada, extensao) {
    const pasta = path.dirname(caminhoEntrada);
    const base = path.basename(caminhoEntrada, path.extname(caminhoEntrada));

    let candidato = path.join(pasta, `${base}_LIMPO${extensao}`);
    let contador = 1;
    while (fs.existsSync(candidato)) {
        candidato = path.join(pasta, `${base}_LIMPO_${contador}${extensao}`);
        contador++;
    }
    return candidato;
}

/** Lê só o começo do arquivo — não vale carregar uma base inteira para ver a linha 1. */
function primeiraLinha(caminhoEntrada) {
    const buffer = Buffer.alloc(BYTES_AMOSTRA);
    const descritor = fs.openSync(caminhoEntrada, 'r');
    try {
        const lidos = fs.readSync(descritor, buffer, 0, BYTES_AMOSTRA, 0);
        return buffer.toString('utf8', 0, lidos).split(/\r?\n/)[0] || '';
    } finally {
        fs.closeSync(descritor);
    }
}

/** Base exportada no Brasil costuma vir com ponto e vírgula; lá fora, com vírgula. */
function detectarDelimitador(caminhoEntrada) {
    const linha = primeiraLinha(caminhoEntrada);
    const pontoEVirgula = (linha.match(/;/g) || []).length;
    const virgula = (linha.match(/,/g) || []).length;
    return pontoEVirgula > virgula ? ';' : ',';
}

async function lerPrimeiraAba(caminhoEntrada) {
    const extensao = path.extname(caminhoEntrada).toLowerCase();
    if (extensao === '.xls') {
        throw new Error('formato .xls antigo nao e suportado, converta para .xlsx');
    }

    const workbook = new ExcelJS.Workbook();
    if (extensao === '.csv') {
        // `map` identidade: sem ele o ExcelJS converte "04252011000110" em número
        // na leitura e o zero à esquerda se perde antes das regras rodarem.
        await workbook.csv.readFile(caminhoEntrada, {
            parserOptions: { delimiter: detectarDelimitador(caminhoEntrada) },
            map: (valor) => valor,
        });
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

/**
 * No CSV a célula vazia precisa ser string, não null: o escritor do ExcelJS
 * monta a linha a partir de `row.values`, e um null no fim encurta o array —
 * a linha sairia com duas colunas em vez de três.
 */
function valorDigitos(digitos, formato) {
    if (digitos === '') return formato.numerico ? null : '';
    return formato.numerico ? Number(digitos) : digitos;
}

function copiarLinhas(origem, destino, colunas, formato) {
    const vazio = formato.numerico ? null : '';
    let linhas = 0;

    for (let numero = 2; numero <= origem.rowCount; numero++) {
        const linha = origem.getRow(numero);
        const nome = textoDe(linha.getCell(colunas.NOME).value);
        const cpf = normalizarDigitos(linha.getCell(colunas.CPF).value);
        const fone = normalizarDigitos(linha.getCell(colunas.FONE1).value);

        if (nome === '' && cpf === '' && fone === '') continue;

        destino.addRow([nome || vazio, valorDigitos(cpf, formato), valorDigitos(fone, formato)]);
        linhas++;
    }
    return linhas;
}

async function escreverArquivo(workbook, caminhoSaida, formato) {
    if (formato === FORMATOS.CSV) {
        await workbook.csv.writeFile(caminhoSaida, {
            formatterOptions: { delimiter: DELIMITADOR_SAIDA, writeBOM: true },
        });
        return;
    }
    await workbook.xlsx.writeFile(caminhoSaida);
}

/**
 * Limpa uma planilha. Nunca lança: devolve `{ ok: false, motivo }` para que um
 * arquivo problemático não derrube o lote inteiro.
 */
async function limparArquivo(caminhoEntrada) {
    try {
        const origem = await lerPrimeiraAba(caminhoEntrada);
        const colunas = localizarColunas(origem);
        const formato = formatoDeSaida(caminhoEntrada);

        const workbook = new ExcelJS.Workbook();
        const destino = montarAbaDestino(workbook);
        const linhas = copiarLinhas(origem, destino, colunas, formato);
        if (formato.numerico) aplicarFormatos(destino);

        const caminhoSaida = caminhoDisponivel(caminhoEntrada, formato.extensao);
        await escreverArquivo(workbook, caminhoSaida, formato);

        return { ok: true, caminhoSaida, linhas };
    } catch (erro) {
        return { ok: false, motivo: erro.message };
    }
}

module.exports = { limparArquivo, caminhoDisponivel };
