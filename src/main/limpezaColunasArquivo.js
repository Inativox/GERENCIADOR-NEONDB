/**
 * Processamento de arquivo da Limpeza de Colunas: lê uma planilha com qualquer cabeçalho,
 * reconhece nome, documento e telefones e escreve a versão no padrão C6 (layout de 8 telefones)
 * ao lado do original, em .xlsx e num .csv adicional.
 * Sem Electron — de propósito, para poder testar sem subir o app.
 */
const fs = require('fs');
const path = require('path');
const ExcelJS = require('exceljs');

const { LAYOUT_C6, mapearColunas, descreverMapa, linhaC6 } = require('./regrasLimpezaColunas');

const FORMATO_CPF = '00000000000000';
const FORMATO_FONE = '0';
const NOME_ABA_SAIDA = 'Lista';
const LINHAS_AMOSTRA = 200; // linhas lidas para reconhecer as colunas pelo conteúdo
const COLUNA_CPF = LAYOUT_C6.indexOf('cpf') + 1;
const PRIMEIRA_FONE = LAYOUT_C6.indexOf('fone1') + 1;

/** Devolve o primeiro `<base>_LIMPO*` cujo .xlsx e .csv ainda não existem na pasta do original (caminho do .xlsx). */
function caminhoDisponivel(caminhoEntrada) {
    const pasta = path.dirname(caminhoEntrada);
    const base = path.basename(caminhoEntrada, path.extname(caminhoEntrada));
    const livre = nome => !fs.existsSync(path.join(pasta, `${nome}.xlsx`)) && !fs.existsSync(path.join(pasta, `${nome}.csv`));

    let nome = `${base}_LIMPO`;
    for (let contador = 1; !livre(nome); contador++) nome = `${base}_LIMPO_${contador}`;
    return path.join(pasta, `${nome}.xlsx`);
}

/**
 * Só `.xlsx` entra.
 *
 * CSV é recusado de propósito: quando o Excel exporta uma coluna exibida como
 * "5,52199E+12", ele grava o texto exibido, e os dígitos que faltam já não
 * existem no arquivo. No `.xlsx` o valor numérico está guardado inteiro e o
 * científico é só máscara de exibição — por isso a conversão funciona aqui.
 */
async function lerPrimeiraAba(caminhoEntrada) {
    const extensao = path.extname(caminhoEntrada).toLowerCase();
    if (extensao === '.xls') {
        throw new Error('formato .xls antigo nao e suportado, converta para .xlsx');
    }
    if (extensao === '.csv') {
        throw new Error('CSV nao e aceito: os digitos em notacao cientifica ja vem perdidos. Use o .xlsx');
    }

    const workbook = new ExcelJS.Workbook();
    await workbook.xlsx.readFile(caminhoEntrada);

    const aba = workbook.worksheets[0];
    if (!aba) throw new Error('o arquivo nao possui nenhuma aba');
    return aba;
}

/** Valores de uma linha da planilha como array 0-based. */
function valoresDaLinha(aba, numero) {
    const linha = aba.getRow(numero), valores = [];
    for (let coluna = 1; coluna <= aba.columnCount; coluna++) valores.push(linha.getCell(coluna).value);
    return valores;
}

function montarAbaDestino(workbook) {
    const aba = workbook.addWorksheet(NOME_ABA_SAIDA);
    aba.addRow(LAYOUT_C6);
    aba.getColumn(1).width = 40;
    aba.getColumn(COLUNA_CPF).width = 20;
    for (let i = 0; i < 8; i++) aba.getColumn(PRIMEIRA_FONE + i).width = 15;
    return aba;
}

/**
 * Precisa rodar DEPOIS das linhas entrarem: no ExcelJS, atribuir `numFmt` a uma
 * coluna propaga o estilo para as células que já existem, não para as futuras.
 */
function aplicarFormatos(aba) {
    aba.getColumn(COLUNA_CPF).numFmt = FORMATO_CPF;
    for (let i = 0; i < 8; i++) aba.getColumn(PRIMEIRA_FONE + i).numFmt = FORMATO_FONE;
}

/** Linha do padrão C6 na ordem do layout; documento e telefones como texto de dígitos. */
function comoLayout(linha) {
    const fones = Array.from({ length: 8 }, (_, i) => linha.fones[i] || '');
    const { livre1, chave, livre3, livre4, livre5, livre7 } = linha.livres;
    return [linha.nome, linha.cpf, livre1, chave, livre3, livre4, livre5, livre7, ...fones];
}

// CSV no formato do Importar Mailing: BOM, ";" e aspas. Célula que começa com = + - @ ganha ' para o Excel não executar.
function celulaCsv(valor) {
    const texto = String(valor ?? '');
    const seguro = /^[\s﻿]*[=+@-]/u.test(texto) || /^[\t\r\n]/u.test(texto) ? `'${texto}` : texto;
    return `"${seguro.replace(/"/g, '""')}"`;
}
const linhaCsv = valores => valores.map(celulaCsv).join(';') + '\r\n';

/** No CSV o documento sai com os zeros à esquerda que o número do xlsx só mostra pela máscara. */
const documentoCsv = cpf => (cpf ? cpf.padStart(cpf.length > 11 ? 14 : 11, '0') : '');

function copiarLinhas(origem, destino, mapa) {
    const csv = [linhaCsv(LAYOUT_C6)];
    const totais = { linhas: 0, truncados: 0, ddisRemovidos: 0, fonesSobrando: 0 };

    for (let numero = 2; numero <= origem.rowCount; numero++) {
        const linha = linhaC6(valoresDaLinha(origem, numero), mapa);
        totais.truncados += linha.truncados;
        totais.ddisRemovidos += linha.ddis;
        totais.fonesSobrando += linha.sobrando;
        if (linha.vazia) continue;

        const valores = comoLayout(linha);
        destino.addRow(valores.map((valor, i) => {
            if (valor === '') return null;
            return i + 1 === COLUNA_CPF || i + 1 >= PRIMEIRA_FONE ? Number(valor) : valor;
        }));
        valores[COLUNA_CPF - 1] = documentoCsv(linha.cpf);
        csv.push(linhaCsv(valores));
        totais.linhas++;
    }
    return { ...totais, csv };
}

/**
 * Limpa uma planilha. Nunca lança: devolve `{ ok: false, motivo }` para que um
 * arquivo problemático não derrube o lote inteiro.
 */
async function limparArquivo(caminhoEntrada) {
    try {
        const origem = await lerPrimeiraAba(caminhoEntrada);
        const cabecalhos = valoresDaLinha(origem, 1);
        const amostra = [];
        for (let numero = 2; numero <= Math.min(origem.rowCount, LINHAS_AMOSTRA + 1); numero++) amostra.push(valoresDaLinha(origem, numero));
        const mapa = mapearColunas(cabecalhos, amostra);

        const workbook = new ExcelJS.Workbook();
        const destino = montarAbaDestino(workbook);
        const { csv, ...totais } = copiarLinhas(origem, destino, mapa);
        aplicarFormatos(destino);

        const caminhoSaida = caminhoDisponivel(caminhoEntrada);
        const caminhoCsv = caminhoSaida.replace(/\.xlsx$/, '.csv');
        await workbook.xlsx.writeFile(caminhoSaida);
        await fs.promises.writeFile(caminhoCsv, '﻿' + csv.join(''));

        return { ok: true, caminhoSaida, caminhoCsv, colunas: descreverMapa(mapa, cabecalhos), semNome: !mapa.nome, ...totais };
    } catch (erro) {
        return { ok: false, motivo: erro.message };
    }
}

module.exports = { limparArquivo, caminhoDisponivel };
