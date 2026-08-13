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
    // CSV salvo pelo Excel começa com BOM; sem tirar, o cabeçalho não casa por exato.
    return String(bruto).replace(/^﻿/, '').trim();
}

function normalizarCabecalho(valor) {
    return textoDe(valor)
        .normalize('NFD')
        .replace(/[̀-ͯ]/g, '')
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
 * Corta o sufixo decimal (",00") do texto.
 * Um separador que aparece mais de uma vez é separador de milhar, não decimal:
 * sem essa checagem, um CNPJ escrito como "04.252.011.0001.10" perderia os dois
 * últimos dígitos e sairia com 12 em vez de 14.
 */
function cortarDecimal(texto) {
    const decimal = texto.match(/([.,])\d{1,2}$/);
    if (!decimal) return texto;

    const separador = decimal[1];
    const ocorrencias = texto.split(separador).length - 1;
    return ocorrencias === 1 ? texto.slice(0, decimal.index) : texto;
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
            texto = cortarDecimal(texto);
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
