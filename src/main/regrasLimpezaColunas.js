/**
 * Regras puras de limpeza de colunas: normalização de dígitos e busca de cabeçalho.
 * Sem I/O e sem Electron — de propósito, para poder testar com `node --test`.
 */

const DDI_BRASIL = '55';
// telefone brasileiro sem DDI: DDD (2) + fixo (8) ou celular (9)
const TAMANHOS_SEM_DDI = [10, 11];

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

/** Converte número em texto sem cair em notação científica. */
function numeroParaTexto(numero) {
    return Math.round(numero).toLocaleString('fullwide', { useGrouping: false });
}

/**
 * Expande notação científica que veio como TEXTO.
 *
 * Um CSV exportado do Excel guarda o texto exibido, não o valor: uma coluna que
 * aparecia como "5,52199E+12" grava só 6 algarismos significativos, e os outros 7
 * dígitos do telefone deixaram de existir no arquivo. Expandir isso produziria
 * 5521990000000 — um número plausível e errado. Melhor devolver vazio do que
 * inventar dígito.
 *
 * Científico com todos os significativos ("5,521998364849E+12") é expandido normal.
 */
function expandirCientifico(texto) {
    const numero = Number(texto.replace(',', '.'));
    if (!Number.isFinite(numero)) return '';

    const significativos = (texto.split(/[eE]/)[0].match(/\d/g) || []).length;
    const expandido = numeroParaTexto(numero).replace(/\D/g, '');
    return significativos < expandido.length ? '' : expandido;
}

/** Um texto científico cujos dígitos já se perderam na origem. */
function ehCientificoTruncado(valor) {
    const bruto = desembrulhar(valor);
    if (typeof bruto !== 'string') return false;
    if (!/\d\s*[eE][+-]?\d/.test(bruto)) return false;
    return expandirCientifico(bruto.trim()) === '';
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
            texto = expandirCientifico(texto);
        } else {
            texto = cortarDecimal(texto);
        }
    }

    const digitos = texto.replace(/\D/g, '');
    return /^0*$/.test(digitos) ? '' : digitos;
}

/**
 * Tira o 55 do DDI do telefone, replicando o "Ajuste de Fones"
 * (`handlers/limpeza.js`), com uma guarda a mais.
 *
 * O original corta o 55 de qualquer número que comece com 55. Isso engole o DDD
 * de quem é de Santa Maria/RS: 5599887766 (DDD 55, sem DDI) viraria 99887766.
 * Aqui o corte só acontece quando o que sobra continua sendo um telefone válido,
 * então DDI sai e DDD 55 fica.
 *
 * Vale só para telefone — CNPJ que começa com 55 nunca passa por aqui.
 */
function removerDdi(digitos) {
    if (!digitos.startsWith(DDI_BRASIL)) return digitos;

    const semDdi = digitos.slice(DDI_BRASIL.length);
    return TAMANHOS_SEM_DDI.includes(semDdi.length) ? semDdi : digitos;
}

/* ---------- Padrão C6: qualquer cabeçalho entra, sai o layout de 8 telefones ---------- */

// Mesmo layout do fluxo C6 BANK ("LAYOUT PADRÃO · 8 telefones" em flows/formats.json).
const MAX_FONES = 8;
const LAYOUT_C6 = ['nome', 'cpf', 'livre1', 'chave', 'livre3', 'livre4', 'livre5', 'livre7', ...Array.from({ length: MAX_FONES }, (_, i) => `fone${i + 1}`)];
const OPERACAO_C6 = 'C6';
const AMOSTRA_MINIMA = 0.8; // fração das células preenchidas que precisa ter a cara do campo

/**
 * Cabeçalhos conhecidos de cada campo, do mais confiável para o mais frouxo.
 * Cada teste recebe o cabeçalho já normalizado (sem acento, maiúsculo).
 */
const CABECALHOS = {
    cpf: [t => ['CPF', 'CNPJ', 'CPF/CNPJ', 'CNPJ/CPF', 'CPF CNPJ', 'CNPJ CPF', 'DOCUMENTO', 'DOC'].includes(t), t => t.includes('CNPJ'),
        t => t.includes('DOCUMENTO'), t => /\bCPF\b/.test(t) && !t.includes('SOCIO')],
    nome: [t => ['NOME', 'RAZAO SOCIAL', 'NOME DO NEGOCIO', 'NOME EMPRESARIAL'].includes(t), t => t.includes('RAZAO'), t => t.includes('NEGOCIO'),
        t => t.includes('EMPRESA'), t => t.includes('NOME') && !t.includes('SOCIO'), t => t.includes('CLIENTE')],
    chave: [t => t === 'CHAVE', t => /E[ -]?MAIL/.test(t)],
    livre1: [t => t === 'LIVRE1', t => t.includes('ABERTURA')],
    livre3: [t => t === 'LIVRE3', t => t.includes('CNAE'), t => t.includes('ATIVIDADE')],
    livre4: [t => t === 'LIVRE4'],
    uf: [t => t === 'UF' || t === 'ESTADO'],
    cidade: [t => t === 'CIDADE' || t === 'MUNICIPIO'],
};
const ehCabecalhoFone = t => /FONE|TELEFONE|CELULAR|WHATS|^TEL\b|^CEL\b/.test(t);

/** Tira . , ( ) - / do nome: ponto some ("LTDA." → "LTDA"), o resto vira espaço ("AUTO-PECAS" → "AUTO PECAS"). */
function limparNome(valor) {
    return textoDe(valor).replace(/\./g, '').replace(/[,()\-/]/g, ' ').replace(/\s+/g, ' ').trim();
}

const temLetra = texto => /\p{L}/u.test(texto);
const ehDocumento = digitos => digitos.length === 14 || ((digitos.length === 12 || digitos.length === 13) && !TAMANHOS_SEM_DDI.includes(removerDdi(digitos).length));
const ehTelefone = digitos => TAMANHOS_SEM_DDI.includes(removerDdi(digitos).length);

/** Proporção das células preenchidas da coluna que passam no teste. */
function proporcao(amostra, coluna, teste) {
    let preenchidas = 0, passam = 0;
    for (const linha of amostra) {
        const bruto = desembrulhar(linha[coluna]);
        if (bruto === null || String(bruto).trim() === '') continue;
        preenchidas++;
        if (teste(bruto)) passam++;
    }
    return preenchidas ? passam / preenchidas : 0;
}

/**
 * Descobre qual coluna é cada campo do padrão C6: primeiro pelo cabeçalho; o que não achar
 * (CNPJ, nome e telefones) procura pelo conteúdo das primeiras linhas.
 * Devolve { campo: { indice, por } } e `fones` como lista, na ordem da planilha.
 */
function mapearColunas(cabecalhos, amostra = []) {
    const normalizados = cabecalhos.map(normalizarCabecalho), usadas = new Set(), mapa = { fones: [] };
    const largura = Math.max(normalizados.length, ...amostra.map(linha => linha.length));
    const livres = (comCabecalho = true) => Array.from({ length: largura }, (_, i) => i)
        .filter(i => !usadas.has(i) && (!comCabecalho || (normalizados[i] || '') !== ''));
    const pegar = (campo, indice, por) => { if (indice === undefined) return; mapa[campo] = { indice, por }; usadas.add(indice); };
    for (const [campo, testes] of Object.entries(CABECALHOS)) {
        for (const teste of testes) {
            const indice = livres().find(i => teste(normalizados[i]));
            if (indice !== undefined) { pegar(campo, indice, 'cabeçalho'); break; }
        }
    }
    for (const i of livres()) if (ehCabecalhoFone(normalizados[i])) { mapa.fones.push({ indice: i, por: 'cabeçalho' }); usadas.add(i); }

    // Pelo conteúdo: só para o que o cabeçalho não resolveu (vale até coluna sem cabeçalho).
    const porConteudo = teste => livres(false).filter(i => proporcao(amostra, i, teste) >= AMOSTRA_MINIMA);
    if (!mapa.cpf) pegar('cpf', porConteudo(v => ehDocumento(normalizarDigitos(v)))[0], 'conteúdo');
    if (!mapa.fones.length) for (const i of porConteudo(v => ehTelefone(normalizarDigitos(v)))) { mapa.fones.push({ indice: i, por: 'conteúdo' }); usadas.add(i); }
    // Nome: entre as colunas de texto, a de mais letras em média (um código como "x1" também tem letra).
    if (!mapa.nome) {
        const letras = i => amostra.reduce((total, linha) => total + (textoDe(linha[i]).match(/\p{L}/gu) || []).length, 0);
        const textos = porConteudo(v => typeof v === 'string' && temLetra(v) && !v.includes('@'));
        pegar('nome', textos.sort((a, b) => letras(b) - letras(a))[0], 'conteúdo');
    }

    if (!mapa.cpf) throw new Error('nenhuma coluna de CNPJ/CPF encontrada, nem pelo cabeçalho nem pelo conteúdo');
    if (!mapa.fones.length) throw new Error('nenhuma coluna de telefone encontrada, nem pelo cabeçalho nem pelo conteúdo');
    return mapa;
}

/** Texto para o log: nome ← "Razão Social" · cpf ← "Doc" (pelo conteúdo) · fones ← "Cel", "Tel 2". */
function descreverMapa(mapa, cabecalhos) {
    const rotulo = ({ indice, por }) => `"${textoDe(cabecalhos[indice]) || `coluna ${indice + 1}`}"${por === 'conteúdo' ? ' (pelo conteúdo)' : ''}`;
    const partes = ['nome', 'cpf', 'livre1', 'chave', 'livre3', 'livre4', 'uf', 'cidade'].filter(c => mapa[c]).map(c => `${c} ← ${rotulo(mapa[c])}`);
    partes.push(`fones ← ${mapa.fones.map(rotulo).join(', ')}`);
    return partes.join(' · ');
}

function textoCelula(valor) {
    const bruto = desembrulhar(valor);
    if (bruto instanceof Date) return bruto.toLocaleDateString('pt-BR', { timeZone: 'UTC' });
    return textoDe(bruto);
}

/**
 * Monta uma linha no padrão C6. Documento e telefones saem só com dígitos; telefones sem DDI,
 * sem repetição e compactados da esquerda para a direita (até 8).
 */
function linhaC6(linha, mapa) {
    const celula = campo => (mapa[campo] ? linha[mapa[campo].indice] : null);
    const brutoCpf = celula('cpf');
    let truncados = ehCientificoTruncado(brutoCpf) ? 1 : 0, ddis = 0;
    const fones = [];
    for (const { indice } of mapa.fones) {
        const bruto = linha[indice];
        if (ehCientificoTruncado(bruto)) truncados++;
        const comDdi = normalizarDigitos(bruto), fone = removerDdi(comDdi);
        if (fone !== comDdi) ddis++;
        if (fone && !fones.includes(fone)) fones.push(fone);
    }
    const livre4 = mapa.livre4 ? textoCelula(celula('livre4')) : [textoCelula(celula('uf')), textoCelula(celula('cidade'))].filter(Boolean).join(' - ');
    const nome = limparNome(celula('nome')), cpf = normalizarDigitos(brutoCpf);
    return {
        nome, cpf, fones: fones.slice(0, MAX_FONES), sobrando: Math.max(0, fones.length - MAX_FONES), truncados, ddis,
        vazia: !nome && !cpf && !fones.length,
        livres: { livre1: textoCelula(celula('livre1')), chave: textoCelula(celula('chave')), livre3: textoCelula(celula('livre3')), livre4, livre5: '', livre7: OPERACAO_C6 },
    };
}

module.exports = {
    LAYOUT_C6,
    mapearColunas,
    descreverMapa,
    linhaC6,
    limparNome,
    removerDdi,
    desembrulhar,
    textoDe,
    normalizarCabecalho,
    normalizarDigitos,
    ehCientificoTruncado,
    numeroParaTexto,
};
