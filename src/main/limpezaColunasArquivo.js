/**
 * Processamento de arquivo da Limpeza de Colunas: lê uma planilha, aplica as
 * regras e escreve a versão limpa ao lado do original.
 * Sem Electron — de propósito, para poder testar sem subir o app.
 */
const fs = require('fs');
const path = require('path');
const ExcelJS = require('exceljs');

const {
    ALVOS,
    textoDe,
    acharCabecalho,
    normalizarDigitos,
    ehCientificoTruncado,
    removerDdi,
} = require('./regrasLimpezaColunas');

const FORMATO_CPF = '00000000000000';
const FORMATO_FONE = '0';
const NOME_ABA_SAIDA = 'Lista';
const EXTENSAO_SAIDA = '.xlsx';

/** Devolve o primeiro `<base>_LIMPO*.xlsx` que ainda não existe na pasta do original. */
function caminhoDisponivel(caminhoEntrada) {
    const pasta = path.dirname(caminhoEntrada);
    const base = path.basename(caminhoEntrada, path.extname(caminhoEntrada));

    let candidato = path.join(pasta, `${base}_LIMPO${EXTENSAO_SAIDA}`);
    let contador = 1;
    while (fs.existsSync(candidato)) {
        candidato = path.join(pasta, `${base}_LIMPO_${contador}${EXTENSAO_SAIDA}`);
        contador++;
    }
    return candidato;
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
    let truncados = 0;
    let ddisRemovidos = 0;

    for (let numero = 2; numero <= origem.rowCount; numero++) {
        const linha = origem.getRow(numero);
        const brutoCpf = linha.getCell(colunas.CPF).value;
        const brutoFone = linha.getCell(colunas.FONE1).value;

        const nome = textoDe(linha.getCell(colunas.NOME).value);
        const cpf = normalizarDigitos(brutoCpf);
        const foneComDdi = normalizarDigitos(brutoFone);
        // só o telefone perde o DDI; CNPJ iniciado em 55 fica intacto
        const fone = removerDdi(foneComDdi);

        if (fone !== foneComDdi) ddisRemovidos++;
        if (ehCientificoTruncado(brutoCpf)) truncados++;
        if (ehCientificoTruncado(brutoFone)) truncados++;

        if (nome === '' && cpf === '' && fone === '') continue;

        destino.addRow([nome || null, cpf ? Number(cpf) : null, fone ? Number(fone) : null]);
        linhas++;
    }
    return { linhas, truncados, ddisRemovidos };
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
        const { linhas, truncados, ddisRemovidos } = copiarLinhas(origem, destino, colunas);
        aplicarFormatos(destino);

        const caminhoSaida = caminhoDisponivel(caminhoEntrada);
        await workbook.xlsx.writeFile(caminhoSaida);

        return { ok: true, caminhoSaida, linhas, truncados, ddisRemovidos };
    } catch (erro) {
        return { ok: false, motivo: erro.message };
    }
}

module.exports = { limparArquivo, caminhoDisponivel };
