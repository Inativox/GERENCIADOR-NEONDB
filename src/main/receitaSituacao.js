'use strict';
const fs = require('node:fs');
const path = require('node:path');
const { randomUUID } = require('node:crypto');
const { once } = require('node:events');
const { finished } = require('node:stream/promises');
const ExcelJS = require('exceljs');
const { parse } = require('csv-parse');
const { getReceitaMetadata } = require('./flows/receita');
const { cnpjText } = require('./documentos');
const { restoreCnpj } = require('./flows/bq');
const { safeCell, csvRow } = require('./flows/pipeline');
const SITUACOES = { '01': 'Nula', '02': 'Ativa', '03': 'Suspensa', '04': 'Inapta', '08': 'Baixada' };
function error(message) { return Object.assign(new Error(message), { code: 'RECEITA_SITUACAO' }); }
function cancelled(signal) { if (signal?.aborted) throw error('Consulta cancelada. O arquivo original foi preservado.'); }
function document(value) { return cnpjText(value) || restoreCnpj(value); }
const quote = value => `"${value.replace(/"/g, '""')}"`;
// A coluna cnpj da Receita é character(14). Comparada com text[], o Postgres converte a
// coluna, perde o índice da chave primária e varre os 65 mi de linhas (estourava os 60 s
// até com 1 CNPJ). No tipo da própria coluna, 1.000 CNPJs saem em ~1 s pelo índice.
function byCnpj(metadata) {
    const arrayType = metadata.types?.[metadata.fields.cnpj] === 'character' ? 'bpchar' : 'text';
    return `${quote(metadata.fields.cnpj)} = ANY($1::${arrayType}[])`;
}
// A Receita do BigQuery é agrupada por CNPJ: a consulta lê só os blocos dos CNPJs pedidos
// (~80 KB por CNPJ, 4 GB a cada 50 mil) e nunca mais que a tabela inteira (~30 GB).
const EMPRESAS = '`mbtech-bronze.receita.empresas`';
const BATCH_SIZE = 50000;
const MAX_BYTES_BILLED = '40000000000';
const FIELDS = ['razao_social', 'situacao_cadastral_cod', 'situacao_cadastral', 'situacao_cadastral_data', 'situacao_motivo', 'ultima_atualizacao', 'opcao_mei'];
const SELECTION = FIELDS.map(field => `JSON_VALUE(e.payload, '$.${field}') AS ${field}`).join(', ');
const strings = (name, values) => ({ name, parameterType: { type: 'ARRAY', arrayType: { type: 'STRING' } }, parameterValue: { arrayValues: values.map(value => ({ value })) } });
const integer = (name, value) => ({ name, parameterType: { type: 'INT64' }, parameterValue: { value: String(value) } });
function situation(row) {
    const code = String(row.situacao_cadastral_cod || '').padStart(2, '0');
    return { ...Object.fromEntries(FIELDS.map(field => [field, row[field] ?? null])), cnpj: row.cnpj, situacao_cadastral_cod: code, situacao_cadastral: row.situacao_cadastral || SITUACOES[code] || 'Não informada' };
}
// Falhas do BigQuery viram mensagens desta tela. O login vencido fica marcado para o app renovar.
async function fromBq(run, signal) {
    try { return await run(); }
    catch (cause) {
        cancelled(signal);
        if (cause.code === 'RECEITA_SITUACAO') throw cause;
        if (cause.code === 'BQ_AUTH_REQUIRED') throw Object.assign(error('Seu login Google precisa ser renovado. Entre de novo pela tela de Fluxos e repita a consulta.'), { authRequired: true });
        if (cause.code === 'BQ_CREDENTIAL_INVALID') throw error('A credencial do BigQuery precisa ser substituída em Fluxos, Acessos às fontes.');
        if (['FLOW_VALIDATION', 'BQ_FORBIDDEN'].includes(cause.code)) throw error('Sua conta Google não tem acesso à Receita no BigQuery. Peça acesso ao dataset receita.');
        if (cause.code === 'BQ_BYTES_LIMIT') throw error('A consulta passou do limite de custo do BigQuery e foi interrompida.');
        throw error('Não foi possível consultar a Receita no BigQuery. Confira a conexão e tente novamente.');
    }
}
async function lookup(bq, values, { signal } = {}) {
    const documents = [...new Set(values.map(document).filter(Boolean))];
    if (values.length > 1000) throw error('Consulte até 1.000 CNPJs por vez.');
    if (!documents.length) return new Map();
    const rows = [];
    await fromBq(() => bq.queryPages({ query: `SELECT e.cnpj, ${SELECTION} FROM ${EMPRESAS} e WHERE e.cnpj IN UNNEST(@cnpjs)`, params: [strings('cnpjs', documents)], signal,
        pageSize: BATCH_SIZE, maxBytesBilled: MAX_BYTES_BILLED, onPage: async page => { rows.push(...page); } }), signal);
    return new Map(rows.map(row => [row.cnpj, situation(row)]));
}
// A lista sobe em partes para uma tabela temporária da sessão e volta numa só consulta, na
// ordem dela: a Receita é lida uma vez por lista, qualquer que seja o tamanho.
function listSession(bq, signal, batchSize) {
    let session = null, sent = 0;
    const run = (query, params, options = {}) => fromBq(() => bq.queryPages({ query, params, signal, pageSize: batchSize, maxBytesBilled: MAX_BYTES_BILLED, session, ...options }), signal);
    return {
        get sent() { return sent; },
        async send(cnpjs) {
            if (!session) {
                ({ session } = await run('CREATE TEMP TABLE lista (idx INT64, cnpj STRING)', [], { createSession: true }));
                if (!session) throw error('O BigQuery não abriu a sessão da consulta. Tente novamente.');
            }
            await run('INSERT INTO lista SELECT @base + posicao, cnpj FROM UNNEST(@cnpjs) AS cnpj WITH OFFSET posicao', [integer('base', sent), strings('cnpjs', cnpjs)]);
            sent += cnpjs.length;
        },
        // Um erro de quem grava as páginas sai como está, não como falha do BigQuery.
        async read(onPage) {
            let failure;
            try { await run(`SELECT l.idx, l.cnpj, e.cnpj IS NOT NULL AS encontrado, ${SELECTION} FROM lista l LEFT JOIN ${EMPRESAS} e ON e.cnpj = l.cnpj ORDER BY l.idx`, [], { onPage: async page => { try { await onPage(page); } catch (cause) { failure = cause; throw cause; } } }); }
            catch (cause) { throw failure || cause; }
        },
        async close() { if (session) await bq.abortSession(session); },
    };
}
// CNAE principal de cada CNPJ, para o "Manter só estes CNAEs" da Limpeza Local. Lotes de 1.000 pela chave primária.
async function lookupCnaes(pool, values, { onProgress = () => {} } = {}) {
    const documents = [...new Set(values.map(document).filter(Boolean))], cnaes = new Map();
    if (!documents.length) return cnaes;
    const metadata = await getReceitaMetadata(pool), cnae = metadata.fields.atividade_principal_cod;
    if (!cnae) throw error('A base da Receita não tem a coluna atividade_principal_cod. O filtro de CNAEs não pode ser aplicado.');
    for (let start = 0; start < documents.length; start += 1000) {
        const batch = documents.slice(start, start + 1000);
        let rows;
        try { rows = (await pool.query(`SELECT ${quote(metadata.fields.cnpj)}::text AS cnpj, ${quote(cnae)}::text AS cnae FROM public.empresas WHERE ${byCnpj(metadata)}`, [batch])).rows; }
        catch { throw error('Não foi possível consultar o CNAE no banco da Receita. Confira a conexão e tente novamente.'); }
        rows.forEach(row => cnaes.set(String(row.cnpj).trim(), String(row.cnae ?? '').trim()));
        onProgress(start + batch.length);
    }
    return cnaes;
}
// O que pode ficar na saída. 'outra' é código fora dos cinco da Receita.
const KEEP_OPTIONS = ['ativa', 'baixada', 'inapta', 'suspensa', 'nula', 'outra', 'nao_encontrado', 'invalido'];
const CATEGORY_BY_CODE = { '01': 'nula', '02': 'ativa', '03': 'suspensa', '04': 'inapta', '08': 'baixada' };
function category(cnpj, result) {
    if (!cnpj) return 'invalido';
    if (!result) return 'nao_encontrado';
    return CATEGORY_BY_CODE[result.situacao_cadastral_cod] || 'outra';
}
// Mesma regra da geração dos Fluxos: MEI é opcao_mei = S. Fora da base não dá para saber, então fica.
const isMei = result => String(result?.opcao_mei ?? '').trim().toUpperCase() === 'S';
// CSV adicional: mesmas linhas e colunas do XLSX, no formato do Importar Mailing (BOM e ";").
function csvWriter(filename) {
    const stream = fs.createWriteStream(filename, { flags: 'wx' }), done = finished(stream);
    done.catch(() => {});
    return {
        async write(text) { if (text && !stream.write(text)) await once(stream, 'drain'); },
        async close() { stream.end(); await done; },
        async abort() { stream.destroy(); await done.catch(() => {}); },
    };
}
// null = manter tudo. Lista vazia ou só com valores desconhecidos é recusada: gerar uma
// planilha vazia por engano é pior do que avisar.
function normalizeKeep(keep) {
    if (keep == null) return null;
    if (!Array.isArray(keep)) throw error('Seleção de situações inválida.');
    const selected = new Set(keep.filter(value => KEEP_OPTIONS.includes(value)));
    if (!selected.size) throw error('Marque ao menos uma situação para manter no resultado.');
    return selected.size === KEEP_OPTIONS.length ? null : selected;
}
async function* inputRows(filename, signal) {
    if (/\.csv$/i.test(filename)) {
        const handle = await fs.promises.open(filename, 'r'); const buffer = Buffer.alloc(4096);
        try { await handle.read(buffer, 0, buffer.length, 0); } finally { await handle.close(); }
        const first = buffer.toString('utf8').split(/\r?\n/)[0];
        const source = fs.createReadStream(filename), parser = parse({ bom: true, delimiter: first.includes(';') ? ';' : ',', relax_column_count: true });
        source.on('error', cause => parser.destroy(cause)); source.pipe(parser);
        try { for await (const row of parser) { cancelled(signal); yield row; } }
        finally { source.destroy(); parser.destroy(); }
    } else if (/\.xlsx$/i.test(filename)) {
        const workbook = new ExcelJS.stream.xlsx.WorkbookReader(filename, { worksheets: 'emit', sharedStrings: 'cache', styles: 'ignore', hyperlinks: 'ignore' });
        for await (const sheet of workbook) {
            for await (const row of sheet) { cancelled(signal); yield Array.from({ length: row.cellCount }, (_, index) => row.getCell(index + 1).text); }
            break;
        }
    } else throw error('Use uma lista XLSX ou CSV com coluna CNPJ ou CPF (com os CNPJs).');
}
function headerOf(row) {
    const names = row.map(value => String(value).trim());
    const normalized = names.map(value => value.normalize('NFD').replace(/[̀-ͯ]/g, '').toLowerCase().trim());
    const matches = normalized.flatMap((value, index) => value === 'cnpj' ? [index] : []);
    if (!matches.length) normalized.forEach((value, index) => { if (value === 'cpf') matches.push(index); });
    if (matches.length !== 1) throw error('A lista precisa de uma única coluna CNPJ (ou CPF contendo CNPJs).');
    return { names, normalized, column: matches[0] };
}
// O cabeçalho primeiro, depois só as linhas com dados. As duas leituras da lista passam por aqui.
async function* listRows(filename, signal) {
    let header = null;
    for await (const row of inputRows(filename, signal)) {
        if (!header) { header = headerOf(row); yield header; continue; }
        if (!row.some(value => String(value || '').trim())) continue;
        if (row.length > header.names.length && row.slice(header.names.length).some(Boolean)) throw error('Há dados em colunas sem cabeçalho. Ajuste os títulos da lista e tente novamente.');
        yield row;
    }
    if (!header) throw error('A lista está vazia.');
}
// Primeira leitura: confere a lista inteira e manda os CNPJs, na ordem dela, antes de criar o resultado.
async function sendList(filename, list, { signal, batchSize, onStage }) {
    const rows = listRows(filename, signal), { value: header } = await rows.next();
    let batch = [];
    const send = async () => { onStage(`Enviando a lista ao BigQuery: ${(list.sent + batch.length).toLocaleString('pt-BR')} CNPJs.`); await list.send(batch); batch = []; };
    for await (const row of rows) {
        const cnpj = document(row[header.column]);
        if (cnpj) batch.push(cnpj);
        if (batch.length === batchSize) await send();
    }
    if (batch.length) await send();
    return header;
}
// XLSX do resultado e, com a chave ligada, o CSV com as mesmas linhas para o Importar Mailing.
async function openOutputs({ filename, outputDirectory, header, csv }) {
    const base = path.join(outputDirectory || path.dirname(filename), `${path.basename(filename, path.extname(filename))}_situacao_receita_${randomUUID().slice(0, 8)}`);
    const output = `${base}.xlsx`, temporary = output + '.partial';
    const csvOutput = csv ? `${base}.csv` : null, csvTemporary = csvOutput && csvOutput + '.partial';
    const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({ filename: temporary, useSharedStrings: false, useStyles: false });
    const completion = finished(workbook.stream); completion.catch(() => {});
    workbook.zip.on('error', cause => workbook.stream.destroy(cause));
    const sheet = workbook.addWorksheet('Situação Receita'), csvFile = csvTemporary ? csvWriter(csvTemporary) : null;
    const abort = async () => {
        workbook.zip?.abort?.(); workbook.stream.destroy(); await completion.catch(() => {});
        await csvFile?.abort();
        await Promise.all([temporary, csvTemporary].filter(Boolean).map(file => fs.promises.unlink(file).catch(() => {})));
    };
    let lines = '';
    try {
        const used = new Set(header.names.map(value => value.toUpperCase()));
        const extras = ['RECEITA_CODIGO', 'RECEITA_SITUACAO', 'RECEITA_DATA_SITUACAO', 'RECEITA_MOTIVO', 'RECEITA_ATUALIZACAO_BASE', 'RECEITA_RESULTADO', 'RECEITA_CONSULTADO_EM'].map(name => { let unique = name, suffix = 2; while (used.has(unique)) unique = `${name}_${suffix++}`; used.add(unique); return unique; });
        sheet.addRow([...header.names, ...extras].map(safeCell)).commit();
        // No CSV, a coluna CNPJ sai como "cpf", o nome que o Importar Mailing exige. Se a lista já tem
        // uma coluna cpf, os nomes ficam como estão para não gerar duas colunas cpf.
        const csvHeader = header.names.map((value, index) => index === header.column && header.normalized[index] === 'cnpj' && !header.normalized.includes('cpf') ? 'cpf' : value);
        await csvFile?.write('﻿' + csvRow([...csvHeader, ...extras]));
    } catch (cause) { await abort(); throw cause; }
    return {
        output, csvOutput, abort,
        add(values) { sheet.addRow(values.map(safeCell)).commit(); if (csvFile) lines += csvRow(values); },
        async flush() { await csvFile?.write(lines); lines = ''; },
        async finish(signal) {
            await this.flush(); sheet.commit(); await Promise.race([workbook.commit(), completion]); await completion;
            await csvFile?.close(); cancelled(signal);
            await fs.promises.rename(temporary, output);
            if (csvOutput) await fs.promises.rename(csvTemporary, csvOutput);
        },
    };
}
async function annotateFile({ filename, outputDirectory, bq, signal, keep, removeMei = false, csv = false, batchSize = BATCH_SIZE, onProgress = () => {}, onStage = () => {} }) {
    const selected = normalizeKeep(keep), checkedAt = new Date().toISOString();
    if (!Number.isSafeInteger(batchSize) || batchSize < 1 || batchSize > BATCH_SIZE) throw error('Lote de consulta inválido.');
    // processed/found/notFound/invalid medem a lista inteira, antes dos filtros; removed é o
    // que o filtro de situação tirou da saída e mei, o que o "Remover MEI" tirou depois dele.
    const counts = { processed: 0, found: 0, notFound: 0, invalid: 0, removed: 0, mei: 0 };
    const list = listSession(bq, signal, batchSize), changed = () => error('A lista mudou durante a consulta. Consulte de novo.');
    let outputs, rows;
    try {
        const header = await sendList(filename, list, { signal, batchSize, onStage });
        outputs = await openOutputs({ filename, outputDirectory, header, csv });
        const record = (row, cnpj, result) => {
            const status = !cnpj ? 'Documento inválido' : result ? 'Encontrado na base' : 'Não encontrado na base';
            counts.processed++; counts[!cnpj ? 'invalid' : result ? 'found' : 'notFound']++;
            if (selected && !selected.has(category(cnpj, result))) { counts.removed++; return; }
            if (removeMei && isMei(result)) { counts.mei++; return; }
            const original = Array.from({ length: header.names.length }, (_, index) => row[index] || '');
            outputs.add([...original, result?.situacao_cadastral_cod, result?.situacao_cadastral, result?.situacao_cadastral_data, result?.situacao_motivo, result?.ultima_atualizacao, status, checkedAt]);
        };
        // Segunda leitura: as linhas voltam na ordem da consulta; as sem CNPJ válido saem pelo caminho.
        rows = listRows(filename, signal); await rows.next();
        const nextValid = async () => {
            for (let item = await rows.next(); !item.done; item = await rows.next()) {
                const cnpj = document(item.value[header.column]);
                if (cnpj) return { row: item.value, cnpj };
                record(item.value, '', null);
            }
            return null;
        };
        let position = 0;
        if (list.sent) {
            onStage('Cruzando a lista com a Receita no BigQuery.');
            await list.read(async page => {
                for (const result of page) {
                    const next = await nextValid();
                    if (!next || Number(result.idx) !== position++ || result.cnpj !== next.cnpj) throw changed();
                    record(next.row, next.cnpj, String(result.encontrado) === 'true' ? situation(result) : null);
                }
                await outputs.flush(); onProgress({ ...counts });
            });
        }
        if (position !== list.sent || await nextValid()) throw changed();
        cancelled(signal); await outputs.finish(signal);
        return { ...counts, output: outputs.output, csvOutput: outputs.csvOutput, checkedAt };
    } catch (cause) {
        await outputs?.abort();
        throw cause.code === 'RECEITA_SITUACAO' || cause.code === 'FLOW_VALIDATION' ? cause : error('Não foi possível processar a lista. Confira o formato, a pasta e o espaço disponível. O original foi preservado.');
    } finally { await rows?.return(); await list.close(); }
}
module.exports = { lookup, lookupCnaes, annotateFile, document, normalizeKeep, KEEP_OPTIONS };
