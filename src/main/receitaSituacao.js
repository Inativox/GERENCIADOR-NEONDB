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
async function lookup(pool, values, metadata) {
    const documents = [...new Set(values.map(document).filter(Boolean))];
    if (values.length > 1000) throw error('Consulte até 1.000 CNPJs por lote.');
    if (!documents.length) return new Map();
    metadata ||= await getReceitaMetadata(pool);
    const quote = value => `"${value.replace(/"/g, '""')}"`;
    const fields = ['cnpj', 'razao_social', 'situacao_cadastral_cod', 'situacao_cadastral', 'situacao_cadastral_data', 'situacao_motivo', 'ultima_atualizacao', 'opcao_mei'];
    const selection = fields.map(field => `${metadata.fields[field] ? `${quote(metadata.fields[field])}::text` : 'NULL::text'} AS ${quote(field)}`).join(', ');
    // A coluna cnpj da Receita é character(14). Comparada com text[], o Postgres converte a
    // coluna, perde o índice da chave primária e varre os 65 mi de linhas (estourava os 60 s
    // até com 1 CNPJ). No tipo da própria coluna, 1.000 CNPJs saem em ~1 s pelo índice.
    const arrayType = metadata.types?.[metadata.fields.cnpj] === 'character' ? 'bpchar' : 'text';
    let rows;
    try { rows = (await pool.query(`SELECT ${selection} FROM public.empresas WHERE ${quote(metadata.fields.cnpj)} = ANY($1::${arrayType}[])`, [documents])).rows; }
    catch { throw error('Não foi possível consultar a situação no banco da Receita. Confira a conexão e tente novamente.'); }
    return new Map(rows.map(row => {
        const code = String(row.situacao_cadastral_cod || '').padStart(2, '0');
        return [String(row.cnpj).trim(), { ...row, cnpj: String(row.cnpj).trim(), situacao_cadastral_cod: code, situacao_cadastral: row.situacao_cadastral || SITUACOES[code] || 'Não informada' }];
    }));
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
async function annotateFile({ filename, outputDirectory, pool, signal, keep, removeMei = false, csv = false, onProgress = () => {} }) {
    const selected = normalizeKeep(keep);
    const metadata = await getReceitaMetadata(pool), checkedAt = new Date().toISOString();
    if (removeMei && !metadata.fields.opcao_mei) throw error('A base da Receita configurada não informa MEI (coluna opcao_mei). Desligue "Remover MEI" para consultar esta lista.');
    const base = path.join(outputDirectory || path.dirname(filename), `${path.basename(filename, path.extname(filename))}_situacao_receita_${randomUUID().slice(0, 8)}`);
    const output = `${base}.xlsx`, temporary = output + '.partial';
    const csvOutput = csv ? `${base}.csv` : null, csvTemporary = csvOutput && csvOutput + '.partial';
    let workbook, sheet, completion, csvFile, header, column, batch = [];
    // processed/found/notFound/invalid medem a lista inteira, antes dos filtros; removed é o
    // que o filtro de situação tirou da saída e mei, o que o "Remover MEI" tirou depois dele.
    const counts = { processed: 0, found: 0, notFound: 0, invalid: 0, removed: 0, mei: 0 };
    const flush = async () => {
        if (!batch.length) return;
        cancelled(signal);
        const results = await lookup(pool, batch.map(row => row[column]), metadata);
        cancelled(signal);
        let lines = '';
        for (const row of batch) {
            const cnpj = document(row[column]), result = results.get(cnpj);
            const status = !cnpj ? 'Documento inválido' : result ? 'Encontrado na base' : 'Não encontrado na base';
            counts.processed++; counts[!cnpj ? 'invalid' : result ? 'found' : 'notFound']++;
            if (selected && !selected.has(category(cnpj, result))) { counts.removed++; continue; }
            if (removeMei && isMei(result)) { counts.mei++; continue; }
            const original = Array.from({ length: header.length }, (_, index) => row[index] || '');
            const extra = [result?.situacao_cadastral_cod, result?.situacao_cadastral, result?.situacao_cadastral_data, result?.situacao_motivo, result?.ultima_atualizacao, status, checkedAt];
            sheet.addRow([...original, ...extra].map(safeCell)).commit();
            if (csvFile) lines += csvRow([...original, ...extra]);
        }
        await csvFile?.write(lines);
        batch = []; onProgress({ ...counts });
    };
    try {
        for await (const row of inputRows(filename, signal)) {
            if (!header) {
                header = row.map(value => String(value).trim());
                const normalized = header.map(value => value.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase().trim());
                const matches = normalized.flatMap((value, index) => value === 'cnpj' ? [index] : []);
                if (!matches.length) normalized.forEach((value, index) => { if (value === 'cpf') matches.push(index); });
                if (matches.length !== 1) throw error('A lista precisa de uma única coluna CNPJ (ou CPF contendo CNPJs).');
                column = matches[0];
                workbook = new ExcelJS.stream.xlsx.WorkbookWriter({ filename: temporary, useSharedStrings: false, useStyles: false });
                completion = finished(workbook.stream); completion.catch(() => {});
                workbook.zip.on('error', cause => workbook.stream.destroy(cause));
                sheet = workbook.addWorksheet('Situação Receita');
                const used = new Set(header.map(value => value.toUpperCase()));
                const extras = ['RECEITA_CODIGO', 'RECEITA_SITUACAO', 'RECEITA_DATA_SITUACAO', 'RECEITA_MOTIVO', 'RECEITA_ATUALIZACAO_BASE', 'RECEITA_RESULTADO', 'RECEITA_CONSULTADO_EM'].map(name => { let unique = name, suffix = 2; while (used.has(unique)) unique = `${name}_${suffix++}`; used.add(unique); return unique; });
                sheet.addRow([...header, ...extras].map(safeCell)).commit();
                // No CSV, a coluna CNPJ sai como "cpf", o nome que o Importar Mailing exige. Se a lista j\u00E1 tem
                // uma coluna cpf, os nomes ficam como est\u00E3o para n\u00E3o gerar duas colunas cpf.
                const csvHeader = header.map((value, index) => index === column && normalized[index] === 'cnpj' && !normalized.includes('cpf') ? 'cpf' : value);
                if (csvTemporary) { csvFile = csvWriter(csvTemporary); await csvFile.write('\uFEFF' + csvRow([...csvHeader, ...extras])); }
                continue;
            }
            if (!row.some(value => String(value || '').trim())) continue;
            if (row.length > header.length && row.slice(header.length).some(Boolean)) throw error('Há dados em colunas sem cabeçalho. Ajuste os títulos da lista e tente novamente.');
            batch.push(row); if (batch.length === 1000) await flush();
        }
        if (!header) throw error('A lista está vazia.');
        await flush(); cancelled(signal); sheet.commit(); await Promise.race([workbook.commit(), completion]); await completion;
        await csvFile?.close(); cancelled(signal);
        await fs.promises.rename(temporary, output);
        if (csvOutput) await fs.promises.rename(csvTemporary, csvOutput);
        return { ...counts, output, csvOutput, checkedAt };
    } catch (cause) {
        if (workbook) { workbook.zip?.abort?.(); workbook.stream.destroy(); await completion.catch(() => {}); }
        await csvFile?.abort();
        await Promise.all([temporary, csvTemporary].filter(Boolean).map(file => fs.promises.unlink(file).catch(() => {})));
        throw cause.code === 'RECEITA_SITUACAO' || cause.code === 'FLOW_VALIDATION' ? cause : error('Não foi possível processar a lista. Confira o formato, a pasta e o espaço disponível. O original foi preservado.');
    }
}
module.exports = { lookup, annotateFile, document, normalizeKeep, KEEP_OPTIONS };
