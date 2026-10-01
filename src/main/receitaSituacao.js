'use strict';
const fs = require('node:fs');
const path = require('node:path');
const { randomUUID } = require('node:crypto');
const { finished } = require('node:stream/promises');
const ExcelJS = require('exceljs');
const { parse } = require('csv-parse');
const { getReceitaMetadata } = require('./flows/receita');
const { cnpjText } = require('./documentos');
const { restoreCnpj } = require('./flows/bq');
const { safeCell } = require('./flows/pipeline');
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
    const fields = ['cnpj', 'razao_social', 'situacao_cadastral_cod', 'situacao_cadastral', 'situacao_cadastral_data', 'situacao_motivo', 'ultima_atualizacao'];
    const selection = fields.map(field => `${metadata.fields[field] ? `${quote(metadata.fields[field])}::text` : 'NULL::text'} AS ${quote(field)}`).join(', ');
    let rows;
    try { rows = (await pool.query(`SELECT ${selection} FROM public.empresas WHERE ${quote(metadata.fields.cnpj)} = ANY($1::text[])`, [documents])).rows; }
    catch { throw error('Não foi possível consultar a situação no banco da Receita. Confira a conexão e tente novamente.'); }
    return new Map(rows.map(row => {
        const code = String(row.situacao_cadastral_cod || '').padStart(2, '0');
        return [row.cnpj, { ...row, situacao_cadastral_cod: code, situacao_cadastral: row.situacao_cadastral || SITUACOES[code] || 'Não informada' }];
    }));
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
    } else throw error('Use uma lista XLSX ou CSV com coluna CNPJ.');
}
async function annotateFile({ filename, outputDirectory, pool, signal, onProgress = () => {} }) {
    const metadata = await getReceitaMetadata(pool), checkedAt = new Date().toISOString();
    const output = path.join(outputDirectory || path.dirname(filename), `${path.basename(filename, path.extname(filename))}_situacao_receita_${randomUUID().slice(0, 8)}.xlsx`);
    const temporary = output + '.partial';
    let workbook, sheet, completion, header, column, batch = [];
    const counts = { processed: 0, found: 0, notFound: 0, invalid: 0 };
    const flush = async () => {
        if (!batch.length) return;
        cancelled(signal);
        const results = await lookup(pool, batch.map(row => row[column]), metadata);
        cancelled(signal);
        for (const row of batch) {
            const cnpj = document(row[column]), result = results.get(cnpj);
            const status = !cnpj ? 'Documento inválido' : result ? 'Encontrado na base' : 'Não encontrado na base';
            counts.processed++; counts[!cnpj ? 'invalid' : result ? 'found' : 'notFound']++;
            const original = Array.from({ length: header.length }, (_, index) => row[index] || '');
            const extra = [result?.situacao_cadastral_cod, result?.situacao_cadastral, result?.situacao_cadastral_data, result?.situacao_motivo, result?.ultima_atualizacao, status, checkedAt];
            sheet.addRow([...original, ...extra].map(safeCell)).commit();
        }
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
                continue;
            }
            if (!row.some(value => String(value || '').trim())) continue;
            if (row.length > header.length && row.slice(header.length).some(Boolean)) throw error('Há dados em colunas sem cabeçalho. Ajuste os títulos da lista e tente novamente.');
            batch.push(row); if (batch.length === 1000) await flush();
        }
        if (!header) throw error('A lista está vazia.');
        await flush(); cancelled(signal); sheet.commit(); await Promise.race([workbook.commit(), completion]); await completion; cancelled(signal);
        await fs.promises.rename(temporary, output);
        return { ...counts, output, checkedAt };
    } catch (cause) {
        if (workbook) { workbook.zip?.abort?.(); workbook.stream.destroy(); await completion.catch(() => {}); }
        await fs.promises.unlink(temporary).catch(() => {});
        throw cause.code === 'RECEITA_SITUACAO' || cause.code === 'FLOW_VALIDATION' ? cause : error('Não foi possível processar a lista. Confira o formato, a pasta e o espaço disponível. O original foi preservado.');
    }
}
module.exports = { lookup, annotateFile, document };
