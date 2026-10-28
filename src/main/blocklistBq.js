'use strict';
/**
 * Blocklist no BigQuery (mbtech-bronze.blocklist.telefones), desde a 1.8.26.
 * Até a 1.8.25 a lista vivia na tabela `blocklist` do Neon. O hub e este app gravam aqui;
 * o hub ainda copia para o Neon, e de 15 em 15 min traz para cá o que uma versão antiga
 * do app gravar lá. Telefone é a chave: o MERGE só insere o que ainda não existe.
 */
const TABLE = '`mbtech-bronze.blocklist.telefones`';
const LOOKUP_CHUNK = 50000;
const INSERT_CHUNK = 50000;
const MAX_BYTES_BILLED = '5000000000';

const strings = (name, values) => ({ name, parameterType: { type: 'ARRAY', arrayType: { type: 'STRING' } }, parameterValue: { arrayValues: values.map(value => ({ value: String(value) })) } });
const scalar = (name, type, value) => ({ name, parameterType: { type }, parameterValue: { value: String(value) } });

function friendly(error) {
    if (error.code === 'BQ_AUTH_REQUIRED' || error.code === 'FLOW_CANCELLED') return error;
    // queryPages troca o 403 por um texto da Receita; aqui o acesso que falta é o da blocklist.
    if (error.code === 'FLOW_VALIDATION' || error.code === 'BQ_FORBIDDEN') {
        return Object.assign(new Error('Sem acesso à blocklist no BigQuery (mbtech-bronze.blocklist). Peça acesso ao dataset ou importe a credencial do Gerenciador em Fluxos.'), { code: 'BQ_FORBIDDEN' });
    }
    return error;
}

async function run(bq, query, params = [], { signal, onPage } = {}) {
    try {
        return await bq.queryPages({ query, params, signal, pageSize: 50000, onPage, maxBytesBilled: MAX_BYTES_BILLED, jobTimeoutMs: '600000', waitMs: 600000 });
    } catch (error) { throw friendly(error); }
}

function digits(values) {
    return [...new Set((values || []).map(value => String(value ?? '').replace(/\D/g, '')).filter(Boolean))];
}

/** Map telefone -> data de entrada (ISO) dos que estão na blocklist. */
async function findBlocked(bq, phones, { signal } = {}) {
    const list = digits(phones), found = new Map();
    for (let start = 0; start < list.length; start += LOOKUP_CHUNK) {
        await run(bq, `SELECT telefone, FORMAT_TIMESTAMP('%d/%m/%Y %H:%M:%S', data_adicao, 'America/Sao_Paulo') AS data_adicao FROM ${TABLE} WHERE telefone IN UNNEST(@phones)`,
            [strings('phones', list.slice(start, start + LOOKUP_CHUNK))], { signal, onPage: async rows => { for (const row of rows) found.set(row.telefone, row.data_adicao); } });
    }
    return found;
}

/** Quantos telefones eram novos. Repetidos e já bloqueados não contam. */
async function addPhones(bq, phones, { origem = 'gerenciador', signal } = {}) {
    const list = digits(phones);
    let added = 0;
    for (let start = 0; start < list.length; start += INSERT_CHUNK) {
        const result = await run(bq, `MERGE ${TABLE} t
            USING (SELECT DISTINCT p AS telefone FROM UNNEST(@phones) p) s ON t.telefone = s.telefone
            WHEN NOT MATCHED THEN INSERT (telefone, data_adicao, origem) VALUES (s.telefone, CURRENT_TIMESTAMP(), @origem)`,
        [strings('phones', list.slice(start, start + INSERT_CHUNK)), scalar('origem', 'STRING', origem)], { signal });
        added += Number(result.dml || 0);
    }
    return added;
}

/** Total da lista e quantos entraram hoje (horário de Brasília). */
async function stats(bq, { signal } = {}) {
    let data = { total: 0, addedToday: 0 };
    await run(bq, `SELECT COUNT(*) AS total,
        COUNTIF(data_adicao >= TIMESTAMP(CURRENT_DATE('America/Sao_Paulo'), 'America/Sao_Paulo')) AS addedToday FROM ${TABLE}`, [],
    { signal, onPage: async rows => { if (rows[0]) data = { total: Number(rows[0].total) || 0, addedToday: Number(rows[0].addedToday) || 0 }; } });
    return data;
}

module.exports = { TABLE, LOOKUP_CHUNK, INSERT_CHUNK, findBlocked, addPhones, stats, digits };
