'use strict';
const { normalizeFilters, generationRow, fail, checkCancelled, MAX_BATCH_SIZE, FIELD_ALIASES } = require('./receita');

const EMPRESAS_TABLE = '`mbtech-bronze.receita.empresas`';
const LIMPEZA_TABLE = '`mbtech-bronze.receita.limpeza_api`';
const MAX_BYTES_BILLED = '80000000000';
const JOB_TIMEOUT_MS = '900000';
const WAIT_MS = 900000;
const PAGE_SIZE = 50000;
const READ_ERROR = 'Não foi possível ler a base Receita no BigQuery. Retome do último lote salvo.';
const BYTES_LIMIT_ERROR = 'A consulta da Receita no BigQuery passou do limite de custo configurado (80 GB). Restrinja os filtros ou use a fonte Neon.';

const field = name => `JSON_VALUE(e.payload, '$.${name}')`;
// Same accent folding as the Neon query (translate after upper).
const normalized = name => `TRANSLATE(UPPER(COALESCE(${field(name)}, '')), 'ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ', 'AAAAAEEEEIIIIOOOOOUUUUC')`;
// Neon numeric(18,2)::text always has 2 decimals; the payload keeps a JSON number.
const SELECT_EXPRESSIONS = Object.freeze({ capital_social: `FORMAT('%.2f', SAFE_CAST(${field('capital_social')} AS NUMERIC))` });
const present = name => `NULLIF(TRIM(${field(name)}, ' '), '') IS NOT NULL`;
const scalar = (name, type, value) => ({ name, parameterType: { type }, parameterValue: { value: String(value) } });
const list = (name, values) => ({ name, parameterType: { type: 'ARRAY', arrayType: { type: 'STRING' } }, parameterValue: { arrayValues: values.map(value => ({ value })) } });

function whereClauses(filters, params) {
    const where = [];
    const bind = (param, reference = `@${param.name}`) => { params.push(param); return reference; };
    where.push(`LPAD(${field('situacao_cadastral_cod')}, 2, '0') IN UNNEST(${bind(list('situacoes', filters.situacoes))})`);
    if (filters.availability === 'available') where.push(`EXISTS (SELECT 1 FROM ${LIMPEZA_TABLE} a WHERE a.cnpj = e.cnpj AND a.status = ${bind(scalar('disponivel', 'STRING', 'disponivel'))})`);
    for (const [name, key, input] of [['estado', 'uf', filters.uf], ['cidade', 'cidade', filters.cidade]]) {
        if (input.length) where.push(`${normalized(name)} IN UNNEST(${bind(list(key, input))})`);
    }
    for (const [name, key, input] of [['atividade_principal_cod', 'cnaes', filters.cnaes], ['natureza_juridica_cod', 'naturezas', filters.naturezas]]) {
        if (input.length) where.push(`${field(name)} IN UNNEST(${bind(list(key, input))})`);
    }
    if (filters.bairro.length) {
        // BigQuery LIKE escapes with backslash, as the Neon query does with ESCAPE '\'.
        const like = filters.bairro.map((value, index) => `${normalized('bairro')} LIKE ${bind(scalar(`bairro${index}`, 'STRING', `%${value.replace(/[\\%_]/g, char => `\\${char}`)}%`))}`);
        where.push(`(${like.join(' OR ')})`);
    }
    const date = `SAFE_CAST(${field('data_abertura')} AS DATE)`;
    if (filters.dateFrom) where.push(`${date} >= ${bind(scalar('dateFrom', 'DATE', filters.dateFrom))}`);
    if (filters.dateTo) where.push(`${date} <= ${bind(scalar('dateTo', 'DATE', filters.dateTo))}`);
    if (filters.mei !== 'all') {
        const mei = bind(scalar('mei', 'STRING', filters.mei === 'yes' ? 'S' : 'N'));
        where.push(filters.mei === 'yes' ? `UPPER(${field('opcao_mei')}) = ${mei}` : `(UPPER(${field('opcao_mei')}) = ${mei} OR ${field('opcao_mei')} IS NULL)`);
    }
    for (const [input, names] of [[filters.phone, ['telefone_principal', 'telefone_secundario']], [filters.email, ['email']]]) {
        if (input === 'all') continue;
        const condition = names.map(present).join(' OR ');
        where.push(input === 'with' ? `(${condition})` : `NOT (${condition})`);
    }
    return where;
}

// The payload is billed per JSON field read: selecting only what the flow uses reads less and downloads less.
const REQUIRED = ['cnpj', 'razao_social', 'situacao_cadastral_cod', 'situacao_cadastral'];
function selectedFields(fields) {
    if (fields == null) return Object.keys(FIELD_ALIASES);
    if (!Array.isArray(fields) || fields.some(name => !Object.hasOwn(FIELD_ALIASES, name))) fail('Campos Receita inválidos.');
    return Object.keys(FIELD_ALIASES).filter(name => REQUIRED.includes(name) || fields.includes(name));
}

function queryFor(filters, total, fields) {
    const params = [];
    const where = whereClauses(filters, params);
    const selection = selectedFields(fields).map(name => `${SELECT_EXPRESSIONS[name] || field(name)} AS ${name}`).join(', ');
    let query = `SELECT ${selection} FROM ${EMPRESAS_TABLE} e WHERE ${where.join(' AND ')}`;
    if (total != null) { params.push(scalar('limit', 'INT64', total)); query += ' LIMIT @limit'; }
    return { query, params };
}

function totalFor(filters, savedCount) {
    if (!Number.isSafeInteger(savedCount) || savedCount < 0) fail('Cache Receita inválido.');
    const total = filters.limit == null ? null : filters.limit + savedCount;
    if (total !== null && !Number.isSafeInteger(total)) fail('Limite Receita inválido.');
    return total;
}

// Same filters as buildQuery (receita.js), reading the payload JSON. Values only as named parameters.
function buildBqQuery(filters, { savedCount = 0, fields } = {}) {
    const selected = normalizeFilters(filters);
    return queryFor(selected, totalFor(selected, savedCount), fields);
}

// queryPages awaits onPage, so holding the page until the generator takes it
// keeps at most one page waiting while the previous one is being saved.
function pageQueue() {
    const pages = []; let ended = false, error, wake, stopped = false;
    const releases = [];
    const notify = () => { const resolve = wake; wake = null; resolve?.(); };
    return {
        onPage: rows => new Promise((resolve, reject) => {
            if (stopped) { reject(new Error('Leitura encerrada.')); return; }
            pages.push(rows); releases.push({ resolve, reject }); notify();
        }),
        finish(value) { if (value) error = value; ended = true; notify(); },
        async next() {
            while (!pages.length && !ended) await new Promise(resolve => { wake = resolve; });
            if (pages.length) { releases.shift().resolve(); return pages.shift(); }
            if (error) throw error;
            return null;
        },
        stop() { stopped = true; for (const release of releases.splice(0)) release.reject(new Error('Leitura encerrada.')); pages.length = 0; notify(); }
    };
}

async function* iterateReceitaBq({ bq, filters = {}, batchSize = 50000, savedCount = 0, isSaved = () => false, signal, fields } = {}) {
    const selected = normalizeFilters(filters);
    if (!Number.isSafeInteger(batchSize) || batchSize < 1 || batchSize > MAX_BATCH_SIZE) fail('Lote Receita inválido.');
    if (!bq || typeof bq.queryPages !== 'function') fail('Configure o acesso ao BigQuery antes de gerar a base Receita.');
    const { query, params } = queryFor(selected, totalFor(selected, savedCount), fields);
    checkCancelled(signal);
    const controller = new AbortController();
    const onAbort = () => controller.abort();
    signal?.addEventListener('abort', onAbort, { once: true });
    const queue = pageQueue();
    const running = Promise.resolve()
        .then(() => bq.queryPages({ query, params, signal: controller.signal, pageSize: PAGE_SIZE, onPage: queue.onPage, maxBytesBilled: MAX_BYTES_BILLED, jobTimeoutMs: JOB_TIMEOUT_MS, waitMs: WAIT_MS }))
        .then(() => queue.finish(), error => queue.finish(error));
    const abortWake = () => queue.finish();
    signal?.addEventListener('abort', abortWake, { once: true });
    let remaining = selected.limit, output = [];
    try {
        while (true) {
            checkCancelled(signal);
            const page = await queue.next();
            checkCancelled(signal);
            if (!page) break;
            for (const record of page) {
                checkCancelled(signal);
                const row = generationRow(record, isSaved);
                if (!row) continue;
                output.push(row);
                if (remaining !== null) remaining--;
                if (output.length === batchSize || remaining === 0) { yield { rows: output }; output = []; }
                if (remaining === 0) return;
            }
        }
        if (output.length) yield { rows: output };
    } catch (value) {
        checkCancelled(signal);
        // Access errors keep their message; an expired login keeps its code so the app can renew it.
        if (['FLOW_VALIDATION', 'BQ_AUTH_REQUIRED', 'BQ_CREDENTIAL_INVALID'].includes(value.code)) throw value;
        if (value.code === 'BQ_BYTES_LIMIT') fail(BYTES_LIMIT_ERROR);
        fail(READ_ERROR);
    } finally {
        signal?.removeEventListener('abort', onAbort);
        signal?.removeEventListener('abort', abortWake);
        controller.abort();
        queue.stop();
        await running; // lets queryPages cancel the remote job before returning
    }
}

module.exports = { buildBqQuery, iterateReceitaBq };
