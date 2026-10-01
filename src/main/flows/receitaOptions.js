'use strict';
const { getReceitaMetadata } = require('./receita');
const fs = require('node:fs/promises');
const path = require('node:path');
const { createHash } = require('node:crypto');
const CACHE_TTL = 24 * 60 * 60000;
const CATALOG_LIMITS = { uf: 100, cnaes: 10000, naturezas: 2000, cidade: 30000, bairro: 50000 };
const FIELDS = { uf: ['estado'], cidade: ['cidade'], bairro: ['bairro'], cnaes: ['atividade_principal_cod', 'atividade_principal'], naturezas: ['natureza_juridica_cod', 'natureza_juridica'] };
const normalize = value => String(value).normalize('NFD').replace(/[\u0300-\u036f]/g, '').toUpperCase().trim();
function validateRequest(input) {
    if (!input || !Object.hasOwn(FIELDS, input.field)) throw new Error('Campo de opções Receita inválido.');
    const list = (value, pattern, max) => {
        if (!Array.isArray(value) || value.length > max || value.some(item => typeof item !== 'string' || item.length > 80 || !pattern.test(item))) throw new Error('Filtros de opções Receita inválidos.');
        return [...new Set(value.map(normalize))].sort();
    };
    const uf = list(input.uf || [], /^[A-Za-z]{2}$/, 100), cidade = list(input.cidade || [], /^.+$/u, 500);
    if (typeof (input.search ?? '') !== 'string' || (input.search || '').length > 100 || /[\u0000-\u001f]/u.test(input.search || '')) throw new Error('Busca Receita inválida.');
    const offset = input.offset ?? 0;
    if (!Number.isSafeInteger(offset) || offset < 0 || offset > 100000) throw new Error('Página Receita inválida.');
    return { field: input.field, search: normalize(input.search || ''), offset, uf, cidade };
}
function buildOptionsQuery(metadata, input, { catalog = false } = {}) {
    const request = validateRequest(input), values = [], bind = value => { values.push(value); return `$${values.length}`; };
    const field = name => { const actual = metadata.fields[name]; if (!actual) throw new Error(`A fonte Receita não possui o campo ${name}.`); return `e."${actual.replace(/"/g, '""')}"`; };
    const normalized = expression => `translate(upper(btrim(${expression}::text)), 'ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ', 'AAAAAEEEEIIIIOOOOOUUUUC')`;
    const [name, description] = FIELDS[request.field], expression = field(name);
    const value = `btrim(${expression}::text)`;
    const label = description && metadata.fields[description] ? `COALESCE(NULLIF(btrim(${field(description)}::text), ''), ${value})` : value;
    if (['uf', 'cnaes'].includes(request.field) && metadata.indexedFields?.includes(metadata.fields[name])) {
        const clauses = [];
        if (!catalog && request.search) { const parameter = bind(`%${request.search.replace(/[\\%_]/g, char => `\\${char}`)}%`); clauses.push(`(${normalized('code')} LIKE ${parameter} ESCAPE E'\\\\' OR ${normalized('description')} LIKE ${parameter} ESCAPE E'\\\\')`); }
        // A seek per distinct code, rather than grouping millions of repeated companies.
        return { text: `WITH RECURSIVE choices AS (
            (SELECT ${expression} AS code, ${label} AS description FROM public.empresas e WHERE ${expression} > '' ORDER BY ${expression} LIMIT 1)
            UNION ALL
            SELECT next.code, next.description FROM choices previous CROSS JOIN LATERAL
                (SELECT ${expression} AS code, ${label} AS description FROM public.empresas e WHERE ${expression} > previous.code ORDER BY ${expression} LIMIT 1) next
            ) SELECT btrim(code::text) AS value, description AS label FROM choices ${clauses.length ? `WHERE ${clauses.join(' AND ')}` : ''} ORDER BY code LIMIT ${catalog ? CATALOG_LIMITS[request.field] + 1 : 51} OFFSET ${bind(catalog ? 0 : request.offset)}::integer`, values };
    }
    if (catalog) {
        // Aggregate raw columns first; trimming/descriptions run once per option, not per company.
        const where = [`${expression} IS NOT NULL`, `${expression} <> ''`];
        if (request.field === 'bairro') {
            if (request.uf.length) where.push(`${field('estado')} = ANY(${bind(request.uf)}::${metadata.types[metadata.fields.estado] === 'character' ? 'bpchar' : 'text'}[])`);
            if (request.cidade.length) where.push(`${normalized(field('cidade'))} = ANY(${bind(request.cidade)}::text[])`);
        }
        const state = request.field === 'cidade' ? field('estado') : null;
        const aggregatedLabel = description && metadata.fields[description] ? `COALESCE(NULLIF(btrim(MIN(${field(description)})::text), ''), ${value})` : value;
        return { text: `SELECT ${value} AS value, ${aggregatedLabel} AS label${state ? `, btrim(${state}::text) AS uf` : ''} FROM public.empresas e WHERE ${where.join(' AND ')} GROUP BY ${expression}${state ? `, ${state}` : ''} ORDER BY value LIMIT ${CATALOG_LIMITS[request.field] + 1}`, values };
    }
    const where = [`${expression} IS NOT NULL`, `${value} <> ''`];
    let exact = false;
    if (['cidade', 'bairro'].includes(request.field) && request.uf.length) where.push(`${field('estado')} = ANY(${bind(request.uf)}::${metadata.types[metadata.fields.estado] === 'character' ? 'bpchar' : 'text'}[])`);
    if (request.field === 'bairro' && request.cidade.length) where.push(`${normalized(field('cidade'))} = ANY(${bind(request.cidade)}::text[])`);
    if (request.search && ((request.field === 'cnaes' && /^\d{7}$/.test(request.search)) || (request.field === 'naturezas' && /^\d{4}$/.test(request.search)) || (request.field === 'uf' && /^[A-Z]{2}$/.test(request.search)))) {
        exact = true;
        const type = metadata.types[name] === 'character' ? 'bpchar' : 'text';
        where.push(`${expression} = ${bind(request.search)}::${type}`);
    } else if (request.search) {
        const pattern = `%${request.search.replace(/[\\%_]/g, char => `\\${char}`)}%`;
        const parameter = bind(pattern);
        where.push(`(${normalized(expression)} LIKE ${parameter} ESCAPE E'\\\\' OR ${normalized(label)} LIKE ${parameter} ESCAPE E'\\\\')`);
    }
    return { text: `SELECT ${exact ? '' : 'DISTINCT '}${value} AS value, ${label} AS label FROM public.empresas e WHERE ${where.join(' AND ')} ${exact ? '' : 'ORDER BY value, label '}LIMIT ${exact ? 1 : 51} OFFSET ${bind(request.offset)}::integer`, values };
}
function createReceitaOptions({ getConnection, poolFactory, cacheDirectory, now = Date.now }) {
    let source, pool, metadata;
    const cache = new Map(), pending = new Map();
    let active = 0;
    async function load(input) {
        const request = validateRequest(input);
        // Neighborhood searches require a location; avoid a nationwide scan while the dropdown opens.
        if (request.field === 'bairro' && !request.cidade.length) return { options: [], hasMore: false, message: 'Selecione uma ou mais cidades para buscar bairros.' };
        const connection = getConnection();
        if (!connection) throw new Error('Configure a fonte Receita antes de consultar as opções.');
        if (connection !== source) {
            const previous = pool; source = connection; pool = poolFactory(connection); metadata = null; cache.clear(); pending.clear();
            const created = pool;
            created.on?.('error', () => { if (pool === created) metadata = null; });
            if (previous) void previous.end().catch(() => {});
        }
        const scope = { field: request.field, uf: request.field === 'bairro' ? request.uf : [], cidade: request.field === 'bairro' ? request.cidade : [] };
        const key = JSON.stringify(scope);
        const page = rows => {
            const filtered = rows.filter(row => (request.field !== 'cidade' || !request.uf.length || request.uf.includes(normalize(row.uf || ''))) && (!request.search || normalize(row.value).includes(request.search) || normalize(row.label).includes(request.search)));
            const options = [...new Map(filtered.map(row => [row.value, row])).values()];
            return { options: options.slice(request.offset, request.offset + 50).map(row => ({ value: row.value, label: row.label === row.value ? row.value : `${row.value} · ${row.label}` })), hasMore: options.length > request.offset + 50 };
        };
        const cached = cache.get(key);
        if (cached && now() - cached.at < CACHE_TTL) return page(cached.rows);
        if (pending.has(key)) return page(await pending.get(key));
        if (active >= 2) throw new Error('Aguarde a consulta de opções Receita em andamento e tente novamente.');
        const currentPool = pool, currentSource = source;
        const filename = cacheDirectory && path.join(cacheDirectory, createHash('sha256').update(currentSource + key).digest('hex') + '.json');
        const validRows = rows => Array.isArray(rows) && rows.length <= CATALOG_LIMITS[request.field] && rows.every(row => typeof row.value === 'string' && row.value.length <= 100 && typeof row.label === 'string' && row.label.length <= 1000 && (request.field !== 'cidade' || row.uf == null || typeof row.uf === 'string'));
        active++;
        const promise = (async () => {
            try {
                if (filename) {
                    try {
                        const stat = await fs.stat(filename);
                        if (stat.size <= 20 * 1024 * 1024 && now() - stat.mtimeMs < CACHE_TTL) {
                            const rows = JSON.parse(await fs.readFile(filename, 'utf8'));
                            if (validRows(rows)) { if (source === currentSource) cache.set(key, { at: now(), rows }); return rows; }
                        }
                    } catch { /* An absent or damaged local cache is rebuilt from SQL. */ }
                }
                if (!metadata) metadata = getReceitaMetadata(currentPool).then(async result => {
                    const indexes = await currentPool.query("SELECT a.attname AS column_name FROM pg_index i JOIN pg_class t ON t.oid = i.indrelid JOIN pg_namespace n ON n.oid = t.relnamespace JOIN pg_class idx ON idx.oid = i.indexrelid JOIN pg_am am ON am.oid = idx.relam JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = i.indkey[0] WHERE n.nspname = 'public' AND t.relname = 'empresas' AND i.indisvalid AND i.indpred IS NULL AND am.amname = 'btree'");
                    return { ...result, indexedFields: (indexes.rows || []).map(row => row.column_name).filter(Boolean) };
                });
                const currentMetadata = await metadata;
                const query = buildOptionsQuery(currentMetadata, scope, { catalog: true });
                const result = await currentPool.query(query.text, query.values);
                const rows = result.rows || [];
                if (!validRows(rows)) throw new Error();
                const clean = rows.map(row => ({ value: row.value.trim(), label: row.label.trim(), ...(request.field === 'cidade' ? { uf: (row.uf || '').trim() } : {}) })).filter(row => row.value).sort((a, b) => a.value.localeCompare(b.value, 'pt-BR'));
                if (source === currentSource) { if (cache.size >= 100) cache.delete(cache.keys().next().value); cache.set(key, { at: now(), rows: clean }); }
                if (filename) {
                    try {
                        await fs.mkdir(cacheDirectory, { recursive: true });
                        await fs.writeFile(filename + '.tmp', JSON.stringify(clean), { mode: 0o600 });
                        await fs.rename(filename + '.tmp', filename);
                        const files = (await fs.readdir(cacheDirectory)).filter(name => /^[a-f0-9]{64}\.json$/.test(name));
                        const entries = await Promise.all(files.map(async name => ({ filename: path.join(cacheDirectory, name), modified: (await fs.stat(path.join(cacheDirectory, name))).mtimeMs })));
                        entries.sort((a, b) => b.modified - a.modified);
                        for (const [index, entry] of entries.entries()) if (index >= 100 || now() - entry.modified >= CACHE_TTL) await fs.unlink(entry.filename);
                    } catch { /* A read-only/full local disk must not prevent a successful query. */ }
                }
                return clean;
            } catch { if (source === currentSource) metadata = null; throw new Error('Não foi possível carregar as opções da Receita. Tente novamente ou refine a busca e a localização.'); }
            finally { active--; if (pending.get(key) === promise) pending.delete(key); }
        })();
        pending.set(key, promise);
        return page(await promise);
    }
    return { load, close: async () => { if (pool) await pool.end(); } };
}
module.exports = { validateRequest, buildOptionsQuery, createReceitaOptions };
