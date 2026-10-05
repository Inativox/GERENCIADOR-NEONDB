'use strict';
const { CNPJ_FORMAT } = require('../documentos');
const { telefoneParaGeracao } = require('./telefones');

const MAX_ROWS = Number.MAX_SAFE_INTEGER;
const MAX_BATCH_SIZE = 100000;
const CNAE_SCAN_SIZE = 10000;
const MIN_CNAE_SCAN_SIZE = 500;
const SITUACOES = Object.freeze({ '01': 'Nula', '02': 'Ativa', '03': 'Suspensa', '04': 'Inapta', '08': 'Baixada' });
const FIELD_ALIASES = Object.freeze({
    cnpj: ['cnpj'], razao_social: ['razao_social'],
    situacao_cadastral_cod: ['situacao_cadastral_cod', 'codigo_situacao_cadastral'],
    situacao_cadastral: ['situacao_cadastral', 'descricao_situacao_cadastral'],
    situacao_cadastral_data: ['situacao_cadastral_data', 'data_situacao_cadastral'],
    situacao_motivo: ['situacao_motivo', 'motivo_situacao_cadastral'],
    ...Object.fromEntries([
        'email', 'telefone_principal', 'telefone_secundario', 'atividade_principal_cod',
        'atividade_principal', 'natureza_juridica_cod', 'natureza_juridica', 'data_abertura',
        'estado', 'cidade', 'logradouro', 'bairro', 'cep', 'nome_socio', 'cpf_socio',
        'nome_fantasia', 'numero', 'complemento', 'porte', 'capital_social',
        'situacao_especial', 'opcao_mei', 'municipio_ibge', 'tipo', 'pais', 'ultima_atualizacao'
    ].map(field => [field, [field]]))
});
const REQUIRED_FIELDS = ['cnpj', 'razao_social', 'situacao_cadastral_cod'];
const TEXT_SQL_TYPES = Object.freeze({ text: 'text', 'character varying': 'varchar', character: 'bpchar', varchar: 'varchar', bpchar: 'bpchar', citext: 'citext' });
const TEXT_TYPES = new Set(Object.keys(TEXT_SQL_TYPES));
const quote = value => `"${value.replace(/"/g, '""')}"`;

function fail(message) { throw Object.assign(new Error(message), { code: 'FLOW_VALIDATION' }); }
function checkCancelled(signal) {
    if (!signal?.aborted) return;
    const error = new Error('Geração Receita cancelada.');
    error.name = 'AbortError';
    throw error;
}

async function getReceitaMetadata(pool) {
    if (!pool || typeof pool.query !== 'function') fail('Configure a conexão de leitura da Receita antes de gerar a base.');
    let result;
    try {
        result = await pool.query(
            'SELECT column_name, data_type, udt_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position',
            ['public', 'empresas']
        );
    } catch {
        fail('Não foi possível verificar o esquema da Receita. Confira a conexão e a permissão de leitura.');
    }
    const columns = (result.rows || []).map(row => row.column_name);
    const types = Object.fromEntries((result.rows || []).map(row => [row.column_name, row.data_type === 'USER-DEFINED' ? row.udt_name : row.data_type]));
    const fields = Object.fromEntries(Object.entries(FIELD_ALIASES).map(([field, aliases]) => [field, aliases.find(alias => columns.includes(alias)) || null]));
    const missing = REQUIRED_FIELDS.filter(field => !fields[field]);
    if (missing.length) fail(`Esquema Receita incompatível: public.empresas não contém as colunas obrigatórias ${missing.join(', ')}.`);
    if (!TEXT_TYPES.has(types[fields.cnpj])) fail('Esquema Receita incompatível: a coluna cnpj deve ser textual para preservar zeros e permitir paginação segura.');
    return { schema: 'public', table: 'empresas', columns, types, fields };
}

async function getAvailabilityMetadata(pool) {
    let result;
    try {
        result = await pool.query(
            'SELECT column_name, data_type, udt_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position',
            ['public', 'limpeza_api']
        );
    } catch {
        fail('Não foi possível verificar a disponibilidade salva. Confira a conexão e a permissão de leitura da Limpeza API.');
    }
    const types = Object.fromEntries((result.rows || []).map(row => [row.column_name, row.data_type === 'USER-DEFINED' ? row.udt_name : row.data_type]));
    if (!TEXT_TYPES.has(types.cnpj) || !TEXT_TYPES.has(types.status)) fail('Para gerar somente disponíveis, a tabela public.limpeza_api deve conter as colunas textuais cnpj e status no banco da Receita.');
    return { cnpjType: TEXT_SQL_TYPES[types.cnpj], statusType: TEXT_SQL_TYPES[types.status] };
}

function textList(value, name) {
    if (value == null) return [];
    if (!Array.isArray(value) || value.length > 500 || value.some(item => typeof item !== 'string' || !item.trim())) fail(`Filtro Receita inválido: ${name} deve ser uma lista de textos (máximo 500).`);
    return [...new Set(value.map(item => item.trim()))];
}
function normalizeText(value) { return value.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toUpperCase().trim(); }
function isoDate(value, name) {
    if (value == null || value === '') return '';
    if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(value) || !Number.isFinite(Date.parse(`${value}T00:00:00Z`)) || new Date(`${value}T00:00:00Z`).toISOString().slice(0, 10) !== value) fail(`Filtro Receita inválido: ${name} deve ser uma data válida no formato AAAA-MM-DD.`);
    return value;
}
function mode(value, name, values) {
    const selected = value == null ? 'all' : value;
    if (!values.includes(selected)) fail(`Filtro Receita inválido: ${name}.`);
    return selected;
}
function normalizeFilters(filters) {
    if (!filters || typeof filters !== 'object' || Array.isArray(filters)) fail('Filtros Receita inválidos.');
    const limit = filters.limit == null || filters.limit === '' ? null : filters.limit;
    if (limit !== null && (!Number.isSafeInteger(limit) || limit < 1)) fail('O limite da geração Receita deve ser um inteiro positivo, ou vazio para sem limite.');
    const situacoes = textList(filters.situacoes ?? ['02'], 'situacoes').map(code => code.padStart(2, '0'));
    if (!situacoes.length || situacoes.some(code => !SITUACOES[code])) fail('Selecione pelo menos uma situação cadastral válida: 01, 02, 03, 04 ou 08.');
    const normalized = {
        limit, situacoes, uf: textList(filters.uf, 'uf').map(normalizeText),
        cidade: textList(filters.cidade, 'cidade').map(normalizeText), bairro: textList(filters.bairro, 'bairro').map(normalizeText),
        cnaes: textList(filters.cnaes, 'cnaes'), naturezas: textList(filters.naturezas, 'naturezas'),
        dateFrom: isoDate(filters.dateFrom, 'dateFrom'), dateTo: isoDate(filters.dateTo, 'dateTo'),
        mei: mode(filters.mei, 'mei', ['all', 'yes', 'no']),
        availability: mode(filters.availability, 'availability', ['all', 'available']),
        phone: mode(filters.phone, 'phone', ['all', 'with', 'without']), email: mode(filters.email, 'email', ['all', 'with', 'without'])
    };
    if (normalized.dateFrom && normalized.dateTo && normalized.dateFrom > normalized.dateTo) fail('A data inicial da Receita deve ser anterior ou igual à data final.');
    return normalized;
}

function buildQuery(metadata, filters, cursor, limit, throughCnpj) {
    const values = [];
    const bind = value => { values.push(value); return `$${values.length}`; };
    const field = (name, required = false) => {
        const actual = metadata.fields[name];
        if (!actual && required) fail(`O filtro solicitado depende da coluna Receita ausente: ${name}.`);
        return actual ? `e.${quote(actual)}` : null;
    };
    const normalized = name => `translate(upper(COALESCE(${field(name, true)}::text, '')), 'ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ', 'AAAAAEEEEIIIIOOOOOUUUUC')`;
    const situationField = field('situacao_cadastral_cod', true);
    const cnpjType = TEXT_SQL_TYPES[metadata.types[metadata.fields.cnpj]];
    const situationType = TEXT_SQL_TYPES[metadata.types[metadata.fields.situacao_cadastral_cod]];
    const situation = situationType ? situationField : `lpad(${situationField}::text, 2, '0')`;
    // Cast the parameters to the stored types. Casting CHAR columns to TEXT
    // prevents PostgreSQL from using their indexes for filtering and pagination.
    const where = [`${field('cnpj', true)} > ${bind(cursor)}::${cnpjType}`, `${situation} = ANY(${bind(filters.situacoes)}::${situationType || 'text'}[])`];
    if (filters.availability === 'available') {
        const saved = metadata.availability;
        // Keep the saved CNPJ index usable; filter before LIMIT and avoid duplicating companies.
        const cnpj = `${field('cnpj', true)}${saved.cnpjType === cnpjType ? '' : `::${saved.cnpjType}`}`;
        where.push(`EXISTS (SELECT 1 FROM "public"."limpeza_api" a WHERE a."cnpj" = ${cnpj} AND a."status" = ${bind('disponivel')}::${saved.statusType})`);
    }
    for (const [name, input] of [['estado', filters.uf], ['cidade', filters.cidade]]) {
        if (input.length) where.push(`${normalized(name)} = ANY(${bind(input)}::text[])`);
    }
    for (const [name, input] of [['atividade_principal_cod', filters.cnaes], ['natureza_juridica_cod', filters.naturezas]]) {
        if (input.length) where.push(`${field(name, true)}::text = ANY(${bind(input)}::text[])`);
    }
    if (filters.bairro.length) {
        const expression = normalized('bairro');
        where.push(`(${filters.bairro.map(value => `${expression} LIKE ${bind(`%${value.replace(/[\\%_]/g, char => `\\${char}`)}%`)} ESCAPE E'\\\\'`).join(' OR ')})`);
    }
    if (filters.dateFrom || filters.dateTo) {
        const date = field('data_abertura', true);
        if (filters.dateFrom) where.push(`${date}::date >= ${bind(filters.dateFrom)}::date`);
        if (filters.dateTo) where.push(`${date}::date <= ${bind(filters.dateTo)}::date`);
    }
    if (filters.mei !== 'all') {
        const mei = field('opcao_mei', true);
        where.push(filters.mei === 'yes' ? `upper(${mei}::text) = ${bind('S')}` : `(upper(${mei}::text) = ${bind('N')} OR ${mei} IS NULL)`);
    }
    for (const [input, names] of [[filters.phone, ['telefone_principal', 'telefone_secundario']], [filters.email, ['email']]]) {
        if (input === 'all') continue;
        const present = names.map(name => field(name)).filter(Boolean);
        if (!present.length) fail(`O filtro solicitado depende das colunas Receita ausentes: ${names.join(', ')}.`);
        const condition = present.map(column => `NULLIF(btrim(${column}::text), '') IS NOT NULL`).join(' OR ');
        where.push(input === 'with' ? `(${condition})` : `NOT (${condition})`);
    }
    const selection = Object.keys(FIELD_ALIASES).map(name => `${field(name) ? `${field(name)}::text` : 'NULL::text'} AS ${quote(name)}`).join(', ');
    if (throughCnpj) where.push(`${field('cnpj', true)} <= ${bind(throughCnpj)}::${cnpjType}`);
    return {
        text: `SELECT ${selection} FROM "public"."empresas" e WHERE ${where.join(' AND ')} ORDER BY ${field('cnpj', true)} ASC LIMIT ${bind(limit)}::integer`,
        values
    };
}

async function* iterateReceita({ pool, filters = {}, batchSize = 2000, afterCnpj = '', signal } = {}) {
    const selected = normalizeFilters(filters);
    if (!Number.isSafeInteger(batchSize) || batchSize < 1 || batchSize > MAX_BATCH_SIZE) fail(`O lote Receita deve estar entre 1 e ${MAX_BATCH_SIZE}.`);
    if (typeof afterCnpj !== 'string' || (afterCnpj && !CNPJ_FORMAT.test(afterCnpj))) fail('Cursor Receita inválido: informe um CNPJ textual com 14 posições.');
    checkCancelled(signal);
    const metadata = await getReceitaMetadata(pool);
    checkCancelled(signal);
    if (selected.availability === 'available') metadata.availability = await getAvailabilityMetadata(pool);
    checkCancelled(signal);
    let remaining = selected.limit;
    let cursor = afterCnpj;
    // A selective CNAE filter can make an ordered LIMIT traverse millions of
    // companies. Bound the CNPJ range first using the existing primary index.
    // All business filters (including active status and MEI) stay in SQL.
    let scanSize = selected.cnaes.length ? CNAE_SCAN_SIZE : null;
    while (remaining === null || remaining > 0) {
        checkCancelled(signal);
        const size = remaining === null ? batchSize : Math.min(batchSize, remaining);
        // Validate requested columns even if the source window is empty.
        let query = buildQuery(metadata, selected, cursor, size);
        let windowEnd;
        let result;
        try {
            if (scanSize) {
                const cnpj = `e.${quote(metadata.fields.cnpj)}`;
                const type = TEXT_SQL_TYPES[metadata.types[metadata.fields.cnpj]];
                const boundary = await pool.query(`SELECT max(cnpj)::text AS __scan_end, count(*)::integer AS __scan_count FROM (SELECT ${cnpj} AS cnpj FROM "public"."empresas" e WHERE ${cnpj} > $1::${type} ORDER BY ${cnpj} ASC LIMIT $2::integer) source_window`, [cursor, scanSize]);
                const window = boundary.rows?.[0];
                if (!window || !Number.isSafeInteger(window.__scan_count) || window.__scan_count < 0 || window.__scan_count > scanSize) fail('Resposta Receita inválida: janela de leitura inconsistente.');
                if (window.__scan_count === 0) return;
                windowEnd = window.__scan_end;
                if (typeof windowEnd !== 'string' || !CNPJ_FORMAT.test(windowEnd) || windowEnd <= cursor) fail('Resposta Receita inválida: cursor da janela inconsistente.');
                checkCancelled(signal);
                query = buildQuery(metadata, selected, cursor, size, windowEnd);
            }
            result = await pool.query(query.text, query.values);
        }
        catch (error) {
            checkCancelled(signal);
            if (error.code === 'FLOW_VALIDATION') throw error;
            if (error.code === '57014' && scanSize > MIN_CNAE_SCAN_SIZE) {
                scanSize = Math.max(MIN_CNAE_SCAN_SIZE, Math.floor(scanSize / 2));
                continue; // No rows/cursor were confirmed; retry the same range.
            }
            if (error.code === '57014') fail('A consulta à Receita excedeu o tempo limite do banco. Refine os filtros e retome o fluxo.');
            fail('Não foi possível ler a base Receita. Verifique a conexão e tente retomar o fluxo.');
        }
        checkCancelled(signal);
        if (!Array.isArray(result.rows) || result.rows.length > size) fail('Resposta Receita inválida: o lote excede o limite solicitado.');
        if (!result.rows.length && !windowEnd) return;
        const rows = result.rows.map(source => {
            const row = Object.fromEntries(Object.keys(FIELD_ALIASES).map(name => [name, source[name] == null ? '' : String(source[name])]));
            row.telefone_principal = telefoneParaGeracao(row.telefone_principal);
            row.telefone_secundario = telefoneParaGeracao(row.telefone_secundario);
            if (!CNPJ_FORMAT.test(row.cnpj) || row.cnpj <= cursor || (windowEnd && row.cnpj > windowEnd)) fail('Base Receita incompatível: CNPJs inválidos ou fora da ordem de paginação.');
            cursor = row.cnpj;
            row.situacao_cadastral_cod = row.situacao_cadastral_cod.padStart(2, '0');
            if (!row.situacao_cadastral) row.situacao_cadastral = SITUACOES[row.situacao_cadastral_cod] || '';
            return row;
        });
        // A short filtered page exhausts this range, not the whole table. The
        // cursor may advance past nonmatches, including an entirely empty range.
        // Full pages keep the last returned CNPJ to preserve remaining matches.
        if (windowEnd && rows.length < size) cursor = windowEnd;
        if (remaining !== null) remaining -= rows.length;
        yield { rows, cursor };
        if (!windowEnd && rows.length < size) return;
    }
}

module.exports = { iterateReceita, getReceitaMetadata, MAX_ROWS, MAX_BATCH_SIZE, FIELD_ALIASES };
