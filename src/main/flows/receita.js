'use strict';
const { CNPJ_FORMAT } = require('../documentos');
const { telefoneParaGeracao } = require('./telefones');

const MAX_ROWS = Number.MAX_SAFE_INTEGER;
const MAX_BATCH_SIZE = 10000;
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
        phone: mode(filters.phone, 'phone', ['all', 'with', 'without']), email: mode(filters.email, 'email', ['all', 'with', 'without'])
    };
    if (normalized.dateFrom && normalized.dateTo && normalized.dateFrom > normalized.dateTo) fail('A data inicial da Receita deve ser anterior ou igual à data final.');
    return normalized;
}

function buildQuery(metadata, filters, cursor, limit) {
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
    let remaining = selected.limit;
    let cursor = afterCnpj;
    while (remaining === null || remaining > 0) {
        checkCancelled(signal);
        const size = remaining === null ? batchSize : Math.min(batchSize, remaining);
        const query = buildQuery(metadata, selected, cursor, size);
        let result;
        try { result = await pool.query(query.text, query.values); }
        catch (error) {
            checkCancelled(signal);
            if (error.code === '57014') fail('A consulta à Receita excedeu o tempo limite do banco. Refine os filtros e retome o fluxo.');
            fail('Não foi possível ler a base Receita. Verifique a conexão e tente retomar o fluxo.');
        }
        checkCancelled(signal);
        if (!Array.isArray(result.rows) || result.rows.length > size) fail('Resposta Receita inválida: o lote excede o limite solicitado.');
        if (!result.rows.length) return;
        const rows = result.rows.map(source => {
            const row = Object.fromEntries(Object.keys(FIELD_ALIASES).map(name => [name, source[name] == null ? '' : String(source[name])]));
            row.telefone_principal = telefoneParaGeracao(row.telefone_principal);
            row.telefone_secundario = telefoneParaGeracao(row.telefone_secundario);
            if (!CNPJ_FORMAT.test(row.cnpj) || row.cnpj <= cursor) fail('Base Receita incompatível: CNPJs inválidos ou fora da ordem de paginação.');
            cursor = row.cnpj;
            row.situacao_cadastral_cod = row.situacao_cadastral_cod.padStart(2, '0');
            if (!row.situacao_cadastral) row.situacao_cadastral = SITUACOES[row.situacao_cadastral_cod] || '';
            return row;
        });
        if (remaining !== null) remaining -= rows.length;
        yield { rows, cursor };
        if (rows.length < size) return;
    }
}

module.exports = { iterateReceita, getReceitaMetadata, MAX_ROWS, MAX_BATCH_SIZE, FIELD_ALIASES };
