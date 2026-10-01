const test = require('node:test');
const assert = require('node:assert/strict');
const { buildOptionsQuery, validateRequest, createReceitaOptions } = require('../src/main/flows/receitaOptions');
const fields = Object.fromEntries(['cnpj', 'razao_social', 'situacao_cadastral_cod', 'estado', 'cidade', 'bairro', 'atividade_principal_cod', 'atividade_principal', 'natureza_juridica_cod', 'natureza_juridica'].map(field => [field, field]));
const metadata = { fields, types: { cnpj: 'text', atividade_principal_cod: 'character', natureza_juridica_cod: 'character' } };
test('database dropdown queries whitelist columns, parameterize searches and cascade only relevant locations', () => {
    const query = buildOptionsQuery(metadata, { field: 'bairro', search: "São_%'; DROP TABLE empresas;--", uf: ['sp'], cidade: ['Campinas'], offset: 50 });
    assert.match(query.text, /SELECT DISTINCT/);
    assert.match(query.text, /LIMIT 51 OFFSET/);
    assert.doesNotMatch(query.text, /DROP TABLE/);
    assert.deepEqual(query.values.slice(0, 2), [['SP'], ['CAMPINAS']]);
    assert.equal(query.values.at(-1), 50);
    assert.ok(query.values[2].includes('\\_\\%'));
    const cnae = buildOptionsQuery(metadata, { field: 'cnaes', search: '4711302', uf: ['SP'], cidade: ['Campinas'] });
    assert.match(cnae.text, /"atividade_principal_cod" = \$1::bpchar/);
    assert.doesNotMatch(cnae.text, /"estado"|"cidade"|LIKE/);
    assert.deepEqual(cnae.values, ['4711302', 0]);
    const descriptions = buildOptionsQuery(metadata, { field: 'naturezas', search: 'sociedade' });
    assert.match(descriptions.text, /natureza_juridica".*LIKE/);
    assert.throws(() => buildOptionsQuery({ ...metadata, fields: {} }, { field: 'uf' }), /estado/);
});
test('invalid field/search/page fails before any database access', () => {
    for (const input of [{ field: '__proto__' }, { field: 'uf', offset: -1 }, { field: 'cidade', uf: ['SP;DROP'] }, { field: 'cnaes', search: 'x'.repeat(101) }]) assert.throws(() => validateRequest(input));
});
test('options cache combines same requests, pages bounded results and resets after source changes', async () => {
    let source = 'first', queries = 0, ended = 0;
    const service = createReceitaOptions({ getConnection: () => source, poolFactory: () => ({ query: async sql => {
        if (sql.includes('information_schema')) return { rows: Object.keys(fields).map(column_name => ({ column_name, data_type: 'text' })) };
        if (sql.includes('pg_index')) return { rows: [] };
        queries++;
        return { rows: Array.from({ length: 51 }, (_, index) => ({ value: String(index), label: `Description ${index}` })) };
    }, end: async () => { ended++; } }) });
    const [first, duplicate] = await Promise.all([service.load({ field: 'cnaes' }), service.load({ field: 'cnaes' })]);
    assert.equal(queries, 1); assert.deepEqual(first, duplicate); assert.equal(first.options.length, 50); assert.equal(first.hasMore, true);
    first.options[0].value = 'mutated'; assert.equal((await service.load({ field: 'cnaes' })).options[0].value, '0');
    assert.equal(queries, 1);
    source = 'second'; await service.load({ field: 'cnaes' }); assert.equal(queries, 2);
    assert.equal(ended, 1); await service.close(); assert.equal(ended, 2);
});
test('neighborhood selector waits for a city and database errors expose no credentials', async () => {
    let reads = 0;
    const service = createReceitaOptions({ getConnection: () => 'private connection', poolFactory: () => ({ query: async () => { reads++; throw new Error('secret credential'); }, end: async () => {} }) });
    assert.match((await service.load({ field: 'bairro' })).message, /cidades/); assert.equal(reads, 0);
    await assert.rejects(service.load({ field: 'uf' }), error => !error.message.includes('secret') && /opções/.test(error.message));
});

test('indexed catalogs seek distinct codes, while location catalogs aggregate raw fields before formatting', () => {
    const indexed = { ...metadata, indexedFields: ['estado', 'atividade_principal_cod'] };
    const cnaes = buildOptionsQuery(indexed, { field: 'cnaes' }, { catalog: true });
    assert.match(cnaes.text, /WITH RECURSIVE choices/);
    assert.match(cnaes.text, /"atividade_principal_cod" > previous.code/);
    assert.doesNotMatch(cnaes.text, /SELECT DISTINCT|GROUP BY/);
    const naturezas = buildOptionsQuery(metadata, { field: 'naturezas' }, { catalog: true });
    assert.match(naturezas.text, /MIN\(e\."natureza_juridica"\)/);
    assert.match(naturezas.text, /GROUP BY e\."natureza_juridica_cod"/);
    const cities = buildOptionsQuery(metadata, { field: 'cidade', uf: ['SP'] }, { catalog: true });
    assert.match(cities.text, /GROUP BY e\."cidade", e\."estado"/);
    assert.doesNotMatch(cities.text, /ANY/); // One complete city catalog supplies all UF combinations.
});

test('search, pagination and UF combinations reuse the complete SQL catalog without additional scans', async () => {
    let queries = 0;
    const service = createReceitaOptions({ getConnection: () => 'fixture', poolFactory: () => ({ query: async sql => {
        if (sql.includes('information_schema')) return { rows: Object.keys(fields).map(column_name => ({ column_name, data_type: 'text' })) };
        if (sql.includes('pg_index')) return { rows: [] };
        queries++;
        return { rows: [...Array.from({ length: 60 }, (_, n) => ({ value: `Cidade ${String(n).padStart(2, '0')}`, label: `Cidade ${String(n).padStart(2, '0')}`, uf: n % 2 ? 'RJ' : 'SP' })), { value: 'Paris', label: 'Paris', uf: null }] };
    }, end: async () => {} }) });
    const all = await service.load({ field: 'cidade' });
    assert.equal(all.options.length, 50); assert.equal(all.hasMore, true);
    const next = await service.load({ field: 'cidade', offset: 50 }); assert.equal(next.options.length, 11);
    const sp = await service.load({ field: 'cidade', uf: ['SP'] }); assert.equal(sp.options.length, 30); assert.equal(sp.hasMore, false);
    const search = await service.load({ field: 'cidade', search: 'cidade 05', uf: ['RJ'] }); assert.equal(search.options[0].value, 'Cidade 05');
    assert.equal(queries, 1); await service.close();
});

test('persistent catalogs survive restart, expire and contain no connection credentials', async t => {
    const fs = require('node:fs/promises'), os = require('node:os'), path = require('node:path');
    const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'receita-options-'));
    t.after(() => fs.rm(directory, { recursive: true, force: true }));
    let reads = 0;
    const settings = { getConnection: () => 'private-password', cacheDirectory: directory, poolFactory: () => ({ query: async sql => {
        if (sql.includes('information_schema')) return { rows: Object.keys(fields).map(column_name => ({ column_name, data_type: 'text' })) };
        if (sql.includes('pg_index')) return { rows: [] };
        reads++; return { rows: [{ value: '2062', label: 'Sociedade Empresária Limitada' }] };
    }, end: async () => {} }) };
    const first = createReceitaOptions(settings); await first.load({ field: 'naturezas' }); await first.close();
    const file = path.join(directory, (await fs.readdir(directory)).find(name => name.endsWith('.json')));
    assert.ok(!(await fs.readFile(file, 'utf8')).includes('private-password'));
    const restarted = createReceitaOptions(settings);
    assert.equal((await restarted.load({ field: 'naturezas', search: 'empresaria' })).options[0].value, '2062');
    assert.equal(reads, 1); await restarted.close();
    const old = new Date(Date.now() - 25 * 60 * 60000); await fs.utimes(file, old, old);
    const expired = createReceitaOptions(settings); await expired.load({ field: 'naturezas' }); assert.equal(reads, 2); await expired.close();
});

test('oversized catalogs fail without caching or returning incomplete results', async () => {
    const service = createReceitaOptions({ getConnection: () => 'fixture', poolFactory: () => ({ query: async sql => {
        if (sql.includes('information_schema')) return { rows: Object.keys(fields).map(column_name => ({ column_name, data_type: 'text' })) };
        if (sql.includes('pg_index')) return { rows: [] };
        return { rows: Array.from({ length: 101 }, () => ({ value: 'SP', label: 'SP' })) };
    }, end: async () => {} }) });
    await assert.rejects(service.load({ field: 'uf' }), /carregar/); await service.close();
});
