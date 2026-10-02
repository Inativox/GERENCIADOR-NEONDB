'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { iterateReceita, getReceitaMetadata, MAX_ROWS } = require('../src/main/flows/receita');

const COLUMNS = {
    cnpj: 'character varying', razao_social: 'text', situacao_cadastral_cod: 'text', situacao_cadastral: 'text',
    situacao_cadastral_data: 'date', situacao_motivo: 'text', telefone_principal: 'text', telefone_secundario: 'text',
    email: 'text', estado: 'text', cidade: 'text', bairro: 'text', atividade_principal_cod: 'text',
    natureza_juridica_cod: 'text', data_abertura: 'date', opcao_mei: 'text'
};
function fixturePool({ columns = COLUMNS, rows = [], readError } = {}) {
    const calls = [];
    return {
        calls,
        async query(sql, values) {
            calls.push({ sql, values });
            if (sql.includes('information_schema.columns')) return { rows: Object.entries(columns).map(([column_name, data_type]) => ({ column_name, data_type })) };
            if (readError) throw readError;
            return { rows: rows.filter(row => row.cnpj > values[0]).slice(0, values.at(-1)) };
        }
    };
}
async function collect(options) {
    const pages = [];
    for await (const page of iterateReceita(options)) pages.push(page);
    return pages;
}
const row = index => ({ cnpj: `0000000000000${index}`, razao_social: `Empresa ${index}`, situacao_cadastral_cod: '02' });

test('Receita streams bounded pages with stable CNPJ cursor and default situation 02', async () => {
    const pool = fixturePool({ rows: [1, 2, 3, 4, 5, 6].map(row) });
    const pages = await collect({ pool, filters: { limit: 5 }, batchSize: 2 });
    assert.deepEqual(pages.map(page => page.rows.length), [2, 2, 1]);
    assert.deepEqual(pages.map(page => page.cursor), ['00000000000002', '00000000000004', '00000000000005']);
    assert.equal(pages[0].rows[0].cnpj, '00000000000001');
    assert.equal(pages[0].rows[0].situacao_cadastral, 'Ativa');
    assert.equal(pages[0].rows[0].cep, '');
    const reads = pool.calls.slice(1);
    assert.deepEqual(reads.map(call => call.values[0]), ['', '00000000000002', '00000000000004']);
    assert.deepEqual(reads.map(call => call.values.at(-1)), [2, 2, 1]);
    for (const call of reads) {
        assert.deepEqual(call.values[1], ['02']);
        assert.match(call.sql, /ORDER BY e\."cnpj" ASC LIMIT \$\d+::integer/);
        assert.match(call.sql, /e\."cnpj" > \$1::varchar/);
        assert.doesNotMatch(call.sql, /COUNT\(|SELECT \*|OFFSET|custo/i);
    }
});

test('Receita resumes strictly after a supplied cursor, stops on empty or short batches', async () => {
    const pool = fixturePool({ rows: [1, 2, 3].map(row) });
    const pages = await collect({ pool, filters: { limit: 10 }, batchSize: 2, afterCnpj: '00000000000002' });
    assert.deepEqual(pages.flatMap(page => page.rows.map(item => item.cnpj)), ['00000000000003']);
    assert.equal(pool.calls.length, 2);
    assert.deepEqual(await collect({ pool: fixturePool() }), []);
});
test('empty or omitted limit reads all matching rows in bounded pages and accepts explicit values above 500k', async () => {
    for (const limit of [null, '', undefined, 500001]) {
        const pool = fixturePool({ rows: [1, 2, 3, 4, 5, 6].map(row) });
        const pages = await collect({ pool, filters: { limit }, batchSize: 2 });
        assert.equal(pages.flatMap(page => page.rows).length, 6);
        assert.ok(pool.calls.slice(1).every(call => call.values.at(-1) === 2));
        assert.equal(pool.calls.at(-1).values[0], '00000000000006');
    }
});

test('Receita parameters cover all generation filters without SQL interpolation', async () => {
    const pool = fixturePool();
    await collect({ pool, filters: {
        limit: 15, uf: ['sp'], cidade: ["São Paulo'); DROP TABLE empresas; --"], bairro: ['São_%'],
        cnaes: ['0111301'], naturezas: ['2062'], dateFrom: '2026-01-01', dateTo: '2026-09-30',
        mei: 'no', phone: 'with', email: 'without', situacoes: ['02', '08']
    } });
    const { sql, values } = pool.calls[1];
    assert.doesNotMatch(sql, /DROP TABLE|2026-01-01|0111301|2062/);
    assert.deepEqual(values, ['', ['02', '08'], ['SP'], ["SAO PAULO'); DROP TABLE EMPRESAS; --"], ['0111301'], ['2062'], '%SAO\\_\\%%', '2026-01-01', '2026-09-30', 'N', 15]);
    assert.match(sql, /LIKE \$7 ESCAPE E'\\\\'/);
    assert.match(sql, /data_abertura"::date >= \$8::date/);
    assert.match(sql, /opcao_mei" IS NULL/);
    assert.match(sql, /telefone_principal.* IS NOT NULL.* OR .*telefone_secundario.* IS NOT NULL/);
    assert.match(sql, /NOT \(NULLIF\(btrim\(e\."email"::text\)/);
});

test('Receita supports MEI-only, phone-without, email-with and one-sided date filtering', async () => {
    const pool = fixturePool();
    await collect({ pool, filters: { mei: 'yes', phone: 'without', email: 'with', dateFrom: '2026-01-01' } });
    assert.ok(pool.calls[1].values.includes('S'));
    assert.match(pool.calls[1].sql, /NOT \(NULLIF\(btrim\(e\."telefone_principal/);
    assert.doesNotMatch(pool.calls[1].sql, /data_abertura"::date <=/);
    assert.equal(pool.calls[1].values.at(-1), 2000);
});

test('Receita metadata rejects missing required and numeric CNPJ columns with friendly errors', async () => {
    await assert.rejects(getReceitaMetadata(fixturePool({ columns: { cnpj: 'text' } })), /colunas obrigatórias razao_social, situacao_cadastral_cod/);
    await assert.rejects(getReceitaMetadata(fixturePool({ columns: { ...COLUMNS, cnpj: 'bigint' } })), /cnpj deve ser textual/);
    await assert.rejects(getReceitaMetadata({ query: async () => { throw new Error('private connection secret'); } }), error => !error.message.includes('secret') && /permissão de leitura/.test(error.message));
});

test('Receita adapts optional situation aliases, derives description and preserves date and reason', async () => {
    const columns = { cnpj: 'text', razao_social: 'text', codigo_situacao_cadastral: 'integer', data_situacao_cadastral: 'date', motivo_situacao_cadastral: 'text' };
    const pool = fixturePool({ columns, rows: [{ ...row(1), situacao_cadastral_cod: 8, situacao_cadastral_data: '2026-09-01', situacao_motivo: 'Encerramento' }] });
    const metadata = await getReceitaMetadata(pool);
    assert.equal(metadata.fields.situacao_cadastral_data, 'data_situacao_cadastral');
    const pages = await collect({ pool, filters: { situacoes: ['08'] } });
    assert.equal(pages[0].rows[0].situacao_cadastral_cod, '08');
    assert.equal(pages[0].rows[0].situacao_cadastral, 'Baixada');
    assert.equal(pages[0].rows[0].situacao_cadastral_data, '2026-09-01');
    assert.equal(pages[0].rows[0].situacao_motivo, 'Encerramento');
    assert.match(pool.calls.at(-1).sql, /lpad\(e\."codigo_situacao_cadastral"::text, 2, '0'\)/);
    assert.match(pool.calls.at(-1).sql, /"data_situacao_cadastral"::text AS "situacao_cadastral_data"/);
});

test('Receita refuses a requested filter whose optional column is missing', async () => {
    const columns = { cnpj: 'text', razao_social: 'text', situacao_cadastral_cod: 'text' };
    await assert.rejects(collect({ pool: fixturePool({ columns }), filters: { cidade: ['Campinas'] } }), /coluna Receita ausente: cidade/);
    await assert.rejects(collect({ pool: fixturePool({ columns }), filters: { phone: 'without' } }), /colunas Receita ausentes: telefone_principal, telefone_secundario/);
});

test('Receita validates bounds, situation, lists, dates and cursors before database access', async () => {
    for (const filters of [{ limit: MAX_ROWS + 1 }, { limit: 0 }, { situacoes: [] }, { situacoes: ['99'] }, { cidade: 'Campinas' }, { dateFrom: '2026-02-30' }, { dateFrom: '2026-09-01', dateTo: '2026-01-01' }, { mei: 'invalid' }]) {
        const pool = fixturePool();
        await assert.rejects(collect({ pool, filters }));
        assert.equal(pool.calls.length, 0);
    }
    await assert.rejects(collect({ pool: fixturePool(), batchSize: 50001 }), /lote Receita/);
    await assert.rejects(collect({ pool: fixturePool(), afterCnpj: 123 }), /Cursor Receita inválido/);
});

test('Receita fetches 50000-row pages and keeps the final partial page and cursor intact', async () => {
    const pool = fixturePool({ rows: Array.from({ length: 50002 }, (_, index) => ({ ...row(index + 1), cnpj: String(index + 1).padStart(14, '0') })) });
    const pages = await collect({ pool, filters: { limit: null }, batchSize: 50000 });
    assert.deepEqual(pages.map(page => page.rows.length), [50000, 2]);
    assert.deepEqual(pages.map(page => page.cursor), ['00000000050000', '00000000050002']);
    assert.deepEqual(pool.calls.slice(1).map(call => call.values.at(-1)), [50000, 50000]);
    assert.equal(new Set(pages.flatMap(page => page.rows.map(record => record.cnpj))).size, 50002);
});

test('Receita cancellation prevents requests and discards a returned batch', async () => {
    const controller = new AbortController();
    controller.abort();
    const pool = fixturePool();
    await assert.rejects(collect({ pool, signal: controller.signal }), { name: 'AbortError' });
    assert.equal(pool.calls.length, 0);
    const during = new AbortController();
    const active = fixturePool({ rows: [row(1)] });
    const query = active.query;
    active.query = async (sql, values) => {
        const result = await query(sql, values);
        if (!sql.includes('information_schema.columns')) during.abort();
        return result;
    };
    await assert.rejects(collect({ pool: active, signal: during.signal }), { name: 'AbortError' });
});

test('Receita rejects unsafe response ordering and hides database error details', async () => {
    const pool = fixturePool();
    const query = pool.query;
    pool.query = async (sql, values) => sql.includes('information_schema.columns') ? query(sql, values) : { rows: [row(2), row(1)] };
    await assert.rejects(collect({ pool }), /fora da ordem de paginação/);
    await assert.rejects(collect({ pool: fixturePool({ readError: new Error('postgres secret credentials') }) }), error => !error.message.includes('secret') && /retomar o fluxo/.test(error.message));
});
test('alphanumeric CNPJ survives page boundaries and resume without removing letters', async () => {
    const source = [{ ...row(1), cnpj: '00000000E08G12' }, { ...row(2), cnpj: '12ABC34501DE35' }];
    const pool = fixturePool({ rows: source });
    const result = await collect({ pool, filters: { limit: 2 }, batchSize: 1 });
    assert.deepEqual(result.flatMap(batch => batch.rows.map(row => row.cnpj)), source.map(row => row.cnpj));
    assert.equal(result[0].cursor, '00000000E08G12');
    const resumed = await collect({ pool: fixturePool({ rows: source }), afterCnpj: '00000000E08G12', batchSize: 1 });
    assert.equal(resumed[0].rows[0].cnpj, '12ABC34501DE35');
});
