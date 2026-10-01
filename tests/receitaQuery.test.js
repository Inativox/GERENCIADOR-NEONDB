const test = require('node:test');
const assert = require('node:assert/strict');
const { iterateReceita } = require('../src/main/flows/receita');

function poolFixture({ cnpjType = 'character', situationType = 'character', error } = {}) {
    const queries = [];
    const rows = [
        { cnpj: '00000000000001', razao_social: 'Synthetic A', situacao_cadastral_cod: '02' },
        { cnpj: '00000000000002', razao_social: 'Synthetic B', situacao_cadastral_cod: '02' },
    ];
    return {
        queries,
        async query(text, values) {
            if (text.startsWith('SELECT column_name')) return { rows: [
                { column_name: 'cnpj', data_type: cnpjType },
                { column_name: 'razao_social', data_type: 'text' },
                { column_name: 'situacao_cadastral_cod', data_type: situationType },
            ] };
            queries.push({ text, values });
            if (error) throw error;
            return { rows: rows.filter(row => row.cnpj > values[0]).slice(0, values.at(-1)) };
        },
    };
}

test('paginação de CNPJ CHAR mantém a coluna indexada e tipa o cursor como bpchar', async () => {
    const pool = poolFixture();
    const batches = [];
    for await (const batch of iterateReceita({ pool, batchSize: 1, filters: { limit: 2 } })) batches.push(batch);
    assert.equal(batches.length, 2);
    assert.equal(batches[1].cursor, '00000000000002');
    assert.match(pool.queries[0].text, /e\."cnpj" > \$1::bpchar/);
    assert.match(pool.queries[0].text, /e\."situacao_cadastral_cod" = ANY\(\$2::bpchar\[\]\)/);
    assert.equal(pool.queries[1].values[0], '00000000000001');
});

test('paginação textual e normalização de situação numérica continuam suportadas', async () => {
    const pool = poolFixture({ cnpjType: 'text', situationType: 'integer' });
    for await (const _ of iterateReceita({ pool, filters: { limit: 1 } })) { /* Read one batch. */ }
    assert.match(pool.queries[0].text, /e\."cnpj" > \$1::text/);
    assert.match(pool.queries[0].text, /lpad\(e\."situacao_cadastral_cod"::text, 2, '0'\) = ANY\(\$2::text\[\]\)/);
});

test('cancelamento por statement_timeout informa tempo limite em vez de conexão inválida', async () => {
    const pool = poolFixture({ error: Object.assign(new Error('internal query details'), { code: '57014' }) });
    await assert.rejects(iterateReceita({ pool }).next(), error => error.code === 'FLOW_VALIDATION' && /tempo limite/.test(error.message) && !error.message.includes('internal query details'));
});
