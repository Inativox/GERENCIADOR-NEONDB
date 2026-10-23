const test = require('node:test');
const assert = require('node:assert/strict');
const { iterateReceita } = require('../src/main/flows/receita');
const { validateFlow } = require('../src/main/flows/config');
const { ALLOWED_NATUREZAS } = require('../src/main/flows/naturezas');

function pool() {
    const queries = [];
    return {
        queries,
        async query(text, values) {
            if (text.startsWith('SELECT column_name')) return { rows: [
                { column_name: 'cnpj', data_type: 'character' }, { column_name: 'razao_social', data_type: 'text' },
                { column_name: 'situacao_cadastral_cod', data_type: 'character' }, { column_name: 'natureza_juridica_cod', data_type: 'text' },
            ] };
            queries.push({ text, values });
            return { rows: [] };
        },
    };
}
const generate = async filters => { const fake = pool(); for await (const _ of iterateReceita({ pool: fake, batchSize: 10, filters })) { /* drain */ } return fake.queries[0]; };

test('sem natureza escolhida a geração usa só as 11 naturezas permitidas', async () => {
    const query = await generate({ limit: 10 });
    assert.match(query.text, /natureza_juridica_cod"::text = ANY/);
    assert.ok(query.values.some(value => Array.isArray(value) && value.join() === ALLOWED_NATUREZAS.join()));
});
test('natureza fora da lista é recusada na geração', async () => {
    await assert.rejects(generate({ limit: 10, naturezas: ['2062', '2054'] }), /não permitida: 2054/);
});
test('fluxo salvo com natureza fora da lista perde só a natureza bloqueada', () => {
    const flow = validateFlow({ name: 'Teste', generation: { naturezas: ['2062', '2054', '4014'] } });
    assert.deepEqual(flow.generation.naturezas.sort(), ['2062', '4014']);
});
