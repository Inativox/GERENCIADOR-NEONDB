const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const blocklistBq = require('../src/main/blocklistBq');

function fakeBq(answer = () => ({ rows: [] })) {
    const calls = [];
    return {
        calls,
        async queryPages(options) {
            calls.push(options);
            const result = await answer(options, calls.length);
            await options.onPage?.(result.rows || []);
            return { rows: (result.rows || []).length, jobId: 'j' + calls.length, bytes: 1, ...(result.dml != null ? { dml: result.dml } : {}) };
        },
    };
}
const values = param => param.parameterValue.arrayValues.map(item => item.value);

test('consulta a blocklist do BigQuery em lotes, por parâmetro, e devolve a data de entrada', async () => {
    const bq = fakeBq(options => ({ rows: values(options.params[0]).filter(phone => phone === '21998364849').map(telefone => ({ telefone, data_adicao: '27/10/2026 10:00:00' })) }));
    const phones = Array.from({ length: blocklistBq.LOOKUP_CHUNK + 1 }, (_, index) => String(11900000000 + index));
    phones.push('(21) 99836-4849', '21998364849');
    const found = await blocklistBq.findBlocked(bq, phones);
    assert.deepEqual([...found], [['21998364849', '27/10/2026 10:00:00']]);
    assert.equal(bq.calls.length, 2);
    assert.match(bq.calls[0].query, /FROM `mbtech-bronze\.blocklist\.telefones` WHERE telefone IN UNNEST\(@phones\)/);
    assert.ok(!bq.calls[0].query.includes('21998364849'));
    assert.ok(bq.calls.every(call => call.maxBytesBilled));
});

test('alimentar é MERGE que só insere o que falta e soma os novos de cada lote', async () => {
    const bq = fakeBq((_options, call) => ({ dml: call === 1 ? 3 : 1 }));
    const phones = Array.from({ length: blocklistBq.INSERT_CHUNK + 1 }, (_, index) => String(21900000000 + index));
    const added = await blocklistBq.addPhones(bq, [...phones, phones[0], ''], { origem: 'gerenciador-feed' });
    assert.equal(added, 4);
    assert.equal(bq.calls.length, 2);
    const [first] = bq.calls;
    assert.match(first.query, /^MERGE `mbtech-bronze\.blocklist\.telefones`/);
    assert.match(first.query, /WHEN NOT MATCHED THEN INSERT/);
    assert.doesNotMatch(first.query, /WHEN MATCHED/);
    assert.equal(values(first.params[0]).length, blocklistBq.INSERT_CHUNK);
    assert.equal(first.params[1].parameterValue.value, 'gerenciador-feed');
});

test('estatísticas contam o total e o que entrou hoje no horário de Brasília', async () => {
    const bq = fakeBq(() => ({ rows: [{ total: '13832048', addedToday: '408' }] }));
    assert.deepEqual(await blocklistBq.stats(bq), { total: 13832048, addedToday: 408 });
    assert.match(bq.calls[0].query, /America\/Sao_Paulo/);
});

test('sem acesso ao dataset a mensagem fala da blocklist, não da Receita', async () => {
    const bq = { async queryPages() { throw Object.assign(new Error('Sem acesso à Receita no BigQuery.'), { code: 'FLOW_VALIDATION' }); } };
    await assert.rejects(blocklistBq.findBlocked(bq, ['21998364849']), error => error.code === 'BQ_FORBIDDEN' && /blocklist/.test(error.message));
    const expired = { async queryPages() { throw Object.assign(new Error('Renove o login.'), { code: 'BQ_AUTH_REQUIRED' }); } };
    await assert.rejects(blocklistBq.stats(expired), { code: 'BQ_AUTH_REQUIRED' });
});

test('nenhum código do app consulta ou grava a blocklist no Neon', () => {
    const base = path.join(__dirname, '..', 'src', 'main');
    const files = [];
    const walk = directory => { for (const entry of fs.readdirSync(directory, { withFileTypes: true })) { const full = path.join(directory, entry.name); if (entry.isDirectory()) walk(full); else if (entry.name.endsWith('.js')) files.push(full); } };
    walk(base);
    for (const file of files) {
        const source = fs.readFileSync(file, 'utf8');
        assert.doesNotMatch(source, /(FROM|INTO)\s+blocklist\b/i, path.relative(base, file));
    }
});
