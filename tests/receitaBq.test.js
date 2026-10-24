'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { buildBqQuery, iterateReceitaBq } = require('../src/main/flows/receitaBq');
const { iterateReceitaFast, FIELD_ALIASES } = require('../src/main/flows/receita');
const { ALLOWED_NATUREZAS } = require('../src/main/flows/naturezas');

// REST queryParameters -> plain value (array, scalar) for assertions.
const param = (params, name) => {
    const found = params.find(p => p.name === name);
    if (!found) return undefined;
    const value = found.parameterValue;
    return value.arrayValues ? value.arrayValues.map(item => item.value) : value.value;
};
const paramType = (params, name) => params.find(p => p.name === name)?.parameterType;

function fakeBq(pages, { fail } = {}) {
    const calls = [];
    return { calls, async queryPages(options) {
        calls.push(options);
        if (fail) throw fail;
        let rows = 0;
        for (const page of pages) {
            if (options.signal?.aborted) throw new Error('Consulta BQ cancelada.');
            await options.onPage(page);
            rows += page.length;
        }
        return { rows, jobId: 'job', bytes: 1 };
    } };
}
const drain = async iterator => { const batches = []; for await (const batch of iterator) batches.push(batch); return batches; };
const row = n => ({ cnpj: String(n).padStart(14, '0'), razao_social: 'Sintética', situacao_cadastral_cod: '02' });

test('sem natureza escolhida filtra pelas 11 permitidas', () => {
    const { query, params } = buildBqQuery({});
    assert.deepEqual(param(params, 'naturezas'), [...ALLOWED_NATUREZAS]);
    assert.deepEqual(paramType(params, 'naturezas'), { type: 'ARRAY', arrayType: { type: 'STRING' } });
    assert.match(query, /JSON_VALUE\(e\.payload, '\$\.natureza_juridica_cod'\) IN UNNEST\(@naturezas\)/);
    assert.deepEqual(param(params, 'situacoes'), ['02']);
    assert.match(query, /LPAD\(JSON_VALUE\(e\.payload, '\$\.situacao_cadastral_cod'\), 2, '0'\) IN UNNEST\(@situacoes\)/);
    assert.match(query, /FROM `mbtech-bronze\.receita\.empresas` e/);
    assert.doesNotMatch(query, /ORDER BY|LIMIT/);
});
test('natureza fora da lista é recusada', () => {
    assert.throws(() => buildBqQuery({ naturezas: ['2054'] }), /não permitida: 2054/);
});
test('valores entram como parâmetro, nunca no SQL', () => {
    const { query, params } = buildBqQuery({ cidade: ["SAO PAULO'; DROP"], uf: ['sp'], bairro: ['Centro_%'], cnaes: ['4711302'] });
    assert.doesNotMatch(query, /DROP|SAO PAULO|Centro|CENTRO|4711302/);
    assert.deepEqual(param(params, 'cidade'), ["SAO PAULO'; DROP"]);
    assert.deepEqual(param(params, 'uf'), ['SP']);
    assert.deepEqual(param(params, 'cnaes'), ['4711302']);
    assert.equal(param(params, 'bairro0'), '%CENTRO\\_\\%%');
    assert.match(query, /TRANSLATE\(UPPER\(COALESCE\(JSON_VALUE\(e\.payload, '\$\.cidade'\), ''\)\), 'ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ', 'AAAAAEEEEIIIIOOOOOUUUUC'\) IN UNNEST\(@cidade\)/);
    assert.match(query, /\$\.estado'\), ''\)\), '[^']+', '[^']+'\) IN UNNEST\(@uf\)/);
    assert.match(query, /\(TRANSLATE\(UPPER\(COALESCE\(JSON_VALUE\(e\.payload, '\$\.bairro'\), ''\)\), '[^']+', '[^']+'\) LIKE @bairro0\)/);
    assert.match(query, /JSON_VALUE\(e\.payload, '\$\.atividade_principal_cod'\) IN UNNEST\(@cnaes\)/);
});
test('só disponíveis cruza com receita.limpeza_api', () => {
    const { query, params } = buildBqQuery({ availability: 'available' });
    assert.match(query, /EXISTS \(SELECT 1 FROM `mbtech-bronze\.receita\.limpeza_api` a WHERE a\.cnpj = e\.cnpj AND a\.status = @disponivel\)/);
    assert.equal(param(params, 'disponivel'), 'disponivel');
    assert.doesNotMatch(buildBqQuery({}).query, /limpeza_api/);
});
test('seleciona todos os campos do gerador pelo payload', () => {
    const { query } = buildBqQuery({});
    for (const name of Object.keys(FIELD_ALIASES).filter(name => name !== 'capital_social')) assert.match(query, new RegExp(`JSON_VALUE\\(e\\.payload, '\\$\\.${name}'\\) AS ${name}[,\\s]`));
});
test('capital_social sai com 2 casas como o numeric(18,2) do Neon', () => {
    assert.match(buildBqQuery({}).query, /FORMAT\('%\.2f', SAFE_CAST\(JSON_VALUE\(e\.payload, '\$\.capital_social'\) AS NUMERIC\)\) AS capital_social,/);
});
test('filtros de MEI, telefone, e-mail e data geram os mesmos critérios do Neon', () => {
    const yes = buildBqQuery({ mei: 'yes', phone: 'with', email: 'without', dateFrom: '2020-01-01', dateTo: '2020-12-31' });
    assert.match(yes.query, /UPPER\(JSON_VALUE\(e\.payload, '\$\.opcao_mei'\)\) = @mei/);
    assert.equal(param(yes.params, 'mei'), 'S');
    assert.match(yes.query, /\(NULLIF\(TRIM\(JSON_VALUE\(e\.payload, '\$\.telefone_principal'\), ' '\), ''\) IS NOT NULL OR NULLIF\(TRIM\(JSON_VALUE\(e\.payload, '\$\.telefone_secundario'\), ' '\), ''\) IS NOT NULL\)/);
    assert.match(yes.query, /NOT \(NULLIF\(TRIM\(JSON_VALUE\(e\.payload, '\$\.email'\), ' '\), ''\) IS NOT NULL\)/);
    assert.match(yes.query, /SAFE_CAST\(JSON_VALUE\(e\.payload, '\$\.data_abertura'\) AS DATE\) >= @dateFrom/);
    assert.match(yes.query, /SAFE_CAST\(JSON_VALUE\(e\.payload, '\$\.data_abertura'\) AS DATE\) <= @dateTo/);
    assert.equal(param(yes.params, 'dateFrom'), '2020-01-01');
    assert.deepEqual(paramType(yes.params, 'dateTo'), { type: 'DATE' });
    const no = buildBqQuery({ mei: 'no', phone: 'without', email: 'with' });
    assert.match(no.query, /\(UPPER\(JSON_VALUE\(e\.payload, '\$\.opcao_mei'\)\) = @mei OR JSON_VALUE\(e\.payload, '\$\.opcao_mei'\) IS NULL\)/);
    assert.equal(param(no.params, 'mei'), 'N');
    assert.match(no.query, /NOT \(NULLIF\(TRIM\(JSON_VALUE\(e\.payload, '\$\.telefone_principal'\)/);
    assert.match(no.query, /AND \(NULLIF\(TRIM\(JSON_VALUE\(e\.payload, '\$\.email'\), ' '\), ''\) IS NOT NULL\)/);
    const all = buildBqQuery({});
    assert.doesNotMatch(all.query, /opcao_mei'\)\)|NULLIF|AS DATE\) [<>]/);
});
test('LIMIT só com limite, somando os já salvos, como parâmetro', () => {
    const { query, params } = buildBqQuery({ limit: 10 }, { savedCount: 5 });
    assert.match(query, / LIMIT @limit$/);
    assert.equal(param(params, 'limit'), '15');
    assert.deepEqual(paramType(params, 'limit'), { type: 'INT64' });
});

function neonPool(source) {
    const client = { connection: { stream: { pause() {}, resume() {}, destroy() {} } }, release() {}, query(query) {
        setImmediate(() => { for (const item of source) query.emit('row', item); query.emit('end', {}); });
    } };
    return { async connect() { return client; }, async query() {
        return { rows: Object.keys(FIELD_ALIASES).map(column_name => ({ column_name, data_type: 'text' })) };
    } };
}
test('CNPJ com zero à esquerda sai com 14 dígitos e mesma forma do gerador Neon', async () => {
    const full = Object.fromEntries(Object.keys(FIELD_ALIASES).map(name => [name, null]));
    const source = [
        { ...full, cnpj: '00012345000199', razao_social: 'Zero à Esquerda', situacao_cadastral_cod: '2', telefone_principal: '(11) 3333-4444', telefone_secundario: '11987654321', capital_social: '1000.00', natureza_juridica_cod: '2062', data_abertura: '2020-01-02', opcao_mei: 'S', ultima_atualizacao: '2026-10-14 07:52:17.877701' },
        { ...full, cnpj: '12345678000195', razao_social: 'Outra', situacao_cadastral_cod: '08', situacao_cadastral: 'Baixada por omissão' }
    ];
    const bq = fakeBq([source]);
    const fromBq = await drain(iterateReceitaBq({ bq, filters: {} }));
    const fromNeon = await drain(iterateReceitaFast({ pool: neonPool(source), filters: {} }));
    assert.deepEqual(fromBq, fromNeon);
    const [first, second] = fromBq[0].rows;
    assert.equal(first.cnpj, '00012345000199');
    assert.equal(first.situacao_cadastral_cod, '02');
    assert.equal(first.situacao_cadastral, 'Ativa');
    assert.equal(first.email, '');
    assert.equal(first.capital_social, '1000.00');
    assert.equal(first.ultima_atualizacao, '2026-10-14 07:52:17.877701');
    assert.equal(second.situacao_cadastral, 'Baixada por omissão');
    assert.deepEqual(Object.keys(first), Object.keys(FIELD_ALIASES));
    assert.equal(bq.calls[0].maxBytesBilled, '80000000000');
    assert.equal(bq.calls[0].jobTimeoutMs, '900000');
    assert.equal(bq.calls[0].waitMs, 900000);
    assert.equal(bq.calls[0].pageSize, 50000);
});
test('retomada pula CNPJs já salvos e respeita o limite restante', async () => {
    const bq = fakeBq([[row(9), row(1)], [row(7), row(3), row(2)]]);
    const batches = await drain(iterateReceitaBq({ bq, filters: { limit: 2 }, savedCount: 2, isSaved: cnpj => [row(9).cnpj, row(3).cnpj].includes(cnpj), batchSize: 1 }));
    assert.deepEqual(batches.map(b => b.rows.map(r => r.cnpj)), [[row(1).cnpj], [row(7).cnpj]]);
    assert.equal(param(bq.calls[0].params, 'limit'), '4');
    assert.equal(bq.calls[0].signal.aborted, true, 'interrompe a leitura ao atingir o limite');
});
test('páginas viram lotes de batchSize sem esperar o resultado inteiro', async () => {
    let delivered = 0;
    const pages = [[row(1), row(2), row(3)], [row(4), row(5)], [row(6)]];
    const bq = { async queryPages(options) { for (const page of pages) { await options.onPage(page); delivered++; } return {}; } };
    const iterator = iterateReceitaBq({ bq, filters: {}, batchSize: 2 });
    const first = await iterator.next();
    assert.deepEqual(first.value.rows.map(r => r.cnpj), [row(1).cnpj, row(2).cnpj]);
    assert.ok(delivered <= 1, 'não lê páginas além da próxima');
    const rest = [];
    for (let next = await iterator.next(); !next.done; next = await iterator.next()) rest.push(next.value.rows.map(r => r.cnpj));
    assert.deepEqual(rest, [[row(3).cnpj, row(4).cnpj], [row(5).cnpj, row(6).cnpj]]);
});
test('CNPJ inválido no BigQuery é recusado', async () => {
    await assert.rejects(drain(iterateReceitaBq({ bq: fakeBq([[{ cnpj: '123', razao_social: 'x', situacao_cadastral_cod: '02' }]]), filters: {} })), { code: 'FLOW_VALIDATION', message: /CNPJ inválido/ });
});
test('erro de acesso mantém a mensagem; outros erros viram mensagem amigável', async () => {
    const forbidden = Object.assign(new Error('Sem acesso à Receita no BigQuery.'), { code: 'FLOW_VALIDATION' });
    await assert.rejects(drain(iterateReceitaBq({ bq: fakeBq([], { fail: forbidden }), filters: {} })), forbidden);
    await assert.rejects(drain(iterateReceitaBq({ bq: fakeBq([], { fail: new Error('private detail') }), filters: {} })), { code: 'FLOW_VALIDATION', message: 'Não foi possível ler a base Receita no BigQuery. Retome do último lote salvo.' });
    await assert.rejects(drain(iterateReceitaBq({ bq: null, filters: {} })), { code: 'FLOW_VALIDATION' });
});
test('cancelamento interrompe a leitura', async () => {
    const controller = new AbortController();
    const bq = fakeBq([[row(1), row(2)], [row(3), row(4)]]);
    const iterator = iterateReceitaBq({ bq, filters: {}, batchSize: 2, signal: controller.signal });
    assert.equal((await iterator.next()).value.rows.length, 2);
    controller.abort();
    await assert.rejects(iterator.next(), { name: 'AbortError' });
    assert.equal(bq.calls[0].signal.aborted, true);
});
test('login Google expirado chega ao fluxo com o código para renovar', async () => {
    const expired = Object.assign(new Error('Seu login Google precisa ser renovado.'), { code: 'BQ_AUTH_REQUIRED' });
    await assert.rejects(drain(iterateReceitaBq({ bq: fakeBq([], { fail: expired }), filters: {} })), { code: 'BQ_AUTH_REQUIRED', message: expired.message });
});
test('limite de bytes do BigQuery vira mensagem própria (FLOW_VALIDATION)', async () => {
    const limit = Object.assign(new Error('limite'), { code: 'BQ_BYTES_LIMIT' });
    await assert.rejects(drain(iterateReceitaBq({ bq: fakeBq([], { fail: limit }), filters: {} })), { code: 'FLOW_VALIDATION', message: 'A consulta da Receita no BigQuery passou do limite de custo configurado (80 GB). Restrinja os filtros ou use a fonte Neon.' });
});
test('com campos pedidos, seleciona só eles e sempre os obrigatórios; sem campos, todos', () => {
    const { query } = buildBqQuery({}, { fields: ['data_abertura', 'telefone_principal'] });
    const selected = [...query.split(' FROM ')[0].matchAll(/ AS ([a-z_]+)/g)].map(match => match[1]);
    assert.deepEqual(selected, ['cnpj', 'razao_social', 'situacao_cadastral_cod', 'situacao_cadastral', 'telefone_principal', 'data_abertura']);
    // Filters still read their own fields, whatever is selected.
    assert.match(query, /natureza_juridica_cod'\) IN UNNEST\(@naturezas\)/);
    assert.equal([...buildBqQuery({}, { fields: null }).query.split(' FROM ')[0].matchAll(/ AS ([a-z_]+)/g)].length, Object.keys(FIELD_ALIASES).length);
    assert.throws(() => buildBqQuery({}, { fields: ['payload'] }), /Campos Receita inválidos/);
});
