const test = require('node:test');
const assert = require('node:assert/strict');
const { createBqClient } = require('../src/main/flows/bq');

function fixture(responder) {
    const requests = [];
    const client = createBqClient({
        credentialsProvider: () => ({ type: 'authorized_user', client_id: 's', client_secret: 's', refresh_token: 's' }),
        http: {
            post: async () => ({ data: { access_token: 'synthetic-token', expires_in: 3600 } }),
            request: async request => { requests.push(request); return { data: await responder(request, requests.length) }; }
        }
    });
    return { client, requests };
}
const schema = { fields: [{ name: 'cnpj' }, { name: 'nome' }] };
const params = [{ name: 'uf', parameterType: { type: 'STRING' }, parameterValue: { value: 'SP' } }];

test('queryPages converte linhas por schema, chama onPage em ordem e usa parâmetros nomeados', async () => {
    const f = fixture((request, n) => {
        if (n === 1) return { jobComplete: true, jobReference: { jobId: 'j1' }, schema, totalBytesProcessed: '99', pageToken: 'p2', rows: [{ f: [{ v: '1' }, { v: null }] }] };
        return { jobComplete: true, schema, rows: [{ f: [{ v: '2' }, { v: 'B' }] }] };
    });
    const pages = [];
    const result = await f.client.queryPages({ query: 'SELECT 1 WHERE uf=@uf', params, pageSize: 1, onPage: async rows => { await new Promise(r => setTimeout(r, 5)); pages.push(rows); } });
    assert.deepEqual(pages, [[{ cnpj: '1', nome: null }], [{ cnpj: '2', nome: 'B' }]]);
    assert.deepEqual(result, { rows: 2, jobId: 'j1', bytes: 99 });
    assert.equal(f.requests[0].data.parameterMode, 'NAMED');
    assert.deepEqual(f.requests[0].data.queryParameters, params);
    assert.equal(f.requests[0].data.query, 'SELECT 1 WHERE uf=@uf');
    assert.equal(f.requests[1].params.pageToken, 'p2');
});

test('queryPages cancela o job no abort', async () => {
    const controller = new AbortController();
    const f = fixture(request => request.method === 'POST' && request.url.endsWith('/cancel') ? {} : { jobComplete: true, jobReference: { jobId: 'j2' }, schema, pageToken: 'p2', rows: [{ f: [{ v: '1' }, { v: 'A' }] }] });
    await assert.rejects(f.client.queryPages({ query: 'q', signal: controller.signal, onPage: async () => controller.abort() }), /cancelada/);
    assert.ok(f.requests.some(r => r.method === 'POST' && r.url.endsWith('/jobs/j2/cancel') && r.params.location));
});

test('queryPages traduz 403 em FLOW_VALIDATION', async () => {
    const f = fixture(() => { throw { response: { status: 403 } }; });
    await assert.rejects(f.client.queryPages({ query: 'q' }), error => error.code === 'FLOW_VALIDATION' && error.message === 'Sem acesso à Receita no BigQuery. Use a fonte Neon neste fluxo ou peça acesso ao dataset receita.');
});

test('queryPages usa limites padrão e aceita maxBytesBilled, jobTimeoutMs e waitMs', async () => {
    const done = { jobComplete: true, jobReference: { jobId: 'j3' }, schema, rows: [] };
    const d = fixture(() => done);
    await d.client.queryPages({ query: 'q' });
    assert.equal(d.requests[0].data.maximumBytesBilled, '1000000000');
    assert.equal(d.requests[0].data.jobTimeoutMs, '180000');
    const c = fixture(() => done);
    await c.client.queryPages({ query: 'q', maxBytesBilled: '40000000000', jobTimeoutMs: '900000' });
    assert.equal(c.requests[0].data.maximumBytesBilled, '40000000000');
    assert.equal(c.requests[0].data.jobTimeoutMs, '900000');
});

test('queryPages respeita waitMs na espera local', async () => {
    const f = fixture(request => request.method === 'POST' && request.url.endsWith('/cancel') ? {} : { jobComplete: false, jobReference: { jobId: 'j4' } });
    await assert.rejects(f.client.queryPages({ query: 'q', waitMs: -1 }), /demorou/);
});

function retryFixture(responder) {
    const requests = [], sleeps = [];
    const client = createBqClient({
        credentialsProvider: () => ({ type: 'authorized_user', client_id: 's', client_secret: 's', refresh_token: 's' }),
        retryDelaysMs: [1, 2],
        sleep: async ms => { sleeps.push(ms); },
        http: {
            post: async () => ({ data: { access_token: 'synthetic-token', expires_in: 3600 } }),
            request: async request => { requests.push(request); return { data: await responder(request, requests.length) }; }
        }
    });
    return { client, requests, sleeps };
}
const firstPage = { jobComplete: true, jobReference: { jobId: 'r1' }, schema, pageToken: 'p2', rows: [{ f: [{ v: '1' }, { v: 'A' }] }] };
const lastPage = { jobComplete: true, schema, rows: [{ f: [{ v: '2' }, { v: 'B' }] }] };
const isPageGet = request => request.method === 'GET' && request.params?.pageToken === 'p2';

for (const [label, failure] of [['5xx', { response: { status: 503 } }], ['rede', { code: 'ECONNRESET' }], ['timeout', { code: 'ECONNABORTED' }]]) {
    test(`queryPages retenta a página de resultado em ${label} e completa`, async () => {
        let pageGets = 0;
        const f = retryFixture(request => {
            if (request.method === 'POST' && !request.url.endsWith('/cancel')) return firstPage;
            if (isPageGet(request) && ++pageGets <= 2) throw failure;
            return lastPage;
        });
        const pages = [];
        const result = await f.client.queryPages({ query: 'q', onPage: async rows => { pages.push(rows); } });
        assert.equal(result.rows, 2);
        assert.equal(pageGets, 3);
        assert.deepEqual(f.sleeps, [1, 2]);
        assert.deepEqual(pages[1], [{ cnpj: '2', nome: 'B' }]);
    });
}

test('queryPages desiste da página depois de 3 tentativas e cancela o job', async () => {
    let pageGets = 0;
    const f = retryFixture(request => {
        if (request.method === 'POST' && request.url.endsWith('/cancel')) return {};
        if (request.method === 'POST') return firstPage;
        pageGets++; throw { response: { status: 500 } };
    });
    await assert.rejects(f.client.queryPages({ query: 'q' }), /Falha ao consultar o BQ/);
    assert.equal(pageGets, 3);
    assert.ok(f.requests.some(r => r.url.endsWith('/jobs/r1/cancel')));
});

test('queryPages também retenta a espera do job (jobComplete=false)', async () => {
    let gets = 0;
    const f = retryFixture(request => {
        if (request.method === 'POST') return { jobComplete: false, jobReference: { jobId: 'r2' } };
        if (++gets === 1) throw { response: { status: 502 } };
        return lastPage;
    });
    const result = await f.client.queryPages({ query: 'q' });
    assert.equal(result.rows, 1);
    assert.equal(gets, 2);
});

for (const status of [400, 403, 401]) {
    test(`queryPages não retenta página com ${status}`, async () => {
        let pageGets = 0;
        const f = retryFixture(request => {
            if (request.method === 'POST' && request.url.endsWith('/cancel')) return {};
            if (request.method === 'POST') return firstPage;
            pageGets++; throw { response: { status } };
        });
        await assert.rejects(f.client.queryPages({ query: 'q' }));
        assert.equal(f.sleeps.length, 0);
        assert.equal(pageGets, status === 401 ? 2 : 1); // 401: one token refresh, no backoff retry
    });
}

test('queryPages não retenta depois do abort', async () => {
    const controller = new AbortController();
    let pageGets = 0;
    const f = retryFixture(request => {
        if (request.method === 'POST' && request.url.endsWith('/cancel')) return {};
        if (request.method === 'POST') return firstPage;
        pageGets++; controller.abort(); throw { code: 'ERR_CANCELED' };
    });
    await assert.rejects(f.client.queryPages({ query: 'q', signal: controller.signal }), /cancelada/);
    assert.equal(pageGets, 1);
    assert.equal(f.sleeps.length, 0);
});

test('queryPages marca bytesBilledLimitExceeded com BQ_BYTES_LIMIT', async () => {
    const f = retryFixture(() => { throw { response: { status: 400, data: { error: { errors: [{ reason: 'bytesBilledLimitExceeded' }] } } } }; });
    await assert.rejects(f.client.queryPages({ query: 'q' }), { code: 'BQ_BYTES_LIMIT' });
    assert.equal(f.sleeps.length, 0);
    const g = retryFixture(request => request.method === 'POST' && request.url.endsWith('/cancel') ? {} : { jobComplete: true, jobReference: { jobId: 'r3' }, errors: [{ reason: 'bytesBilledLimitExceeded' }] });
    await assert.rejects(g.client.queryPages({ query: 'q' }), { code: 'BQ_BYTES_LIMIT' });
});

test('queryPages abre uma sessão quando pedido, devolve o id e as consultas seguintes vão na mesma sessão', async () => {
    const f = fixture(() => ({ jobComplete: true, jobReference: { jobId: 'j3' }, sessionInfo: { sessionId: 'sessao-sintetica' }, schema, rows: [] }));
    const opened = await f.client.queryPages({ query: 'CREATE TEMP TABLE lista (idx INT64, cnpj STRING)', createSession: true });
    assert.equal(opened.session, 'sessao-sintetica');
    assert.equal(f.requests[0].data.createSession, true); assert.equal(f.requests[0].data.connectionProperties, undefined);
    await f.client.queryPages({ query: 'SELECT 1', session: opened.session });
    assert.deepEqual(f.requests[1].data.connectionProperties, [{ key: 'session_id', value: 'sessao-sintetica' }]);
    assert.equal(f.requests[1].data.createSession, undefined);
});

test('abortSession encerra a sessão pelo BQ.ABORT_SESSION e não falha se o BigQuery não responder', async () => {
    const f = fixture(() => ({ jobComplete: true }));
    await f.client.abortSession('sessao-sintetica');
    assert.equal(f.requests[0].data.query, 'CALL BQ.ABORT_SESSION()');
    assert.deepEqual(f.requests[0].data.connectionProperties, [{ key: 'session_id', value: 'sessao-sintetica' }]);
    const failing = fixture(() => { throw { response: { status: 500 } }; });
    await failing.client.abortSession('sessao-sintetica');
});
