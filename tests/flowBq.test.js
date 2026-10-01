const test = require('node:test');
const assert = require('node:assert/strict');
const { createBqClient, restoreCnpj, ROOT_SQL } = require('../src/main/flows/bq');
function fixture(responder) {
    const requests = [], tokens = [];
    const client = createBqClient({ credentialsProvider: () => ({ type: 'authorized_user', client_id: 'synthetic', client_secret: 'synthetic', refresh_token: 'synthetic' }),
        http: { post: async (...args) => { tokens.push(args); return { data: { access_token: 'synthetic-token', expires_in: 3600 } }; }, request: async request => { requests.push(request); return { data: await responder(request, requests.length) }; } } });
    return { client, requests, tokens };
}
const row = value => ({ f: [{ v: value }] });
test('historic SQL includes every available membership, parameterizes pipelines and restores only verifiable lost zeros', () => {
    assert.match(ROOT_SQL, /UNION DISTINCT/); assert.match(ROOT_SQL, /stage_history/); assert.match(ROOT_SQL, /JOIN versions/);
    assert.ok(!ROOT_SQL.includes('deals_latest')); assert.ok(!ROOT_SQL.includes('\\`'));
    assert.equal(restoreCnpj('04.252.011/0001-10'), '04252011000110');
    assert.equal(restoreCnpj('4252011000110'), '04252011000110'); assert.equal(restoreCnpj('1234567890123'), ''); assert.equal(restoreCnpj('11111111111111'), '');
    assert.equal(restoreCnpj('0000000000000'), '');
});
test('BQ dry run, asynchronous completion, pagination and OAuth cache yield one normalized root', async () => {
    const f = fixture((request, n) => {
        if (n === 1) return { totalBytesProcessed: '200' };
        if (n === 2) return { jobComplete: false, jobReference: { jobId: 'synthetic' } };
        if (n === 3) return { jobComplete: true, pageToken: 'next', rows: [row('4252011000110'), row('11111111111111')] };
        return { jobComplete: true, rows: [row('04252011000110'), row('12345678000190')] };
    });
    const root = await f.client.loadRoot([90, 119]); assert.deepEqual(root.documents, ['04252011000110', '12345678000190']);
    assert.equal(root.info.count, 2); assert.equal(root.info.restored, 1); assert.equal(root.info.skipped, 1); assert.equal(root.info.estimatedBytes, 200);
    assert.equal(f.tokens.length, 1); assert.equal(f.requests[0].data.dryRun, true); assert.equal(f.requests[1].data.maximumBytesBilled, '1000000000');
    assert.deepEqual(f.requests[1].data.queryParameters[0].parameterValue.arrayValues, [{ value: '90' }, { value: '119' }]);
    assert.equal(f.requests[3].params.pageToken, 'next');
});
test('BQ rejects empty, over-budget, malformed pipelines and private provider failures', async () => {
    const f = fixture((request, n) => n === 1 ? { totalBytesProcessed: '1' } : { jobComplete: true, rows: [] });
    await assert.rejects(f.client.loadRoot([90]), /não retornou/);
    await assert.rejects(f.client.loadRoot(['90']), /Pipelines/);
    const expensive = fixture(() => ({ totalBytesProcessed: '1000000001' })); await assert.rejects(expensive.client.loadRoot([90]), /1 GB/); assert.equal(expensive.requests.length, 1);
    const privateError = createBqClient({ credentialsProvider: () => { throw new Error('sensitive private content'); } });
    await assert.rejects(privateError.test(), error => !error.message.includes('sensitive') && /Acesso/.test(error.message));
});
test('BQ cancellation requests server job cancellation and never returns partial root', async () => {
    const controller = new AbortController();
    const f = fixture((request, n) => { if (n === 1) return { totalBytesProcessed: '1' }; if (n === 2) { controller.abort(); return { jobComplete: false, jobReference: { jobId: 'synthetic' } }; } return {}; });
    await assert.rejects(f.client.loadRoot([90], { signal: controller.signal }), /cancelada/);
    assert.ok(f.requests[2].url.endsWith('/jobs/synthetic/cancel'));
    assert.equal(f.requests[2].data, undefined); assert.equal(f.requests[2].params.location, 'southamerica-east1');
});
test('BQ abort during poll still cancels remote job with region and no aborted signal', async () => {
    const controller = new AbortController();
    const f = fixture((request, n) => {
        if (n === 1) return { totalBytesProcessed: '1' };
        if (n === 2) return { jobComplete: false, jobReference: { jobId: 'polling' } };
        if (n === 3) { controller.abort(); throw new Error('cancelled network'); }
        return {};
    });
    await assert.rejects(f.client.loadRoot([90], { signal: controller.signal }), /cancelada/);
    assert.equal(f.requests.length, 4); assert.ok(f.requests[3].url.endsWith('/jobs/polling/cancel'));
    assert.equal(f.requests[3].signal, undefined);
});

test('expired access token refreshes once without requiring a browser login', async () => {
    let calls = 0;
    const f = fixture(() => { if (++calls === 1) throw { response: { status: 401 } }; return {}; });
    await f.client.test();
    assert.equal(f.tokens.length, 2);
    assert.equal(calls, 4);
});
test('persistent 401 requires human renewal but permission denial never does', async () => {
    const expired = fixture(() => { throw { response: { status: 401 } }; });
    await assert.rejects(expired.client.test(), error => error.code === 'BQ_AUTH_REQUIRED');
    assert.equal(expired.tokens.length, 2);
    assert.equal(expired.requests.length, 2);
    const forbidden = fixture(() => { throw { response: { status: 403, data: { secret: 'private response' } } }; });
    await assert.rejects(forbidden.client.test(), error => error.code === 'BQ_FORBIDDEN' && !error.message.includes('private'));
    assert.equal(forbidden.tokens.length, 1);
});
test('revoked refresh token requests renewal; network error exposes no secret and never opens login', async () => {
    const make = error => createBqClient({ credentialsProvider: () => ({ type: 'authorized_user' }), http: { post: async () => { throw error; } } });
    await assert.rejects(make({ response: { data: { error: 'invalid_grant', error_description: 'secret' } } }).test(), error => error.code === 'BQ_AUTH_REQUIRED' && !error.message.includes('secret'));
    await assert.rejects(make({ message: 'private token', response: { status: 503 } }).test(), error => !error.code && !error.message.includes('private'));
});
test('revoked service account asks for replacement, never a human Google login', async () => {
    const crypto = require('node:crypto');
    const key = crypto.generateKeyPairSync('rsa', { modulusLength: 1024 }).privateKey.export({ type: 'pkcs8', format: 'pem' });
    const client = createBqClient({ credentialsProvider: () => ({ type: 'service_account', client_email: 'synthetic@example.test', private_key: key }), http: { post: async () => { throw { response: { data: { error: 'invalid_grant' } } }; } } });
    await assert.rejects(client.test(), error => error.code === 'BQ_CREDENTIAL_INVALID');
});
