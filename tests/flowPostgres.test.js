const test = require('node:test');
const assert = require('node:assert/strict');
const { readOnlyPoolOptions } = require('../src/main/flows/postgres');
test('Neon pooled flow connection uses same direct endpoint and retains credentials, database and TLS', () => {
    // Synthetic URL for a pure transformation test; no database is contacted.
    const endpoint = new URL('postgresql://ep-synthetic-pooler.sa-east-1.aws.neon.tech/receita');
    endpoint.username = 'fixture'; endpoint.password = 'p@ss';
    endpoint.searchParams.set('sslmode', 'require'); endpoint.searchParams.set('channel_binding', 'require');
    const uri = endpoint.toString();
    const result = readOnlyPoolOptions(uri), url = new URL(result.connectionString);
    assert.equal(url.hostname, 'ep-synthetic.sa-east-1.aws.neon.tech');
    assert.equal(url.username, 'fixture'); assert.equal(url.password, 'p%40ss'); assert.equal(url.pathname, '/receita');
    assert.equal(url.searchParams.get('sslmode'), 'require'); assert.equal(url.searchParams.get('channel_binding'), 'require');
    assert.equal(result.max, 2); assert.equal(result.statement_timeout, 60000);
    assert.match(result.options, /default_transaction_read_only=on -c statement_timeout=60000$/);
});
test('URL startup parameters cannot override readonly and timeout; original object stays unchanged', () => {
    const source = { connectionString: 'postgresql://fixture@localhost/source?options=-c%20default_transaction_read_only%3Doff&statement_timeout=0', application_name: 'flow-fixture', max: 100 };
    const result = readOnlyPoolOptions(source, { max: 1, timeout: 15000, connectionTimeout: 10000 });
    assert.equal(new URL(result.connectionString).hostname, 'localhost');
    assert.equal(new URL(result.connectionString).searchParams.has('options'), false);
    assert.equal(new URL(result.connectionString).searchParams.has('statement_timeout'), false);
    assert.match(result.options, /off -c default_transaction_read_only=on -c statement_timeout=15000$/);
    assert.equal(result.statement_timeout, 15000); assert.equal(result.connectionTimeoutMillis, 10000); assert.equal(result.max, 1);
    assert.equal(source.max, 100); assert.equal(result.application_name, 'flow-fixture');
});
test('non-Neon hosts with pooler names are never redirected', () => {
    const uri = 'postgresql://fixture@postgres-pooler.example.test/receita?sslmode=require';
    assert.equal(new URL(readOnlyPoolOptions(uri).connectionString).hostname, 'postgres-pooler.example.test');
});

test('disabled query timeout overrides inherited server and client limits without changing the source', () => {
    const ConnectionParameters = require('pg/lib/connection-parameters');
    const source = { connectionString: 'postgresql://fixture@localhost/source?statement_timeout=60000&query_timeout=30000&options=-c%20statement_timeout%3D60000', query_timeout: 15000 };
    const result = readOnlyPoolOptions(source, { timeout: 0 });
    const effective = new ConnectionParameters(result);
    assert.ok(!effective.statement_timeout);
    assert.ok(!effective.query_timeout);
    assert.match(effective.options, /default_transaction_read_only=on -c statement_timeout=0$/);
    assert.equal(new URL(result.connectionString).searchParams.has('query_timeout'), false);
    assert.equal(result.connectionTimeoutMillis, 15000);
    assert.equal(source.query_timeout, 15000);
    assert.equal(new URL(source.connectionString).searchParams.get('query_timeout'), '30000');
});
