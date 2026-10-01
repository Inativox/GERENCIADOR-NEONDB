'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { createHash } = require('node:crypto');
const { createUserStore } = require('../src/main/flows/store');

const hash = value => createHash('sha256').update(value).digest('hex');
const recordPath = (store, id) => path.join(store.directory, 'job-records', `${hash(id)}.json`);
function fixture(t, username = 'Davi') {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'flow-store-'));
    t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
    const store = createUserStore(directory, username);
    return { directory, store, index: path.join(store.directory, 'history.json') };
}
function job(id, extra = {}) {
    return { id, owner: 'Davi', status: 'running', createdAt: '2026-09-30T12:00:00.000Z', logs: ['Started'], counts: { generated: 0 }, flowSnapshot: { generation: { limit: 1000 } }, ...extra };
}

test('job updates rewrite only their individual record, leaving the small index and other logs untouched', t => {
    const { store, index } = fixture(t);
    store.saveFlow({ id: 'flow', name: 'Fixture' });
    store.saveJob(job('one', { logs: ['first '.repeat(20000)] }));
    store.saveJob(job('two', { logs: ['second '.repeat(20000)] }));
    const beforeIndex = fs.readFileSync(index, 'utf8');
    const secondRecord = recordPath(store, 'two');
    const beforeSecond = fs.readFileSync(secondRecord, 'utf8');
    const fixedDate = new Date('2000-01-01T00:00:00Z');
    fs.utimesSync(index, fixedDate, fixedDate);
    fs.utimesSync(secondRecord, fixedDate, fixedDate);
    const indexTime = fs.statSync(index).mtimeMs, secondTime = fs.statSync(secondRecord).mtimeMs;
    store.saveJob(job('one', { logs: ['updated'], counts: { generated: 2000 } }));
    assert.equal(fs.readFileSync(index, 'utf8'), beforeIndex);
    assert.equal(fs.statSync(index).mtimeMs, indexTime);
    assert.equal(fs.readFileSync(secondRecord, 'utf8'), beforeSecond);
    assert.equal(fs.statSync(secondRecord).mtimeMs, secondTime);
    assert.equal(store.getJob('one').counts.generated, 2000);
    const savedIndex = JSON.parse(beforeIndex);
    assert.equal(savedIndex.version, 2);
    assert.deepEqual(savedIndex.jobs, [
        { id: 'one', owner: 'Davi', createdAt: '2026-09-30T12:00:00.000Z' },
        { id: 'two', owner: 'Davi', createdAt: '2026-09-30T12:00:00.000Z' }
    ]);
    assert.ok(Buffer.byteLength(beforeIndex) < 1000);
    assert.doesNotMatch(beforeIndex, /logs|flowSnapshot|generated/);
});

test('v1 migration retains every full job, flows and the original backup', t => {
    const { directory, store, index } = fixture(t);
    const originals = [job('old-one', { status: 'failed', logs: ['legacy '.repeat(20000)] }), job('old-two', { outputs: [{ path: 'C:/synthetic.xlsx' }] }), job('foreign', { owner: 'Outro' })];
    const legacy = JSON.stringify({ version: 1, flows: [{ id: 'flow', name: 'Original' }], jobs: originals });
    fs.writeFileSync(index, legacy);
    const migrated = createUserStore(directory, 'Davi');
    assert.equal(JSON.parse(fs.readFileSync(index, 'utf8')).version, 2);
    assert.equal(fs.readFileSync(path.join(store.directory, 'history.v1.backup.json'), 'utf8'), legacy);
    assert.deepEqual(migrated.getJob('old-one'), originals[0]);
    assert.deepEqual(migrated.getJob('old-two'), originals[1]);
    assert.deepEqual(JSON.parse(fs.readFileSync(recordPath(store, 'foreign'), 'utf8')), originals[2]);
    assert.equal(migrated.getJob('foreign'), undefined);
    assert.deepEqual(migrated.listJobs().map(item => item.id), ['old-two', 'old-one']);
    assert.deepEqual(migrated.listFlows(), [{ id: 'flow', name: 'Original' }]);
    assert.deepEqual(createUserStore(directory, 'Davi').getJob('old-one'), originals[0]);
});

test('interrupted migration preserves v1 and can safely retry without losing records', t => {
    const { directory, store, index } = fixture(t);
    const legacy = JSON.stringify({ version: 1, flows: [], jobs: [job('a'), job('b')] });
    fs.writeFileSync(index, legacy);
    const block = recordPath(store, 'b') + '.tmp';
    fs.mkdirSync(block);
    assert.throws(() => createUserStore(directory, 'Davi'));
    assert.equal(fs.readFileSync(index, 'utf8'), legacy);
    assert.equal(fs.readFileSync(path.join(store.directory, 'history.v1.backup.json'), 'utf8'), legacy);
    fs.rmdirSync(block);
    const retried = createUserStore(directory, 'Davi');
    assert.deepEqual(retried.listJobs().map(item => item.id), ['b', 'a']);
    assert.deepEqual(retried.getJob('a'), job('a'));
});

test('failed job/flow writes preserve prior records and never publish a new indexed job', t => {
    const { directory, store, index } = fixture(t);
    store.saveFlow({ id: 'flow', name: 'Before' });
    store.saveJob(job('existing'));
    const record = recordPath(store, 'existing');
    const before = fs.readFileSync(record, 'utf8');
    fs.mkdirSync(record + '.tmp');
    assert.throws(() => store.saveJob(job('existing', { status: 'completed', logs: ['after'] })));
    assert.equal(fs.readFileSync(record, 'utf8'), before);
    assert.equal(store.getJob('existing').status, 'running');
    fs.rmdirSync(record + '.tmp');
    const blockedIndex = index + '.tmp';
    fs.mkdirSync(blockedIndex);
    assert.throws(() => store.saveJob(job('new-job')));
    assert.equal(store.getJob('new-job'), undefined);
    assert.equal(fs.existsSync(recordPath(store, 'new-job')), false);
    assert.throws(() => store.saveFlow({ id: 'flow', name: 'Rejected' }));
    assert.equal(store.getFlow('flow').name, 'Before');
    assert.throws(() => store.deleteFlow('flow'));
    assert.equal(store.getFlow('flow').name, 'Before');
    // Existing job writes intentionally do not touch the unavailable index.
    store.saveJob(job('existing', { status: 'failed' }));
    assert.equal(store.getJob('existing').status, 'failed');
    fs.rmdirSync(blockedIndex);
    const reopened = createUserStore(directory, 'Davi');
    assert.equal(reopened.getJob('existing').status, 'failed');
    assert.equal(reopened.getJob('new-job'), undefined);
});

test('all getters and save returns are detached; records are isolated by authenticated owner', t => {
    const { directory, store } = fixture(t);
    const input = job('../traversal', { logs: ['original'] });
    const returned = store.saveJob(input);
    input.logs.push('input mutation'); returned.logs.push('return mutation');
    const read = store.getJob(input.id); read.logs.push('getter mutation');
    const listed = store.listJobs(); listed[0].logs.push('list mutation');
    assert.deepEqual(store.getJob(input.id).logs, ['original']);
    const savedFlow = store.saveFlow({ id: 'flow', name: 'Before' }); savedFlow.name = 'return mutation';
    const flows = store.listFlows(); flows[0].name = 'list mutation';
    assert.equal(store.getFlow('flow').name, 'Before');
    const other = createUserStore(directory, '../Outro');
    assert.equal(other.getJob(input.id), undefined);
    assert.deepEqual(other.listJobs(), []);
    assert.throws(() => other.saveJob(job('foreign')), /outro usuário/);
    assert.equal(path.dirname(other.directory), directory);
    assert.equal(path.dirname(recordPath(store, input.id)), path.join(store.directory, 'job-records'));
});

test('UI history returns the most recent 200 jobs with at most 64KiB of valid UTF-8 logs per job', t => {
    const { store } = fixture(t);
    for (let index = 0; index < 205; index++) store.saveJob(job(`job-${index}`));
    const hugeLogs = Array.from({ length: 300 }, (_, index) => `${index}: ${'😀'.repeat(500)}`);
    store.saveJob(job('job-204', { logs: hugeLogs }));
    const jobs = store.listJobs();
    assert.equal(jobs.length, 200);
    assert.equal(jobs[0].id, 'job-204');
    assert.equal(jobs.at(-1).id, 'job-5');
    assert.equal(jobs[0].logsTruncated, true);
    assert.ok(jobs[0].logs.reduce((sum, log) => sum + Buffer.byteLength(log, 'utf8'), 0) <= 64 * 1024);
    assert.ok(jobs[0].logs.every(log => !log.includes('\ufffd')));
    assert.equal(jobs[0].logs.at(-1), hugeLogs.at(-1));
    assert.deepEqual(store.getJob('job-204').logs, hugeLogs);
    store.saveJob(job('job-204', { logs: ['😀'.repeat(20000)] }));
    const single = store.listJobs()[0];
    assert.ok(Buffer.byteLength(single.logs[0], 'utf8') <= 64 * 1024);
    assert.ok(!single.logs[0].includes('\ufffd'));
});

test('missing or mismatched indexed records fail explicitly instead of silently dropping history', t => {
    const { store } = fixture(t);
    store.saveJob(job('one'));
    fs.writeFileSync(recordPath(store, 'one'), JSON.stringify(job('one', { owner: 'Other' })));
    assert.throws(() => store.getJob('one'), /Histórico de fluxos inválido/);
    fs.unlinkSync(recordPath(store, 'one'));
    assert.throws(() => store.listJobs(), /Registro de execução indisponível/);
});
