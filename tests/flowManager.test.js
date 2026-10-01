const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { EventEmitter } = require('node:events');
const { defaults, validateFlow, effectiveFlow } = require('../src/main/flows/config');
const { createUserStore } = require('../src/main/flows/store');
const { createFlowManager } = require('../src/main/flows/manager');
const tick = () => new Promise(resolve => setTimeout(resolve, 5));
async function idle(manager) { for (let n = 0; n < 100; n++) { if (!manager.isBusy()) return; await tick(); } throw new Error('Manager stayed busy'); }
function fixture(t, extra = {}) {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'flow-manager-'));
    t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
    let user = { username: 'Davi', role: 'admin' }, workers = [];
    const manager = createFlowManager({ baseDirectory: directory, getUser: () => user,
        resolveConnections: async () => ({ receita: 'synthetic', enrichment: 'synthetic' }),
        resolveRoot: async () => ({ documents: ['04252011000110'], info: { source: 'bq', count: 1, queriedAt: 'synthetic' } }),
        workerFactory: data => { const worker = new EventEmitter(); worker.data = data; worker.messages = []; worker.postMessage = value => worker.messages.push(value); worker.terminate = async () => { worker.emit('exit', 1); }; workers.push(worker); return worker; }, ...extra });
    return { manager, directory, workers, setUser: value => { user = value; } };
}
test('configuration validates bounds/dates and snapshot enforces authenticated blocklist, CPF and situation', () => {
    const input = defaults(); input.enrichment = { enabled: true, strategy: 'append', fillCpf: true }; input.cleaning.blocklist = false;
    const flow = validateFlow(input);
    const other = effectiveFlow(flow, { username: 'Outro' });
    assert.equal(other.cleaning.blocklist, true);
    assert.equal(effectiveFlow(flow, { username: 'Davi' }).cleaning.blocklist, false);
    assert.equal(effectiveFlow(flow, { username: 'davi' }).cleaning.blocklist, true);
    assert.ok(other.output.formatSnapshot.colunas.some(column => column.header === 'livre6'));
    assert.ok(!other.output.formatSnapshot.colunas.some(column => column.campo === 'situacao_cadastral_cod'));
    other.generation.situacoes.push('04'); assert.deepEqual(flow.generation.situacoes, ['02']);
    assert.equal(validateFlow({ ...input, generation: { ...input.generation, limit: 500001 } }).generation.limit, 500001);
    assert.equal(validateFlow({ ...input, generation: { ...input.generation, limit: null } }).generation.limit, null);
    assert.throws(() => validateFlow({ ...input, generation: { ...input.generation, limit: 0 } }), /inteiro/);
    assert.throws(() => validateFlow({ ...input, generation: { ...input.generation, dateFrom: '2026-02-30' } }), /Data inválida/);
    assert.throws(() => validateFlow({ ...input, generation: { ...input.generation, dateFrom: '2026-99-99' } }), /Data inválida/);
    assert.throws(() => validateFlow({ ...input, generation: { ...input.generation, situacoes: [] } }), /situação/);
    assert.throws(() => validateFlow({ ...input, pipelines: ['90'] }), /inteiro/);
});

test('final API is optional per C6 flow, fixed dual/one-minute, and unavailable for other operations', () => {
    const input = defaults();
    assert.equal(validateFlow(input).api.enabled, true);
    const off = validateFlow({ ...input, api: { enabled: false, keyMode: 'chave1', delayMs: 1 } });
    assert.deepEqual(off.api, { enabled: false, keyMode: 'dupla', delayMs: 60000 });
    assert.equal(effectiveFlow(off, { username: 'Davi' }).api.enabled, false);
    const { api, ...legacy } = input;
    assert.equal(effectiveFlow(legacy, { username: 'Davi' }).api.enabled, true);
    for (const operation of ['santander', 'pagbank', 'mercadopago']) {
        assert.equal(validateFlow({ ...input, operation }).api.enabled, false);
        assert.equal(effectiveFlow({ ...input, operation }, { username: 'Davi' }).api.enabled, false);
    }
});

test('API credentials travel only through private worker messages, and failure releases the lease', async t => {
    let reserves = 0, releases = 0, checks = 0;
    const secret = 'synthetic-private-only';
    const f = fixture(t, { resolveApiSession: async owner => {
        assert.equal(owner, 'Davi'); reserves++;
        return { credentials: { c6: { clientId: secret } }, nextAllowedAt: 123, assert: async () => { checks++; }, release: async () => { releases++; } };
    } });
    const flow = f.manager.save(defaults()), job = f.manager.start({ flowId: flow.id, outputDirectory: f.directory });
    for (let n = 0; n < 100 && !f.workers.length; n++) await tick();
    const worker = f.workers[0];
    assert.ok(!JSON.stringify(worker.data).includes(secret)); assert.equal(reserves, 0);
    worker.emit('message', { type: 'api-request', id: 1, action: 'acquire' }); await tick();
    assert.equal(reserves, 1); assert.equal(worker.messages[0].credentials.c6.clientId, secret);
    worker.emit('message', { type: 'api-request', id: 2, action: 'assert' }); await tick(); assert.equal(checks, 1);
    worker.emit('error', new Error('synthetic failure')); await idle(f.manager);
    assert.equal(releases, 1);
    assert.ok(!JSON.stringify(f.manager.bootstrap()).includes(secret));
    const saved = JSON.stringify(createUserStore(f.directory, 'Davi').getJob(job.id)); assert.ok(!saved.includes(secret));
});

test('a late API reservation is released if the worker exits during acquisition', async t => {
    let finish, releases = 0;
    const f = fixture(t, { resolveApiSession: () => new Promise(resolve => { finish = resolve; }) });
    const flow = f.manager.save(defaults()); f.manager.start({ flowId: flow.id, outputDirectory: f.directory });
    for (let n = 0; n < 100 && !f.workers.length; n++) await tick();
    const worker = f.workers[0]; worker.emit('message', { type: 'api-request', id: 1, action: 'acquire' }); await tick();
    worker.emit('exit', 1); await idle(f.manager);
    finish({ release: async () => { releases++; } }); await tick(); assert.equal(releases, 1);
});

test('disabled and older frozen jobs cannot acquire API keys', async t => {
    let reserves = 0;
    const f = fixture(t, { resolveApiSession: async () => { reserves++; throw Error('should not acquire'); } });
    const flow = f.manager.save({ ...defaults(), api: { enabled: false } });
    f.manager.start({ flowId: flow.id, outputDirectory: f.directory });
    for (let n = 0; n < 100 && !f.workers.length; n++) await tick();
    const worker = f.workers[0]; worker.emit('message', { type: 'api-request', id: 1, action: 'acquire' }); await tick();
    assert.equal(reserves, 0); assert.ok(worker.messages[0].error); assert.equal(worker.messages[0].credentials, undefined);
    worker.emit('message', { type: 'result', data: { outputs: [] } }); await idle(f.manager);
});
test('stores isolate owner and preserve persistence, revision and duplicate semantics', t => {
    const { directory } = fixture(t);
    const a = createUserStore(directory, 'Davi'), b = createUserStore(directory, '../Outro');
    const flow = validateFlow(defaults()); a.saveFlow(flow);
    a.saveJob({ id: 'job', owner: 'Davi', status: 'completed' });
    assert.equal(b.getFlow(flow.id), undefined); assert.equal(b.getJob('job'), undefined);
    assert.throws(() => b.saveJob({ id: 'foreign', owner: 'Davi' }), /outro usuário/);
    assert.equal(createUserStore(directory, 'Davi').getFlow(flow.id).revision, 1);
    assert.equal(validateFlow(flow, flow).revision, 2);
    assert.notEqual(validateFlow({ ...flow, id: '' }).id, flow.id);
    assert.equal(path.dirname(b.directory), directory);
    const blocked = path.join(a.directory, 'history.json.tmp'); fs.mkdirSync(blocked);
    const rejected = validateFlow({ ...defaults(), name: 'Rejected' });
    assert.throws(() => a.saveFlow(rejected)); assert.equal(a.getFlow(rejected.id), undefined);
    assert.throws(() => a.deleteFlow(flow.id)); assert.equal(a.getFlow(flow.id).name, flow.name);
    const detached = a.getFlow(flow.id); detached.name = 'Mutation'; assert.equal(a.getFlow(flow.id).name, flow.name);
    fs.rmdirSync(blocked);
});
test('one worker per app, frozen snapshot, owner isolation and confirmed outputs', async t => {
    const f = fixture(t), m = f.manager;
    const flow = m.save(defaults()); const job = m.start({ flowId: flow.id, outputDirectory: f.directory });
    assert.throws(() => m.start({ flowId: flow.id, outputDirectory: f.directory }), /execução/);
    for (let n = 0; n < 100 && !f.workers.length; n++) await tick();
    const worker = f.workers[0]; assert.ok(worker);
    m.save({ ...flow, name: 'Changed', generation: { ...flow.generation, limit: 10 } });
    assert.equal(worker.data.flow.name, 'Novo fluxo'); assert.equal(worker.data.flow.generation.limit, 100000);
    f.setUser({ username: 'Outro', role: 'admin' });
    assert.deepEqual(m.bootstrap().jobs, []); assert.throws(() => m.cancel(job.id), /conta/); assert.throws(() => m.resume(job.id), /execução|retomada/);
    f.setUser({ username: 'Davi', role: 'admin' });
    const output = path.join(f.directory, 'result.xlsx'); fs.writeFileSync(output, 'synthetic');
    worker.emit('message', { type: 'result', data: { counts: { kept: 3 }, outputs: [{ path: output, kind: 'xlsx', rows: 3 }] } });
    await idle(m); assert.equal(m.bootstrap().jobs[0].status, 'completed'); assert.equal(m.output(job.id, output), output);
    assert.throws(() => m.output(job.id, path.join(f.directory, 'else.xlsx')), /não pertence/);
    f.setUser({ username: 'Davi', role: 'limited' }); assert.throws(() => m.bootstrap(), /perfil/);
});
test('cancel/resume preserves root and snapshot; worker failure never claims success', async t => {
    let roots = 0;
    const f = fixture(t, { resolveRoot: async () => { roots++; return { documents: [], info: { source: 'none', count: 0 } }; } }), m = f.manager;
    const flow = m.save(defaults()), job = m.start({ flowId: flow.id, outputDirectory: f.directory });
    for (let n = 0; n < 10 && !f.workers.length; n++) await tick();
    m.cancel(job.id); assert.deepEqual(f.workers[0].messages, [{ type: 'cancel' }]);
    f.workers[0].emit('message', { type: 'error', message: 'cancel', code: 'FLOW_CANCELLED' }); await idle(m);
    assert.equal(m.bootstrap().jobs[0].status, 'cancelled');
    m.resume(job.id); for (let n = 0; n < 10 && f.workers.length < 2; n++) await tick();
    assert.equal(roots, 1); assert.equal(f.workers[1].data.jobDir, job.jobDir);
    f.workers[1].emit('error', new Error('private credentials')); await idle(m);
    assert.equal(m.bootstrap().jobs[0].status, 'failed'); assert.ok(!m.bootstrap().jobs[0].error.includes('private'));
});
test('failed preparation clears active and restart recovers running history', async t => {
    const f = fixture(t, { resolveConnections: async () => { throw new Error('Configure Receita'); } }), m = f.manager;
    const flow = m.save(defaults()), job = m.start({ flowId: flow.id, outputDirectory: f.directory }); await idle(m);
    assert.equal(m.bootstrap().jobs[0].status, 'failed'); assert.match(m.bootstrap().jobs[0].error, /Configure Receita/);
    const store = createUserStore(f.directory, 'Davi'); store.saveJob({ ...store.getJob(job.id), status: 'running' });
    const restarted = createFlowManager({ baseDirectory: f.directory, getUser: () => ({ username: 'Davi', role: 'admin' }) });
    assert.equal(restarted.bootstrap().jobs[0].status, 'interrupted');
});
test('expired BQ login leaves a recoverable job and resume clears the auth flag', async t => {
    let expired = true;
    const f = fixture(t, { resolveRoot: async () => {
        if (expired) throw Object.assign(new Error('Renove o login Google.'), { code: 'BQ_AUTH_REQUIRED' });
        return { documents: ['04252011000110'], info: { source: 'bq', count: 1 } };
    } }), m = f.manager;
    const flow = m.save(defaults()), job = m.start({ flowId: flow.id, outputDirectory: f.directory });
    await idle(m);
    assert.equal(m.bootstrap().jobs[0].status, 'failed');
    assert.equal(m.bootstrap().jobs[0].errorCode, 'BQ_AUTH_REQUIRED');
    assert.equal(f.workers.length, 0);
    expired = false;
    assert.equal(m.resume(job.id).errorCode, '');
    for (let n = 0; n < 15 && !f.workers.length; n++) await tick();
    assert.equal(f.workers.length, 1);
    assert.equal(f.workers[0].data.jobDir, job.jobDir);
    assert.deepEqual(f.workers[0].data.flow.output.formatSnapshot, job.flowSnapshot.output.formatSnapshot);
    f.workers[0].emit('message', { type: 'result', data: { status: 'empty', outputs: [] } });
    await idle(m);
    assert.equal(m.bootstrap().jobs[0].status, 'empty');
});
test('custom layouts persist per owner, reject stale updates and stay frozen through deletion/resume', async t => {
    const f = fixture(t), m = f.manager;
    const layout = m.saveLayout({ nome: 'Discagem', colunas: [{ header: 'Documento', campo: 'cnpj' }, { header: 'Celular', campo: 'telefone_1' }] });
    const savedFlow = m.save({ ...defaults(), output: { ...defaults().output, formatId: layout.id } });
    assert.equal(m.bootstrap().formats.find(item => item.id === layout.id).nome, 'Discagem');
    const reloaded = createFlowManager({ baseDirectory: f.directory, getUser: () => ({ username: 'Davi', role: 'admin' }) });
    assert.deepEqual(reloaded.bootstrap().formats.find(item => item.id === layout.id), layout);
    const job = m.start({ flowId: savedFlow.id, outputDirectory: f.directory });
    for (let n = 0; n < 15 && !f.workers.length; n++) await tick();
    const edited = m.saveLayout({ ...layout, colunas: [{ header: 'Documento novo', campo: 'cnpj' }, { header: 'WhatsApp', campo: 'telefone_1' }] });
    assert.equal(edited.revision, 2);
    assert.throws(() => m.saveLayout(layout), /revisão atual/);
    assert.throws(() => m.saveLayout({ ...layout, id: 'padrao' }), /cópia/);
    assert.equal(f.workers[0].data.flow.output.formatSnapshot.colunas[1].header, 'Celular');
    assert.throws(() => m.deleteLayout(layout.id), /usado por um fluxo/);
    f.setUser({ username: 'Outro', role: 'admin' });
    assert.equal(m.bootstrap().formats.some(item => item.id === layout.id), false);
    assert.throws(() => m.saveLayout(edited), /nesta conta/);
    assert.throws(() => m.deleteLayout(layout.id), /nesta conta/);
    assert.throws(() => m.save({ ...defaults(), output: { ...defaults().output, formatId: layout.id } }), /desconhecido/);
    f.setUser({ username: 'Davi', role: 'admin' });
    f.workers[0].emit('error', new Error('synthetic')); await idle(m);
    m.delete(savedFlow.id); m.deleteLayout(layout.id);
    assert.equal(m.bootstrap().formats.some(item => item.id === layout.id), false);
    m.resume(job.id);
    for (let n = 0; n < 15 && f.workers.length < 2; n++) await tick();
    assert.equal(f.workers[1].data.flow.output.formatSnapshot.colunas[1].header, 'Celular');
    f.workers[1].emit('message', { type: 'result', data: { status: 'empty', outputs: [] } }); await idle(m);
});
test('window observer errors are isolated and disk failure in worker update clears active', async t => {
    const f = fixture(t, { onUpdate: () => { throw new Error('window closed'); } }), m = f.manager;
    const flow = m.save(defaults()); m.start({ flowId: flow.id, outputDirectory: f.directory });
    for (let n = 0; n < 15 && !f.workers.length; n++) await tick();
    const store = createUserStore(f.directory, 'Davi'); const jobId = store.listJobs()[0].id;
    const block = path.join(store.directory, 'job-records', require('node:crypto').createHash('sha256').update(jobId).digest('hex') + '.json.tmp'); fs.mkdirSync(block);
    f.workers[0].emit('message', { type: 'update', data: { stage: 'generation' } }); await idle(m);
    fs.rmdirSync(block); assert.equal(m.isBusy(), false);
    assert.equal(m.bootstrap().jobs[0].status, 'interrupted');
    m.start({ flowId: flow.id, outputDirectory: f.directory });
    for (let n = 0; n < 15 && f.workers.length < 2; n++) await tick();
    f.workers[1].emit('message', { type: 'result', data: { status: 'empty', outputs: [] } }); await idle(m);
});
