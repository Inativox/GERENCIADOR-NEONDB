const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { loginModeFor, runGcloud, createBqLogin } = require('../src/main/flows/bqAuth');

test('renewal follows active credential source; imported/service credentials are never silently replaced', t => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'bq-auth-'));
    t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
    const environment = { CLOUDSDK_CONFIG: directory };
    assert.equal(loginModeFor(null, environment), 'gcloud');
    const adc = path.join(directory, 'application_default_credentials.json');
    fs.writeFileSync(adc, JSON.stringify({ type: 'authorized_user' }));
    assert.equal(loginModeFor(null, environment), 'adc');
    assert.equal(loginModeFor(adc, environment), 'adc');
    const imported = path.join(directory, 'imported.json');
    fs.writeFileSync(imported, JSON.stringify({ type: 'authorized_user' }));
    assert.equal(loginModeFor(imported, environment), 'imported');
    fs.writeFileSync(imported, JSON.stringify({ type: 'service_account' }));
    assert.equal(loginModeFor(null, { ...environment, GOOGLE_APPLICATION_CREDENTIALS: imported }), 'service_account');
});
test('CLI launches browser through constant arguments with bounded timeout and no exposed output', async () => {
    let invocation;
    await runGcloud(['auth', 'login', '--force', '--launch-browser', '--brief', '--quiet'], { platform: 'linux', commandRunner: async (...args) => { invocation = args; return { stdout: 'private output' }; } });
    assert.equal(invocation[0], 'gcloud');
    assert.deepEqual(invocation[1], ['auth', 'login', '--force', '--launch-browser', '--brief', '--quiet']);
    assert.equal(invocation[2].windowsHide, true);
    await assert.rejects(runGcloud(['auth', 'login;echo secret'], { platform: 'linux' }), /inválido/);
});
test('one browser per machine, correct owner, validated readiness and automatic cooldown', async () => {
    const updates = [], calls = [];
    let release, validations = 0;
    const login = createBqLogin({ getMode: () => 'gcloud', now: () => 1000, onUpdate: update => updates.push(update), validate: async () => { validations++; }, runner: async (...args) => { calls.push(args); await new Promise(resolve => { release = resolve; }); } });
    const first = login.renew('Davi', { automatic: true });
    const second = login.renew('Davi');
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(calls.length, 1);
    assert.equal(login.status('Davi').state, 'renewing');
    assert.equal(login.status('Outro').state, 'idle');
    await assert.rejects(login.renew('Outro'), /Aguarde/);
    release(); await Promise.all([first, second]);
    assert.equal(validations, 1);
    assert.equal(login.isBusy(), false);
    assert.equal(login.status('Davi').state, 'ready');
    assert.deepEqual(updates.map(update => update.state), ['renewing', 'ready']);
    assert.ok(!JSON.stringify(updates).includes('private'));
    await assert.rejects(login.renew('Davi', { automatic: true }), error => error.code === 'BQ_AUTH_REQUIRED');
});
test('ADC renewal targets application-default; imported files never open a browser', async () => {
    let args;
    const login = createBqLogin({ getMode: () => 'adc', validate: async () => {}, runner: async value => { args = value; } });
    await login.renew('Davi');
    assert.deepEqual(args, ['auth', 'application-default', 'login', '--launch-browser', '--disable-quota-project', '--quiet']);
    for (const mode of ['service_account', 'imported']) {
        const unsupported = createBqLogin({ getMode: () => mode, runner: async () => { throw new Error('Must not launch'); } });
        await assert.rejects(unsupported.renew('Davi'), error => error.code === 'BQ_CREDENTIAL_INVALID');
    }
});
test('cancel/session end aborts login, bounds waiting and rejects sensitive CLI failures', async () => {
    const login = createBqLogin({ getMode: () => 'gcloud', validate: async () => { throw new Error('Must not validate'); }, runner: async (args, options) => {
        assert.equal(options.timeout, 300000);
        await new Promise((resolve, reject) => options.signal.addEventListener('abort', () => reject(new Error('private token'))));
    } });
    const pending = login.renew('Davi');
    await new Promise(resolve => setImmediate(resolve));
    login.cancel(); await assert.rejects(pending, /cancelada/);
    assert.equal(login.isBusy(), false);
    assert.equal(login.status('Davi').state, 'failed');
    assert.ok(!login.status('Davi').message.includes('private'));
});
test('successful browser login with insufficient BQ permissions remains failed', async () => {
    const login = createBqLogin({ getMode: () => 'gcloud', runner: async () => {}, validate: async () => { throw Object.assign(new Error('Sem permissão BQ.'), { code: 'BQ_FORBIDDEN' }); } });
    await assert.rejects(login.renew('Davi'), /permissão/);
    assert.equal(login.status('Davi').state, 'failed');
});
