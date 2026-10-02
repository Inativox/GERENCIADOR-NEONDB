const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawn } = require('node:child_process');
const Conf = require('conf');
const withWriteRetry = require('../src/main/settingsWriteRetry');
const loadModule = require('./helpers/loadModule');

test('a second app exits before loading modules that write shared settings', () => {
    let quit = false;
    loadModule('main.js', {
        electron: { app: { requestSingleInstanceLock: () => false, quit() { quit = true; } } },
    });
    assert.equal(quit, true);
});

test('a second launch focuses the original minimized app', () => {
    const handlers = new Map(), actions = [];
    loadModule('main.js', {
        electron: { app: { requestSingleInstanceLock: () => true, on: (name, fn) => handlers.set(name, fn) } },
        './src/main/index.js': {},
        './src/main/state': { mainWindow: { isDestroyed: () => false, isMinimized: () => true, restore() { actions.push('restore'); }, show() { actions.push('show'); }, focus() { actions.push('focus'); } } },
    });
    handlers.get('second-instance')();
    assert.deepEqual(actions, ['restore', 'show', 'focus']);
});

test('persistent write denial preserves the prior settings and reports a friendly bounded error', t => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'settings-write-'));
    t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
    let denied = false;
    class DeniedStore extends Conf {
        _write(value) { if (denied) throw Object.assign(new Error('Synthetic permission denial'), { code: 'EPERM' }); return super._write(value); }
    }
    const store = new (withWriteRetry(DeniedStore))({ cwd: directory });
    store.set('saved', 'preserved');
    const before = fs.readFileSync(store.path);
    denied = true;
    assert.throws(() => store.set('saved', 'new'), error => error.code === 'EPERM' && error.message.includes('Não foi possível salvar'));
    assert.deepEqual(fs.readFileSync(store.path), before);
    denied = false;
    store.set('saved', 'new');
    assert.equal(store.get('saved'), 'new');
});

test('Windows file lock releases during retry without losing existing settings', { skip: process.platform !== 'win32' }, async t => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'settings-lock-'));
    t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
    const store = new (withWriteRetry(Conf))({ cwd: directory });
    store.set({ saved: 'preserved', preference: 'before' });
    const escaped = store.path.replace(/'/g, "''");
    const script = `$lockedFile=[System.IO.File]::Open('${escaped}',[System.IO.FileMode]::Open,[System.IO.FileAccess]::Read,[System.IO.FileShare]::ReadWrite); try { [Console]::WriteLine('READY'); Start-Sleep -Milliseconds 350 } finally { $lockedFile.Dispose() }`;
    const child = spawn('powershell.exe', ['-NoProfile', '-EncodedCommand', Buffer.from(script, 'utf16le').toString('base64')], { windowsHide: true });
    t.after(() => child.kill());
    await new Promise((resolve, reject) => {
        let text = '';
        child.stdout.on('data', data => { text += data; if (text.includes('READY')) resolve(); });
        child.once('error', reject);
        child.once('exit', code => { if (!text.includes('READY')) reject(new Error(`Lock helper exited: ${code}`)); });
    });
    store.set('preference', 'after');
    assert.equal(store.get('saved'), 'preserved');
    assert.equal(store.get('preference'), 'after');
});
