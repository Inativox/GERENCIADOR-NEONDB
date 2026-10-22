const test = require('node:test');
const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const path = require('node:path');
const loadModule = require('./helpers/loadModule');

test('reabrir janela não duplica controles IPC e janela fechada é ignorada', () => {
    const ipcMain = new EventEmitter();
    ipcMain.handle = () => {};
    const state = {};
    class BrowserWindow extends EventEmitter {
        constructor() { super(); this.webContents = new EventEmitter(); this.webContents.send = () => {}; this.minimizations = 0; }
        loadFile() {}
        isDestroyed() { return false; }
        minimize() { this.minimizations++; }
    }
    const auth = loadModule('src/main/handlers/auth.js', {
        electron: { ipcMain, BrowserWindow, app: { isPackaged: false }, dialog: {} },
        fs: { readFileSync: () => '{}' }, path,
        'electron-store': class {}, '../state': state,
        '../settingsWriteRetry': require('../src/main/settingsWriteRetry'),
        '../runtimeConfig': { loadUsers: () => ({}) },
        '../database/connection': {}, '../database/cache': {}, '../keyfile': {},
        pg: {}, '../flows/receita': {}, '../flows/postgres': {},
        'electron-updater': { autoUpdater: {} },
        './cnpj': { getCurrentLockedKeys: () => [] },
        './alertaMailing': { manterNaBandeja: () => false, sincronizar() {} },
    });
    auth.register();
    auth.createMainWindow();
    const previous = state.mainWindow;
    auth.createMainWindow();
    const current = state.mainWindow;
    previous.emit('closed');
    assert.equal(state.mainWindow, current);
    ipcMain.emit('minimize-window');
    assert.equal(state.mainWindow.minimizations, 1);
    for (const channel of ['minimize-window', 'maximize-window', 'close-window']) assert.equal(ipcMain.listenerCount(channel), 1);
    state.mainWindow = null;
    assert.doesNotThrow(() => ipcMain.emit('minimize-window'));
});

function closingFixture({ crashed = false, naBandeja = false } = {}) {
    const state = {}, ipcMain = new EventEmitter(), dialogs = [];
    ipcMain.handle = () => {};
    class BrowserWindow extends EventEmitter {
        constructor(options) { super(); this.options = options; this.webContents = new EventEmitter(); this.webContents.sent = []; this.webContents.send = (...args) => this.webContents.sent.push(args); this.webContents.isCrashed = () => crashed; this.closes = 0; this.hides = 0; }
        hide() { this.hides++; }
        loadFile() {}
        isDestroyed() { return false; }
        close() { this.closes++; }
    }
    const auth = loadModule('src/main/handlers/auth.js', {
        electron: { ipcMain, BrowserWindow, app: { isPackaged: false }, dialog: { showMessageBox: async (window, options) => { dialogs.push(options); return { response: 1 }; } } },
        fs: { readFileSync: () => '{}' }, path,
        'electron-store': class {}, '../state': state,
        '../settingsWriteRetry': require('../src/main/settingsWriteRetry'),
        '../runtimeConfig': { loadUsers: () => ({}) },
        '../database/connection': {}, '../database/cache': {}, '../keyfile': {},
        pg: {}, '../flows/receita': {}, '../flows/postgres': {},
        'electron-updater': { autoUpdater: {} },
        './cnpj': { getCurrentLockedKeys: () => [] },
        './alertaMailing': { manterNaBandeja: () => naBandeja, sincronizar() {} },
    });
    let busy = false, prevented = 0;
    state.flowManager = { isBusy: () => busy, activeStage: () => 'upload' };
    auth.register();
    auth.createMainWindow();
    const window = state.mainWindow;
    return {
        window, ipcMain, dialogs, state, setBusy: value => { busy = value; }, prevented: () => prevented,
        close: () => { window.emit('close', { preventDefault: () => { prevented++; } }); return new Promise(setImmediate); },
        answer: (close, sender = window.webContents) => { ipcMain.emit('close-during-flow-answer', { sender }, close); return new Promise(setImmediate); },
    };
}

test('fechar a janela durante um fluxo abre o modal do app e só fecha se o usuário aceitar', async () => {
    const f = closingFixture();
    await f.close();
    assert.equal(f.prevented(), 0);
    f.setBusy(true);
    await f.close();
    assert.equal(f.prevented(), 1);
    assert.deepEqual(JSON.parse(JSON.stringify(f.window.webContents.sent)), [['confirm-close-during-flow', { uploading: true, mailing: false }]]);
    await f.answer(false);
    assert.equal(f.window.closes, 0);
    await f.close();
    await f.answer(true, {});
    assert.equal(f.window.closes, 0, 'outra janela não decide o fechamento');
    await f.answer(true);
    assert.equal(f.window.closes, 1);
    await f.close();
    assert.equal(f.prevented(), 2);
    assert.equal(f.window.webContents.sent.length, 2);
    assert.equal(f.dialogs.length, 0);
});

test('com a tela travada, o fechamento durante um fluxo usa o diálogo nativo', async () => {
    const f = closingFixture({ crashed: true });
    f.setBusy(true);
    await f.close();
    assert.equal(f.window.webContents.sent.length, 0);
    assert.match(f.dialogs[0].message, /Fastway/);
    assert.equal(f.window.closes, 1);
});

test('fechar a janela durante uma importação manual pede confirmação mesmo sem fluxo rodando', async () => {
    const f = closingFixture();
    f.state.mailingUpload = { status: 'running' };
    await f.close();
    assert.equal(f.prevented(), 1);
    assert.deepEqual(JSON.parse(JSON.stringify(f.window.webContents.sent)), [['confirm-close-during-flow', { uploading: true, mailing: true }]]);
    await f.answer(true);
    assert.equal(f.window.closes, 1);
});

test('com a tela travada, a importação manual usa o diálogo nativo sem falar em execução interrompida', async () => {
    const f = closingFixture({ crashed: true });
    f.state.mailingUpload = { status: 'running' };
    await f.close();
    assert.match(f.dialogs[0].message, /Fastway/);
    assert.match(f.dialogs[0].detail, /painel/);
    assert.doesNotMatch(f.dialogs[0].detail, /interrompida/);
    assert.equal(f.window.closes, 1);
});

test('atualização pronta avisa sem forçar encerramento durante tarefa', () => {
    const autoUpdater = new EventEmitter();
    let forcedQuit = 0;
    autoUpdater.quitAndInstall = () => { forcedQuit++; };
    const messages = [];
    const noopHandler = { register() {} };
    const dependencies = {
        dotenv: { config() {} },
        './runtimeConfig': { initialize() {} },
        electron: { app: { whenReady: () => ({ then() {} }), on() {} } },
        'electron-updater': { autoUpdater },
        'electron-store': class {},
        './settingsWriteRetry': require('../src/main/settingsWriteRetry'),
        'electron-log': { transports: { file: {} } },
        './keyfile': {}, './state': { mainWindow: { webContents: { send: (...args) => messages.push(args) } } },
        './database/cache': noopHandler,
    };
    for (const name of ['auth', 'files', 'limpeza', 'cnpj', 'enriquecimento', 'blocklist', 'monitoramento', 'relacionamento', 'limpezaColunas', 'fluxos', 'receitaSituacao', 'importarMailing', 'alertaMailing']) dependencies[`./handlers/${name}`] = noopHandler;
    loadModule('src/main/index.js', dependencies);
    autoUpdater.emit('update-downloaded', { version: '2.0.0' });
    assert.equal(forcedQuit, 0);
    assert.equal(messages[0][0], 'update-ready');
    assert.equal(autoUpdater.autoInstallOnAppQuit, true);
});

test('com o alerta de mailing rodando, fechar só esconde a janela e não pergunta sobre o fluxo', async () => {
    const f = closingFixture({ naBandeja: true });
    f.setBusy(true);
    await f.close();
    assert.deepEqual([f.prevented(), f.window.hides, f.window.closes], [1, 1, 0]);
    assert.deepEqual(f.window.webContents.sent, []);
    assert.equal(f.window.options.show, true);
});

test('aberta com o Windows pelo alerta, a janela principal nasce escondida na bandeja', () => {
    const state = {}, ipcMain = new EventEmitter(), opcoes = [];
    ipcMain.handle = () => {};
    class BrowserWindow extends EventEmitter {
        constructor(options) { super(); opcoes.push(options); this.webContents = new EventEmitter(); this.webContents.send = () => {}; }
        loadFile() {}
        isDestroyed() { return false; }
    }
    const auth = loadModule('src/main/handlers/auth.js', {
        electron: { ipcMain, BrowserWindow, app: { isPackaged: false }, dialog: {} },
        fs: { readFileSync: () => '{}' }, path,
        'electron-store': class {}, '../state': state,
        '../settingsWriteRetry': require('../src/main/settingsWriteRetry'),
        '../runtimeConfig': { loadUsers: () => ({}) },
        '../database/connection': {}, '../database/cache': {}, '../keyfile': {},
        pg: {}, '../flows/receita': {}, '../flows/postgres': {},
        'electron-updater': { autoUpdater: {} },
        './cnpj': { getCurrentLockedKeys: () => [] },
        './alertaMailing': { manterNaBandeja: () => false, sincronizar() {} },
    });
    auth.register();
    auth.createMainWindow({ hidden: true });
    assert.equal(opcoes[0].show, false);
});
