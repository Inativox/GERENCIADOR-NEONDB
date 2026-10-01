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
        '../runtimeConfig': { loadUsers: () => ({}) },
        '../database/connection': {}, '../database/cache': {}, '../keyfile': {},
        'electron-updater': { autoUpdater: {} },
        './cnpj': { getCurrentLockedKeys: () => [] },
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
        'electron-log': { transports: { file: {} } },
        './keyfile': {}, './state': { mainWindow: { webContents: { send: (...args) => messages.push(args) } } },
        './database/cache': noopHandler,
    };
    for (const name of ['auth', 'files', 'limpeza', 'cnpj', 'enriquecimento', 'blocklist', 'monitoramento', 'relacionamento', 'limpezaColunas', 'fluxos', 'receitaSituacao']) dependencies[`./handlers/${name}`] = noopHandler;
    loadModule('src/main/index.js', dependencies);
    autoUpdater.emit('update-downloaded', { version: '2.0.0' });
    assert.equal(forcedQuit, 0);
    assert.equal(messages[0][0], 'update-ready');
    assert.equal(autoUpdater.autoInstallOnAppQuit, true);
});
