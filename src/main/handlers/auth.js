/**
 * Handlers de autenticação, sessão e configurações de UI.
 */
const { ipcMain, dialog } = require('electron');
const fs = require('fs');
const path = require('path');
const Store = require('electron-store');
const store = new Store();

const state = require('../state');
const { initializePool } = require('../database/connection');
const { loadStoredCnpjs } = require('../database/cache');

const users = JSON.parse(fs.readFileSync(path.join(__dirname, '../../../users.json'), 'utf8'));

function createLoginWindow() {
    const { BrowserWindow } = require('electron');
    state.loginWindow = new BrowserWindow({
        width: 480,
        height: 650,
        webPreferences: {
            preload: path.join(__dirname, '../../../preload.js'),
            nodeIntegration: false,
            contextIsolation: true,
        },
        resizable: false,
        frame: false,
        center: true,
    });

    state.loginWindow.loadFile(path.join(__dirname, '../../../login.html'));

    state.loginWindow.on('closed', () => {
        state.loginWindow = null;
    });
    return state.loginWindow;
}

function createMainWindow() {
    const { BrowserWindow, app } = require('electron');
    const { autoUpdater } = require('electron-updater');
    const { releaseApiLock, getCurrentLockedKeys, setCurrentLockedKeys } = require('./cnpj');

    state.mainWindow = new BrowserWindow({
        width: 1400,
        height: 950,
        frame: false,
        webPreferences: {
            nodeIntegration: false,
            contextIsolation: true,
            preload: path.join(__dirname, '../../../preload.js')
        }
    });

    state.mainWindow.on('close', async (e) => {
        const lockedKeys = getCurrentLockedKeys();
        if (lockedKeys.length > 0) {
            e.preventDefault();
            console.log("Liberando chaves de API antes de fechar...");
            await releaseApiLock(lockedKeys);
            setCurrentLockedKeys([]);
            state.mainWindow.destroy();
        }
    });

    ipcMain.on('minimize-window', () => state.mainWindow.minimize());
    ipcMain.on('maximize-window', () => {
        if (state.mainWindow.isMaximized()) { state.mainWindow.unmaximize(); } else { state.mainWindow.maximize(); }
    });
    ipcMain.on('close-window', () => state.mainWindow.close());

    state.mainWindow.loadFile(path.join(__dirname, '../../../index.html'));

    state.mainWindow.webContents.on("did-finish-load", async () => {
        if (state.currentUser) {
            state.mainWindow.webContents.send('user-info', state.currentUser);

            if (state.currentUser.role === 'admin') {
                const dbConnectionString = store.get('db_connection_string');
                try {
                    await initializePool(dbConnectionString, state.mainWindow);
                    if (state.pool) {
                        await loadStoredCnpjs();
                    }
                } catch (error) {
                    // O erro já é logado dentro de initializePool
                }
            }
        }
        autoUpdater.checkForUpdatesAndNotify();
    });

    state.mainWindow.on('closed', () => {
        state.mainWindow = null;
    });
}

function register() {
    ipcMain.handle('get-db-connection-string', () => {
        return store.get('db_connection_string');
    });

    ipcMain.handle('save-and-test-db-connection', async (event, connectionString) => {
        if (!connectionString) {
            return { success: false, message: 'A chave de conexão não pode estar vazia.' };
        }
        try {
            await initializePool(connectionString);
            store.set('db_connection_string', connectionString);
            return { success: true, message: 'Conexão bem-sucedida e salva!' };
        } catch (error) {
            console.error("❌ Falha ao testar/salvar conexão com o BD:", error.message);
            state.pool = null;
            return { success: false, message: error.message };
        }
    });

    ipcMain.handle('login-attempt', async (event, username, password, rememberMe) => {
        const user = users[username];
        if (user && user.password === password) {
            state.currentUser = {
                username: username,
                role: user.role,
                teamId: user.teamId || null
            };

            if (rememberMe) {
                store.set('credentials', { username, password });
            } else {
                store.delete('credentials');
            }

            createMainWindow();
            if (state.loginWindow) state.loginWindow.close();

            return { success: true };
        } else {
            store.delete('credentials');
            return { success: false, message: 'Usuário ou senha inválidos.' };
        }
    });

    ipcMain.on('logout', () => {
        store.delete('credentials');
        state.currentUser = null;
        if (state.pool) {
            state.pool.end();
            state.pool = null;
        }
        if (state.mainWindow) {
            state.mainWindow.close();
        }
        if (!state.loginWindow) {
            createLoginWindow();
        }
    });

    ipcMain.handle('get-ui-settings', () => {
        return store.get('ui_settings', {});
    });

    ipcMain.on('save-ui-settings', (event, settings) => {
        store.set('ui_settings', settings);
    });

    ipcMain.handle('show-confirm-dialog', async (event, options) => {
        const result = await dialog.showMessageBox(state.mainWindow, {
            type: 'warning',
            buttons: ['Cancelar', 'Confirmar'],
            defaultId: 1,
            title: options.title,
            message: options.message,
        });
        return result.response === 1;
    });
}

module.exports = { register, createLoginWindow, createMainWindow, users };
