/**
 * Handlers de autenticação, sessão e configurações de UI.
 */
const { ipcMain, dialog } = require('electron');
const fs = require('fs');
const path = require('path');
const Store = require('electron-store');
const store = new Store();

const state = require('../state');
const { initializePool, closePool } = require('../database/connection');
const { loadStoredCnpjs } = require('../database/cache');
const { loadKeyFile, clearCredentials, hasCredentials } = require('../keyfile');
const { Pool } = require('pg');
const { getReceitaMetadata } = require('../flows/receita');
const { readOnlyPoolOptions } = require('../flows/postgres');

const privateConfig = require('../runtimeConfig');
const users = privateConfig.loadUsers();

function createLoginWindow() {
    const { BrowserWindow } = require('electron');
    state.loginWindow = new BrowserWindow({
        width: 480,
        height: 790,
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

    const mainWindow = new BrowserWindow({
        width: 1400,
        height: 950,
        frame: false,
        webPreferences: {
            nodeIntegration: false,
            contextIsolation: true,
            preload: path.join(__dirname, '../../../preload.js')
        }
    });
    state.mainWindow = mainWindow;

    mainWindow.on('close', async (e) => {
        const lockedKeys = getCurrentLockedKeys();
        if (lockedKeys.length > 0) {
            e.preventDefault();
            console.log("Liberando chaves de API antes de fechar...");
            await releaseApiLock(lockedKeys);
            setCurrentLockedKeys([]);
            mainWindow.destroy();
        }
    });

    mainWindow.loadFile(path.join(__dirname, '../../../index.html'));

    mainWindow.webContents.on("did-finish-load", async () => {
        if (mainWindow !== state.mainWindow || mainWindow.isDestroyed()) return;
        if (state.currentUser) {
            mainWindow.webContents.send('user-info', state.currentUser);

            if (state.currentUser.role === 'admin') {
                const dbConnectionString = store.get('db_connection_string');
                try {
                    await initializePool(dbConnectionString, mainWindow);
                    if (state.pool) {
                        await loadStoredCnpjs();
                    }
                } catch (error) {
                    // O erro já é logado dentro de initializePool
                }
            }
        }
        if (app.isPackaged) autoUpdater.checkForUpdatesAndNotify().catch(error => {
            console.warn('Não foi possível verificar atualizações:', error.message);
        });
    });

    mainWindow.on('closed', () => {
        if (state.mainWindow === mainWindow) state.mainWindow = null;
    });
}

function register() {
    ipcMain.handle('get-access-status', () => privateConfig.status());
    ipcMain.handle('import-private-access', async (event) => {
        if (!state.loginWindow || event.sender !== state.loginWindow.webContents) return { success: false, message: 'Importe o acesso pela tela de login.' };
        const result = await dialog.showOpenDialog(state.loginWindow, { title: 'Importar acesso da empresa', properties: ['openFile'], filters: [{ name: 'Acesso MB Finance', extensions: ['mbconfig'] }] });
        if (result.canceled || !result.filePaths.length) return { cancelled: true };
        try {
            const file = result.filePaths[0];
            if (fs.statSync(file).size > 5 * 1024 * 1024) throw new Error('Arquivo de acesso muito grande.');
            privateConfig.importBundle(JSON.parse(fs.readFileSync(file, 'utf8')));
            for (const key of Object.keys(users)) delete users[key];
            Object.assign(users, privateConfig.loadUsers());
            const keyPath = privateConfig.keyFilePath();
            if (keyPath) {
                try { loadKeyFile(keyPath); store.set('key_file_path', keyPath); }
                catch { return { success: true, message: 'Acesso importado. Importe novamente a licença de API.' }; }
            }
            return { success: true, message: 'Acesso importado. Entre com seu usuário e senha.' };
        } catch { return { success: false, message: 'Não foi possível importar. Verifique o arquivo de acesso fornecido pela empresa.' }; }
    });
    const withMainWindow = (action) => {
        const window = state.mainWindow;
        if (window && !window.isDestroyed()) action(window);
    };
    ipcMain.on('minimize-window', () => withMainWindow(window => window.minimize()));
    ipcMain.on('maximize-window', () => withMainWindow(window => {
        if (window.isMaximized()) window.unmaximize();
        else window.maximize();
    }));
    ipcMain.on('close-window', () => withMainWindow(window => window.close()));

    const isLoginSender = event => state.loginWindow && event.sender === state.loginWindow.webContents;
    ipcMain.handle('get-login-database-status', event => {
        if (!isLoginSender(event)) return { success: false, message: 'Configure os bancos pela tela de login.' };
        return { success: true, neonConfigured: Boolean(store.get('db_connection_string')), receitaConfigured: Boolean(store.get('receita_connection_string') || process.env.RECEITA_DATABASE_URL) };
    });

    ipcMain.handle('save-and-test-receita-connection', async (event, connectionString) => {
        if (!isLoginSender(event)) return { success: false, message: 'Configure a Receita pela tela de login.' };
        if (state.flowManager?.isBusy()) return { success: false, message: 'Aguarde o fluxo terminar antes de mudar o acesso à Receita.' };
        if (typeof connectionString !== 'string' || connectionString.length > 4096 || !/^postgres(ql)?:\/\//i.test(connectionString)) return { success: false, message: 'Informe uma conexão PostgreSQL válida da base Receita.' };
        let pool;
        try {
            pool = new Pool(readOnlyPoolOptions(connectionString, { max: 1, timeout: 15000, connectionTimeout: 10000 }));
            await getReceitaMetadata(pool);
            store.set('receita_connection_string', connectionString);
            return { success: true, message: 'Base da Receita verificada e salva neste computador.' };
        } catch (error) {
            return { success: false, message: error.code === 'FLOW_VALIDATION' ? error.message : 'Não foi possível validar a base da Receita. Confira o acesso e tente novamente.' };
        } finally { if (pool) await pool.end().catch(() => {}); }
    });

    ipcMain.handle('save-and-test-db-connection', async (event, connectionString) => {
        if (!isLoginSender(event)) return { success: false, message: 'Configure o Neon pela tela de login.' };
        if (typeof connectionString !== 'string' || connectionString.length > 4096 || !/^postgres(ql)?:\/\//i.test(connectionString)) return { success: false, message: 'Informe uma conexão PostgreSQL válida do Neon.' };
        try {
            await initializePool(connectionString);
            store.set('db_connection_string', connectionString);
            return { success: true, message: 'Conexão bem-sucedida e salva!' };
        } catch (error) {
            console.error("❌ Falha ao testar/salvar conexão com o BD:", error.message);
            return { success: false, message: 'Não foi possível conectar ao Neon. Confira o acesso e tente novamente.' };
        }
    });

    ipcMain.handle('login-attempt', async (event, username, password, rememberMe) => {
        if (!privateConfig.hasAccess()) return { success: false, message: 'Importe o arquivo de acesso fornecido pela empresa antes de entrar.' };
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
        clearCredentials();
        state.currentUser = null;
        closePool();
        if (state.mainWindow) {
            state.mainWindow.close();
        }
        if (!state.loginWindow) {
            createLoginWindow();
        }
    });

    ipcMain.handle('load-key-file', async () => {
        const result = await dialog.showOpenDialog({
            title: 'Importar Licença de API',
            filters: [{ name: 'Licença de API', extensions: ['mbkey'] }],
            properties: ['openFile'],
        });

        if (result.canceled || result.filePaths.length === 0) {
            return { success: false, cancelled: true };
        }

        const filePath = result.filePaths[0];
        try {
            loadKeyFile(filePath);
            store.set('key_file_path', filePath);
            return { success: true, path: filePath };
        } catch (error) {
            return { success: false, message: error.message };
        }
    });

    ipcMain.handle('get-key-file-status', () => {
        return { loaded: hasCredentials(), path: store.get('key_file_path') || null };
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
