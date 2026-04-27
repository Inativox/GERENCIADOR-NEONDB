/**
 * Ponto de entrada do processo principal do Electron.
 * Registra todos os handlers e configura o ciclo de vida da aplicação.
 */
require('dotenv').config();

const { app } = require('electron');
const { autoUpdater } = require('electron-updater');
const Store = require('electron-store');
const store = new Store();
const { loadKeyFile } = require('./keyfile');

const state = require('./state');

// Handlers
const auth = require('./handlers/auth');
const files = require('./handlers/files');
const limpeza = require('./handlers/limpeza');
const cnpj = require('./handlers/cnpj');
const enriquecimento = require('./handlers/enriquecimento');
const blocklist = require('./handlers/blocklist');
const monitoramento = require('./handlers/monitoramento');
const relacionamento = require('./handlers/relacionamento');

// Cache/BD
const cache = require('./database/cache');

// Configura o auto-updater
autoUpdater.logger = require('electron-log');
autoUpdater.logger.transports.file.level = 'info';
autoUpdater.autoDownload = true;
autoUpdater.autoInstallOnAppQuit = true;

autoUpdater.on('update-available', (info) => {
    if (state.mainWindow && state.mainWindow.webContents) {
        state.mainWindow.webContents.send('update-downloading', { version: info.version });
    }
});
autoUpdater.on('download-progress', (p) => {
    if (state.mainWindow && state.mainWindow.webContents) {
        state.mainWindow.webContents.send('update-progress', { percent: Math.round(p.percent) });
    }
});
autoUpdater.on('update-downloaded', (info) => {
    if (state.mainWindow && state.mainWindow.webContents) {
        state.mainWindow.webContents.send('update-ready', { version: info.version });
    }
    setTimeout(() => {
        autoUpdater.quitAndInstall(true, true);
    }, 3000);
});
autoUpdater.on('error', (err) => {
    console.error('Erro no auto-updater:', err);
});

// Registra todos os handlers IPC
auth.register();
files.register();
limpeza.register();
cnpj.register();
enriquecimento.register();
blocklist.register();
monitoramento.register();
relacionamento.register();
cache.register();

// Ciclo de vida do app
app.whenReady().then(async () => {
    const keyFilePath = store.get('key_file_path');
    if (keyFilePath) {
        try {
            loadKeyFile(keyFilePath);
            console.log('Licença de API carregada automaticamente.');
        } catch (e) {
            console.warn('Falha ao carregar licença de API salva:', e.message);
            store.delete('key_file_path');
        }
    }

    const savedCredentials = store.get('credentials');

    if (savedCredentials && savedCredentials.username && savedCredentials.password) {
        const { username, password } = savedCredentials;
        const user = auth.users[username];

        if (user && user.password === password) {
            console.log('Login automático bem-sucedido.');
            state.currentUser = {
                username,
                role: user.role,
                teamId: user.teamId || null
            };
            auth.createMainWindow();
        } else {
            console.log('Credenciais salvas inválidas. Abrindo tela de login.');
            auth.createLoginWindow();
        }
    } else {
        console.log('Nenhuma credencial salva. Abrindo tela de login.');
        auth.createLoginWindow();
    }
});

app.on('window-all-closed', () => {
    if (process.platform !== 'darwin') {
        app.quit();
    }
});
