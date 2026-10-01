const { app, BrowserWindow, dialog, ipcMain } = require('electron');
const fs = require('fs');
const path = require('path');
const os = require('os');
const assert = require('assert/strict');
const asar = require('@electron/asar');
const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mb-access-smoke-'));
app.setPath('userData', path.join(root, 'user'));
app.disableHardwareAcceleration();
app.on('window-all-closed', () => {});
const appRoot = process.argv[2] ? path.resolve(process.argv[2]) : path.resolve(__dirname, '..');
let window;
let exitCode = 0;
const timeout = setTimeout(() => { console.error('Timeout do teste de acesso.'); app.exit(1); }, 20000);
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
async function waitFor(source) {
    for (let i = 0; i < 100; i++) {
        if (await window.webContents.executeJavaScript(source)) return;
        await delay(20);
    }
    throw new Error(`Estado não alcançado: ${source}`);
}
app.whenReady().then(async () => {
    const { createPrivateConfig } = require(path.join(appRoot, 'src/main/privateConfig'));
    const folders = { userData: path.join(root, 'user'), appData: path.join(root, 'roaming') };
    const config = createPrivateConfig({ app: { isPackaged: true, getPath: name => folders[name] }, projectRoot: root, environment: {} });
    config.initialize();
    assert.equal(config.hasAccess(), false);
    require.cache[require.resolve(path.join(appRoot, 'src/main/runtimeConfig'))] = { exports: config };
    const auth = require(path.join(appRoot, 'src/main/handlers/auth'));
    auth.register();
    const errors = [];
    window = new BrowserWindow({ show: false, width: 480, height: 790, webPreferences: { preload: path.join(appRoot, 'preload.js'), contextIsolation: true, nodeIntegration: false, backgroundThrottling: false, offscreen: true } });
    require(path.join(appRoot, 'src/main/state')).loginWindow = window;
    window.webContents.session.webRequest.onBeforeRequest({ urls: ['https://*/*', 'http://*/*'] }, (_, cb) => cb({ cancel: true }));
    window.webContents.on('console-message', (_, level, message) => { if (level === 3 && !message.includes('ERR_BLOCKED_BY_CLIENT')) errors.push(message); });
    await window.loadFile(path.join(appRoot, 'login.html'));
    await waitFor(`document.getElementById('access-status').textContent.includes('Importe o arquivo')`);
    await waitFor(`document.getElementById('db-status-message').textContent.length > 0 && document.getElementById('receita-status-message').textContent.length > 0`);
    assert.equal(await window.webContents.executeJavaScript(`document.querySelector('label[for="receita-connection-string"]').textContent`), 'Receita · Banco do PortalDados');
    const input = path.join(root, 'teste.mbconfig');
    fs.writeFileSync(input, JSON.stringify({ version: 1, users: { Teste: { password: 'senha-ficticia', role: 'admin' } }, env: {} }));
    dialog.showOpenDialog = async () => ({ canceled: false, filePaths: [input] });
    await window.webContents.executeJavaScript(`document.getElementById('import-access-btn').click()`);
    await waitFor(`document.getElementById('access-status').textContent.includes('Acesso importado')`);
    assert.equal(auth.users.Teste.password, 'senha-ficticia');
    assert.equal(config.hasAccess(), true);
    const rejected = await window.webContents.executeJavaScript(`window.electronAPI.loginAttempt('Teste', 'incorreta', false)`);
    assert.equal(rejected.success, false);

    // Exercise the real preload/login UI with synthetic save results; never connect to a database.
    const Store = require('electron-store'), settings = new Store(), saved = [];
    await window.webContents.executeJavaScript(`document.getElementById('database-settings').open=true`);
    for (const [channel, key] of [['save-and-test-db-connection', 'db_connection_string'], ['save-and-test-receita-connection', 'receita_connection_string']]) {
        ipcMain.removeHandler(channel);
        ipcMain.handle(channel, (_event, uri) => {
            saved.push({ key, uri });
            if (uri.endsWith('/invalid')) return { success: false, message: 'Falha sintética. O acesso anterior foi preservado.' };
            settings.set(key, uri);
            return { success: true };
        });
    }
    await window.webContents.executeJavaScript(`document.getElementById('db-connection-string').value='postgresql://synthetic/neon'; document.getElementById('test-db-btn').click()`);
    await waitFor(`document.getElementById('db-status-message').textContent.includes('configurado')`);
    await window.webContents.executeJavaScript(`document.getElementById('receita-connection-string').value='postgresql://synthetic/receita'; document.getElementById('test-receita-btn').click()`);
    await waitFor(`document.getElementById('receita-status-message').textContent.includes('configurado')`);
    assert.deepEqual(saved.map(item => item.key), ['db_connection_string', 'receita_connection_string']);
    assert.equal(await window.webContents.executeJavaScript(`document.getElementById('db-connection-string').value + document.getElementById('receita-connection-string').value`), '');
    await window.webContents.executeJavaScript(`document.getElementById('receita-connection-string').value='postgresql://synthetic/invalid'; document.getElementById('test-receita-btn').click()`);
    await waitFor(`document.getElementById('receita-status-message').textContent.includes('preservado')`);
    assert.equal(settings.get('receita_connection_string'), 'postgresql://synthetic/receita');
    await window.webContents.executeJavaScript(`document.getElementById('username').value='Teste'; document.getElementById('password').value='incorreta'; document.getElementById('login-form').dispatchEvent(new Event('submit',{cancelable:true,bubbles:true}))`);
    await waitFor(`document.getElementById('message').textContent.includes('Salvar e testar')`);
    await window.loadFile(path.join(appRoot, 'login.html'));
    await waitFor(`document.getElementById('db-status-message').textContent.includes('Neon salvo') && document.getElementById('receita-status-message').textContent.includes('Receita salva')`);
    assert.equal(await window.webContents.executeJavaScript(`document.getElementById('db-connection-string').value + document.getElementById('receita-connection-string').value`), '');
    // Hidden BrowserWindows pause animation frames; measure the settled layout.
    await window.webContents.insertCSS('.login-container { animation: none !important; transform: none !important; }');
    assert.equal(await window.webContents.executeJavaScript(`document.querySelector('.login-container').getBoundingClientRect().bottom <= window.innerHeight`), true);
    assert.deepEqual(errors, []);
    fs.mkdirSync(path.resolve(__dirname, '../out'), { recursive: true });
    await delay(100);
    fs.writeFileSync(path.resolve(__dirname, '../out/private-access.png'), (await window.webContents.capturePage()).toPNG());
    await window.webContents.executeJavaScript(`document.getElementById('database-settings').open=true`);
    await delay(100);
    fs.writeFileSync(path.resolve(__dirname, '../out/login-databases.png'), (await window.webContents.capturePage()).toPNG());

    // Exercise Electron's real ASAR reader with an old, synthetic installation.
    const old = path.join(root, 'old');
    fs.mkdirSync(old);
    fs.writeFileSync(path.join(old, 'users.json'), JSON.stringify({ Legado: { password: 'senha-ficticia', role: 'admin' } }));
    fs.writeFileSync(path.join(old, '.env'), 'SMTP_USER=teste@example.invalid');
    const backup = path.join(folders.appData, 'MB Finance', 'Gerenciador de Bases', 'legacy-app.asar');
    fs.mkdirSync(path.dirname(backup), { recursive: true });
    await asar.createPackage(old, backup);
    const migrated = createPrivateConfig({ app: { isPackaged: true, getPath: name => name === 'userData' ? path.join(root, 'migrated') : folders[name] }, projectRoot: root, environment: {} });
    migrated.initialize();
    assert.equal(migrated.loadUsers().Legado.password, 'senha-ficticia');
    console.log('Acesso aprovado: instalação nova, importação privada, dois bancos no login, salvamento independente, persistência sem expor acessos, senha inválida e migração de ASAR real, sem serviços externos.');
}).catch(error => { console.error(error); exitCode = 1; }).finally(() => {
    clearTimeout(timeout);
    if (window && !window.isDestroyed()) window.destroy();
    try { require('original-fs').rmSync(root, { recursive: true, force: true }); }
    catch { console.warn('Pasta temporária de teste ainda em uso pelo Electron.'); }
    app.exit(exitCode);
});
