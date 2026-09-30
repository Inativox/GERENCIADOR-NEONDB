/** Teste de integração local: não carrega main.js, usuários ou conexões reais. */
const { app, BrowserWindow, ipcMain } = require('electron');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const fsSync = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const ExcelJS = require('exceljs');

const userData = fsSync.mkdtempSync(path.join(os.tmpdir(), 'base-manager-smoke-'));
app.setPath('userData', userData);
let window;
let exitCode = 0;
let startCount = 0;
let pathsReceived;
const appRoot = process.argv[2] ? path.resolve(process.argv[2]) : path.resolve(__dirname, '..');
const errors = [];
let selectedPaths;
let savedUiSettings;
let releaseUiSettings;
let localStartCount = 0;
const timeout = setTimeout(() => { console.error('Smoke: tempo limite excedido'); app.exit(1); }, 25000);
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

async function evaluate(source) { return window.webContents.executeJavaScript(source); }
async function waitFor(source) {
    for (let attempt = 0; attempt < 150; attempt++) {
        if (await evaluate(source)) return;
        await delay(20);
    }
    throw new Error(`Estado não alcançado: ${source}`);
}

app.disableHardwareAcceleration();
app.on('window-all-closed', () => {});
app.whenReady().then(async () => {
    const input = path.join(userData, 'base & teste.xlsx');
    const skipped = path.join(userData, 'invalido.csv');
    const workbook = new ExcelJS.Workbook();
    const sheet = workbook.addWorksheet('Base');
    sheet.addRow(['Nome do Negócio', 'CNPJ', 'Telefone Celular']);
    sheet.addRow(['Empresa Sol', '04.252.011/0001-10', '5521998364849']);
    await workbook.xlsx.writeFile(input);
    await fs.writeFile(skipped, 'nome;cnpj;telefone');
    selectedPaths = [input, skipped];
    // Registra somente o handler local de colunas; banco e usuários reais não são carregados.
    require(path.join(appRoot, 'src/main/state')).currentUser = { username: 'Davi', role: 'admin' };
    require(path.join(appRoot, 'src/main/handlers/limpezaColunas')).register();
    require(path.join(appRoot, 'src/main/handlers/limpeza')).register();
    ipcMain.handle('get-ui-settings', () => new Promise(resolve => {
        releaseUiSettings = () => resolve({ checkBlocklist: false, checkDb: true, saveToDb: true, autoAdjust: false, organizeType: 'empresaAqui', mergeStrategy: 'custom' });
    }));
    ipcMain.on('save-ui-settings', (_event, settings) => { savedUiSettings = settings; });
    ipcMain.on('start-cleaning', () => { localStartCount++; });
    ipcMain.handle('get-db-connection-string', () => '');
    ipcMain.handle('get-enriched-cnpj-count', () => 0);
    ipcMain.handle('get-blocklist-stats', () => ({ success: true, total: 0 }));
    ipcMain.handle('get-key-file-status', () => ({ loaded: false }));
    ipcMain.handle('select-file', () => selectedPaths);
    ipcMain.on('start-limpeza-colunas', (_event, paths) => { startCount++; pathsReceived = paths; });
    window = new BrowserWindow({
        show: false, width: 1400, height: 1000,
        webPreferences: {
            preload: path.join(appRoot, 'preload.js'),
            contextIsolation: true, nodeIntegration: false, backgroundThrottling: false, offscreen: true,
        },
    });
    // O app precisa carregar suas abas mesmo sem acesso às fontes e ao CDN.
    window.webContents.session.webRequest.onBeforeRequest({ urls: ['https://*/*', 'http://*/*'] }, (_details, callback) => callback({ cancel: true }));
    window.webContents.on('console-message', (_event, level, message) => {
        if (level === 3 && !/ERR_BLOCKED_BY_CLIENT/.test(message)) errors.push(message);
    });
    await window.loadFile(path.join(appRoot, 'index.html'));
    await waitFor(`!!document.getElementById('columns-select')`);
    window.webContents.send('user-info', { username: 'Outro', role: 'admin' });
    await waitFor(`document.getElementById('checkBlocklistCheckbox').disabled`);
    releaseUiSettings();
    await waitFor(`document.getElementById('log').textContent.includes('Configurações da última sessão foram restauradas')`);
    assert.equal(await evaluate(`document.getElementById('checkBlocklistCheckbox').checked`), true, 'Preferência antiga não desativa blocklist obrigatória');
    await evaluate(`document.getElementById('resetLocalBtn').click()`);
    assert.equal(await evaluate(`document.getElementById('checkBlocklistCheckbox').checked && document.getElementById('checkBlocklistCheckbox').disabled`), true);
    window.webContents.send('user-info', { username: 'Davi', role: 'admin' });
    await waitFor(`!document.getElementById('checkBlocklistCheckbox').disabled`);
    await evaluate(`document.getElementById('resetLocalBtn').click(); document.getElementById('checkBlocklistCheckbox').click()`);
    assert.equal(await evaluate(`document.getElementById('checkBlocklistCheckbox').checked`), true);
    await evaluate(`document.getElementById('checkBlocklistCheckbox').click()`);
    assert.equal(await evaluate(`document.getElementById('checkBlocklistCheckbox').checked`), false);
    assert.equal(await evaluate(`document.body.classList.contains('light-theme')`), true);
    assert.equal(await evaluate(`['adjustPhonesBtn', 'feedBlocklistBtn', 'saveStoredCnpjsBtn'].some(id => document.getElementById(id)) || !!document.querySelector('.auxiliary-tools')`), false);
    assert.equal(await evaluate(`['startAdjustPhones', 'saveStoredCnpjsToExcel'].some(key => key in window.electronAPI)`), false);
    assert.equal(await evaluate(`document.getElementById('autoAdjustPhonesCheckbox').checked && document.getElementById('autoAdjustPhonesCheckbox').disabled`), true);
    assert.equal(await evaluate(`document.getElementById('cadence-mode-flag').hidden`), false);
    await evaluate(`document.getElementById('autoRootBtn').click()`);
    await delay(30);
    assert.equal(await evaluate(`document.getElementById('cadence-mode-flag').hidden`), true);
    assert.equal(savedUiSettings.autoRoot, true);
    await evaluate(`document.getElementById('autoRootBtn').click()`);
    await delay(30);
    assert.equal(await evaluate(`document.getElementById('cadence-mode-flag').hidden`), false);
    assert.equal(savedUiSettings.autoRoot, false);
    assert.equal(await evaluate(`['deleteBatchBtn', 'splitListBtn', 'organizeDailySheetBtn', 'startMergeBtn', 'checkDbCheckbox', 'saveToDbCheckbox', 'consultDbBtn', 'updateBlocklistBtn', 'extra-tools-panel'].some(id => document.getElementById(id))`), false);
    assert.equal(await evaluate(`['deleteBatch', 'splitList', 'organizeDailySheet', 'startMerge', 'startDbOnlyCleaning', 'updateBlocklist'].some(key => typeof window.electronAPI[key] !== 'undefined')`), false);
    await evaluate(`document.querySelector('[data-tab-name="limpezaColunas"]').click()`);
    await waitFor(`document.getElementById('limpezaColunas').classList.contains('active')`);
    assert.equal(await evaluate(`document.getElementById('columns-start').disabled`), true);
    await evaluate(`document.getElementById('columns-select').click()`);
    await waitFor(`document.querySelectorAll('.columns-files li').length === 2`);
    assert.equal(await evaluate(`document.querySelectorAll('.columns-files img').length`), 0);
    await evaluate(`document.getElementById('columns-start').click(); document.getElementById('columns-start').click()`);
    await waitFor(`document.querySelector('.columns-status--running') !== null`);
    await delay(30);
    assert.equal(startCount, 1);
    assert.deepEqual(pathsReceived, selectedPaths);
    assert.equal(await evaluate(`document.getElementById('columns-select').disabled`), true);
    for (let index = 0; index < 2000; index++) {
        window.webContents.send('limpeza-colunas-log', `linha ${index}`);
        window.webContents.send('log', `legado ${index}`);
    }
    window.webContents.send('limpeza-colunas-log', '<img src=x onerror=alert(1)>');
    await waitFor(`document.querySelector('.columns-status--done') !== null && document.getElementById('columns-activity').textContent.includes('Finalizado:')`);
    assert.ok(await evaluate(`document.querySelectorAll('#columns-activity > p').length <= 300`));
    assert.equal(await evaluate(`document.querySelectorAll('#columns-activity img').length`), 0);
    assert.equal(await evaluate(`document.querySelector('.columns-progress progress').value`), 100);
    assert.equal(await evaluate(`document.getElementById('columns-start').disabled`), false);
    const output = new ExcelJS.Workbook();
    await output.xlsx.readFile(path.join(userData, 'base & teste_LIMPO.xlsx'));
    assert.deepEqual(output.worksheets[0].getRow(2).values.slice(1), ['Empresa Sol', 4252011000110, 21998364849]);
    await waitFor(`document.getElementById('log').textContent.includes('legado 1999')`);
    assert.ok(await evaluate(`document.querySelectorAll('#log > p').length <= 1000`));
    window.webContents.send('update-ready', { version: '2.0.0' });
    await waitFor(`document.getElementById('upd-title').textContent === 'Atualização pronta'`);
    assert.equal(await evaluate(`getComputedStyle(document.getElementById('update-overlay')).pointerEvents`), 'none');
    assert.equal(await evaluate(`typeof window.require`), 'undefined');
    assert.deepEqual(errors, []);
    await delay(300);
    await fs.mkdir(path.resolve(__dirname, '../out'), { recursive: true });
    await fs.writeFile(path.resolve(__dirname, '../out/renderer-smoke.png'), (await window.webContents.capturePage()).toPNG());

    // A limpeza local usa somente arquivos temporários; nenhum banco real é aberto.
    const localFiles = [path.join(userData, 'lista A.xlsx'), path.join(userData, 'lista B.xlsx')];
    for (const [index, file] of localFiles.entries()) {
        const localWorkbook = new ExcelJS.Workbook();
        const localSheet = localWorkbook.addWorksheet('Base');
        localSheet.addRow(['cnpj', 'nome', 'fone1', 'livre5', 'fone2']);
        localSheet.addRow(['12345678000199', '123 Empresa Sol 456', '5521998364849', '']);
        if (index === 1) {
            localSheet.addRow(['22345678000199', '123 Empresa Lua 456', '21998364849', '', '11987654321']);
            localSheet.addRow(['32345678000199', 'Sem contato', '11111111', '', '9999999']);
        }
        await localWorkbook.xlsx.writeFile(file);
    }
    selectedPaths = localFiles;
    await evaluate(`document.getElementById('update-overlay').classList.remove('visible'); document.querySelector('[data-tab-name="local"]').click(); document.getElementById('resetLocalBtn').click()`);
    await evaluate(`document.getElementById('fillLivre5Checkbox').click(); document.getElementById('backupCheckbox').click()`);
    await waitFor(`document.getElementById('backupCheckbox').checked`);
    assert.equal(savedUiSettings.fillLivre5, true);
    assert.equal(savedUiSettings.backup, true);
    assert.equal(savedUiSettings.autoAdjust, true);
    assert.equal(Object.hasOwn(savedUiSettings, 'checkDb'), false);
    assert.equal(Object.hasOwn(savedUiSettings, 'saveToDb'), false);
    await evaluate(`document.getElementById('addCleanFileBtn').click()`);
    await waitFor(`document.querySelectorAll('#progressContainer .file-progress').length === 2`);
    await evaluate(`document.getElementById('startCleaningBtn').click(); document.getElementById('startCleaningBtn').click()`);
    assert.equal(await evaluate(`document.getElementById('resetLocalBtn').disabled`), true);
    await waitFor(`document.getElementById('localCleaningStatus').textContent === 'Lote concluído'`);
    assert.equal(localStartCount, 1);
    assert.equal(await evaluate(`document.getElementById('startCleaningBtn').disabled`), false);
    assert.equal(await evaluate(`document.getElementById('checkBlocklistCheckbox').disabled`), false);
    assert.equal(await evaluate(`document.getElementById('autoAdjustPhonesCheckbox').checked && document.getElementById('autoAdjustPhonesCheckbox').disabled`), true);
    await waitFor(`document.getElementById('log').textContent.includes('Processo concluído para todos os arquivos')`);
    for (const [index, file] of localFiles.entries()) {
        const cleaned = new ExcelJS.Workbook();
        await cleaned.xlsx.readFile(file);
        assert.equal(cleaned.worksheets[0].rowCount, 2);
        assert.equal(cleaned.worksheets[0].getCell('B2').value, index === 0 ? 'Empresa Sol' : 'Empresa Lua');
        assert.equal(cleaned.worksheets[0].getCell('C2').value, index === 0 ? 21998364849 : 11987654321);
        assert.equal(cleaned.worksheets[0].getCell('E2').value, null);
        assert.match(cleaned.worksheets[0].getCell('D2').value, /lista [AB] \| \d{2}\/\d{2}\/\d{4}/);
    }
    await delay(100);
    await fs.writeFile(path.resolve(__dirname, '../out/workspace-light.png'), (await window.webContents.capturePage()).toPNG());
    await evaluate(`document.getElementById('dark-theme-btn').click()`);
    assert.equal(await evaluate(`document.body.classList.contains('dark-theme')`), true);
    await evaluate(`document.getElementById('light-theme-btn').click()`);
    window.setSize(960, 800);
    await delay(100);
    assert.equal(await evaluate(`document.getElementById('localGrid').scrollWidth <= document.getElementById('localGrid').clientWidth + 1`), true);
    await fs.writeFile(path.resolve(__dirname, '../out/workspace-compact.png'), (await window.webContents.capturePage()).toPNG());
    assert.deepEqual(errors, []);
    console.log('Smoke aprovado: React/worker, limpeza local sequencial, controles removidos, preferências, bloqueio de lote, temas e layout compacto, sem banco de produção.');
}).catch(error => {
    console.error(error);
    if (errors.length) console.error('Erros do renderer:', errors);
    exitCode = 1;
}).finally(async () => {
    clearTimeout(timeout);
    if (window && !window.isDestroyed()) window.destroy();
    if (userData) await fs.rm(userData, { recursive: true, force: true }).catch(() => {});
    app.exit(exitCode);
});
