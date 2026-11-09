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
let localBqLoads = 0;
let localBqFailure = false;
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
    const mainState = require(path.join(appRoot, 'src/main/state'));
    mainState.currentUser = { username: 'Davi', role: 'admin' };
    mainState.bqRootService = { async loadRoot(pipelines, { onProgress }) {
        localBqLoads++;
        assert.deepEqual(pipelines, [119]);
        await delay(30);
        if (localBqFailure) throw new Error('Falha BQ sintética. Originais preservados.');
        onProgress({ documents: 1 });
        return { documents: ['04252011000110'] };
    } };
    require(path.join(appRoot, 'src/main/handlers/limpezaColunas')).register();
    require(path.join(appRoot, 'src/main/handlers/limpeza')).register();
    ipcMain.handle('get-ui-settings', () => new Promise(resolve => {
        releaseUiSettings = () => resolve({ autoRoot: true, autoRootSource: 'bq', autoRootOperation: 'santander', machineCnaes: true, checkBlocklist: false, checkDb: true, saveToDb: true, autoAdjust: false, organizeType: 'empresaAqui', mergeStrategy: 'custom' });
    }));
    ipcMain.on('save-ui-settings', (_event, settings) => { savedUiSettings = settings; });
    ipcMain.on('start-cleaning', () => { localStartCount++; });
    ipcMain.handle('get-db-connection-string', () => '');
    ipcMain.handle('get-enriched-cnpj-count', () => 0);
    ipcMain.handle('get-blocklist-stats', () => ({ success: true, total: 0 }));
    ipcMain.handle('get-key-file-status', () => ({ loaded: false }));
    const { defaults, validateFlow, OPERATIONS, MAX_ROWS } = require(path.join(appRoot, 'src/main/flows/config'));
    const flowFormats = require(path.join(appRoot, 'src/main/flows/formats'));
    const syntheticJob = { id: 'smoke-job', flowId: 'smoke-flow', flowName: 'Fluxo de teste', owner: 'Davi', status: 'running', stage: 'cleaning', counts: { generated: 1000, kept: 100 }, progress: { stage: 'cleaning', processed: 250, total: 1000 }, flowSnapshot: { ...defaults(), api: { enabled: false } }, logs: [], outputs: [] };
    let savedFlow;
    ipcMain.handle('flows-bootstrap', () => ({ success: true, user: { username: 'Davi', role: 'admin' }, flows: savedFlow ? [savedFlow] : [], jobs: [syntheticJob], defaults: defaults(), operations: OPERATIONS, formats: flowFormats.listFormats(), layoutFields: [], access: {}, limits: { maxRows: MAX_ROWS } }));
    ipcMain.handle('flows-save', (_event, input) => {
        try { savedFlow = validateFlow(input, savedFlow); return { success: true, flow: savedFlow }; }
        catch (error) { return { success: false, message: error.message }; }
    });
    ipcMain.handle('flows-receita-options', () => ({ success: true, options: [] }));
    ipcMain.handle('flows-test-bq', () => ({ success: true, message: 'Acesso BQ sintético aprovado.' }));
    ipcMain.handle('receita-situacao-state', () => ({ success: true, job: null, templates: [], configured: false }));
    ipcMain.handle('mailing-import-state', () => ({ success: true, admin: true, configured: true, maxRows: 250000, maxQueue: 50, queue: { running: false, items: [] }, services: [{ id: 63, group: 'Santander', label: 'Santander', name: 'SANTANDER' }] }));
    // Alerta de Mailing com dados sintéticos: uma lista para trocar, uma acabando, um transbordo e um serviço parado.
    const linhaAlerta = (servicoId, servico, extra) => ({ servicoId, servico, mailingId: servicoId * 10, arquivo: `lista-${servicoId}.csv`, registros: 10000, restantes: 4000, percentual: 60, situacao: 'OK', acao: 'NADA', reciclagensHoje: 0, limiteReciclagens: 2, minutosRestantes: 95, ritmoPorMinuto: 42, transbordo: false, parado: false, ...extra });
    const alertaPainel = { configurado: true, login: { saved: true, login: 'synthetic', canSave: true }, reciclador: false, foco: null, conectado: true, erro: null, verificando: false, intervaloMinutos: 3, verificadoEm: new Date().toISOString(),
        linhas: [linhaAlerta(92, 'PAG CADENCIA', { restantes: 0, percentual: 100, situacao: 'ESGOTADO', acao: 'TROCAR', reciclagensHoje: 2, minutosRestantes: null }),
            linhaAlerta(87, 'PAG PRINCIPAL', { restantes: 70, percentual: 99.3, situacao: 'QUASE', acao: 'PODE_RECICLAR', reciclagensHoje: 1, minutosRestantes: 2 }),
            linhaAlerta(120, 'TRANSBORDO LTDA', { transbordo: true }), linhaAlerta(18, 'MP PRINCIPAL', { parado: true, ritmoPorMinuto: 0, minutosRestantes: null })],
        eventos: [{ id: 'e1', hora: '09:12', tipo: 'TROCAR', servico: 'PAG CADENCIA', texto: 'lista-92.csv já foi reciclada 2x hoje (limite 2). Suba uma lista nova.', servicoId: 92, mailingId: 920, reciclagens: 2, status: 'aberta', motivo: '', horaFim: '', oculto: false },
            { id: 'e2', hora: '08:40', tipo: 'RECICLADA', servico: 'PAG CADENCIA', texto: 'lista-92.csv · 600 registros voltaram para a discagem · reciclagens hoje 2/2', servicoId: 92, mailingId: 920, reciclagens: 2, status: 'resolvida', motivo: 'reciclagem automática', horaFim: '08:40', oculto: false }] };
    ipcMain.handle('alerta-mailing-state', () => ({ success: true, painel: alertaPainel }));
    // Ritmo do Discador com dados sintéticos: um serviço prestes a descer, um sem regra, um pausado por mudança na Fastway e um sem ninguém logado.
    const ritmoPainel = painelRitmoSintetico();
    let ritmoAssistindo = null;
    ipcMain.handle('ritmo-discador-state', () => ({ success: true, painel: ritmoPainel }));
    ipcMain.handle('ritmo-discador-assistir', (_event, { ativo }) => { ritmoAssistindo = ativo; return { success: true }; });
    const ritmoAtivacoes = [];
    ipcMain.handle('ritmo-discador-ativa', (_event, { ligar }) => { ritmoAtivacoes.push(ligar); ritmoPainel.ativa = ligar; return { success: true, painel: ritmoPainel }; });
    ipcMain.handle('select-file', () => selectedPaths);
    ipcMain.on('start-limpeza-colunas', (_event, paths) => { startCount++; pathsReceived = paths; });
    window = new BrowserWindow({
        show: false, width: 1400, height: 1000,
        webPreferences: {
            preload: path.join(appRoot, 'preload.js'),
            contextIsolation: true, nodeIntegration: false, backgroundThrottling: false, offscreen: true,
        },
    });
    mainState.mainWindow = window;
    // O app precisa carregar suas abas mesmo sem acesso às fontes e ao CDN.
    window.webContents.session.webRequest.onBeforeRequest({ urls: ['https://*/*', 'http://*/*'] }, (_details, callback) => callback({ cancel: true }));
    window.webContents.on('console-message', (_event, level, message) => {
        if (level === 3 && !/ERR_BLOCKED_BY_CLIENT/.test(message)) errors.push(message);
    });
    await window.loadFile(path.join(appRoot, 'index.html'));
    await waitFor(`!!document.getElementById('columns-select')`);
    await waitFor(`!!document.querySelector('#fluxos-react-root .flows-app')`);
    await evaluate(`document.querySelector('[data-tab-name="fluxos"]').click()`);
    assert.equal(await evaluate(`document.getElementById('fluxos').classList.contains('active')`), true);
    window.webContents.send('user-info', { username: 'Outro', role: 'admin' });
    await waitFor(`document.getElementById('checkBlocklistCheckbox').disabled`);
    // A pilha da barra lateral tem uma placa por seção visível e acende a da aba ativa.
    const navStack = `(() => { const links = [...document.querySelectorAll('.sidebar-nav .tab-button')], plates = [...document.querySelectorAll('.nav-plate:not(.nav-plate--slot)')];
        return { visible: links.filter(link => link.parentElement.style.display !== 'none').length, plates: plates.filter(plate => !plate.hidden).length,
            active: plates.filter(plate => plate.classList.contains('is-active')).map(plate => plate.dataset.plate), current: links.find(link => link.classList.contains('active'))?.dataset.tabName }; })()`;
    await waitFor(`(${navStack}).active[0] === (${navStack}).current`);
    const stack = await evaluate(navStack);
    assert.equal(stack.plates, stack.visible); assert.deepEqual(stack.active, [stack.current]);
    assert.equal(await evaluate(`document.getElementById('currentUserInitial').textContent`), 'O');
    releaseUiSettings();
    await waitFor(`document.getElementById('log').textContent.includes('Configurações da última sessão foram restauradas')`);
    assert.equal(await evaluate(`document.getElementById('autoRootBtn').dataset.on`), 'true');
    assert.equal(await evaluate(`document.getElementById('autoRootSource').value`), 'bq');
    assert.equal(await evaluate(`document.getElementById('autoRootOperation').value`), 'santander');
    assert.equal(await evaluate(`document.getElementById('checkBlocklistCheckbox').checked`), true, 'Preferência antiga não desativa blocklist obrigatória');
    assert.equal(await evaluate(`document.getElementById('machineCnaesCheckbox').checked`), true, 'CNAEs de máquina volta ligada ao abrir o app');
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
    assert.equal(await evaluate(`document.getElementById('autoRootBqOptions').hidden`), false);
    assert.equal(await evaluate(`document.getElementById('autoRootOperation').options.length`), 4);
    await evaluate(`(() => { const select = document.getElementById('autoRootSource'); select.value = 'neon'; select.dispatchEvent(new Event('change')); })()`);
    assert.equal(await evaluate(`document.getElementById('autoRootBqOptions').hidden`), true);
    assert.equal(savedUiSettings.autoRootSource, 'neon');
    await evaluate(`(() => { const select = document.getElementById('autoRootSource'); select.value = 'bq'; select.dispatchEvent(new Event('change')); })()`);
    assert.equal(savedUiSettings.autoRootSource, 'bq');
    assert.equal(savedUiSettings.autoRootOperation, 'santander');
    window.webContents.send('flow-bq-auth-update', { owner: 'Davi', state: 'renewing', message: 'Login Google sintético em andamento.' });
    await waitFor(`document.getElementById('localBqLoginBtn').disabled`);
    await evaluate(`document.getElementById('startCleaningBtn').click()`);
    await delay(30);
    assert.equal(localStartCount, 0);
    window.webContents.send('flow-bq-auth-update', { owner: 'Davi', state: 'ready', message: 'Login verificado.' });
    await waitFor(`!document.getElementById('localBqLoginBtn').disabled`);
    assert.equal(await evaluate(`document.getElementById('localBqStatus').textContent`), 'Acesso Google pronto. Você pode iniciar a limpeza local.');
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
    await evaluate(`document.querySelector('[data-tab-name="importarMailing"]').click()`);
    assert.equal(await evaluate(`document.querySelector('[data-tab-name="importarMailing"] span').textContent`), 'Importar Mailing');
    await waitFor(`!!document.querySelector('#importarMailing .flow-view-switch')`);
    const visible = selector => `document.querySelector('#importarMailing ${selector}').offsetParent !== null`;
    assert.equal(await evaluate(`${visible('.mailing-app')} && !(${visible('.receita-situacao-app')})`), true, 'Importar Mailing abre primeiro');
    await evaluate(`Array.from(document.querySelectorAll('#importarMailing .flow-view-switch button')).find(item => item.textContent === 'Situação Receita').click()`);
    await waitFor(`${visible('.receita-situacao-app')} && !(${visible('.mailing-app')})`);
    assert.equal(await evaluate(`typeof window.require`), 'undefined');
    assert.deepEqual(errors, []);
    await delay(300);
    await fs.mkdir(path.resolve(__dirname, '../out'), { recursive: true });
    await fs.writeFile(path.resolve(__dirname, '../out/renderer-smoke.png'), (await window.webContents.capturePage()).toPNG());
    // A aba do Alerta de Mailing é um mural: o serviço mais grave vem primeiro, maior, e o clique abre o que fazer.
    await evaluate(`document.querySelector('[data-tab-name="alertaMailing"]').click()`);
    await waitFor(`document.querySelectorAll('#alertaMailing .alerta-tank').length === 4`);
    assert.equal(await evaluate(`document.querySelector('#alertaMailing .alerta-tank .alerta-tank-nome').textContent`), 'PAG CADENCIA');
    assert.equal(await evaluate(`document.querySelector('#alertaMailing .alerta-tank[data-big] .alerta-tank-verbo').textContent`), 'Trocar a lista');
    assert.equal(await evaluate(`document.querySelectorAll('#alertaMailing .alerta-tank[data-big]').length`), 1);
    await evaluate(`document.querySelector('#alertaMailing .alerta-tank').click()`);
    await waitFor(`document.querySelector('#alertaMailing .alerta-sheet .alerta-acao strong')?.textContent === 'Trocar a lista'`);
    await delay(300);
    await fs.writeFile(path.resolve(__dirname, '../out/alerta-mailing.png'), (await window.webContents.capturePage()).toPNG());
    // Ritmo do Discador: um canal por serviço, o alvo da próxima leitura no canal pendente e a regra na gaveta.
    await evaluate(`document.querySelector('[data-tab-name="ritmoDiscador"]').click()`);
    await waitFor(`document.querySelectorAll('#ritmoDiscador .rd-canal').length === 4`);
    // Teto geral: a soma acima do teto aparece no cabeçalho e num aviso.
    assert.equal(await evaluate(`document.querySelector('#ritmoDiscador .rd-teto').textContent.includes('URAs 1.669 de 1.500')`), true);
    assert.equal(await evaluate(`[...document.querySelectorAll('#ritmoDiscador .rd-faixa')].some(f => f.textContent.includes('A soma das URAs (1.669) passou do teto de 1.500'))`), true);
    assert.match(await evaluate(`document.querySelector('#ritmoDiscador .rd-canal[data-id="120"] .rd-acao').textContent`), /Desce para 1\.270 se a próxima leitura confirmar/);
    assert.equal(await evaluate(`document.querySelector('#ritmoDiscador .rd-canal[data-id="120"] .rd-fader-alvo').dataset.alvo`), '1270');
    assert.equal(await evaluate(`document.querySelector('#ritmoDiscador .rd-canal[data-id="87"] .rd-tubo-vazio').textContent`), 'ninguém logado');
    assert.equal(await evaluate(`document.querySelector('#ritmoDiscador .rd-canal[data-id="123"] .rd-canal-abrir').textContent`), 'Criar regra');
    // Ajustes de hoje: agrupados por campanha, com o saldo do dia e o detalhe de cada ajuste.
    assert.deepEqual(await evaluate(`[...document.querySelectorAll('#ritmoDiscador .rd-camp-nome b')].map(b => b.textContent)`), ['MERCADO PAGO 3', 'C6 BANK PT1', 'Automação']);
    const c6 = `[...document.querySelectorAll('#ritmoDiscador .rd-camp')].find(c => c.querySelector('.rd-camp-nome b').textContent === 'C6 BANK PT1')`;
    assert.equal(await evaluate(`${c6}.querySelector('.rd-camp-dia').textContent`), '1.000 → 1.370+370');
    assert.match(await evaluate(`${c6}.querySelector('.rd-camp-resumo').textContent`), /^4 subidas · 1 descida · último às /);
    assert.equal(await evaluate(`${c6}.querySelectorAll('.rd-hist li').length`), 4, 'mostra os 4 últimos');
    assert.equal(await evaluate(`${c6}.querySelectorAll('.rd-hist li')[1].querySelector('p').textContent`), 'ocupação 40% · 4 falando, 6 livres, 3 em pausa, 2 em outro estado · abaixo de 60% · subiu só o que cabia no teto das URAs');
    await evaluate(`${c6}.querySelector('.rd-camp-mais').click()`);
    await waitFor(`${c6}.querySelectorAll('.rd-hist li').length === 5`);
    await evaluate(`document.querySelector('#ritmoDiscador .rd-hist-rolagem').scrollIntoView({ block: 'end' })`);
    await delay(300);
    await fs.writeFile(path.resolve(__dirname, '../out/ritmo-discador-ajustes.png'), (await window.webContents.capturePage()).toPNG());
    for (let attempt = 0; attempt < 50 && ritmoAssistindo !== true; attempt++) await delay(20);
    assert.equal(ritmoAssistindo, true, 'A aba visível pede leituras ao main');
    await evaluate(`document.querySelector('#ritmoDiscador .rd-canal[data-id="120"] .rd-canal-abrir').click()`);
    await waitFor(`!!document.querySelector('#ritmoDiscador .rd-gaveta input[name="desce"]')`);
    assert.deepEqual(await evaluate(`['desce', 'sobe', 'passo', 'minimo', 'maximo'].map(k => document.querySelector('#ritmoDiscador .rd-gaveta input[name="' + k + '"]').value)`), ['80', '45', '100', '800', '1600']);
    assert.equal(await evaluate(`document.activeElement.getAttribute('aria-label')`), 'Fechar');
    await delay(300);
    await fs.writeFile(path.resolve(__dirname, '../out/ritmo-discador.png'), (await window.webContents.capturePage()).toPNG());
    await evaluate(`document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape' }))`);
    await waitFor(`!document.querySelector('#ritmoDiscador .rd-gaveta') && document.activeElement?.dataset.id === '120'`);
    // Desligar é direto; ligar passa por um modal que mostra os serviços com regra e só liga ao confirmar.
    const chaveAtiva = `[...document.querySelectorAll('#ritmoDiscador .rd-switch')].find(s => s.textContent.includes('Ativa neste PC')).querySelector('input')`;
    assert.equal(await evaluate(`[...document.querySelectorAll('#ritmoDiscador .rd-switch')].some(s => s.textContent.includes('Simulação'))`), false);
    await evaluate(`${chaveAtiva}.click()`);
    await waitFor(`!${chaveAtiva}.checked`);
    await evaluate(`${chaveAtiva}.click()`);
    await waitFor(`!!document.querySelector('#ritmoDiscador .rd-modal')`);
    assert.equal(await evaluate(`document.querySelector('#ritmoDiscador .rd-modal h2').textContent`), 'Ligar a automação neste PC?');
    assert.equal(await evaluate(`document.querySelectorAll('#ritmoDiscador .rd-modal-lista li').length`), 3);
    assert.equal(await evaluate(`document.activeElement.textContent`), 'Cancelar', 'O foco começa no botão seguro');
    await delay(400);
    await fs.writeFile(path.resolve(__dirname, '../out/ritmo-discador-modal.png'), (await window.webContents.capturePage()).toPNG());
    await evaluate(`document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape' }))`);
    await waitFor(`!document.querySelector('#ritmoDiscador .rd-modal')`);
    assert.deepEqual(ritmoAtivacoes, [false], 'Cancelar não liga');
    await evaluate(`${chaveAtiva}.click()`);
    await waitFor(`!!document.querySelector('#ritmoDiscador .rd-modal')`);
    await evaluate(`[...document.querySelectorAll('#ritmoDiscador .rd-modal button')].find(b => b.textContent.includes('Ligar automação')).click()`);
    await waitFor(`!document.querySelector('#ritmoDiscador .rd-modal') && ${chaveAtiva}.checked`);
    assert.deepEqual(ritmoAtivacoes, [false, true]);

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
    for (let attempt = 0; attempt < 50 && ritmoAssistindo !== false; attempt++) await delay(20);
    assert.equal(ritmoAssistindo, false, 'Fora da aba, o Ritmo do Discador para de pedir leituras');
    await evaluate(`document.getElementById('fillLivre5Checkbox').click(); document.getElementById('backupCheckbox').click()`);
    await waitFor(`document.getElementById('backupCheckbox').checked`);
    assert.equal(savedUiSettings.fillLivre5, true);
    assert.equal(savedUiSettings.backup, true);
    assert.equal(savedUiSettings.keepCnaes, false);
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
        // O CSV adicional espelha a lista já limpa.
        const csvLines = (await fs.readFile(file.replace(/\.xlsx$/, '.csv'), 'utf8')).split('\r\n').filter(Boolean);
        assert.equal(csvLines[0], '\uFEFF"cnpj";"nome";"fone1";"livre5";"fone2"');
        assert.equal(csvLines.length, 2);
        assert.match(csvLines[1], new RegExp(`^"[12]2345678000199";"${index === 0 ? 'Empresa Sol' : 'Empresa Lua'}";"${index === 0 ? 21998364849 : 11987654321}";"lista [AB] \\| \\d{2}/\\d{2}/\\d{4}";""$`));
    }
    // "Manter só estes CNAEs": a chave mostra a caixa, a lista colada é salva e só fica o CNPJ
    // cujo CNAE principal está na lista. A Receita é sintética; a planilha não tem coluna de CNAE.
    const receitaCnaes = { '12345678000199': '4781400', '22345678000199': '4711302' };
    mainState.receitaCnaeService = { configured: () => true, async lookupCnaes(documents) { return new Map(documents.filter(cnpj => receitaCnaes[cnpj]).map(cnpj => [cnpj, receitaCnaes[cnpj]])); } };
    const cnaeFile = path.join(userData, 'lista CNAE.xlsx');
    const cnaeWorkbook = new ExcelJS.Workbook(), cnaeSheet = cnaeWorkbook.addWorksheet('Base');
    cnaeSheet.addRow(['cnpj', 'nome', 'fone1']);
    cnaeSheet.addRow(['12345678000199', 'Fica', '21998364849']);
    cnaeSheet.addRow(['22345678000199', 'Sai', '11987654321']);
    await cnaeWorkbook.xlsx.writeFile(cnaeFile);
    selectedPaths = [cnaeFile];
    await evaluate(`document.getElementById('resetLocalBtn').click()`);
    assert.equal(await evaluate(`document.getElementById('keepCnaesField').hidden`), true);
    await evaluate(`document.getElementById('keepCnaesCheckbox').click()`);
    await waitFor(`!document.getElementById('keepCnaesField').hidden`);
    await evaluate(`const box = document.getElementById('keepCnaesInput'); box.value = '4781-4/00'; box.dispatchEvent(new Event('input'))`);
    assert.equal(savedUiSettings.keepCnaes, true);
    assert.equal(savedUiSettings.keepCnaesText, '4781-4/00');
    await evaluate(`document.getElementById('addCleanFileBtn').click()`);
    await waitFor(`document.querySelectorAll('#progressContainer .file-progress').length === 1`);
    await evaluate(`document.getElementById('keepCnaesField').scrollIntoView({ block: 'center' })`);
    await delay(300);
    await fs.writeFile(path.resolve(__dirname, '../out/local-keep-cnaes.png'), (await window.webContents.capturePage()).toPNG());
    await evaluate(`document.getElementById('startCleaningBtn').click()`);
    assert.equal(await evaluate(`document.getElementById('keepCnaesInput').disabled && document.getElementById('keepCnaesCheckbox').disabled`), true);
    await waitFor(`document.getElementById('localCleaningStatus').textContent === 'Lote concluído'`);
    assert.equal(await evaluate(`document.getElementById('keepCnaesInput').disabled`), false);
    const cnaeResult = new ExcelJS.Workbook(); await cnaeResult.xlsx.readFile(cnaeFile);
    assert.equal(cnaeResult.worksheets[0].rowCount, 2);
    assert.equal(cnaeResult.worksheets[0].getCell('B2').value, 'Fica');
    assert.equal((await fs.readFile(cnaeFile.replace(/\.xlsx$/, '.csv'), 'utf8')).split('\r\n').filter(Boolean).length, 2);
    await evaluate(`document.getElementById('resetLocalBtn').click()`);
    assert.equal(await evaluate(`document.getElementById('keepCnaesCheckbox').checked || !document.getElementById('keepCnaesField').hidden`), false);
    // "CNAEs de máquina": a chave usa a lista salva sem nada colado, e Reiniciar desliga.
    const machineReceita = { '12345678000199': '5611201', '22345678000199': '6201501' };
    mainState.receitaCnaeService = { configured: () => true, async lookupCnaes(documents) { return new Map(documents.filter(cnpj => machineReceita[cnpj]).map(cnpj => [cnpj, machineReceita[cnpj]])); } };
    await cnaeWorkbook.xlsx.writeFile(cnaeFile);
    await evaluate(`document.getElementById('machineCnaesCheckbox').click()`);
    assert.equal(savedUiSettings.machineCnaes, true);
    assert.equal(savedUiSettings.keepCnaes, false);
    await evaluate(`document.getElementById('addCleanFileBtn').click()`);
    await waitFor(`document.querySelectorAll('#progressContainer .file-progress').length === 1`);
    await evaluate(`document.getElementById('startCleaningBtn').click()`);
    await waitFor(`document.getElementById('localCleaningStatus').textContent === 'Lote concluído'`);
    const machineResult = new ExcelJS.Workbook(); await machineResult.xlsx.readFile(cnaeFile);
    assert.equal(machineResult.worksheets[0].rowCount, 2);
    assert.equal(machineResult.worksheets[0].getCell('B2').value, 'Fica');
    await evaluate(`document.getElementById('resetLocalBtn').click()`);
    assert.equal(await evaluate(`document.getElementById('machineCnaesCheckbox').checked`), false);
    // BQ uses a synthetic provider and temporary XLSX files, never Google or Neon.
    const bqFile = path.join(userData, 'lista BQ.xlsx');
    const bqWorkbook = new ExcelJS.Workbook(), bqSheet = bqWorkbook.addWorksheet('Base');
    bqSheet.addRow(['cnpj', 'nome', 'fone1']);
    bqSheet.addRow([4252011000110, 'Na raiz', '11987654321']);
    bqSheet.addRow(['22345678000199', 'Manter', '21987654321']);
    await bqWorkbook.xlsx.writeFile(bqFile);
    selectedPaths = [bqFile];
    await evaluate(`document.getElementById('resetLocalBtn').click(); document.getElementById('autoRootBtn').click(); document.getElementById('localBqTestBtn').click()`);
    await waitFor(`document.getElementById('localBqStatus').textContent === 'Acesso BQ sintético aprovado.'`);
    await evaluate(`document.getElementById('addCleanFileBtn').click()`);
    await waitFor(`document.querySelectorAll('#progressContainer .file-progress').length === 1`);
    await fs.writeFile(path.resolve(__dirname, '../out/local-bq-options.png'), (await window.webContents.capturePage()).toPNG());
    await evaluate(`document.getElementById('startCleaningBtn').click()`);
    assert.equal(await evaluate(`document.getElementById('autoRootSource').disabled && document.getElementById('autoRootOperation').disabled`), true);
    await waitFor(`document.getElementById('localCleaningStatus').textContent === 'Lote concluído'`);
    assert.equal(localBqLoads, 1);
    assert.equal(await evaluate(`document.getElementById('autoRootSource').disabled || document.getElementById('autoRootOperation').disabled`), false);
    const bqResult = new ExcelJS.Workbook(); await bqResult.xlsx.readFile(bqFile);
    assert.equal(bqResult.worksheets[0].rowCount, 2);
    assert.equal(bqResult.worksheets[0].getCell('A2').value, '22345678000199');
    const bqOriginal = await fs.readFile(bqFile);
    localBqFailure = true;
    await evaluate(`document.getElementById('startCleaningBtn').click()`);
    await waitFor(`document.getElementById('localCleaningStatus').textContent === 'Verifique o log da limpeza'`);
    assert.deepEqual(await fs.readFile(bqFile), bqOriginal);
    assert.equal(await evaluate(`document.getElementById('autoRootOperation').disabled`), false);
    localBqFailure = false;
    await delay(100);
    await fs.writeFile(path.resolve(__dirname, '../out/workspace-light.png'), (await window.webContents.capturePage()).toPNG());
    await evaluate(`document.getElementById('dark-theme-btn').click()`);
    assert.equal(await evaluate(`document.body.classList.contains('dark-theme')`), true);
    await evaluate(`document.getElementById('light-theme-btn').click()`);
    window.setSize(960, 800);
    await delay(100);
    assert.equal(await evaluate(`document.getElementById('localGrid').scrollWidth <= document.getElementById('localGrid').clientWidth + 1`), true);
    await fs.writeFile(path.resolve(__dirname, '../out/workspace-compact.png'), (await window.webContents.capturePage()).toPNG());
    await evaluate(`document.querySelector('[data-tab-name="fluxos"]').click(); Array.from(document.querySelectorAll('#fluxos-react-root button')).find(button => /Histórico/.test(button.textContent))?.click()`);
    await waitFor(`!!document.querySelector('#fluxos-react-root .flow-progress progress')`);
    assert.equal(await evaluate(`document.querySelector('#fluxos-react-root .flow-progress progress').value`), 25);
    window.webContents.send('flow-update', { ...syntheticJob, progress: { stage: 'cleaning', processed: 750, total: 1000 }, counts: { generated: 1000, kept: 300, blockedPhones: 450 } });
    await waitFor(`document.querySelector('#fluxos-react-root .flow-progress progress').value === 75`);
    await delay(250);
    assert.equal(await evaluate(`document.getElementById('fluxos-react-root').textContent.includes('Telefones removidos pela blocklist')`), true);
    assert.equal(await evaluate(`document.getElementById('fluxos-react-root').textContent.includes('Fonte da Receita: BigQuery')`), true);
    const legacySnapshot = { ...syntheticJob.flowSnapshot, generation: { ...syntheticJob.flowSnapshot.generation } };
    delete legacySnapshot.generation.source;
    window.webContents.send('flow-update', { ...syntheticJob, flowSnapshot: legacySnapshot, progress: { stage: 'cleaning', processed: 750, total: 1000 }, counts: { generated: 1000, kept: 300, blockedPhones: 450 } });
    await waitFor(`document.getElementById('fluxos-react-root').textContent.includes('Fonte da Receita: Neon')`);
    window.webContents.send('flow-update', { ...syntheticJob, progress: { stage: 'cleaning', processed: 750, total: 1000 }, counts: { generated: 1000, kept: 300, blockedPhones: 450 } });
    await waitFor(`document.getElementById('fluxos-react-root').textContent.includes('Fonte da Receita: BigQuery')`);
    assert.equal(await evaluate(`document.getElementById('fluxos-react-root').scrollWidth <= document.getElementById('fluxos-react-root').clientWidth + 1`), true);
    await fs.writeFile(path.resolve(__dirname, '../out/flow-progress-smoke.png'), (await window.webContents.capturePage()).toPNG());
    const generationJob = { ...syntheticJob, stage: 'generation', progress: { stage: 'generation', processed: 9460000, total: null }, counts: { generated: 9460000 } };
    window.webContents.send('flow-update', generationJob);
    await waitFor(`!!document.querySelector('.flow-progress-activity')`);
    assert.equal(await evaluate(`document.querySelector('.flow-progress progress') === null`), true);
    assert.equal(await evaluate(`document.querySelector('.flow-progress-activity').hasAttribute('aria-valuenow')`), false);
    assert.equal(await evaluate(`document.querySelector('.flow-progress-detail').textContent`), '9.460.000 registros gerados');
    assert.equal(await evaluate(`document.querySelector('.flow-progress-heading span').textContent`), 'Buscando registros');
    assert.ok(await evaluate(`document.querySelector('.flow-progress-activity span').getBoundingClientRect().width < document.querySelector('.flow-progress-activity').getBoundingClientRect().width / 2`));
    if (!await evaluate(`matchMedia('(prefers-reduced-motion: reduce)').matches`)) {
        const before = await evaluate(`getComputedStyle(document.querySelector('.flow-progress-activity span')).transform`);
        await delay(250);
        assert.notEqual(await evaluate(`getComputedStyle(document.querySelector('.flow-progress-activity span')).transform`), before, 'A busca sem total deve mostrar movimento');
    }
    await fs.writeFile(path.resolve(__dirname, '../out/flow-progress-generation.png'), (await window.webContents.capturePage()).toPNG());
    window.webContents.send('flow-update', { ...generationJob, status: 'interrupted' });
    await waitFor(`!!document.querySelector('.flow-progress-activity.is-paused')`);
    assert.equal(await evaluate(`document.querySelector('.flow-progress-heading span').textContent`), 'Execução pausada');
    if (!await evaluate(`matchMedia('(prefers-reduced-motion: reduce)').matches`)) {
        assert.equal(await evaluate(`getComputedStyle(document.querySelector('.flow-progress-activity span')).animationPlayState`), 'paused');
    }
    window.webContents.send('flow-update', { ...syntheticJob, progress: { stage: 'cleaning', processed: 750, total: 1000 } });
    await waitFor(`document.querySelector('.flow-progress progress')?.value === 75`);
    assert.equal(await evaluate(`document.querySelector('.flow-progress-activity') === null`), true);
    await evaluate(`document.querySelector('#fluxos-react-root .flow-view-switch button').click()`);
    await waitFor(`!!document.querySelector('input[name="outputFileName"]')`);
    const availabilityField = `Array.from(document.querySelectorAll('#fluxos-react-root .flow-field')).find(item => item.firstElementChild.textContent === 'Disponibilidade salva').querySelector('select')`;
    assert.equal(await evaluate(`${availabilityField}.value`), 'all');
    await evaluate(`(() => { const select = ${availabilityField}; select.value = 'available'; select.dispatchEvent(new Event('change', { bubbles: true })); })()`);
    await evaluate(`
        const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set;
        const field = document.querySelector('input[name="outputFileName"]');
        setter.call(field, 'lista rca'); field.dispatchEvent(new Event('input', { bubbles: true }));
        const title = document.querySelector('#fluxos-react-root .flow-section input');
        setter.call(title, 'C6 teste'); title.dispatchEvent(new Event('input', { bubbles: true }));
    `);
    await waitFor(`document.getElementById('fluxos-react-root').textContent.includes('lista rca parte2.xlsx')`);
    await evaluate(`document.querySelector('#fluxos-react-root form').requestSubmit()`);
    await waitFor(`document.getElementById('fluxos-react-root').textContent.includes('Fluxo salvo.')`);
    assert.equal(savedFlow.output.fileName, 'lista rca');
    assert.equal(savedFlow.generation.availability, 'available');
    await evaluate(`document.querySelector('#fluxos-react-root .flow-new').click()`);
    await waitFor(`document.querySelector('input[name="outputFileName"]').value === ''`);
    assert.equal(await evaluate(`${availabilityField}.value`), 'all');
    await evaluate(`document.querySelector('#fluxos-react-root .flow-presets button').click()`);
    await waitFor(`document.querySelector('input[name="outputFileName"]').value === 'lista rca'`);
    assert.equal(await evaluate(`${availabilityField}.value`), 'available');
    await evaluate(`document.querySelectorAll('#fluxos-react-root .flow-view-switch button')[1].click(); document.getElementById('fluxos').scrollTop = 0`);
    const sceneJob = { ...syntheticJob, flowSnapshot: { ...defaults(), enrichment: { enabled: true, strategy: 'append' }, api: { enabled: true, keyMode: 'dupla', delayMs: 60000 } } };
    window.webContents.send('flow-update', sceneJob);
    await waitFor(`document.querySelectorAll('.flow-scene-station').length === 5`);
    assert.equal(await evaluate(`document.querySelector('.flow-scene-station.state-active').dataset.stage`), 'cleaning');
    assert.equal(await evaluate(`document.querySelectorAll('.flow-scene-station.state-complete').length`), 3);
    assert.equal(await evaluate(`document.querySelector('.flow-scene [role="progressbar"]').getAttribute('aria-valuenow')`), '25');
    window.webContents.send('flow-update', { ...sceneJob, progress: { stage: 'cleaning', processed: 750, total: 1000 }, counts: { generated: 1000, kept: 100, blockedPhones: 9000 } });
    await waitFor(`document.querySelector('.flow-scene [role="progressbar"]').getAttribute('aria-valuenow') === '75'`);
    assert.match(await evaluate(`document.querySelector('.flow-scene-station.state-active .flow-scene-station-readout').textContent`), /75%/);
    assert.match(await evaluate(`document.querySelector('.flow-scene-progress-summary').textContent`), /750 de 1.000 registros processados/);
    await evaluate(`document.querySelector('.flow-scene-steps button[data-stage="api"]').click()`);
    assert.equal(await evaluate(`document.querySelector('.flow-scene-insight-label').textContent`), 'API C6');
    assert.equal(await evaluate(`document.querySelector('.flow-scene-station.state-active').dataset.stage`), 'cleaning');
    window.webContents.send('flow-update', { ...sceneJob, status: 'interrupted' });
    await waitFor(`!!document.querySelector('.flow-scene-station.state-paused') && document.querySelector('.flow-scene').dataset.motion === 'off'`);
    window.webContents.send('flow-update', { ...sceneJob, status: 'completed', stage: 'export' });
    await waitFor(`document.querySelectorAll('.flow-scene-station.state-complete').length === 5`);
    window.webContents.send('flow-update', { ...sceneJob, status: 'failed', stage: 'root' });
    await waitFor(`document.querySelector('.flow-scene-status').textContent.includes('Falhou')`);
    await evaluate(`document.querySelector('.flow-scene-toggle').click()`);
    assert.equal(await evaluate(`document.querySelector('.flow-scene-viewport') === null`), true);
    assert.equal(await evaluate(`localStorage.getItem('flows-scene-view')`), 'compact');
    await evaluate(`document.querySelector('.flow-scene-toggle').click()`);
    window.webContents.send('flow-update', sceneJob);
    await waitFor(`!!document.querySelector('.flow-scene-station.state-active')`);
    assert.equal(await evaluate(`document.querySelector('.flow-scene').scrollWidth <= document.querySelector('.flow-scene').clientWidth + 1`), true);
    await evaluate(`document.getElementById('dark-theme-btn').click()`);
    await delay(100);
    await fs.writeFile(path.resolve(__dirname, '../out/flow-scene-dark.png'), (await window.webContents.capturePage()).toPNG());
    await evaluate(`document.getElementById('light-theme-btn').click(); document.querySelector('[data-tab-name="local"]').click()`);
    await waitFor(`document.querySelector('.flow-scene').dataset.motion === 'off'`);
    assert.deepEqual(errors, []);
    console.log('Smoke aprovado: React/worker, limpeza local sequencial, controles removidos, preferências, bloqueio de lote, temas e layout compacto, sem banco de produção.');
}).catch(async error => {
    console.error(error);
    if (window && !window.isDestroyed()) {
        console.error('Estado da limpeza de colunas:', await evaluate(`JSON.stringify({ status: document.querySelector('.columns-status')?.textContent, activity: [...document.querySelectorAll('#columns-activity > p')].slice(-5).map(node => node.textContent) })`).catch(() => 'Janela indisponível.'));
    }
    if (errors.length) console.error('Erros do renderer:', errors);
    exitCode = 1;
}).finally(async () => {
    clearTimeout(timeout);
    if (window && !window.isDestroyed()) window.destroy();
    if (userData) await fs.rm(userData, { recursive: true, force: true }).catch(() => {});
    app.exit(exitCode);
});

/** Painel do Ritmo do Discador no formato do main, com operadores e a série do dia gerados aqui. */
function painelRitmoSintetico() {
    const agora = Date.now(), inicio = new Date(agora); inicio.setHours(8, 0, 0, 0);
    const iso = minutosAtras => new Date(agora - minutosAtras * 60000).toISOString();
    const operadores = plano => plano.flatMap(([estado, detalhe, quantidade, segundos], grupo) => Array.from({ length: quantidade }, (_, i) => ({ id: grupo * 100 + i, nome: `Operador ${grupo * 100 + i}`, estado, detalhe, segundos: segundos + i * 17 })));
    const contagem = ops => ({ falando: ops.filter(o => o.estado === 'falando').length, livres: ops.filter(o => o.estado === 'livre').length, pausa: ops.filter(o => o.estado === 'pausa').length, outros: ops.filter(o => o.estado === 'outro').length, logados: ops.length });
    const serie = (base, agressividade) => {
        const pontos = [];
        for (let t = inicio.getTime(), i = 0; t <= Math.min(agora, inicio.getTime() + 10 * 3600e3); t += 5 * 60000, i++) pontos.push({ t, ocupacao: base == null ? null : Math.min(1, Math.max(0.1, base + Math.sin(i / 5) * 0.18)), agressividade: agressividade(i) });
        return pontos;
    };
    const c6 = operadores([['falando', 'RECEPTIVO', 21, 30], ['falando', 'ATIVO', 2, 50], ['livre', '', 2, 20], ['pausa', 'PAUSA ADM', 2, 640]]);
    const mp = operadores([['falando', 'ATIVO', 2, 80], ['livre', '', 2, 40]]);
    const servico = (id, nome, extra) => ({ id, nome, modo: 'URA', emExecucao: true, agressividade: 1, limite: 100, regra: null, pausado: null, contagem: contagem([]), ocupacao: null, decisao: { tipo: 'semregra' }, operadores: [], parceiros: [], serie: [], ...extra });
    return { configurado: true, login: { saved: true, login: 'synthetic', canSave: true }, admin: true, ativa: true, lidoEm: new Date(agora).toISOString(), lendo: false, conectado: true, erro: null, intervaloSegundos: 30, limitePausa: 0.5, teto: 1500, tetoCallcenter: 99, soma: 1669,
        servicos: [
            servico(120, 'C6 BANK PT1', { agressividade: 1370, limite: 1600, regra: { desce: 80, sobe: 45, passo: 100, minimo: 800, maximo: 1600 }, contagem: contagem(c6), ocupacao: 23 / 27, decisao: { tipo: 'pendente', dir: -1, alvo: 1270 }, operadores: c6, parceiros: [123], serie: serie(0.7, i => (i < 40 ? 1270 : i < 70 ? 1370 : 1470) - (i > 90 ? 100 : 0)) }),
            servico(123, 'C6 BANK PT3', { modo: 'POWER', agressividade: 99, contagem: contagem(c6), ocupacao: 23 / 27, operadores: c6, parceiros: [120], serie: serie(0.7, () => 99) }),
            servico(52, 'MERCADO PAGO 3', { modo: 'POWER', agressividade: 80, regra: { desce: 100, sobe: 25, passo: 5, minimo: 5, maximo: 50 }, pausado: { de: 45, para: 80, em: iso(6) }, contagem: contagem(mp), ocupacao: 0.5, decisao: { tipo: 'pausado' }, operadores: mp, serie: serie(0.5, () => 45) }),
            servico(87, 'PAGBANK 1', { modo: 'POWER', regra: { desce: 90, sobe: 40, passo: 1, minimo: 1, maximo: 10 }, decisao: { tipo: 'semgente' }, serie: serie(null, () => 1) }),
        ],
        eventos: [
            { id: 'r1', em: iso(240), tipo: 'sistema', servicoId: null, servico: '', texto: 'Automação ligada neste PC por Davi: os ajustes valem na Fastway.' },
            ...[[200, 1000, 1140], [180, 1140, 1280], [150, 1280, 1400], [120, 1400, 1470]].map(([min, de, para], i) => ({ id: 'rc' + i, em: iso(min), tipo: 'ajuste', servicoId: 120, servico: 'C6 BANK PT1', modo: 'URA',
                de, para, dir: 1, ocupacao: 0.4, contagem: { falando: 4, livres: 6, pausa: 3, outros: 2, logados: 15 }, faixa: { sobe: 60, desce: 90, passo: 140 }, corte: para - de < 140 ? 'tetoUra' : null, texto: '' })),
            { id: 'r3', em: iso(90), tipo: 'ajuste', servicoId: 120, servico: 'C6 BANK PT1', de: 1470, para: 1370, dir: -1, ocupacao: 0.84, texto: 'C6 BANK PT1 desceu de 1.470 para 1.370' },
            { id: 'r4', em: iso(6), tipo: 'manual', servicoId: 52, servico: 'MERCADO PAGO 3', de: 45, para: 80, texto: 'MERCADO PAGO 3 mudou na Fastway de 45 para 80' },
        ] };
}
