console.log('--- MAIN.JS CARREGADO - VERSÃO NOVA (NEON DB COM CHAVE EXTERNA E FILTRO CNAE) ---');
const { app, BrowserWindow, ipcMain, dialog, shell } = require("electron");
const { autoUpdater } = require("electron-updater");
const path = require("path");
const fs = require("fs");
const fsp = require("fs").promises;
const XLSX = require("xlsx");
const ExcelJS = require("exceljs");
const axios = require("axios");
const os = require('os');
const Store = require('electron-store');
const { Pool } = require('pg');
const { parse } = require('csv-parse');
const nodemailer = require('nodemailer'); // NOVO: Para envio de e-mail
require('dotenv').config(); // Carrega as variáveis de ambiente do arquivo .env

// --- INÍCIO: Adições para a nova aba Relacionamento ---
const xlsx = require('xlsx'); // Verifique se já não está importado
// --- FIM: Adições para a nova aba Relacionamento ---


autoUpdater.logger = require("electron-log");
autoUpdater.logger.transports.file.level = "info";

const store = new Store();

// #################################################################
// #           LISTA DE CNAES PROIBIDOS (NOVA ADIÇÃO)              #
// #################################################################
const PROHIBITED_CNAES = new Set([
    '8299704', '8299706', '9002702', '9200301', '9200302', '9200399',
    '9329803', '9329804', '9491000', '9492800', '9529106', '9609204',
    '1210700', '1220401', '1220402', '1220403', '1220499', '2092401',
    '2442300', '2550101', '2550102', '3211602', '3211603', '4520005',
    '4681801', '4681802', '4681803', '4681804', '4681805', '4732600',
    '4782202', '4783101', '4783102', '4789009', '6434400', '6440900',
    '6491300', '6619399', '7912100', '8422100', '9420100', '9430800',
    '724301', '729404', '893200', '899101', '899102', '899103',
    '899199', '9499500', '9493600', '220906', '5590601', '9411100',
    '8720401', '9412099', '8711504', '7911200'
]);


// #################################################################
// #           CONFIGURAÇÃO DO BANCO DE DADOS (DINÂMICA)           #
// #################################################################

let pool;

async function initializePool(connectionString, windowToLog) {
    if (pool) {
        await pool.end();
        console.log("Pool de conexões anterior encerrado.");
    }

    if (!connectionString) {
        console.log("Chave de conexão não fornecida. A inicialização do pool foi ignorada.");
        if (windowToLog) windowToLog.webContents.send("log", "⚠️ Chave de conexão do BD não configurada. Funções do BD desabilitadas.");
        pool = null;
        return;
    }

    pool = new Pool({
        connectionString: connectionString,
    });

    try {
        await pool.query('SELECT NOW()');
        console.log("✅ Conexão com o banco de dados estabelecida com sucesso.");
        if (windowToLog) windowToLog.webContents.send("log", "✅ Conexão com o Banco de Dados estabelecida com sucesso.");
    } catch (error) {
        console.error("❌ Falha ao estabelecer conexão com o banco de dados:", error.message);
        if (windowToLog) windowToLog.webContents.send("log", `❌ ERRO DE CONEXÃO BD: ${error.message}. Funções do BD podem não funcionar.`);
        pool = null;
        throw error;
    }
}


// #################################################################
// #           SISTEMA DE LOGIN E PERMISSÕES (Com alterações)      #
// #################################################################

const users = {
    'Pablo': { password: 'Vasco@2025', role: 'admin' },
    'Thalles': { password: 'Flamengo@2025', role: 'admin' },
    'Matheus Kauss': { password: 'Flamengo@2025', role: 'admin' },
    'Matheus': { password: 'Botafogo@2025', role: 'admin' },
    'Felipe': { password: 'Flamengo@2025', role: 'admin' },
    'Davi': { password: '080472Fr*', role: 'admin' },
    'Tatiane': { password: '123456', role: 'master' },
    'Gomes': { password: '123456', role: 'master' },
    'Mayko': { password: '123456', role: 'limited', teamId: '106' },
    'Bruna': { password: '123456', role: 'limited', teamId: '85' },
    'Laiane': { password: '123456', role: 'limited', teamId: '123' },
    'Waleska': { password: '123456', role: 'limited', teamId: '87' },
    'Natallia': { password: '123456', role: 'limited', teamId: '106' },
    'Camila': { password: '123456', role: 'limited', teamId: '120' },
    'Tef': { password: '123456', role: 'limited', teamId: '133' }
};
let mainWindow;
let loginWindow;
let currentUser = null;

function createLoginWindow() {
    loginWindow = new BrowserWindow({
        width: 480,
        height: 650,
        webPreferences: {
            preload: path.join(__dirname, 'preload.js'),
            nodeIntegration: false,
            contextIsolation: true,
        },
        resizable: false,
        frame: false,
        center: true,
    });

    loginWindow.loadFile('login.html');

    loginWindow.on('closed', () => {
        loginWindow = null;
    });
    return loginWindow;
}

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
        pool = null;
        return { success: false, message: error.message };
    }
});

ipcMain.handle('login-attempt', async (event, username, password, rememberMe) => {
    const user = users[username];
    if (user && user.password === password) {
        currentUser = {
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
        if (loginWindow) loginWindow.close();

        return { success: true };
    } else {
        store.delete('credentials');
        return { success: false, message: 'Usuário ou senha inválidos.' };
    }
});

ipcMain.on('logout', () => {
    store.delete('credentials');
    currentUser = null;
    if (pool) {
        pool.end();
        pool = null;
    }
    if (mainWindow) {
        mainWindow.close();
    }
    if (!loginWindow) {
        createLoginWindow();
    }
});

ipcMain.handle('get-ui-settings', () => {
    // Retorna as configurações salvas, ou um objeto vazio como padrão.
    return store.get('ui_settings', {});
});

ipcMain.on('save-ui-settings', (event, settings) => {
    store.set('ui_settings', settings);
});

// NOVO: Handler para o diálogo de confirmação
ipcMain.handle('show-confirm-dialog', async (event, options) => {
    const result = await dialog.showMessageBox(mainWindow, {
        type: 'warning',
        buttons: ['Cancelar', 'Confirmar'],
        defaultId: 1,
        title: options.title,
        message: options.message,
    });
    return result.response === 1; // Retorna true se 'Confirmar' foi clicado
});

const isAdmin = () => {
    return currentUser && currentUser.role === 'admin';
};


// #################################################################
// #           LÓGICA DE INICIALIZAÇÃO (COM MODIFICAÇÕES)          #
// #################################################################
let storedCnpjs = new Set();
let blocklistPhones = new Set(); // NOVO: Set para armazenar a blocklist em memória

async function loadStoredCnpjs() {
    if (!isAdmin() || !pool) {
        if (mainWindow && isAdmin()) {
            mainWindow.webContents.send("log", "⚠️ A conexão com o BD não está ativa. Histórico de CNPJs não carregado.");
        }
        return;
    }

    try {
        const result = await pool.query('SELECT cnpj FROM limpeza_cnpjs');
        storedCnpjs = new Set(result.rows.map(row => row.cnpj));
        console.log(`${storedCnpjs.size} CNPJs carregados do Neon DB.`);
        if (mainWindow) {
            mainWindow.webContents.send("log", `✅ Sincronização com o BD concluída. ${storedCnpjs.size} CNPJs carregados.`);
        }
    } catch (err) {
        console.error("Falha ao carregar CNPJs do Neon DB:", err);
        if (mainWindow) {
            mainWindow.webContents.send("log", `❌ ERRO ao carregar histórico do BD: ${err.message}`);
        }
    }
}


function createMainWindow() {
    mainWindow = new BrowserWindow({
        width: 1400,
        height: 950,
        frame: false, // Remove a barra de título padrão
        webPreferences: {
            nodeIntegration: false,
            contextIsolation: true,
            preload: path.join(__dirname, "preload.js")
        }
    });
    // Adiciona handlers para os novos botões da janela
    ipcMain.on('minimize-window', () => mainWindow.minimize());
    ipcMain.on('maximize-window', () => { if (mainWindow.isMaximized()) { mainWindow.unmaximize(); } else { mainWindow.maximize(); } });
    ipcMain.on('close-window', () => mainWindow.close());
    mainWindow.loadFile("index.html");

    mainWindow.webContents.on("did-finish-load", async () => {
        if (currentUser) {
            mainWindow.webContents.send('user-info', currentUser);

            if (isAdmin()) {
                const dbConnectionString = store.get('db_connection_string');
                try {
                    await initializePool(dbConnectionString, mainWindow);
                    if (pool) {
                        await loadStoredCnpjs();
                    }
                } catch (error) {
                    // O erro já é logado dentro de initializePool
                }
            }
        }
        autoUpdater.checkForUpdatesAndNotify();
    });

    mainWindow.on('closed', () => {
        mainWindow = null;
    });
}

app.whenReady().then(async () => {
    const savedCredentials = store.get('credentials');

    if (savedCredentials && savedCredentials.username && savedCredentials.password) {
        const { username, password } = savedCredentials;
        const user = users[username];

        if (user && user.password === password) {
            console.log("Login automático bem-sucedido.");
            currentUser = {
                username,
                role: user.role,
                teamId: user.teamId || null
            };
            createMainWindow();
        } else {
            console.log("Credenciais salvas inválidas. Abrindo tela de login.");
            createLoginWindow();
        }
    } else {
         console.log("Nenhuma credencial salva. Abrindo tela de login.");
         createLoginWindow();
    }
});


app.on("window-all-closed", () => {
    if (process.platform !== "darwin") {
        app.quit();
    }
});

// #################################################################
// #           LÓGICA DE NEGÓCIOS (Refatorada para PostgreSQL)     #
// #################################################################

function sendUpdateStatusToWindow(text) {
    if (mainWindow && mainWindow.webContents) {
        mainWindow.webContents.send("update-message", text);
    }
}
autoUpdater.on("checking-for-update", () => sendUpdateStatusToWindow("Verificando por atualizações..."));
autoUpdater.on("update-available", (info) => sendUpdateStatusToWindow(`Atualização disponível (v${info.version}). Baixando...`));
autoUpdater.on("update-not-available", () => sendUpdateStatusToWindow(""));
autoUpdater.on("error", (err) => sendUpdateStatusToWindow(`Erro na atualização: ${err.toString()}`));
autoUpdater.on("download-progress", (p) => sendUpdateStatusToWindow(`Baixando atualização: ${Math.round(p.percent)}%`));
autoUpdater.on("update-downloaded", (info) => { sendUpdateStatusToWindow(`Atualização v${info.version} baixada. Reinicie para instalar.`); if (mainWindow && mainWindow.webContents) { mainWindow.webContents.executeJavaScript(`const um = document.getElementById("update-message"); if(um){ um.style.cursor="pointer"; um.style.textDecoration="underline"; um.onclick = () => require("electron").ipcRenderer.send("restart-app-for-update"); }`); } });
ipcMain.on("restart-app-for-update", () => autoUpdater.quitAndInstall());
ipcMain.on('open-path', (event, filePath) => { shell.openPath(filePath).catch(err => { const msg = `ERRO: Não foi possível abrir o arquivo em ${filePath}`; console.error("Falha ao abrir o caminho:", err); event.sender.send("log", msg); event.sender.send("automation-log", msg); }); });
ipcMain.handle("select-file", async (event, { title, multi }) => { const { canceled, filePaths } = await dialog.showOpenDialog(mainWindow, { title: title, properties: [ multi ? "multiSelections" : "openFile", "openFile" ], filters: [ { name: "Planilhas", extensions: ["xlsx", "xls", "csv"] } ] }); return canceled ? null : filePaths; });
function letterToIndex(letter) { return letter.toUpperCase().charCodeAt(0) - 65; }
async function readSpreadsheet(filePath) { try { if (path.extname(filePath).toLowerCase() === ".csv") { const data = await fsp.readFile(filePath, "utf8"); return XLSX.read(data, { type: "string", cellDates: true }); } else { const buffer = await fsp.readFile(filePath); return XLSX.read(buffer, { type: 'buffer', cellDates: true }); } } catch (e) { console.error(`Erro ao ler planilha: ${filePath}`, e); throw new Error(`Não foi possível ler o arquivo ${path.basename(filePath)}. Verifique se o caminho está correto e se você tem permissão.`); } }
function writeSpreadsheet(workbook, filePath) { XLSX.writeFile(workbook, filePath); }


// --- FUNÇÕES DA ABA DE ENRIQUECIMENTO (Refatoradas para PostgreSQL) ---

ipcMain.handle("get-enriched-cnpj-count", async () => {
    if (!isAdmin() || !pool) return 0;
    try {
        const result = await pool.query('SELECT COUNT(*) FROM empresas;');
        return parseInt(result.rows[0].count, 10);
    } catch (error) {
        console.error("Erro ao contar CNPJs enriquecidos:", error);
        return 0;
    }
});

ipcMain.handle("download-enriched-data", async () => {
    if (!isAdmin() || !pool) return { success: false, message: "Acesso negado ou conexão com BD inativa." };
    try {
        const { canceled, filePath } = await dialog.showSaveDialog(mainWindow, { title: "Salvar Dados Enriquecidos", defaultPath: `dados_enriquecidos_${Date.now()}.xlsx`, filters: [ { name: "Excel Files", extensions: ["xlsx"] } ] });
        if (canceled || !filePath) return { success: false, message: "Download cancelado." };

        const query = `
            SELECT e.cnpj, array_agg(t.numero ORDER BY t.id) as telefones
            FROM empresas e
            LEFT JOIN telefones t ON e.id = t.empresa_id
            GROUP BY e.id, e.cnpj
            ORDER BY e.id;
        `;
        const { rows } = await pool.query(query);

        if (rows.length === 0) return { success: false, message: "Nenhum dado encontrado." };

        const maxPhones = rows.reduce((max, row) => Math.max(max, row.telefones ? row.telefones.length : 0), 0);
        const headers = ["cpf", ...Array.from({ length: maxPhones }, (_, i) => `fone${i + 1}`)];

        const data = rows.map(row => {
            const phones = row.telefones || [];
            const processedPhones = Array.from({ length: maxPhones }, (_, i) => {
                const phone = phones[i];
                if (!phone) return null;
                // Converte para número para evitar o erro de "número armazenado como texto" no Excel
                const numericPhone = Number(String(phone).replace(/\D/g, ''));
                return isNaN(numericPhone) ? phone : numericPhone;
            });
            return [row.cnpj, ...processedPhones];
        });

        const workbook = new ExcelJS.Workbook();
        const worksheet = workbook.addWorksheet("Dados Enriquecidos");
        worksheet.addRow(headers);
        worksheet.addRows(data);

        // Aplica o formato de número nas colunas de telefone
        for (let i = 2; i <= maxPhones + 1; i++) {
            worksheet.getColumn(i).numFmt = '0';
        }

        await workbook.xlsx.writeFile(filePath);
        return { success: true, message: `Arquivo salvo com sucesso: ${filePath}` };
    } catch (error) {
        console.error("Erro ao baixar dados enriquecidos:", error);
        return { success: false, message: `Erro ao gerar arquivo: ${error.message}` };
    }
});

ipcMain.on("start-db-load", async (event, { masterFiles, year }) => {
    if (!isAdmin() || !pool) {
        event.sender.send("enrichment-log", "❌ Acesso negado ou conexão com BD inativa.");
        event.sender.send("db-load-finished");
        return;
    }
    const log = (msg) => event.sender.send("enrichment-log", msg);
    const progress = (current, total, fileName, cnpjsProcessed) => event.sender.send("db-load-progress", { current, total, fileName, cnpjsProcessed });

    if (!year) {
        log('❌ ERRO CRÍTICO: O ano não foi fornecido para a carga no banco de dados.');
        event.sender.send("db-load-finished");
        return;
    }

    log(`--- Iniciando Carga para o Banco de Dados (Ano: ${year}) ---`);
    let totalCnpjsProcessed = 0;

    const saveChunkToDb = async (dataMap, filePath) => {
        if (dataMap.size === 0) {
            return;
        }
        const client = await pool.connect();
        try {
            await client.query('BEGIN');
            const uniqueCnpjs = Array.from(dataMap.keys());
            const insertEmpresasQuery = `
                INSERT INTO empresas (cnpj, ano)
                SELECT unnest($1::text[]), $2
                ON CONFLICT (cnpj) DO UPDATE
                SET ano = EXCLUDED.ano;
            `;
            await client.query(insertEmpresasQuery, [uniqueCnpjs, year]);

            const getEmpresasQuery = `SELECT id, cnpj FROM empresas WHERE cnpj = ANY($1::text[])`;
            const result = await client.query(getEmpresasQuery, [uniqueCnpjs]);
            const empresaIdMap = new Map(result.rows.map(row => [row.cnpj, row.id]));

            const phoneValues = [];
            for (const [cnpj, phones] of dataMap.entries()) {
                const empresaId = empresaIdMap.get(cnpj);
                if (empresaId) {
                    const uniquePhones = [...new Set(phones)].filter(p => String(p).replace(/\D/g, '').length >= 8);
                    uniquePhones.forEach(phone => phoneValues.push({ empresa_id: empresaId, numero: phone }));
                }
            }

            if (phoneValues.length > 0) {
                 const insertTelefonesQuery = `INSERT INTO telefones (empresa_id, numero) SELECT (d.v->>'empresa_id')::int, d.v->>'numero' FROM jsonb_array_elements($1::jsonb) d(v) ON CONFLICT (empresa_id, numero) DO NOTHING`;
                 await client.query(insertTelefonesQuery, [JSON.stringify(phoneValues)]);
            }

            await client.query('COMMIT');
            totalCnpjsProcessed += dataMap.size;
        } catch (error) {
            await client.query('ROLLBACK');
            log(`❌ ERRO no lote do arquivo ${path.basename(filePath)}: ${error.message}`);
        } finally {
            client.release();
        }
    };

    try {
        for (let fileIndex = 0; fileIndex < masterFiles.length; fileIndex++) {
            const filePath = masterFiles[fileIndex];
            const fileName = path.basename(filePath);
            progress(fileIndex + 1, masterFiles.length, fileName, totalCnpjsProcessed);
            log(`\nProcessando arquivo mestre: ${fileName}`);
            try {
                const workbook = new ExcelJS.Workbook(); await workbook.xlsx.readFile(filePath); const worksheet = workbook.worksheets[0]; if (!worksheet || worksheet.rowCount === 0) { log(`⚠️ Arquivo ${fileName} vazio ou inválido. Pulando.`); continue; } const headerMap = new Map(); worksheet.getRow(1).eachCell({ includeEmpty: true }, (cell, colNum) => headerMap.set(colNum, String(cell.value || "").trim().toLowerCase())); let cnpjColIdx = [...headerMap.entries()].find(([_, h]) => h === "cpf" || h === "cnpj")?.[0] ?? -1; const phoneColIdxs = [...headerMap.entries()].filter(([_, h]) => /^(fone|telefone|celular)/.test(h)).map(([colNum]) => colNum); if (cnpjColIdx === -1 || phoneColIdxs.length === 0) { log(`❌ ERRO: Colunas de documento ou telefone não encontradas. Pulando.`); continue; }
                let cnpjsToUpdate = new Map();
                for (let i = 2; i <= worksheet.rowCount; i++) {
                    const row = worksheet.getRow(i);
                    const cnpj = String(row.getCell(cnpjColIdx).value || "").replace(/\D/g, "").trim();
                    if (cnpj.length < 8) continue;

                    const phones = phoneColIdxs.map(idx => String(row.getCell(idx).value || "").trim()).filter(Boolean);
                    if (phones.length > 0) cnpjsToUpdate.set(cnpj, [...(cnpjsToUpdate.get(cnpj) || []),...phones]);

                    if (i % 5000 === 0) {
                        await saveChunkToDb(cnpjsToUpdate, filePath);
                        cnpjsToUpdate.clear();
                        progress(fileIndex + 1, masterFiles.length, fileName, totalCnpjsProcessed);
                    }
                }
                if (cnpjsToUpdate.size > 0) {
                    await saveChunkToDb(cnpjsToUpdate, filePath);
                }

            } catch (err) {
                log(`❌ ERRO ao processar ${fileName}: ${err.message}`);
            }
        }
    } catch (err) {
        log(`❌ Um erro crítico ocorreu: ${err.message}`);
    } finally {
        log(`\n✅ Carga finalizada. Total de ${totalCnpjsProcessed} CNPJs únicos processados.`);
        event.sender.send("db-load-finished");
    }
});
function formatEta(totalSeconds) { if (!isFinite(totalSeconds) || totalSeconds < 0) return "Calculando..."; const m = Math.floor(totalSeconds / 60); const s = Math.floor(totalSeconds % 60); return `${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`; }
async function runEnrichmentProcess({ filesToEnrich, strategy, backup, year, batchSize, usePadrao, useAllDb }, log, progress, onFinish) { // MODIFICADO
    if (!isAdmin() || !pool){
        log("❌ Acesso negado ou conexão com BD inativa.");
        if(onFinish) onFinish();
        return;
    }

    // MODIFICADO: A validação do ano só é necessária se não for usar o banco todo.
    if (!useAllDb && !year) {
        log('❌ ERRO CRÍTICO: O ano não foi fornecido para o enriquecimento.');
        if (onFinish) onFinish();
        return;
    }

    const BATCH_SIZE = batchSize || 2000; // MODIFICADO: Usa o tamanho do lote recebido ou o padrão
    let anosDeBusca = [];
    if (useAllDb) {
        anosDeBusca = []; // Fica vazio para indicar que não há filtro de ano
    } else if (usePadrao) {
        anosDeBusca = [year, 1];
    } else {
        anosDeBusca = [year];
    }

    log(`--- Iniciando Processo de Enriquecimento ---`);
    log(`Tamanho do Lote: ${BATCH_SIZE.toLocaleString('pt-BR')} registros.`);
    log(`Ano(s) de Busca: ${anosDeBusca.join(', ')} ${usePadrao ? '(213 PADRÃO ATIVADO)' : ''}`);
    let totalEnrichedRowsOverall = 0, totalProcessedRowsOverall = 0, totalNotFoundInDbOverall = 0;
    try {
        for (const fileObj of filesToEnrich) {
            const { path: filePath, id } = fileObj;
            const startTime = Date.now();
            log(`\nProcessando arquivo: ${path.basename(filePath)}`);
            progress(id, 0, null);
            if (backup) { const p = path.parse(filePath); fs.copyFileSync(filePath, path.join(p.dir, `${p.name}.backup_enrich_${Date.now()}${p.ext}`)); log(`Backup criado.`); }
            try {
                const workbook = new ExcelJS.Workbook(); await workbook.xlsx.readFile(filePath); const worksheet = workbook.worksheets[0]; let cnpjCol = -1, statusCol = -1; const phoneCols = []; worksheet.getRow(1).eachCell((cell, colNum) => { const h = String(cell.value || "").trim().toLowerCase(); if (h === "cpf" || h === "cnpj") cnpjCol = colNum; else if (h.startsWith("fone")) phoneCols.push(colNum); else if (h === "status") statusCol = colNum; }); phoneCols.sort((a, b) => a - b); if (cnpjCol === -1) { log(`❌ ERRO: Coluna 'cpf'/'cnpj' não encontrada. Pulando.`); continue; } if (statusCol === -1) { statusCol = worksheet.columnCount + 1; worksheet.getCell(1, statusCol).value = "status"; }

                const totalRows = worksheet.rowCount - 1;
                let enrichedInFile = 0, notFoundInFile = 0;
                const totalBatches = Math.ceil((worksheet.rowCount - 1) / BATCH_SIZE);
                log(`Arquivo possui ${totalRows} linhas, divididas em ${totalBatches} lote(s).`);

                for (let i = 2; i <= worksheet.rowCount; i += BATCH_SIZE) {
                    const currentBatchNum = Math.floor((i - 2) / BATCH_SIZE) + 1;
                    const cnpjsInBatch = new Map();
                    const endIndex = Math.min(i + BATCH_SIZE - 1, worksheet.rowCount);
                    for (let j = i; j <= endIndex; j++) { const row = worksheet.getRow(j); const cnpj = String(row.getCell(cnpjCol).text || "").replace(/\D/g, "").trim(); if (cnpj) cnpjsInBatch.set(cnpj, { rowNum: j, row: row }); }
                    if (cnpjsInBatch.size === 0) continue;

                    log(`Lote ${currentBatchNum}/${totalBatches}: Processando ${cnpjsInBatch.size} CNPJs...`);

                  const enrichmentDataForBatch = new Map();
                  const cnpjKeys = Array.from(cnpjsInBatch.keys());
                  if (cnpjKeys.length > 0) {
                      // MODIFICADO: A query é construída dinamicamente
                      let queryText = `
                          SELECT e.cnpj, array_agg(t.numero ORDER BY t.id) as telefones
                          FROM empresas e
                          JOIN telefones t ON e.id = t.empresa_id
                          WHERE e.cnpj = ANY($1::text[])
                      `;
                      const queryParams = [cnpjKeys];

                      if (!useAllDb) {
                          queryText += ` AND e.ano = ANY($2::integer[])`;
                          queryParams.push(anosDeBusca);
                      }

                      queryText += ` GROUP BY e.id, e.cnpj;`;

                      const result = await pool.query(queryText, queryParams);

                      result.rows.forEach(row => {
                          enrichmentDataForBatch.set(row.cnpj, [...new Set(row.telefones || [])]);
                      });
                }

                log(`Lote ${currentBatchNum}/${totalBatches}: ${enrichmentDataForBatch.size} CNPJs encontrados no BD. Atualizando planilha...`);

                    for (const [cnpj, { row }] of cnpjsInBatch.entries()) {
                        let rowWasEnriched = false;
                        if (enrichmentDataForBatch.has(cnpj)) {
                            const phonesFromDb = enrichmentDataForBatch.get(cnpj);
                            const existingPhones = phoneCols.map(idx => String(row.getCell(idx).value || '').trim()).filter(Boolean);
                            const shouldProcess = (strategy === "overwrite") || (strategy === "append" && existingPhones.length < phoneCols.length) || (strategy === "ignore" && existingPhones.length === 0);

                            if (shouldProcess) {
                                rowWasEnriched = true;
                                
                                // --- INÍCIO DA CORREÇÃO E MELHORIA DA LÓGICA ---
                                let finalPhones = [];

                                // 1. Define a lista final de telefones com base na estratégia, já garantindo que sejam únicos.
                                if (strategy === "overwrite") {
                                    // Substitui completamente os telefones existentes pelos do banco de dados.
                                    finalPhones = [...new Set(phonesFromDb)];
                                } else if (strategy === "append") {
                                    // Combina os telefones existentes com os do banco, mantendo os existentes primeiro e removendo duplicatas.
                                    finalPhones = [...new Set([...existingPhones, ...phonesFromDb])];
                                } else { // A estratégia é "ignore" (já verificado pelo shouldProcess)
                                    // Apenas preenche se não houver telefones existentes.
                                    finalPhones = [...new Set(phonesFromDb)];
                                }
                                
                                // 2. Limpa as colunas de telefone existentes na planilha.
                                // Usar 'null' é crucial para garantir que a célula fique verdadeiramente vazia,
                                // evitando problemas com células que parecem vazias mas contêm espaços ou strings vazias.
                                phoneCols.forEach(idx => {
                                    row.getCell(idx).value = null;
                                });
                                
                                // 3. Escreve os telefones únicos de volta na planilha, respeitando o limite de colunas.
                                finalPhones.slice(0, phoneCols.length).forEach((phone, index) => {
                                    const numericPhoneString = String(phone).replace(/\D/g, '');
                                    if (numericPhoneString) {
                                        // Converte para número para evitar o erro de "número armazenado como texto" no Excel.
                                        // AVISO: Isso removerá quaisquer zeros à esquerda do número.
                                        const cell = row.getCell(phoneCols[index]);
                                        cell.value = Number(numericPhoneString);
                                        // Aplica um formato de número para evitar notação científica em números longos.
                                        cell.numFmt = '0';
                                    }
                                });
                                // --- FIM DA CORREÇÃO E MELHORIA DA LÓGICA ---
                            }
                        } else {
                            if (cnpj) notFoundInFile++;
                        }

                        row.getCell(statusCol).value = rowWasEnriched ? "Enriquecido" : "Pobre";
                        if (rowWasEnriched) enrichedInFile++;
                    }

                    const processedRowsInFile = endIndex - 1;
                    const eta = formatEta((totalRows - processedRowsInFile) / (processedRowsInFile / (Date.now() - startTime)));
                    progress(id, Math.round((processedRowsInFile / totalRows) * 100), eta);
                }
                await workbook.xlsx.writeFile(filePath);
                progress(id, 100, "00:00");
                log(`✅ Arquivo ${path.basename(filePath)} concluído! Total de enriquecidos: ${enrichedInFile}. Não encontrados: ${notFoundInFile}.`);
                totalEnrichedRowsOverall += enrichedInFile;
                totalNotFoundInDbOverall += notFoundInFile;
                totalProcessedRowsOverall += totalRows;
            } catch (err) {
                log(`❌ ERRO catastrófico em ${path.basename(filePath)}: ${err.message}`);
            }
        }
    } finally {
        log(`\n--- ✅ Processo de Enriquecimento Finalizado ---`);
        log(`Resumo Geral: Total Linhas Processadas: ${totalProcessedRowsOverall}, Enriquecidas: ${totalEnrichedRowsOverall}, Não Encontradas: ${totalNotFoundInDbOverall}`);
        if (onFinish) onFinish();
    }
}

ipcMain.on("start-enrichment", async (event, options) => {
    if (!isAdmin() || !pool) {
        event.sender.send("enrichment-log", "❌ Acesso negado ou conexão com BD inativa.");
        event.sender.send("enrichment-finished");
        return;
    }
    await runEnrichmentProcess(
        options,
        (msg) => event.sender.send("enrichment-log", msg),
        (id, pct, eta) => event.sender.send("enrichment-progress", { id, progress: pct, eta }),
        () => event.sender.send("enrichment-finished")
    );
});


// --- FUNÇÕES DA ABA MONITORAMENTO, LOGIN, ETC (Sem alterações relevantes ao DB) ---
ipcMain.handle('fetch-monitoring-report', async (event, { reportUrl, operatorTimesParams }) => { if (!currentUser) { return { success: false, message: 'Acesso negado. Faça o login.' }; } let mainReportResult; try { const response = await axios.get(reportUrl, { timeout: 4000000, headers: { 'User-Agent': 'PostmanRuntime/7.44.1' } }); if (response.status === 200) { const data = (typeof response.data === 'string' && response.data.includes("Nenhum registro encontrado")) ? [] : response.data; mainReportResult = { success: true, data: data, operatorTimesData: null }; } else { return { success: false, message: `A API principal retornou um status inesperado: ${response.status}` }; } } catch (error) { console.error("Erro ao buscar relatório de monitoramento:", error.message); return { success: false, message: `Falha na comunicação com a API principal: ${error.message}` }; } if (mainReportResult.success && operatorTimesParams) { const { data_inicio, data_fim, operador_id, grupo_operador_id } = operatorTimesParams; const baseUrl = 'http://mbfinance.fastssl.com.br/api/relatorio/operador_tempos.php'; const url = `${baseUrl}?data_inicial=${data_inicio}&data_final=${data_fim}&operador_id=${operador_id}&grupo_operador_id=${grupo_operador_id}&servico_id=&operador_ativo=`; try { const timesResponse = await axios.get(url, { timeout: 30000 }); if (timesResponse.status === 200) { mainReportResult.operatorTimesData = timesResponse.data; } else { console.error(`API de tempos retornou status ${timesResponse.status}`); } } catch (error) { console.error('[DEBUG MAIN] ERRO na chamada da API de tempos:', error.message); } } return mainReportResult; });
ipcMain.handle('download-recording', async (event, url, fileName) => { if (!mainWindow) { return { success: false, message: 'Janela principal não encontrada.' }; } const { canceled, filePath } = await dialog.showSaveDialog(mainWindow, { title: 'Salvar Gravação', defaultPath: fileName, filters: [ { name: 'Áudio MP3', extensions: ['mp3'] } ] }); if (canceled || !filePath) { return { success: true, message: 'Download cancelado pelo usuário.' }; } try { const response = await axios({ method: 'get', url: url, responseType: 'stream', headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36' } }); const writer = fs.createWriteStream(filePath); response.data.pipe(writer); return new Promise((resolve, reject) => { writer.on('finish', () => resolve({ success: true, message: `Gravação salva em: ${filePath}` })); writer.on('error', (err) => { console.error("Erro ao salvar o arquivo:", err); reject({ success: false, message: `Falha ao salvar o arquivo: ${err.message}` }); }); }); } catch (error) { console.error("Erro no download da gravação:", error); let errorMessage = error.message; if (error.response && error.response.status === 403) { errorMessage = "Acesso negado (403 Forbidden). Verifique a URL ou permissões no servidor."; } return { success: false, message: `Erro ao baixar a gravação: ${errorMessage}` }; } });
async function runPhoneAdjustment(filePath, event, backup) { 
    if (!isAdmin()) { 
        event.sender.send("log", "❌ Acesso negado: Permissão de administrador necessária."); 
        return; 
    } 
    const log = (msg) => event.sender.send("log", msg); 
    if (!filePath || !fs.existsSync(filePath)) { 
        log(`❌ Erro: Arquivo para ajuste de fones não encontrado em: ${filePath}`); 
        return; 
    } 
    log(`\n--- Iniciando Ajuste de Fones para: ${path.basename(filePath)} ---`); 
    try { 
        if (backup) { 
            const p = path.parse(filePath); 
            const backupPath = path.join(p.dir, `${p.name}.backup_fones_${Date.now()}${p.ext}`); 
            fs.copyFileSync(filePath, backupPath); 
            log(`Backup do arquivo criado em: ${backupPath}`); 
        } 
        const workbook = new ExcelJS.Workbook(); 
        await workbook.xlsx.readFile(filePath); 
        const worksheet = workbook.worksheets[0]; 
        const phoneColumns = []; 
        worksheet.getRow(1).eachCell({ includeEmpty: true }, (cell, colNumber) => { 
            if (cell.value && typeof cell.value === "string" && cell.value.trim().toLowerCase().startsWith("fone")) { 
                phoneColumns.push(colNumber); 
            } 
        }); 
        phoneColumns.sort((a, b) => a - b); 
        if (phoneColumns.length === 0) { 
            log("⚠️ Nenhuma coluna \"fone\" encontrada. Ajuste pulado."); 
            return; 
        } 
        log(`Ajustando ${phoneColumns.length} colunas de telefone...`); 
        let processedRows = 0; 
        worksheet.eachRow((row, rowNumber) => { 
            if (rowNumber === 1) return; 
            const phoneValuesInRow = phoneColumns
                .map(colNumber => row.getCell(colNumber).value)
                .filter(v => v !== null && v !== undefined && String(v).trim() !== ""); 
            
            phoneColumns.forEach((colNumber, index) => {
                const cell = row.getCell(colNumber);
                if (index < phoneValuesInRow.length) {
                    const phone = phoneValuesInRow[index];
                    const numericPhoneString = String(phone).replace(/\D/g, '');
                    if (numericPhoneString) {
                        cell.value = Number(numericPhoneString);
                        cell.numFmt = '0';
                    } else {
                        cell.value = null; // Limpa se for inválido
                    }
                } else {
                    cell.value = null; // Limpa as células extras
                }
            }); 
            processedRows++; 
        }); 
        await workbook.xlsx.writeFile(filePath); 
        log(`✅ Ajuste de fones concluído. ${processedRows} linhas processadas.`); 
    } catch (err) { 
        log(`❌ Erro catastrófico durante o ajuste de fones: ${err.message}`); 
        console.error(err); 
    } 
}

// NOVO: Handler para dividir arquivos CSV grandes (POSIÇÃO CORRIGIDA)
ipcMain.on("split-large-csv", async (event, { filePath, linesPerSplit }) => {
    const log = (msg) => event.sender.send("blocklist-log", msg);

    if (!fs.existsSync(filePath)) {
        log(`❌ ERRO: O arquivo de entrada não foi encontrado em: ${filePath}`);
        return;
    }

    log(`--- Iniciando divisão do arquivo: ${path.basename(filePath)} ---`);
    log(`⚙️  Configuração: ${linesPerSplit.toLocaleString('pt-BR')} linhas por arquivo.`);

    const inputStream = fs.createReadStream(filePath);
    const parser = parse({
        delimiter: ',',
        from_line: 1
    });

    let fileCounter = 1;
    let lineCounter = 0;
    let rowsForCurrentFile = [];
    const outputDir = path.dirname(filePath);
    const baseName = path.basename(filePath, '.csv');

    // Função para salvar um lote de linhas em um arquivo CSV (lógica do split-csv.js)
    const saveChunkToCsv = async (rows, partNumber) => {
        if (rows.length === 0) return;

        const outputFilePath = path.join(outputDir, `${baseName}_parte_${partNumber}.csv`);
        log(`\n⏳ Gerando arquivo: ${path.basename(outputFilePath)} com ${rows.length.toLocaleString('pt-BR')} linhas...`);

        const workbook = new ExcelJS.Workbook();
        const worksheet = workbook.addWorksheet('Telefones');
        worksheet.columns = [{ header: 'telefone', key: 'telefone', width: 20 }];
        
        const cleanedRows = rows.map(row => {
            const cleanedPhone = String(row[1] || '').replace(/\D/g, '');
            return { telefone: cleanedPhone };
        }).filter(r => r.telefone); // Garante que não adiciona linhas vazias

        worksheet.addRows(cleanedRows);
        
        await workbook.csv.writeFile(outputFilePath, { formatterOptions: { delimiter: ';' } });
        log(`✅ Arquivo salvo: ${path.basename(outputFilePath)}`);
    };

    for await (const row of inputStream.pipe(parser)) {
        rowsForCurrentFile.push(row);
        lineCounter++;
        if (lineCounter % 100000 === 0) log(`... ${lineCounter.toLocaleString('pt-BR')} linhas processadas`);
        if (rowsForCurrentFile.length >= linesPerSplit) { await saveChunkToCsv(rowsForCurrentFile, fileCounter); rowsForCurrentFile = []; fileCounter++; }
    }
    if (rowsForCurrentFile.length > 0) { await saveChunkToCsv(rowsForCurrentFile, fileCounter); }
    log(`\n\n🎉 Processo concluído! Total de ${lineCounter.toLocaleString('pt-BR')} linhas divididas em ${fileCounter} arquivo(s).`);
    shell.showItemInFolder(outputDir);
});

// --- INÍCIO: NOVA LÓGICA DE ENVIO DE E-MAIL ---
async function sendBlocklistUpdateEmail(totalNewPhones, finalTotalCount) {
    // ATENÇÃO: Use uma senha de aplicativo se o Gmail tiver 2FA ativado.
    // É altamente recomendado usar variáveis de ambiente para credenciais em um app real.
    const transporter = nodemailer.createTransport({
        host: process.env.SMTP_HOST || "smtp.gmail.com",
        port: parseInt(process.env.SMTP_PORT, 10) || 465,
        secure: (process.env.SMTP_PORT || "465") === "465", // true for 465, false for other ports
        auth: {
            user: process.env.SMTP_USER, // Carregado do .env
            pass: process.env.SMTP_PASS, // Carregado do .env
        },
    });

    const now = new Date();
    const formattedDate = now.toLocaleDateString('pt-BR', { day: '2-digit', month: '2-digit', year: 'numeric' });
    const formattedTime = now.toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit' });

    const mailOptions = {
        from: `"Gerenciador de Bases" <${process.env.SMTP_USER}>`,
        to: "tatiane@mbfinance.com.br", // Destinatários principais, separados por vírgula
        cc: "rodrigo.gadelha@mbfinance.com.br", // Pessoas em cópia (CC), separadas por vírgula
        subject: "✅ Atualização da Blocklist Concluída",
        html: `
            <div style="font-family: Arial, sans-serif; color: #333;">
                <h2>Relatório de Atualização da Blocklist</h2>
                <p>A base de dados da blocklist foi atualizada com sucesso.</p>
                <hr>
                <p><strong>Novos números adicionados:</strong> ${totalNewPhones.toLocaleString('pt-BR')}</p>
                <p><strong>Total de números na blocklist:</strong> ${finalTotalCount.toLocaleString('pt-BR')}</p>
                <hr>
                <p style="font-size: 12px; color: #777;">
                    Data da atualização: ${formattedDate} às ${formattedTime}<br>
                    Processo executado por: ${currentUser.username}
                </p>
            </div>
        `,
    };

    return transporter.sendMail(mailOptions);
}
// --- FIM: NOVA LÓGICA DE ENVIO DE E-MAIL ---

// NOVO: Handler para alimentar a base de dados da blocklist
ipcMain.on("feed-blocklist", async (event, { filePaths, sendEmail }) => { // MODIFICADO: Recebe a opção de e-mail
    if (!isAdmin() || !pool) { event.sender.send("blocklist-log", "❌ Acesso negado ou conexão com BD inativa."); return; }
    const log = (msg) => event.sender.send("blocklist-log", msg); // CORRIGIDO: Envia para o log da aba correta
    log(`--- Iniciando Alimentação da Blocklist na nova aba ---`);
    
    const DB_BATCH_SIZE = 50000; // Tamanho do lote para enviar ao banco de dados
    let totalNewPhonesAdded = 0;

    const processChunk = async (phoneChunk) => {
        if (phoneChunk.size === 0) return;
        try {
            const query = `
                INSERT INTO blocklist (telefone)
                SELECT unnest($1::text[])
                ON CONFLICT (telefone) DO NOTHING;
            `;
            const result = await pool.query(query, [Array.from(phoneChunk)]);
            const newCount = result.rowCount;
            if (newCount > 0) {
                log(`✅ Lote salvo. ${newCount} novos telefones adicionados à blocklist.`);
                // phoneChunk.forEach(phone => blocklistPhones.add(phone)); // REMOVIDO: A contagem em memória é imprecisa.
                totalNewPhonesAdded += newCount;
            }
        } catch (e) {
            log(`❌ Erro ao salvar lote na blocklist: ${e.message}`);
        }
    };

    for (const filePath of filePaths) {
        const fileName = path.basename(filePath);
        log(`\nIniciando processamento do arquivo: ${fileName}`);
        
        let phonesInBatch = new Set();
        let rowsProcessed = 0;
        const fileStream = fs.createReadStream(filePath);

        try {
            const processRow = (row) => {
                row.eachCell({ includeEmpty: true }, (cell) => {
                    const phone = cell.value ? String(cell.value).replace(/\D/g, "").trim() : null;
                    if (phone && phone.length >= 8) {
                        phonesInBatch.add(phone);
                    }
                });
            };

            const checkAndProcessBatch = async () => {
                if (phonesInBatch.size >= DB_BATCH_SIZE) {
                    await processChunk(phonesInBatch);
                    phonesInBatch.clear();
                }
            };

            const logProgress = () => {
                rowsProcessed++;
                if (rowsProcessed % 100000 === 0) {
                    log(`... ${rowsProcessed.toLocaleString('pt-BR')} linhas do arquivo "${fileName}" lidas...`);
                }
            };

            if (path.extname(filePath).toLowerCase().endsWith('.csv')) {
                const csvStream = fileStream.pipe(parse({ delimiter: [',', ';'], relax_column_count: true }));
                for await (const record of csvStream) {
                    record.forEach(value => {
                        const phone = String(value || '').replace(/\D/g, "").trim();
                        if (phone && phone.length >= 8) phonesInBatch.add(phone);
                    });
                    await checkAndProcessBatch();
                    logProgress();
                }
            } else {
                const workbookReader = new ExcelJS.stream.xlsx.WorkbookReader(fileStream);
                for await (const worksheetReader of workbookReader) {
                    for await (const row of worksheetReader) {
                        processRow(row);
                        await checkAndProcessBatch();
                        logProgress();
                    }
                }
            }

            // Processa o lote final que sobrou
            if (phonesInBatch.size > 0) {
                await processChunk(phonesInBatch);
            }
            log(`\n✅ Finalizado o processamento do arquivo ${fileName}. Total de ${rowsProcessed.toLocaleString('pt-BR')} linhas lidas.`);
        } catch (err) {
            log(`❌ Erro catastrófico ao processar o arquivo ${fileName}: ${err.message}`);
        }
    }

    log(`\n--- Alimentação da Blocklist Concluída ---`);
    log(`Total de telefones novos adicionados: ${totalNewPhonesAdded.toLocaleString('pt-BR')}.`);

    try {
        const finalCountResult = await pool.query('SELECT COUNT(*) FROM blocklist;');
        const finalTotalCount = parseInt(finalCountResult.rows[0].count, 10);
        log(`Total na blocklist agora: ${finalTotalCount.toLocaleString('pt-BR')}.`);

        if (sendEmail) {
            log(`\n📧 Opção de e-mail ativada. Enviando notificação para tatiane@mbfinance.com.br...`);
            await sendBlocklistUpdateEmail(totalNewPhonesAdded, finalTotalCount);
            log(`✅ E-mail de notificação enviado com sucesso!`);
        }

    } catch (error) {
        log(`❌ Erro na etapa final (contagem/e-mail): ${error.message}`);
        console.error("Erro na etapa final da blocklist:", error);
    }
});

// NOVO: Handler para buscar estatísticas da blocklist
ipcMain.handle("get-blocklist-stats", async () => {
    if (!isAdmin() || !pool) {
        return { success: false, message: "Acesso negado ou conexão com BD inativa.", data: { total: 0, addedToday: 0 } };
    }
    try {
        const totalQuery = 'SELECT COUNT(*) FROM blocklist;';
        // A query abaixo considera o dia atual no fuso horário do servidor do BD.
        const todayQuery = "SELECT COUNT(*) FROM blocklist WHERE data_adicao >= current_date;";

        const [totalResult, todayResult] = await Promise.all([
            pool.query(totalQuery),
            pool.query(todayQuery)
        ]);

        const stats = {
            total: parseInt(totalResult.rows[0].count, 10) || 0,
            addedToday: parseInt(todayResult.rows[0].count, 10) || 0,
        };
        return { success: true, data: stats };
    } catch (error) {
        console.error("Erro ao buscar estatísticas da blocklist:", error);
        return { success: false, message: error.message, data: { total: 0, addedToday: 0 } };
    }
});

// NOVO: Handler para verificar números na blocklist
ipcMain.handle("check-blocklist-numbers", async (event, numbers) => {
    if (!isAdmin() || !pool) {
        return { success: false, message: "Acesso negado ou conexão com BD inativa." };
    }
    if (!numbers || numbers.length === 0) {
        return { success: false, message: "Nenhum número fornecido para verificação." };
    }

    try {
        // MODIFICADO: A query agora busca a data de adição e a formata.
        const query = `
            SELECT 
                telefone, 
                to_char(data_adicao, 'DD/MM/YYYY HH24:MI:SS') as data_formatada 
            FROM blocklist 
            WHERE telefone = ANY($1::text[])
        `;
        const result = await pool.query(query, [numbers]);
        
        // MODIFICADO: Usamos um Map para associar o número à sua data.
        const foundNumbersMap = new Map(result.rows.map(row => [row.telefone, row.data_formatada]));
        const notFoundNumbers = numbers.filter(num => !foundNumbersMap.has(num));

        // MODIFICADO: O array 'found' agora contém objetos com o telefone e a data.
        const foundData = Array.from(foundNumbersMap.entries()).map(([telefone, data]) => ({ telefone, data_adicao: data }));

        return { 
            success: true, 
            data: {
                found: foundData,
                notFound: notFoundNumbers
            } 
        };
    } catch (error) {
        return { success: false, message: `Erro ao consultar a blocklist: ${error.message}` };
    }
});


// --- FUNÇÃO PARA ALIMENTAR A BASE RAIZ (Refatorada para PostgreSQL) ---
ipcMain.on("feed-root-database", async (event, filePaths) => {
    if (!isAdmin() || !pool) { log("❌ Acesso negado ou conexão com BD inativa."); event.sender.send("root-feed-finished"); return; }
    const log = (msg) => event.sender.send("log", msg);
    log(`--- Iniciando Alimentação da Base Raiz ---`);

    const BATCH_SIZE = 5000;
    let totalNewCnpjsAdded = 0;

    const processChunk = async (cnpjChunk, sourceFile, batchId) => {
        if (cnpjChunk.length === 0) return;
        try {
            const query = `
                INSERT INTO raiz_cnpjs (cnpj, fonte, lote_id)
                SELECT d.cnpj, $2, $3 FROM unnest($1::text[]) AS d(cnpj)
                ON CONFLICT (cnpj) DO NOTHING;
            `;
            const result = await pool.query(query, [cnpjChunk, sourceFile, batchId]);
            const newCount = result.rowCount;
            if (newCount > 0) {
                log(`✅ ${newCount} CNPJs novos salvos na coleção Raiz com sucesso.`);
                totalNewCnpjsAdded += newCount;
            }
        } catch (e) {
            log(`❌ Erro ao salvar lote na coleção Raiz: ${e.message}`);
        }
    };

    for (const filePath of filePaths) {
        const fileName = path.basename(filePath);
        log(`\nIniciando processamento do arquivo: ${fileName}`);
        try {
            const workbook = new ExcelJS.Workbook(); await workbook.xlsx.readFile(filePath); const worksheet = workbook.worksheets[0]; if (!worksheet || worksheet.rowCount <= 1) { log(`⚠️ Arquivo ${fileName} está vazio ou não possui dados. Pulando.`); continue; }
            let cnpjColIdx = -1; worksheet.getRow(1).eachCell((cell, colNumber) => { const header = String(cell.value || "").trim().toLowerCase(); if (header === 'cpf' || header === 'cnpj') cnpjColIdx = colNumber; });
            if (cnpjColIdx === -1) { log(`❌ ERRO: Coluna 'cpf' ou 'cnpj' não encontrada em ${fileName}. Pulando.`); continue; }

            let cnpjsFromFile = new Set();
            const batchId = `raiz-feed-${Date.now()}`;

            for (let i = 2; i <= worksheet.rowCount; i++) {
                const row = worksheet.getRow(i);
                const cellValue = row.getCell(cnpjColIdx).value;
                const cnpj = cellValue ? String(cellValue).replace(/\D/g, "").trim() : null;
                if (cnpj && (cnpj.length === 11 || cnpj.length === 14)) cnpjsFromFile.add(cnpj);

                if (cnpjsFromFile.size >= BATCH_SIZE) {
                    await processChunk(Array.from(cnpjsFromFile), fileName, batchId);
                    cnpjsFromFile.clear();
                }
            }
            if (cnpjsFromFile.size > 0) {
                await processChunk(Array.from(cnpjsFromFile), fileName, batchId);
                cnpjsFromFile.clear();
            }
            log(`\n✅ Finalizado o processamento do arquivo ${fileName}.`);
        } catch (err) {
            log(`❌ Erro catastrófico ao processar o arquivo ${fileName}: ${err.message}`);
        }
    }
    log(`\n--- Alimentação da Base Raiz Concluída ---`);
    log(`Total de CNPJs novos adicionados à Raiz: ${totalNewCnpjsAdded}`);
    event.sender.send("root-feed-finished");
});


// --- FUNÇÕES DA LIMPEZA LOCAL (Refatoradas para PostgreSQL) ---

ipcMain.handle("save-stored-cnpjs-to-excel", async (event) => {
    if (!isAdmin() || !pool) { return { success: false, message: "Acesso negado ou conexão com BD inativa." }; }
    if (storedCnpjs.size === 0) { dialog.showMessageBox(mainWindow, { type: "info", title: "Aviso", message: "Nenhum CNPJ armazenado para salvar." }); return { success: false, message: "Nenhum CNPJ armazenado para salvar." }; }
    const { canceled, filePath } = await dialog.showSaveDialog(mainWindow, { title: "Salvar CNPJs Armazenados", defaultPath: `cnpjs_armazenados_${Date.now()}.xlsx`, filters: [ { name: "Excel Files", extensions: ["xlsx"] } ] });
    if (canceled || !filePath) { return { success: false, message: "Operação de salvar cancelada." }; }
    try { const data = Array.from(storedCnpjs).map(cnpj => [cnpj]); const worksheet = XLSX.utils.aoa_to_sheet([ ["cpf"], ...data ]); const workbook = XLSX.utils.book_new(); XLSX.utils.book_append_sheet(workbook, worksheet, "CNPJs"); XLSX.writeFile(workbook, filePath); dialog.showMessageBox(mainWindow, { type: "info", title: "Sucesso", message: `Arquivo salvo com sucesso em: ${filePath}` }); return { success: true, message: `Arquivo salvo com sucesso em: ${filePath}` }; } catch (err) { console.error("Erro ao salvar Excel:", err); dialog.showMessageBox(mainWindow, { type: "error", title: "Erro", message: `Erro ao salvar arquivo: ${err.message}` }); return { success: false, message: `Erro ao salvar arquivo: ${err.message}` }; }
});

ipcMain.handle("delete-batch", async (event, batchId) => {
    if (!isAdmin() || !pool) { return { success: false, message: "Acesso negado ou conexão com BD inativa." }; }
    const log = (msg) => event.sender.send("log", msg);
    if (!batchId) { return { success: false, message: "ID do lote inválido." }; }
    log(`Buscando e excluindo documentos do lote "${batchId}" no Neon DB...`);
    try {
        const result = await pool.query('DELETE FROM limpeza_cnpjs WHERE batch_id = $1 RETURNING cnpj', [batchId]);
        const deletedCount = result.rowCount;
        if (deletedCount === 0) {
            return { success: false, message: `Nenhum CNPJ encontrado para o lote "${batchId}".` };
        }
        result.rows.forEach(row => storedCnpjs.delete(row.cnpj));
        log(`Total de CNPJs no cache local agora: ${storedCnpjs.size}`);
        return { success: true, message: `✅ ${deletedCount} CNPJs do lote "${batchId}" foram excluídos com sucesso!` };
    } catch (err) {
        console.error("Erro ao excluir lote do Neon DB:", err);
        return { success: false, message: `❌ Erro ao excluir lote: ${err.message}` };
    }
});

ipcMain.handle("update-blocklist", async (event, backup) => { if (!isAdmin()) { return { success: false, message: "Acesso negado." }; } const log = (msg) => event.sender.send("log", msg); try { const blocklistPath = "G:\\Meu Drive\\Marketing\\!Campanhas\\URA - Automatica\\Limpeza de base\\bases para a raiz\\Blocklist.xlsx"; const rootPath = "G:\\Meu Drive\\Marketing\\!Campanhas\\URA - Automatica\\Limpeza de base\\raiz_att.xlsx"; if (backup) { const timestamp = Date.now(); const bkp = path.join(path.dirname(rootPath), `${path.basename(rootPath, path.extname(rootPath))}.backup_${timestamp}${path.extname(rootPath)}`); fs.copyFileSync(rootPath, bkp); log(`Backup da raiz criado em: ${bkp}`); } const wbBlock = await readSpreadsheet(blocklistPath); const dataBlock = XLSX.utils.sheet_to_json(wbBlock.Sheets[wbBlock.SheetNames[0]], { header: 1 }).flat().filter(v => v); const wbRoot = await readSpreadsheet(rootPath); const dataRoot = XLSX.utils.sheet_to_json(wbRoot.Sheets[wbRoot.SheetNames[0]], { header: 1 }).flat().filter(v => v); const merged = Array.from(new Set([...dataRoot, ...dataBlock])).map(v => [v]); const newWB = XLSX.utils.book_new(); XLSX.utils.book_append_sheet(newWB, XLSX.utils.aoa_to_sheet(merged), wbRoot.SheetNames[0]); writeSpreadsheet(newWB, rootPath); return { success: true, message: "Raiz atualizada com blocklist com sucesso." }; } catch (err) { return { success: false, message: err.message }; } });

ipcMain.on("start-cleaning", async (event, args) => {
    if (!isAdmin()) { event.sender.send("log", "❌ Acesso negado."); return; }
    const log = (msg) => event.sender.send("log", msg);

    if ((args.isAutoRoot || args.checkDb || args.saveToDb || args.checkBlocklist) && !pool) { // MODIFICADO
        return log("❌ ERRO: Uma ou mais opções de Banco de Dados estão ativadas, mas a conexão com o BD falhou ou não foi configurada.");
    }

    try {
        const batchId = `batch-${Date.now()}`;
        if (args.saveToDb) log(`Este lote de salvamento terá o ID: ${batchId}`);
        const rootSet = new Set();
        if (args.isAutoRoot) {
            log("Auto Raiz ATIVADO. Carregando lista raiz do Banco de Dados...");
            const result = await pool.query('SELECT cnpj FROM raiz_cnpjs');
            result.rows.forEach(row => rootSet.add(row.cnpj));
            log(`✅ Raiz do BD carregada. Total de CNPJs na raiz: ${rootSet.size}.`);
        } else {
            if (!args.rootFile || !fs.existsSync(args.rootFile)) { return log(`❌ Arquivo raiz não encontrado: ${args.rootFile}`); } const rootIdx = letterToIndex(args.rootCol); const wbRoot = await readSpreadsheet(args.rootFile); const sheetRoot = wbRoot.Sheets[wbRoot.SheetNames[0]]; const rowsRoot = XLSX.utils.sheet_to_json(sheetRoot, { header: 1 }).map(r => r[rootIdx]).filter(v => v).map(v => String(v).trim()); rowsRoot.forEach(item => rootSet.add(item)); log(`Lista raiz do arquivo carregada com ${rootSet.size} valores.`);
        }
        log(`Histórico de CNPJs em memória com ${storedCnpjs.size} registros.`);
        if (args.checkDb) log("Opção \"Consultar Banco de Dados\" está ATIVADA.");
        if (args.checkBlocklist) log(`Opção "Verificar Blocklist" está ATIVADA (consulta via BD).`); // NOVO LOG
        if (args.saveToDb) log("Opção \"Salvar no Banco de Dados\" está ATIVADA.");
        if (args.autoAdjust) log("Opção \"Ajustar Fones Pós-Limpeza\" está ATIVADA.");
        log(`FILTRO DE CNAE PROIBIDO: ATIVADO (Padrão).`);

        const allNewCnpjs = new Set();
        for (const fileObj of args.cleanFiles) {
            // MODIFICADO: Passa o novo argumento 'checkBlocklist' para a função de processo
            const newlyFoundInFile = await processFile(fileObj, rootSet, args.destCol, event, args.backup, args.checkDb, args.saveToDb, storedCnpjs, args.checkBlocklist);
            if (args.saveToDb && newlyFoundInFile.size > 0) {
                newlyFoundInFile.forEach(cnpj => allNewCnpjs.add(cnpj));
            }
            if (args.autoAdjust) {
                await runPhoneAdjustment(fileObj.path, event, false);
            }
        }
        if (args.saveToDb && allNewCnpjs.size > 0) {
            log(`\nEnviando ${allNewCnpjs.size} novos CNPJs para o banco de dados...`);
            const cnpjsArray = Array.from(allNewCnpjs);

            const query = `
                INSERT INTO limpeza_cnpjs (cnpj, batch_id)
                SELECT d.cnpj, $2 FROM unnest($1::text[]) AS d(cnpj)
                ON CONFLICT (cnpj) DO NOTHING;
            `;
            const result = await pool.query(query, [cnpjsArray, batchId]);
            event.sender.send("upload-progress", { current: 1, total: 1 });
            cnpjsArray.forEach(cnpj => storedCnpjs.add(cnpj));

            log(`✅ ${result.rowCount} novos registros adicionados ao banco de dados. Total agora: ${storedCnpjs.size}.`);
            log(`✅ ID do Lote salvo: ${batchId} (use este ID para futuras exclusões)`);
        }
        log(`\n✅ Processo concluído para todos os arquivos.`);
    } catch (err) {
        log(`❌ Erro inesperado no processo de limpeza: ${err.message}`);
        console.error(err);
    }
});

// #################################################################
// #           FUNÇÃO DE LIMPEZA PRINCIPAL (MODIFICADA)            #
// #################################################################
async function processFile(fileObj, rootSet, destCol, event, backup, checkDb, saveToDb, cnpjsHistory, checkBlocklist) { // MODIFICADO: adiciona 'checkBlocklist'
    const file = fileObj.path;
    const id = fileObj.id;
    const log = (msg) => event.sender.send("log", msg);
    const progress = (pct) => event.sender.send("progress", { id, progress: pct });

    /**
     * Limpa o nome do cliente, removendo números e caracteres especiais do início e do fim.
     * Ex: "123.456 NOME CLIENTE 789" -> "NOME CLIENTE"
     */
    const cleanClientName = (name) => {
        if (!name || typeof name !== 'string') return name;
        return name.replace(/^[\d.\- ]+|[\d.\- ]+$/g, '').trim();
    };

    log(`\nProcessando arquivo de limpeza: ${path.basename(file)}...`);
    if (!fs.existsSync(file)) return new Set();

    if (backup) {
        const p = path.parse(file);
        const bkp = path.join(p.dir, `${p.name}.backup_${Date.now()}${p.ext}`);
        fs.copyFileSync(file, bkp);
        log(`Backup criado: ${bkp}`);
    }

    const wb = await readSpreadsheet(file);
    const sheet = wb.Sheets[wb.SheetNames[0]];
    const data = XLSX.utils.sheet_to_json(sheet, { header: 1 });

    if (data.length <= 1) {
        log(`⚠️ Arquivo vazio ou sem dados: ${file}`);
        return new Set();
    }

    const header = data[0];
    const destColIdx = letterToIndex(destCol);

    // Identifica colunas CPF, FONE e a nova coluna CNAE/LIVRE3
    const cpfColIdx = header.findIndex(h => String(h).trim().toLowerCase() === "cpf");
    const nomeColIdx = header.findIndex(h => String(h).trim().toLowerCase() === "nome"); // NOVO: Encontra a coluna 'nome'
    const cnaeColIdx = header.findIndex(h => ["cnae", "livre3"].includes(String(h).trim().toLowerCase()));
    const foneIdxs = header.reduce((acc, cell, i) => {
        // MODIFICADO: Captura todas as colunas de fone1 a fone16 (e além, se houver)
        if (typeof cell === "string" && /^fone([1-9]|1[0-9])$/.test(cell.trim().toLowerCase())) {
            acc.push(i);
        }
        return acc;
    }, []);

    if (cpfColIdx === -1) {
        log(`❌ ERRO: A coluna "cpf" não foi encontrada no arquivo ${path.basename(file)}. Pulando este arquivo.`);
        return new Set();
    }
    if (nomeColIdx === -1) { // NOVO: Avisa se a coluna 'nome' não for encontrada
        log(`⚠️ AVISO: Nenhuma coluna "nome" encontrada em ${path.basename(file)}. A limpeza de nomes será ignorada para este arquivo.`);
    }
    if (foneIdxs.length === 0 && checkBlocklist) { // NOVO
        log(`⚠️ AVISO: A verificação de blocklist está ativa, mas nenhuma coluna 'fone' (fone1 a fone16) foi encontrada.`);
    }
    if (cnaeColIdx === -1) {
        log(`⚠️ AVISO: Nenhuma coluna "cnae" ou "livre3" encontrada em ${path.basename(file)}. A verificação de CNAE será ignorada para este arquivo.`);
    }


    const cleaned = [header];
    let removedByRoot = 0;
    let removedDuplicates = 0;
    let removedByCnae = 0;
    let removedByBlocklist = 0; // NOVO: Contador para blocklist
    let cleanedPhones = 0;
    const BATCH_SIZE = 1000; // Lote para verificação de blocklist
    const totalRows = data.length - 1;
    const newCnpjsInThisFile = new Set();

    for (let i = 1; i < data.length; i++) {
        const row = data[i];
        const key = row[destColIdx] ? String(row[destColIdx]).trim() : "";
        const cnpj = row[cpfColIdx] ? String(row[cpfColIdx]).trim().replace(/\D/g, "") : "";

        if (checkDb && cnpj && cnpjsHistory.has(cnpj)) {
            removedDuplicates++;
            continue;
        }

        if (key && rootSet.has(key)) {
            removedByRoot++;
            continue;
        }

        if (cnaeColIdx !== -1) {
            const cnaeValue = row[cnaeColIdx] ? String(row[cnaeColIdx]).replace(/\D/g, "").trim() : "";
            if (cnaeValue && PROHIBITED_CNAES.has(cnaeValue)) {
                removedByCnae++;
                continue;
            }
        }

        foneIdxs.forEach(idx => {
            const v = row[idx] ? String(row[idx]).trim() : "";
            if (/^\d{10}$/.test(v)) { row[idx] = null; cleanedPhones++; }
        });

        // NOVO: Aplica a limpeza na coluna 'nome', se ela existir
        if (nomeColIdx !== -1 && row[nomeColIdx]) {
            row[nomeColIdx] = cleanClientName(row[nomeColIdx]);
        }

        cleaned.push(row);
        if (saveToDb && cnpj && !cnpjsHistory.has(cnpj)) {
            newCnpjsInThisFile.add(cnpj);
        }
    }

    // Se a verificação de blocklist estiver desativada, podemos pular a próxima etapa
    if (!checkBlocklist || foneIdxs.length === 0) {
        const finalWB = XLSX.utils.book_new();
        XLSX.utils.book_append_sheet(finalWB, XLSX.utils.aoa_to_sheet(cleaned), wb.SheetNames[0]);
        writeSpreadsheet(finalWB, file);
        progress(100);
        log(`Arquivo: ${path.basename(file)}\n • Clientes repetidos (BD): ${removedDuplicates}\n • Removidos pela Raiz: ${removedByRoot}\n • Removidos por Blocklist (Fone): ${removedByBlocklist}\n • Removidos por CNAE Proibido: ${removedByCnae}\n • Fones limpos: ${cleanedPhones}\n • Total final: ${cleaned.length - 1}`);
        return newCnpjsInThisFile;
    }

    // Verificação de Blocklist em Lotes
    log(`Iniciando verificação de blocklist para ${cleaned.length - 1} linhas...`);
    const finalCleaned = [header];
    const dataToVerify = cleaned.slice(1);

    for (let i = 0; i < dataToVerify.length; i += BATCH_SIZE) {
        const batch = dataToVerify.slice(i, i + BATCH_SIZE);
        const phonesInBatch = new Set();
        batch.forEach(row => {
            foneIdxs.forEach(foneIdx => {
                const phoneValue = row[foneIdx] ? String(row[foneIdx]).replace(/\D/g, "").trim() : "";
                if (phoneValue) phonesInBatch.add(phoneValue);
            });
        });

        const blockedPhonesInBatch = new Set();
        if (phonesInBatch.size > 0) {
            const query = 'SELECT telefone FROM blocklist WHERE telefone = ANY($1::text[])';
            const { rows } = await pool.query(query, [Array.from(phonesInBatch)]);
            rows.forEach(row => blockedPhonesInBatch.add(row.telefone));
        }

        for (const row of batch) {
            const isBlocked = foneIdxs.some(foneIdx => {
                const phoneValue = row[foneIdx] ? String(row[foneIdx]).replace(/\D/g, "").trim() : "";
                return phoneValue && blockedPhonesInBatch.has(phoneValue);
            });

            if (isBlocked) {
                removedByBlocklist++;
            } else {
                finalCleaned.push(row);
            }
        }

        if (i % 2000 === 0) {
            progress(Math.floor((i / totalRows) * 100));
            await new Promise(resolve => setImmediate(resolve));
        }
    }

    const newWB = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(newWB, XLSX.utils.aoa_to_sheet(finalCleaned), wb.SheetNames[0]);
    writeSpreadsheet(newWB, file);
    progress(100);

    log(`Arquivo: ${path.basename(file)}\n • Clientes repetidos (BD): ${removedDuplicates}\n • Removidos pela Raiz: ${removedByRoot}\n • Removidos por Blocklist (Fone): ${removedByBlocklist}\n • Removidos por CNAE Proibido: ${removedByCnae}\n • Fones limpos: ${cleanedPhones}\n • Total final: ${finalCleaned.length - 1}`);

    return newCnpjsInThisFile;
}


ipcMain.on("start-db-only-cleaning", async (event, { filesToClean, saveToDb }) => {
    if (!isAdmin() || !pool) { event.sender.send("log", "❌ Acesso negado ou conexão com BD inativa."); return; }
    const log = (msg) => event.sender.send("log", msg);
    const batchId = `batch-${Date.now()}`;
    log(`--- Iniciando Limpeza Apenas pelo Banco de Dados para ${filesToClean.length} arquivo(s) ---`);
    if (saveToDb) log(`Opção \"Salvar no Banco de Dados\" ATIVADA. ID do Lote: ${batchId}`);
    log(`Usando ${storedCnpjs.size} CNPJs do histórico em memória.`);
    const allNewCnpjs = new Set();
    for (const filePath of filesToClean) { log(`\nProcessando: ${path.basename(filePath)}`); try { const wb = await readSpreadsheet(filePath); const sheet = wb.Sheets[wb.SheetNames[0]]; const data = XLSX.utils.sheet_to_json(sheet, { header: 1 }); if (data.length <= 1) { log(`⚠️ Arquivo vazio ou sem dados: ${filePath}`); continue; } const header = data[0]; const cpfColIdx = header.findIndex(h => String(h).trim().toLowerCase() === "cpf"); if (cpfColIdx === -1) { log(`❌ ERRO: A coluna \"cpf\" não foi encontrada em ${path.basename(filePath)}. Pulando.`); continue; } let removedCount = 0; const cleaned = [header]; for (let i = 1; i < data.length; i++) { const row = data[i]; const cnpj = row[cpfColIdx] ? String(row[cpfColIdx]).trim().replace(/\D/g, "") : ""; if (cnpj && storedCnpjs.has(cnpj)) { removedCount++; continue; } cleaned.push(row); if (saveToDb && cnpj) { allNewCnpjs.add(cnpj); } } const newWB = XLSX.utils.book_new(); XLSX.utils.book_append_sheet(newWB, XLSX.utils.aoa_to_sheet(cleaned), wb.SheetNames[0]); writeSpreadsheet(newWB, filePath); log(`✅ Arquivo ${path.basename(filePath)} concluído. Removidos: ${removedCount}. Total final: ${cleaned.length - 1}`); } catch (err) { log(`❌ Erro ao processar ${path.basename(filePath)}: ${err.message}`); console.error(err); } }

    if (saveToDb && allNewCnpjs.size > 0) {
        log(`\nEnviando ${allNewCnpjs.size} novos CNPJs para o banco de dados...`);
        const cnpjsArray = Array.from(allNewCnpjs);
        const query = `INSERT INTO limpeza_cnpjs (cnpj, batch_id) SELECT d.cnpj, $2 FROM unnest($1::text[]) AS d(cnpj) ON CONFLICT (cnpj) DO NOTHING;`;
        const result = await pool.query(query, [cnpjsArray, batchId]);
        event.sender.send("upload-progress", { current: 1, total: 1 });
        cnpjsArray.forEach(cnpj => storedCnpjs.add(cnpj));
        log(`✅ ${result.rowCount} novos registros adicionados. Total agora: ${storedCnpjs.size}.`);
        log(`✅ ID do Lote salvo: ${batchId}`);
    }
    log("\n--- Limpeza Apenas pelo Banco de Dados finalizada. ---");
});

// =================================================================
// =           *** INÍCIO DA MODIFICAÇÃO *** =
// =================================================================
ipcMain.on('organize-daily-sheet', async (event, filePath, organizationType) => { // MODIFICADO
    const log = (msg) => event.sender.send("log", msg);

    /** * Limpa o nome do cliente, removendo números e caracteres especiais do início e do fim.
     * Ex: "123.456 NOME CLIENTE 789" -> "NOME CLIENTE"
     */
    const cleanClientName = (name) => {
        if (!name || typeof name !== 'string') return name;
        return name.replace(/^[\d.\- ]+|[\d.\- ]+$/g, '').trim();
    };

    log(`--- Iniciando Organização (${organizationType}) da Planilha Diária ---`);
    
    // --- LÓGICA PARA NOVA FUNCIONALIDADE DE SEPARAR POR ABAS (CADÊNCIAS) ---
    if (organizationType === 'cadencia') {
        const fileNameLower = path.basename(filePath).toLowerCase();
        const dir = path.dirname(filePath);
        const originalName = path.parse(filePath).name;
        
        try {
            const workbook = new ExcelJS.Workbook();
            await workbook.xlsx.readFile(filePath); 
            let processedSheetCount = 0;

            for (const worksheet of workbook.worksheets) {
                const sheetName = worksheet.name;
                log(`\nProcessando aba: "${sheetName}"...`);
                processedSheetCount++;

                // MODIFICADO: O caminho do arquivo agora é .csv
                const newFilePath = path.join(dir, `${originalName}_${sheetName.replace(/[^a-zA-Z0-9]/g, '_')}_organizado.csv`);

                // NOVO: Lógica para mapeamento de colunas (dinâmico com fallback)
                const headerRow = worksheet.getRow(1);
                const headerMap = {};
                headerRow.eachCell({ includeEmpty: true }, (cell, colNumber) => {
                    if (cell.value) {
                        const normalizedHeader = String(cell.value).trim().toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g, "");
                        headerMap[normalizedHeader] = colNumber;
                    }
                });

                const findColumn = (possibleNames, fallback) => {
                    for (const name of possibleNames) {
                        const normalizedName = name.toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g, "");
                        for (const headerKey in headerMap) {
                            if (headerKey.includes(normalizedName)) {
                                return headerMap[headerKey];
                            }
                        }
                    }
                    log(`⚠️ Cabeçalho para "${possibleNames[0]}" não encontrado. Usando fallback para coluna ${fallback}.`);
                    return fallback; // Retorna a letra da coluna como fallback
                };
                // FIM DA NOVA LÓGICA DE MAPEAMENTO

                const dataForCsv = []; // MODIFICADO: Array para armazenar os dados para o CSV
                worksheet.eachRow((row, rowNumber) => {
                    if (rowNumber > 1 && row.values.length > 1) { // Pula o cabeçalho e ignora linhas vazias
                        let newRowData = null;
                        let nome, cpf, fone1, chave;

                        if (fileNameLower.includes('lista gomes')) {
                            let mapping;
                            if (sheetName.toLowerCase().includes('lista 4')) {
                                mapping = {
                                    nome: findColumn(['nome_responsavel'], 'D'),
                                    cpf: findColumn(['cnpj_cliente'], 'B'),
                                    chave: findColumn(['email_responsavel'], 'E'),
                                    fone1: findColumn(['celular_responsavel'], 'F')
                                };
                            } else if (sheetName.toLowerCase().includes('lista 3') || sheetName.toLowerCase().includes('lista 1')) {
                                mapping = {
                                    cpf: findColumn(['cnpj'], 'A'),
                                    nome: findColumn(['nome do negocio'], 'D'),
                                    fone1: findColumn(['telefone celular'], 'G'),
                                    chave: findColumn(['e-mail'], 'H')
                                };
                            }

                            if (mapping) {
                                nome = row.getCell(mapping.nome).value;
                                cpf = row.getCell(mapping.cpf).value;
                                fone1 = row.getCell(mapping.fone1).value;
                                chave = row.getCell(mapping.chave).value;
                            }
                        } else {
                            // Lógica original para "Cadência Equipes"
                            nome = row.getCell('F').value;
                            cpf = row.getCell('B').value;
                            fone1 = row.getCell('L').value;
                            chave = row.getCell('M').value;
                        }

                        if (nome || cpf) { // Processa se houver pelo menos nome ou cpf
                            newRowData = {
                                nome: cleanClientName(nome) || '',
                                cpf: cpf ? String(cpf).replace(/\D/g, '') : '',
                                fone1: fone1 ? String(fone1).replace(/\D/g, '') : '',
                                chave: chave || '',
                                livre7: 'C6' // Este valor é fixo
                            };
                        }
                        // MODIFICADO: Adiciona a linha ao array de dados
                        if (newRowData) {
                            dataForCsv.push(newRowData);
                        }
                    }
                });

                // MODIFICADO: Lógica para escrever o arquivo CSV
                if (dataForCsv.length > 0) {
                    const csvWorkbook = new ExcelJS.Workbook();
                    const csvWorksheet = csvWorkbook.addWorksheet('Organizado');
                    csvWorksheet.columns = [
                        { header: 'nome', key: 'nome' },
                        { header: 'cpf', key: 'cpf' },
                        { header: 'fone1', key: 'fone1' },
                        { header: 'chave', key: 'chave' },
                        { header: 'livre7', key: 'livre7' }
                    ];
                    csvWorksheet.addRows(dataForCsv);
                    // MODIFICADO: Adiciona opções para usar ';' como delimitador
                    await csvWorkbook.csv.writeFile(newFilePath, {
                        formatterOptions: {
                            delimiter: ';'
                        }
                    });
                    log(`✅ Aba "${sheetName}" concluída. ${dataForCsv.length} linhas salvas em: ${path.basename(newFilePath)}`);
                } else {
                    log(`⚠️ Nenhum dado processado para a aba "${sheetName}". Arquivo não foi gerado.`);
                }
            }

            if (processedSheetCount > 0) {
                log(`\n--- ✅ Processo de separação por abas finalizado com sucesso! ---`);
                shell.showItemInFolder(dir); // Abre a pasta onde os arquivos foram salvos
            } else {
                log(`⚠️ Nenhuma aba encontrada no arquivo.`);
            }

        } catch (error) {
            log(`❌ ERRO GERAL ao separar planilhas por aba: ${error.message}`);
            console.error("Erro detalhado na separação:", error);
        }
        return; // Finaliza a execução para não cair na lógica antiga
    }

    // --- LÓGICA ANTIGA PARA OS FORMATOS 'bernardo' E 'empresaAqui' ---
    const dir = path.dirname(filePath);
    const originalName = path.parse(filePath).name;
    const newFilePath = path.join(dir, `${originalName}_organizado.xlsx`);
    let writer;

    try {
        const writerOptions = {
            filename: newFilePath,
            useStyles: true,
            useSharedStrings: true
        };
        writer = new ExcelJS.stream.xlsx.WorkbookWriter(writerOptions);
        const newWorksheet = writer.addWorksheet('Organizado');

        newWorksheet.columns = [
            { header: 'nome', key: 'nome', width: 40 },
            { header: 'cpf', key: 'cpf', width: 20, style: { numFmt: '0' } },
            { header: 'livre1', key: 'livre1', width: 15 },
            { header: 'chave', key: 'chave', width: 30 },
            { header: 'livre3', key: 'livre3', width: 20, style: { numFmt: '0' } },
            { header: 'livre5', key: 'livre5', width: 10 },
            { header: 'livre7', key: 'livre7', width: 10 },
            { header: 'fone1', key: 'fone1', width: 15, style: { numFmt: '0' } },
            { header: 'fone2', key: 'fone2', width: 15, style: { numFmt: '0' } },
            { header: 'fone3', key: 'fone3', width: 15, style: { numFmt: '0' } },
            { header: 'fone4', key: 'fone4', width: 15, style: { numFmt: '0' } },
            { header: 'fone5', key: 'fone5', width: 15, style: { numFmt: '0' } },
            { header: 'fone6', key: 'fone6', width: 15, style: { numFmt: '0' } },
            { header: 'fone7', key: 'fone7', width: 15, style: { numFmt: '0' } },
            { header: 'fone8', key: 'fone8', width: 15, style: { numFmt: '0' } },
            { header: 'fone9', key: 'fone9', width: 15, style: { numFmt: '0' } },
            { header: 'fone10', key: 'fone10', width: 15, style: { numFmt: '0' } },
            { header: 'fone11', key: 'fone11', width: 15, style: { numFmt: '0' } },
            { header: 'fone12', key: 'fone12', width: 15, style: { numFmt: '0' } },
            { header: 'fone13', key: 'fone13', width: 15, style: { numFmt: '0' } },
            { header: 'fone14', key: 'fone14', width: 15, style: { numFmt: '0' } },
            { header: 'fone15', key: 'fone15', width: 15, style: { numFmt: '0' } },
            { header: 'fone16', key: 'fone16', width: 15, style: { numFmt: '0' } }
        ];
        
        if (organizationType === 'relacionamento') {
            newWorksheet.columns = [
                { header: 'nome', key: 'nome', width: 40 },
                { header: 'cpf', key: 'cpf', width: 20, style: { numFmt: '0' } },
                { header: 'livre1', key: 'livre1', width: 20 }, // Fase
                { header: 'chave', key: 'chave', width: 30 }, // EMAIL
                { header: 'livre2', key: 'livre2', width: 20 }, // VL_CASH_IN_MTD
                { header: 'livre3', key: 'livre3', width: 45 }, // Faixa de faturamento
                { header: 'fone1', key: 'fone1', width: 15, style: { numFmt: '0' } } // TELEFONE_MASTER
            ];
        }

        const reader = new ExcelJS.stream.xlsx.WorkbookReader(filePath);
        const headerMap = {};
        let processedRows = 0;
        const BATCH_LOG_INTERVAL = 20000;
        let useHeaderMapping = true;

        const fallbackMapping = {
            empresaAqui: {
                nome: 'B', cnpj: 'A', tel1: 'E', tel2: 'F',
                email: 'G', cnae: 'H', data: 'L'
            },
            relacionamento: {
                cpf: 'B', livre1: 'C', nome: 'E', fone1: 'G',
                chave: 'H', livre2: 'I', livre3: 'Q'
            }
        };

        reader.read();

        reader.on('worksheet', worksheet => {
            worksheet.on('row', row => {
                if (row.number === 1) {
                    row.values.forEach((value, index) => {
                        if (value) headerMap[String(value).trim().toLowerCase()] = index;
                    });

                    let requiredCols;
                    if (organizationType === 'bernardo') {
                        requiredCols = ['razao_social', 'cnpj_pk', 'data_inicio_atividade_formatado', 'correiro_eletronico', 'cnae_fiscal_principal', 'telefone_1_formatado', 'telefone_2_formatado'];
                    } else { // empresaAqui
                        requiredCols = ['razao', 'cnpj', 'data inicio ativ.', 'e-mail', 'cnae principal', 'telefone 1', 'telefone 2'];
                    } if (organizationType === 'relacionamento') {
                        requiredCols = ['cd_cpf_cnpj_cliente', 'fase', 'nome_cliente', 'telefone_master', 'email', 'vl_cash_in_mtd', 'qual a faixa de faturamento mensal da sua empresa?'];
                    }
                    
                    const allHeadersFound = requiredCols.every(col => {
                         // Corrigido para procurar por 'e-mail' e 'cnae principal' sem acentos
                        const normalizedCol = col.normalize("NFD").replace(/[\u0300-\u036f]/g, "");
                        return Object.keys(headerMap).some(headerKey => 
                            headerKey.normalize("NFD").replace(/[\u0300-\u036f]/g, "").includes(normalizedCol)
                        );
                    });


                    if (!allHeadersFound) {
                        useHeaderMapping = false;
                        log(`⚠️ AVISO: Um ou mais cabeçalhos não foram encontrados. Usando mapeamento de colunas fixo (A, B, M, N...).`);
                    } else {
                        log("✅ Cabeçalho validado. Usando mapeamento por nome de coluna.");
                    }
                } else {
                    let newRowData;

                    if (organizationType === 'bernardo') {
                        const getValue = (colName) => {
                            const colIndex = headerMap[colName.toLowerCase()];
                            // Se a coluna não for encontrada no mapa, retorna null para evitar o erro.
                            return colIndex ? row.getCell(colIndex).value : null;
                        };
                        newRowData = {
                            nome: cleanClientName(getValue('razao_social')),
                            cpf: getValue('cnpj_pk') ? Number(String(getValue('cnpj_pk')).replace(/\D/g, '')) : null,
                            livre1: getValue('data_inicio_atividade_formatado'),
                            chave: getValue('correiro_eletronico'),
                            livre3: getValue('cnae_fiscal_principal') ? Number(String(getValue('cnae_fiscal_principal')).replace(/\D/g, '')) : null,
                            livre5: null, livre7: 'C6',
                            fone1: getValue('telefone_1_formatado') ? Number(String(getValue('telefone_1_formatado')).replace(/\D/g, '')) : null,
                            fone2: getValue('telefone_2_formatado') ? Number(String(getValue('telefone_2_formatado')).replace(/\D/g, '')) : null
                        };
                    } else if (organizationType === 'empresaAqui') {
                        let razao, cnpj, dataInicio, email, cnae, tel1, tel2;

                        if (useHeaderMapping) {
                            // Função auxiliar para encontrar a coluna ignorando acentos e variações
                            const findHeaderIndex = (possibleNames) => {
                                for(const name of possibleNames) {
                                    const normalizedName = name.normalize("NFD").replace(/[\u0300-\u036f]/g, "");
                                    for (const headerKey in headerMap) {
                                        if (headerKey.normalize("NFD").replace(/[\u0300-\u036f]/g, "").includes(normalizedName)) {
                                            return headerMap[headerKey];
                                        }
                                    }
                                }
                                return -1;
                            };

                            razao = row.getCell(findHeaderIndex(['razao'])).value;
                            cnpj = row.getCell(findHeaderIndex(['cnpj'])).value;
                            dataInicio = row.getCell(findHeaderIndex(['data inicio ativ'])).value;
                            email = row.getCell(findHeaderIndex(['e-mail', 'email'])).value;
                            cnae = row.getCell(findHeaderIndex(['cnae principal'])).value;
                            tel1 = row.getCell(findHeaderIndex(['telefone 1'])).value;
                            tel2 = row.getCell(findHeaderIndex(['telefone 2'])).value;

                        } else { // Plano B: Mapeamento Fixo
                            const mapping = fallbackMapping.empresaAqui;
                            razao = row.getCell(mapping.nome).value;
                            cnpj = row.getCell(mapping.cnpj).value;
                            dataInicio = row.getCell(mapping.data).value;
                            email = row.getCell(mapping.email).value;
                            cnae = row.getCell(mapping.cnae).value;
                            tel1 = row.getCell(mapping.tel1).value;
                            tel2 = row.getCell(mapping.tel2).value;
                        }

                        newRowData = {
                            nome: cleanClientName(razao),
                            cpf: cnpj ? Number(String(cnpj).replace(/\D/g, '')) : null,
                            livre1: dataInicio,
                            chave: email,
                            livre3: cnae ? Number(String(cnae).replace(/\D/g, '')) : null,
                            livre5: null, livre7: 'C6',
                            fone1: tel1 ? Number(String(tel1).replace(/\D/g, '')) : null,
                            fone2: tel2 ? Number(String(tel2).replace(/\D/g, '')) : null
                        };
                    } else if (organizationType === 'relacionamento') {
                        let cpf, fase, nome, fone1, email, cashIn, faturamento;

                        if (useHeaderMapping) {
                             const findHeaderIndex = (possibleNames) => {
                                for(const name of possibleNames) {
                                    const normalizedName = name.normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase();
                                    for (const headerKey in headerMap) {
                                        if (headerKey.normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase().includes(normalizedName)) {
                                            return headerMap[headerKey];
                                        }
                                    }
                                }
                                return -1;
                            };
                            cpf = row.getCell(findHeaderIndex(['cd_cpf_cnpj_cliente'])).value;
                            fase = row.getCell(findHeaderIndex(['fase'])).value;
                            nome = row.getCell(findHeaderIndex(['nome_cliente'])).value;
                            fone1 = row.getCell(findHeaderIndex(['telefone_master'])).value;
                            email = row.getCell(findHeaderIndex(['email'])).value;
                            cashIn = row.getCell(findHeaderIndex(['vl_cash_in_mtd'])).value;
                            faturamento = row.getCell(findHeaderIndex(['qual a faixa de faturamento mensal da sua empresa?'])).value;
                        } else {
                            const mapping = fallbackMapping.relacionamento;
                            cpf = row.getCell(mapping.cpf).value;
                            fase = row.getCell(mapping.livre1).value;
                            nome = row.getCell(mapping.nome).value;
                            fone1 = row.getCell(mapping.fone1).value;
                            email = row.getCell(mapping.chave).value;
                            cashIn = row.getCell(mapping.livre2).value;
                            faturamento = row.getCell(mapping.livre3).value;
                        }

                        newRowData = {
                            nome: cleanClientName(nome),
                            cpf: cpf ? Number(String(cpf).replace(/\D/g, '')) : null,
                            livre1: fase, chave: email, livre2: cashIn, livre3: faturamento,
                            fone1: fone1 ? Number(String(fone1).replace(/\D/g, '')) : null
                        };
                    }
                    
                    if (newRowData) {
                        newWorksheet.addRow(newRowData).commit();
                        processedRows++;
                        if (processedRows % BATCH_LOG_INTERVAL === 0) {
                            log(`Processadas ${processedRows.toLocaleString('pt-BR')} linhas...`);
                        }
                    }
                }
            });
        });

        await new Promise((resolve, reject) => {
            reader.on('end', async () => {
                try {
                    await writer.commit();
                    resolve();
                } catch (e) {
                    reject(e);
                }
            });
            reader.on('error', reject);
        });

        log(`✅ Planilha organizada com sucesso! ${processedRows.toLocaleString('pt-BR')} linhas processadas.`);
        log(`O novo arquivo foi salvo em: ${newFilePath}`);
        shell.showItemInFolder(newFilePath);

    } catch (error) {
        log(`❌ ERRO ao organizar a planilha: ${error.message}`);
        console.error("Erro detalhado na organização:", error);
        if (writer && fs.existsSync(newFilePath)) {
            try {
                fs.unlinkSync(newFilePath);
                log(`Arquivo parcial corrompido (${path.basename(newFilePath)}) foi removido.`);
            } catch (e) {
                log(`Não foi possível remover o arquivo parcial corrompido: ${e.message}`);
            }
        }
    }
});
// =================================================================
// =            *** FIM DA MODIFICAÇÃO *** =
// =================================================================


// #################################################################
// #           NOVA LÓGICA - API BITRIX24 (Monitoramento)          #
// #################################################################

const BITRIX_WEBHOOK_URL = "https://mb-finance.bitrix24.com.br/rest/8311";
const USER_GET_TOKEN = "dv95qyhbyrtu49fn";
const VOX_GET_TOKEN = "dv95qyhbyrtu49fn";

function formatDateForBitrix(date) {
    if (!date) return null;
    return date.toISOString();
}

async function fetchAllBitrixPages(method, token, params = {}) {
    const allResults = [];
    let start = 0;
    const BATCH_SIZE = 50;
    let hasMore = true;

    console.log(`[Bitrix] Iniciando busca paginada para o método: ${method}`);

    while (hasMore) {
        try {
            const fullUrl = `${BITRIX_WEBHOOK_URL}/${token}/${method}.json`;
            const response = await axios.post(fullUrl, { ...params,
                start: start
            });

            const result = response.data.result;
            if (result && Array.isArray(result) && result.length > 0) {
                allResults.push(...result);
                console.log(`[Bitrix] Buscados ${result.length} registros para ${method}. Total acumulado: ${allResults.length}`);
                if (result.length < BATCH_SIZE) {
                    hasMore = false;
                } else {
                    start += BATCH_SIZE;
                }
            } else {
                hasMore = false;
            }
        } catch (error) {
            console.error(`[Bitrix] Erro ao buscar página para ${method} (start: ${start}):`, error.response ? error.response.data : error.message);
            throw new Error(`Falha ao comunicar com a API do Bitrix para o método ${method}.`);
        }
    }
    console.log(`[Bitrix] Busca paginada para ${method} concluída. Total de ${allResults.length} registros encontrados.`);
    return allResults;
}


ipcMain.handle('fetch-bitrix-report', async (event, {
    startDate: startDateStr,
    endDate: endDateStr
}) => {
    if (!currentUser) {
        return {
            success: false,
            message: 'Acesso negado. Faça o login.'
        };
    }

    try {
        const users = await fetchAllBitrixPages('user.get', USER_GET_TOKEN, {
            FILTER: {
                "ACTIVE": true,
                "USER_TYPE": "employee"
            },
            SORT: "ID",
            ORDER: "ASC"
        });
        const userMap = new Map(users.map(user => [user.ID, `${user.NAME || ''} ${user.LAST_NAME || ''}`.trim()]));

        const startDate = new Date(startDateStr);
        startDate.setHours(0, 0, 0, 0);
        const endDate = new Date(endDateStr);
        endDate.setHours(23, 59, 59, 999);

        const calls = await fetchAllBitrixPages('voximplant.statistic.get', VOX_GET_TOKEN, {
            FILTER: {
                ">CALL_DURATION": 30,
                "=CALL_TYPE": 1,
                ">=CALL_START_DATE": formatDateForBitrix(startDate),
                "<=CALL_START_DATE": formatDateForBitrix(endDate),
            },
            SORT: "CALL_START_DATE",
            ORDER: "ASC"
        });

        if (calls.length === 0) {
            return {
                success: true,
                data: {
                    generalTma: 0,
                    totalCalls: 0,
                    operatorStats: [],
                    message: "Nenhuma chamada encontrada no Bitrix para o período selecionado."
                }
            };
        }

        let totalDuration = 0;
        const operatorStats = {};

        calls.forEach(call => {
            const duration = parseInt(call.CALL_DURATION, 10);
            const userId = call.PORTAL_USER_ID;
            const userName = userMap.get(userId) || `ID Desconhecido (${userId})`;

            totalDuration += duration;

            if (!operatorStats[userId]) {
                operatorStats[userId] = {
                    userId: userId,
                    name: userName,
                    totalDuration: 0,
                    callCount: 0,
                };
            }
            operatorStats[userId].totalDuration += duration;
            operatorStats[userId].callCount++;
        });

        const generalTma = totalDuration / calls.length;

        const finalOperatorStats = Object.values(operatorStats).map(stats => ({
            ...stats,
            tma: stats.totalDuration / stats.callCount,
        }));

        return {
            success: true,
            data: {
                generalTma,
                totalCalls: calls.length,
                operatorStats: finalOperatorStats,
            },
        };

    } catch (error) {
        console.error("[Bitrix Report] Erro ao gerar relatório:", error);
        return {
            success: false,
            message: error.message
        };
    }
});




// --- LÓGICA DE MESCLAGEM ATUALIZADA ---

async function shuffleFilesInPlace(filePaths, log) {
    log(`\n--- Iniciando a fase de embaralhamento para ${filePaths.length} arquivo(s) ---`);
    for (const filePath of filePaths) {
        try {
            log(`Embaralhando o arquivo: ${path.basename(filePath)}...`);

            const workbook = await readSpreadsheet(filePath);
            const worksheet = workbook.Sheets[workbook.SheetNames[0]];
            const allData = XLSX.utils.sheet_to_json(worksheet, { header: 1, defval: "" });

            if (allData.length <= 1) {
                log(`⚠️ Arquivo ${path.basename(filePath)} muito pequeno para embaralhar. Pulando.`);
                continue;
            }

            const header = allData[0];
            const dataRows = allData.slice(1);

            for (let i = dataRows.length - 1; i > 0; i--) {
                const j = Math.floor(Math.random() * (i + 1));
                [dataRows[i], dataRows[j]] = [dataRows[j], dataRows[i]];
            }

            const shuffledData = [header, ...dataRows];
            const newWorkbook = XLSX.utils.book_new();
            const newWorksheet = XLSX.utils.aoa_to_sheet(shuffledData);
            XLSX.utils.book_append_sheet(newWorkbook, newWorksheet, "Mesclado");
            writeSpreadsheet(newWorkbook, filePath);

            log(`✅ Arquivo ${path.basename(filePath)} embaralhado com sucesso.`);
        } catch (err) {
            log(`❌ Erro ao embaralhar o arquivo ${path.basename(filePath)}: ${err.message}`);
            console.error(err);
        }
    }
    log(`--- Fase de embaralhamento concluída ---`);
}

async function mergeAndSegment(event, options) {
    const { files, strategy, customCount, removeDuplicates } = options;
    const log = (msg) => event.sender.send("log", msg);
    const createdFiles = [];

    try {
        const { canceled, filePath: savePath } = await dialog.showSaveDialog(mainWindow, {
            title: "Salvar Arquivo Mesclado",
            defaultPath: `mesclado_${Date.now()}.xlsx`,
            filters: [{ name: "Planilhas Excel", extensions: ["xlsx"] }]
        });
        if (canceled || !savePath) {
            log("Operação de mesclagem cancelada.");
            return [];
        }

        log("Iniciando pré-scan para contagem de linhas...");
        let totalDataRows = 0;
        for (const filePath of files) {
            const inputWorkbook = new ExcelJS.Workbook();
            await inputWorkbook.xlsx.readFile(filePath);
            const inputWorksheet = inputWorkbook.worksheets[0];
            if (inputWorksheet && inputWorksheet.rowCount > 1) {
                totalDataRows += (inputWorksheet.rowCount - 1);
            }
        }
        log(`Pré-scan concluído. Total de linhas de dados a processar: ${totalDataRows}`);

        const needsSegmentation = totalDataRows > 1000000;
        const numParts = 4;
        const rowsPerPart = needsSegmentation ? Math.ceil(totalDataRows / numParts) : Infinity;

        if (needsSegmentation) {
            log(`Total excede 1 milhão de linhas. O resultado será dividido em ${numParts} partes de aproximadamente ${rowsPerPart} linhas cada.`);
        }

        let header = [];
        const seenCnpjs = new Set();
        let cnpjColumnIndex = -1;
        let totalRowsWritten = 0;
        let rowsInCurrentPart = 0;
        let currentPart = 1;

        const { dir, name, ext } = path.parse(savePath);

        let currentWriter, currentWorksheet;

        const createNewWriter = async () => {
            const partPath = needsSegmentation
                ? path.join(dir, `${name}_parte${currentPart}${ext}`)
                : savePath;

            log(`Criando arquivo de saída: ${path.basename(partPath)}`);
            createdFiles.push(partPath);

            const streamOptions = { filename: partPath, useStyles: false, useSharedStrings: true };
            currentWriter = new ExcelJS.stream.xlsx.WorkbookWriter(streamOptions);
            currentWorksheet = currentWriter.addWorksheet('Mesclado');

            if (header.length > 0) {
                currentWorksheet.columns = header.map(h => ({ header: h, key: h, style: {} }));
            }
        };

        await createNewWriter();

        for (let i = 0; i < files.length; i++) {
            const filePath = files[i];
            log(`Processando arquivo: ${path.basename(filePath)}`);

            const inputWorkbook = new ExcelJS.Workbook();
            await inputWorkbook.xlsx.readFile(filePath);
            const inputWorksheet = inputWorkbook.worksheets[0];

            if (!inputWorksheet || inputWorksheet.rowCount <= 1) {
                log(`⚠️ Arquivo ${path.basename(filePath)} vazio. Pulando.`);
                continue;
            }

            if (i === 0) {
                const headerRow = inputWorksheet.getRow(1).values;
                header = Array.isArray(headerRow) ? headerRow.slice(1) : Object.values(headerRow);
                currentWorksheet.columns = header.map(h => ({ header: h, key: h, style: {} }));

                if (removeDuplicates) {
                    cnpjColumnIndex = header.findIndex(h => String(h || '').trim().toLowerCase() === 'cpf' || String(h || '').trim().toLowerCase() === 'cnpj');
                    if (cnpjColumnIndex === -1) {
                        log('⚠️ Coluna "cpf" ou "cnpj" não encontrada. Remoção de duplicados ignorada.');
                    }
                }
            }

            let rowCountInFile = 0;
            const fileTotalDataRows = inputWorksheet.rowCount - 1;

            for(let rowNum = 2; rowNum <= inputWorksheet.rowCount; rowNum++) {
                const row = inputWorksheet.getRow(rowNum);

                let shouldAdd = true;
                switch (strategy) {
                    case 'partial':
                        const rowsToTake = Math.floor(fileTotalDataRows * 0.25);
                        if (rowCountInFile >= rowsToTake) shouldAdd = false;
                        break;
                    case 'custom':
                        if (rowCountInFile >= customCount) shouldAdd = false;
                        break;
                }
                if (!shouldAdd) continue;

                rowCountInFile++;
                const rowData = Array.isArray(row.values) ? row.values.slice(1) : Object.values(row.values);

                if (removeDuplicates && cnpjColumnIndex !== -1) {
                    const cnpj = String(rowData[cnpjColumnIndex] || '').replace(/\D/g, "").trim();
                    if (cnpj && seenCnpjs.has(cnpj)) {
                        continue;
                    }
                    if(cnpj) seenCnpjs.add(cnpj);
                }

                if (needsSegmentation && rowsInCurrentPart >= rowsPerPart) {
                    await currentWorksheet.commit();
                    await currentWriter.commit();
                    log(`Parte ${currentPart} finalizada com ${rowsInCurrentPart} linhas.`);
                    currentPart++;
                    rowsInCurrentPart = 0;
                    await createNewWriter();
                }

                currentWorksheet.addRow(rowData).commit();
                rowsInCurrentPart++;
                totalRowsWritten++;
            }
            log(`Adicionadas ${rowCountInFile} linhas de ${path.basename(filePath)}.`);
        }

        await currentWorksheet.commit();
        await currentWriter.commit();
        log(`Parte final (parte ${currentPart}) finalizada com ${rowsInCurrentPart} linhas.`);

        log(`\n✅ Mesclagem e segmentação concluídas! Total de ${totalRowsWritten} linhas salvas em ${createdFiles.length} arquivo(s).`);

        return createdFiles;

    } catch (err) {
        log(`❌ Erro catastrófico durante a mesclagem: ${err.message}`);
        console.error(err);
        dialog.showErrorBox("Erro de Mesclagem", `Ocorreu um erro inesperado: ${err.message}`);
        return [];
    }
}


// ROTEADOR PRINCIPAL DE MESCLAGEM
ipcMain.on("start-merge", async (event, options) => {
    if (!isAdmin()) {
        event.sender.send("log", "❌ Acesso negado: Permissão de administrador necessária.");
        return;
    }
    const { files, shuffle } = options;
    const log = (msg) => event.sender.send("log", msg);

    if (!files || files.length < 2) {
        log("❌ Erro: Por favor, selecione pelo menos dois arquivos para mesclar.");
        return;
    }

    log(`\n--- Iniciando Processo de Mesclagem ---`);
    log(`Estratégia: ${options.strategy}. Remover Duplicados: ${options.removeDuplicates}. Embaralhar: ${options.shuffle}.`);
    if (options.strategy === 'custom') {
        log(`Linhas por arquivo (personalizado): ${options.customCount}`);
    }

    const outputFiles = await mergeAndSegment(event, options);

    if (outputFiles && outputFiles.length > 0 && shuffle) {
        await shuffleFilesInPlace(outputFiles, log);
    }

    if (outputFiles && outputFiles.length > 0) {
        const finalMessage = `Processo concluído com sucesso!\n\n${outputFiles.length} arquivo(s) foi(ram) salvo(s) na pasta:\n${path.dirname(outputFiles[0])}`;
        dialog.showMessageBox(mainWindow, { type: "info", title: "Sucesso", message: finalMessage });
        log(`\n🎉 Processo de mesclagem finalizado com sucesso!`);
    } else {
        log(`\n⚠️ Processo de mesclagem finalizado, mas nenhum arquivo foi gerado.`);
    }
});

ipcMain.on("start-adjust-phones", async (event, args) => {
    if (!isAdmin()) {
        event.sender.send("log", "❌ Acesso negado: Permissão de administrador necessária.");
        return;
    }
    const log = (msg) => event.sender.send("log", msg);
    log(`\n--- Iniciando Ajuste de Fones para ${path.basename(args.filePath)} ---
`);
    await runPhoneAdjustment(args.filePath, event, args.backup);
    log(`\n✅ Ajuste de fones concluído para o arquivo.
`);
});

ipcMain.on("split-list", async (event, { filePath, linesPerSplit }) => {
    if (!isAdmin()) {
        event.sender.send("log", "❌ Acesso negado: Permissão de administrador necessária.");
        return;
    }
    const log = (msg) => event.sender.send("log", msg);
    log(`\n--- Iniciando Divisão de Lista para ${path.basename(filePath)} ---
`);

    try {
        const workbook = new ExcelJS.Workbook();
        await workbook.xlsx.readFile(filePath);
        const worksheet = workbook.worksheets[0];

        if (!worksheet || worksheet.rowCount <= 1) {
            log(`⚠️ Arquivo ${path.basename(filePath)} vazio ou sem dados. Pulando.
`);
            return;
        }

        const header = worksheet.getRow(1).values.filter(Boolean); // Get header and remove null/undefined
        const totalRows = worksheet.rowCount - 1; // Exclude header
        const numFiles = Math.ceil(totalRows / linesPerSplit);
        const baseName = path.basename(filePath, path.extname(filePath));
        const outputDir = path.dirname(filePath);

        log(`Total de ${totalRows} linhas de dados. Será dividido em ${numFiles} arquivo(s) com ${linesPerSplit} linhas cada.
`);

        let currentRow = 2; // Start from the second row (after header)
        for (let i = 0; i < numFiles; i++) {
            const newWorkbook = new ExcelJS.Workbook();
            const newWorksheet = newWorkbook.addWorksheet("Sheet1");
            newWorksheet.addRow(header); // Add header to new file

            const startRow = currentRow;
            const endRow = Math.min(currentRow + linesPerSplit - 1, totalRows + 1); // +1 for header offset

            for (let j = startRow; j <= endRow; j++) {
                const row = worksheet.getRow(j);
                newWorksheet.addRow(row.values.filter(Boolean)); // Add row data
            }
            
            const newFilePath = path.join(outputDir, `${baseName}_parte${i + 1}.xlsx`);
            await newWorkbook.xlsx.writeFile(newFilePath);
            log(`✅ Parte ${i + 1} salva em: ${path.basename(newFilePath)}
`);
            currentRow = endRow + 1;
        }
        log(`\n--- Divisão de Lista concluída com sucesso! ---
`);
        event.sender.send("log", `🎉 Arquivos divididos salvos em: ${outputDir}`);
        shell.showItemInFolder(path.join(outputDir, `${baseName}_parte1.xlsx`));

    } catch (error) {
        log(`❌ ERRO ao dividir a lista: ${error.message}
`);
        console.error("Erro detalhado na divisão:", error);
    }
});

let apiQueue = { pending: [], processing: null, completed: [], cancelled: [], clientHeader: null, clientRows: [] };
let isApiQueueRunning = false;
let cancelCurrentApiTask = false;
let isApiQueuePaused = false;
let fishScheduleTimer = null; // NOVO: Timer para o agendamento
let currentApiOptions = { keyMode: 'chave1', removeClients: true };
// NOVO: Variáveis para armazenar os tempos de delay da API
let apiTimingSettings = {
    delayBetweenBatches: null, // em minutos
    retryDelay: null // em minutos
};

// CORRIGIDO: Handler para receber as atualizações de tempo do renderer
ipcMain.on('set-api-delays', (event, settings) => {
    apiTimingSettings = settings;
    event.sender.send("api-log", `⚙️ Configurações de tempo atualizadas: Delay entre Lotes: ${settings.delayBetweenBatches || 'Padrão'}, Delay de Retentativa: ${settings.retryDelay || 'Padrão'}`);
});
let fishModeFilePath = null; // NOVO: para armazenar o caminho do arquivo no modo FISH


ipcMain.on("add-files-to-api-queue", (event, filePaths) => {
    if (!isAdmin()) return;
    apiQueue.pending.push(...filePaths);
    apiQueue.pending = [...new Set(apiQueue.pending)];
    event.sender.send("api-queue-update", { ...apiQueue, isPaused: isApiQueuePaused });
});

ipcMain.on("pause-api-queue", (event) => {
    if (!isAdmin()) return;
    if (isApiQueueRunning && !isApiQueuePaused) {
        isApiQueuePaused = true;
        event.sender.send("api-log", "\n⏸️ Fila de processamento PAUSADA. A tarefa atual será concluída.");
        event.sender.send("api-queue-update", { ...apiQueue, isPaused: isApiQueuePaused });
    }
});

ipcMain.on("resume-api-queue", (event) => {
    if (!isAdmin()) return;
    if (isApiQueueRunning && isApiQueuePaused) {
        isApiQueuePaused = false;
        event.sender.send("api-log", "\n▶️ Fila de processamento RETOMADA.");
        event.sender.send("api-queue-update", { ...apiQueue, isPaused: isApiQueuePaused });
        processNextInApiQueue(event); // Continue processing
    }
});

ipcMain.on("start-api-queue", async (event, options) => { // MODIFICADO para async
    if (!isAdmin()) return;
    if (isApiQueueRunning) return;
    currentApiOptions = options;
    isApiQueueRunning = true;
    isApiQueuePaused = false;
    apiQueue.clientHeader = null;
    apiQueue.clientRows = [];
    fishModeFilePath = null; // Reseta o caminho do arquivo FISH

    // NOVO: Lógica para o modo FISH
    if (options.isFishMode) {
        event.sender.send("api-log", `🐟 Modo FISH ativado. Clientes serão enviados para o webhook N8N.`);
    }

    cancelCurrentApiTask = false;
    event.sender.send("api-queue-update", { ...apiQueue, isPaused: isApiQueuePaused });
    processNextInApiQueue(event);
});

// --- INÍCIO: NOVAS FUNÇÕES DE AGENDAMENTO FISH ---

async function sendErrorEmail(subject, errorDetails) {
    if (!process.env.SMTP_USER || !process.env.SMTP_PASS) {
        console.error("Credenciais SMTP não configuradas no .env. Não é possível enviar e-mail de erro.");
        return;
    }
    const transporter = nodemailer.createTransport({
        host: process.env.SMTP_HOST || "smtp.gmail.com",
        port: parseInt(process.env.SMTP_PORT, 10) || 465,
        secure: (process.env.SMTP_PORT || "465") === "465",
        auth: { user: process.env.SMTP_USER, pass: process.env.SMTP_PASS },
    });

    const mailOptions = {
        from: `"Gerenciador de Bases" <${process.env.SMTP_USER}>`,
        to: "davi.abraao@mbfinance.com.br",
        subject: `🚨 FALHA: ${subject}`,
        html: `
            <div style="font-family: Arial, sans-serif; color: #333;">
                <h2>Relatório de Falha no Processo Agendado</h2>
                <p>Ocorreu um erro durante a execução de uma tarefa agendada no Gerenciador de Bases.</p>
                <hr>
                <p><strong>Detalhes do Erro:</strong></p>
                <pre style="background-color: #f4f4f4; padding: 10px; border-radius: 5px;">${errorDetails}</pre>
                <hr>
                <p style="font-size: 12px; color: #777;">
                    Data da falha: ${new Date().toLocaleString('pt-BR')}<br>
                    Usuário que agendou: ${currentUser.username}
                </p>
            </div>
        `,
    };

    try {
        await transporter.sendMail(mailOptions);
        console.log("E-mail de erro enviado com sucesso.");
    } catch (emailError) {
        console.error("Falha ao enviar o e-mail de erro:", emailError);
    }
}

ipcMain.on('schedule-fish-cleanup', (event, scheduleOptions) => {
    if (fishScheduleTimer) clearTimeout(fishScheduleTimer);

    store.set('fish-schedule', scheduleOptions);
    event.sender.send('api-log', `✅ Agendamento FISH confirmado para ${new Date(scheduleOptions.startTime).toLocaleString('pt-BR')}.`);
    mainWindow.webContents.send('fish-schedule-update', scheduleOptions);

    const delay = new Date(scheduleOptions.startTime).getTime() - Date.now();

    if (delay > 0) {
        fishScheduleTimer = setTimeout(() => runScheduledFishCleanup(scheduleOptions), delay);
    }
});

ipcMain.on('cancel-fish-schedule', (event) => {
    if (fishScheduleTimer) clearTimeout(fishScheduleTimer);
    store.delete('fish-schedule');
    event.sender.send('api-log', `❌ Agendamento cancelado pelo usuário.`);
    mainWindow.webContents.send('fish-schedule-update', null);
});

ipcMain.on("reset-api-queue", (event) => {
    if (!isAdmin()) return;
    isApiQueueRunning = false;
    cancelCurrentApiTask = true; // Signal to stop any ongoing process
    apiQueue = { pending: [], processing: null, completed: [], cancelled: [], clientHeader: null, clientRows: [] };
    isApiQueuePaused = false;
    fishModeFilePath = null; // Limpa o caminho do arquivo
    event.sender.send("api-log", "Fila e status reiniciados.");
    event.sender.send("api-queue-update", { ...apiQueue, isPaused: isApiQueuePaused });
});

ipcMain.on("remove-from-api-queue", (event, filePath) => {
    if (!isAdmin()) return;
    const index = apiQueue.pending.indexOf(filePath);
    if (index > -1) {
        apiQueue.pending.splice(index, 1);
        event.sender.send("api-queue-update", { ...apiQueue, isPaused: isApiQueuePaused });
        event.sender.send("api-log", `Arquivo removido da fila: ${path.basename(filePath)}`);
    }
});

ipcMain.on("prioritize-in-api-queue", (event, filePath) => {
    if (!isAdmin()) return;
    const index = apiQueue.pending.indexOf(filePath);
    if (index > 0) {
        const [item] = apiQueue.pending.splice(index, 1);
        apiQueue.pending.unshift(item);
        event.sender.send("api-queue-update", { ...apiQueue, isPaused: isApiQueuePaused });
        event.sender.send("api-log", `Arquivo priorizado: ${path.basename(filePath)}`);
    }
});

ipcMain.on("cancel-current-api-task", (event) => {
    if (!isAdmin()) return;
    if (isApiQueueRunning && apiQueue.processing) {
        event.sender.send("api-log", `Solicitação de cancelamento para: ${path.basename(apiQueue.processing)}`);
        cancelCurrentApiTask = true;
    }
});

async function saveCollectedClients(event) {
    const log = (msg) => event.sender.send("api-log", msg);

    if (currentApiOptions.isFishMode || !currentApiOptions.extractClients || apiQueue.clientRows.length === 0) {
        return; // Nothing to do
    }

    log(`\n--- Iniciando salvamento do arquivo consolidado de clientes (${apiQueue.clientRows.length} registros) ---`);

    try {
        const { canceled, filePath } = await dialog.showSaveDialog(mainWindow, {
            title: "Salvar Arquivo de Clientes",
            defaultPath: `clientes_consolidados_${Date.now()}.xlsx`,
            filters: [{ name: "Planilhas Excel", extensions: ["xlsx"] }]
        });

        if (canceled || !filePath) {
            log("Salvamento do arquivo de clientes cancelado pelo usuário.");
            return;
        }

        const workbook = new ExcelJS.Workbook();
        const worksheet = workbook.addWorksheet("Clientes");

        if (apiQueue.clientHeader) {
            worksheet.addRow(apiQueue.clientHeader);
        }
        worksheet.addRows(apiQueue.clientRows);

        await workbook.xlsx.writeFile(filePath);
        log(`✅ Arquivo de clientes salvo com sucesso em: ${filePath}`);
        shell.showItemInFolder(filePath);

    } catch (error) {
        log(`❌ Erro ao salvar o arquivo consolidado de clientes: ${error.message}`);
        console.error(error);
    }
}

async function runScheduledFishCleanup(schedule) {
    console.log(`[AGENDADOR] Iniciando execução agendada: ${new Date().toLocaleString()}`);
    const log = (msg) => {
        if (mainWindow && mainWindow.webContents) {
            mainWindow.webContents.send("api-log", `[AGENDADO] ${msg}`);
        }
        console.log(`[AGENDADO] ${msg}`);
    };

    try {
        // Adiciona os arquivos agendados à fila principal
        apiQueue.pending.push(...schedule.files);
        apiQueue.pending = [...new Set(apiQueue.pending)];
        if (mainWindow) mainWindow.webContents.send("api-queue-update", { ...apiQueue, isPaused: isApiQueuePaused });

        // Inicia a fila com as opções agendadas
        isApiQueueRunning = true;
        isApiQueuePaused = false;
        currentApiOptions = schedule.apiOptions;
        cancelCurrentApiTask = false;

        log(`Iniciando processamento de ${schedule.files.length} arquivo(s) agendados.`);
        await processNextInApiQueue({ sender: mainWindow.webContents });

        log("✅ Execução agendada concluída com sucesso.");

    } catch (error) {
        log(`❌ ERRO CRÍTICO na execução agendada: ${error.message}`);
        console.error("[AGENDADOR] Erro:", error);
        await sendErrorEmail("Falha na Execução Agendada FISH", `Erro: ${error.message}\n\nStack: ${error.stack}`);
    } finally {
        // Limpa o agendamento após a execução (seja sucesso ou falha)
        store.delete('fish-schedule');
        if (mainWindow) mainWindow.webContents.send('fish-schedule-update', null);
    }
}

async function processNextInApiQueue(event) {
    if (!isApiQueueRunning) {
        apiQueue.processing = null;
        event.sender.send("api-queue-update", { ...apiQueue, isPaused: isApiQueuePaused });
        event.sender.send("api-log", "\nFila de processamento interrompida.");
        return;
    }

    if (isApiQueuePaused) {
        event.sender.send("api-log", "Fila PAUSADA. Aguardando para retomar...");
        return; // Do not process next, just wait
    }

    if (apiQueue.pending.length === 0) {
        event.sender.send("api-log", "\n✅ Fila de processamento concluída.");
        apiQueue.processing = null;
        isApiQueueRunning = false;

        if (currentApiOptions.extractClients && !currentApiOptions.isFishMode) {
            await saveCollectedClients(event);
        }

        event.sender.send("api-queue-update", { ...apiQueue, isPaused: isApiQueuePaused });
        return;
    }

    apiQueue.processing = apiQueue.pending.shift();
    event.sender.send("api-queue-update", { ...apiQueue, isPaused: isApiQueuePaused });
    event.sender.send("api-log", `--- Iniciando processamento de: ${path.basename(apiQueue.processing)} ---`);
    
    const result = await runApiConsultation(apiQueue.processing, currentApiOptions, (msg) => event.sender.send("api-log", msg), (current, total) => event.sender.send("api-progress", { current, total }), fishModeFilePath);

    if (result && result.success && currentApiOptions.extractClients && result.clientData.rows.length > 0) {
        if (!apiQueue.clientHeader) {
            apiQueue.clientHeader = result.clientData.header;
        }
        apiQueue.clientRows.push(...result.clientData.rows);
        event.sender.send("api-log", `Adicionados ${result.clientData.rows.length} clientes à lista de extração. Total agora: ${apiQueue.clientRows.length}.`);
    }

    if (cancelCurrentApiTask) {
        event.sender.send("api-log", `Processamento de ${path.basename(apiQueue.processing)} foi cancelado.`);
        apiQueue.cancelled.push(apiQueue.processing);
        cancelCurrentApiTask = false; // Reset for next run
    } else {
        if (result && result.success) {
            apiQueue.completed.push(apiQueue.processing);
        } else {
            apiQueue.cancelled.push(apiQueue.processing); // Move to cancelled if it failed
        }
    }
    
    apiQueue.processing = null;
    event.sender.send("api-queue-update", { ...apiQueue, isPaused: isApiQueuePaused });
    
    // If paused, don't call itself recursively
    if (!isApiQueuePaused) {
        processNextInApiQueue(event);
    }
}


// FUNÇÃO runApiConsultation ATUALIZADA COM LÓGICA DE RETENTATIVA CORRIGIDA
async function runApiConsultation(filePath, options, log, progress, fishPath) {
    const { keyMode, removeClients, isFishMode, extractClients } = options;
    const credentials = {
        c6: {
            CLIENT_ID: "EA8ZUFeZVSeqMGr49XJSsZKFuxSZub3i",
            CLIENT_SECRET: "EUomxjGf6BvBZ1HO",
            name: "Chave 1 (Padrão)"
        },
        im: {
            CLIENT_ID: "imWzrW41HcnoJgvZqHCaLvziUGlhAJAH",
            CLIENT_SECRET: "A0lAqZO73uW3wryU",
            name: "Chave 2 (Alternativa)"
        }
    };
    const TOKEN_URL = "https://crm-leads-p.c6bank.info/querie-partner/token";
    const CONSULTA_URL = "https://crm-leads-p.c6bank.info/querie-partner/client/avaliable";

    const BATCH_SIZE_SINGLE = 20000;
    const BATCH_SIZE_DUAL = 40000; // Mantido para referência, mas a lógica de envio é individual

    // MODIFICADO: Usa os valores configurados ou os padrões
    const getRetryDelayMs = () => (parseFloat(apiTimingSettings.retryDelay) || 2) * 60 * 1000;
    const getSuccessDelayMs = () => (parseFloat(apiTimingSettings.delayBetweenBatches) || 2) * 60 * 1000;

    // Valores padrão antigos, mantidos aqui para referência caso queira reverter
    // const RETRY_MS = 2 * 60 * 1000;
    // const DELAY_SUCESSO_MS = 2 * 60 * 1000;
    const MAX_RETRIES = 5;

    const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));
    const normalizeCnpj = (cnpj) => (String(cnpj).replace(/\D/g, "")).padStart(14, "0");

    const sendToN8NWebhook = async (header, rowData) => {
        const N8N_WEBHOOK_URL = 'https://n8n.upscales.com.br/webhook/2ccead38-deb8-48d0-9f44-0edccafcc026';
        if (!rowData) return;

        // Mapeia o cabeçalho para índices
        const headerMap = {};
        header.forEach((h, index) => {
            if (h) headerMap[String(h).toLowerCase()] = index;
        });

        // Constrói um objeto com os parâmetros
        const params = {};
        params.nome = rowData[headerMap['nome']] || '';
        params.cpf = rowData[headerMap['cpf']] || '';
        params.chave = rowData[headerMap['chave']] || '';

        // Adiciona todos os campos 'fone'
        for (const key in headerMap) {
            if (key.startsWith('fone')) {
                const phoneValue = rowData[headerMap[key]];
                if (phoneValue) {
                    params[key] = phoneValue;
                }
            }
        }

        // Filtra chaves com valores vazios antes de criar a query string
        const filteredParams = Object.fromEntries(
            Object.entries(params).filter(([_, v]) => v !== null && v !== '' && v !== undefined)
        );

        const queryString = new URLSearchParams(filteredParams).toString();
        const finalUrl = `${N8N_WEBHOOK_URL}?${queryString}`;

        try {
            await axios.get(finalUrl, { timeout: 15000 });
            log(`🐟 FISH: Cliente ${params.cpf || 'sem CPF'} enviado para o N8N.`);
        } catch (error) {
            const errorMessage = error.response ? JSON.stringify(error.response.data) : error.message;
            log(`❌🐟 FISH ERRO: Falha ao enviar cliente para o N8N: ${errorMessage}`);
        }
    };

    const performApiCall = async (cnpjArray, creds) => {
        log(`Consultando ${cnpjArray.length} CNPJs com a chave: ${creds.name}`);
        const tokenParams = new URLSearchParams({ grant_type: "client_credentials", client_id: creds.CLIENT_ID, client_secret: creds.CLIENT_SECRET });
        const tokenResp = await axios.post(TOKEN_URL, tokenParams.toString(), { headers: { "Content-Type": "application/x-www-form-urlencoded" }, timeout: 30000 });
        const token = tokenResp.data.access_token;

        const consultaResp = await axios.post(CONSULTA_URL, { CNPJ: cnpjArray }, { headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" }, timeout: 30000 });
        const key = Object.keys(consultaResp.data).find(k => k.toLowerCase().includes("cnpj") && Array.isArray(consultaResp.data[k]));
        return key ? new Set(consultaResp.data[key].map(normalizeCnpj)) : new Set();
    };

    try {
        log(`Iniciando processo com o modo de chave: '${keyMode}'.`);
        if (removeClients) {
            log(`Remoção automática de clientes: ATIVADA.`);
        } else {
            log(`Remoção automática de clientes: DESATIVADA.`);
        }
        const workbook = new ExcelJS.Workbook();
        await workbook.xlsx.readFile(filePath);
        const worksheet = workbook.worksheets[0];

        const COLUNA_CNPJ = "cpf";
        let cnpjColNumber = -1;
        let fileHeader = worksheet.getRow(1).values; // Captura o cabeçalho
        worksheet.getRow(1).eachCell({ includeEmpty: true }, (cell, colNumber) => {
            if (cell.value && String(cell.value).trim().toLowerCase() === COLUNA_CNPJ) cnpjColNumber = colNumber;
        });

        if (cnpjColNumber === -1) throw new Error(`A coluna "${COLUNA_CNPJ}" não foi encontrada.`);

        const COLUNA_RESPOSTA_LETTER = "C";
        const registros = [];
        worksheet.eachRow({ includeEmpty: false }, (row, rowNum) => {
            if (rowNum > 1) {
                const cnpjCell = row.getCell(cnpjColNumber);
                const respostaCell = row.getCell(COLUNA_RESPOSTA_LETTER);
                if (!respostaCell.value && cnpjCell.value) {
                    registros.push({cnpj: normalizeCnpj(cnpjCell.value), rowNum });
                }
            }
        });

        if (registros.length === 0) {
            log("✅ Nenhum registro novo para consultar neste arquivo.");
            return;
        }

        log(`Encontrados ${registros.length} registros novos para processar.`);
        const BATCH_SIZE = keyMode === 'dupla' ? BATCH_SIZE_DUAL : BATCH_SIZE_SINGLE;
        const lotes = [];
        for (let i = 0; i < registros.length; i += BATCH_SIZE) {
            lotes.push(registros.slice(i, i + BATCH_SIZE));
        }

        for (let i = 0; i < lotes.length; i++) {
            if (cancelCurrentApiTask) {
                log("Processamento do arquivo cancelado pelo usuário.");
                break;
            }
            if (isApiQueuePaused) { // Check for pause here
                log("Processamento PAUSADO. Aguardando para retomar...");
                // Wait until unpaused. This is a blocking wait.
                while (isApiQueuePaused) {
                    await sleep(1000); // Check every second
                    if (cancelCurrentApiTask) break; // Allow cancellation even when paused
                }
                if (cancelCurrentApiTask) {
                    log("Processamento do arquivo cancelado enquanto pausado.");
                    break;
                }
                log("Processamento RETOMADO.");
            }

            const lote = lotes[i];
            log(`\n=== Processando Lote ${i + 1}/${lotes.length} (${lote.length} registros) ===`);
            progress(i + 1, lotes.length);

            let sucesso = false;
            let retries = 0;
            while (!sucesso && retries < MAX_RETRIES) {
                if (cancelCurrentApiTask) break;
                try {
                    let encontrados = new Set();

                    if (keyMode === 'dupla') {
                        // --- INÍCIO DA LÓGICA CORRIGIDA ---
                        log("Modo 'Dupla' ativado. Consultando com ambas as chaves simultaneamente.");
                        const meio = Math.ceil(lote.length / 2);
                        const lote1 = lote.slice(0, meio);
                        const lote2 = lote.slice(meio);

                        const [res1, res2] = await Promise.allSettled([
                            performApiCall(lote1.map(r => r.cnpj), credentials.c6),
                            performApiCall(lote2.map(r => r.cnpj), credentials.im)
                        ]);

                        if (res1.status === 'rejected' || res2.status === 'rejected') {
                            const errorMessages = [];
                            if (res1.status === 'rejected') errorMessages.push(`Chave 1: ${res1.reason.message}`);
                            if (res2.status === 'rejected') errorMessages.push(`Chave 2: ${res2.reason.message}`);
                            throw new Error(`Falha na consulta dupla. Erros: ${errorMessages.join('; ')}`);
                        }

                        // Se chegou aqui, ambas as chamadas foram bem-sucedidas
                        res1.value.forEach(cnpj => encontrados.add(cnpj));
                        res2.value.forEach(cnpj => encontrados.add(cnpj));
                        // --- FIM DA LÓGICA CORRIGIDA ---

                    } else { // Modos 'chave1', 'chave2', 'intercalar'
                        let currentCreds;
                        if (keyMode === "intercalar") {
                            currentCreds = i % 2 === 0 ? credentials.c6 : credentials.im;
                            log(`Usando credenciais intercaladas: ${currentCreds.name}`);
                        } else if (keyMode === "chave2") {
                            currentCreds = credentials.im;
                        } else {
                            currentCreds = credentials.c6;
                        }
                        if (keyMode !== "intercalar" && i === 0) {
                            log(`Usando credenciais fixas: ${currentCreds.name}`);
                        }
                        encontrados = await performApiCall(lote.map(r => r.cnpj), currentCreds);
                    }

                    log(`Atualizando planilha em memória...`);
                    let countDisponivel = 0;
                    
                    for (const { cnpj, rowNum } of lote) {
                        const row = worksheet.getRow(rowNum);
                        if (encontrados.has(cnpj)) {
                            row.getCell(COLUNA_RESPOSTA_LETTER).value = "disponível";
                            countDisponivel++;
                        } else {
                            row.getCell(COLUNA_RESPOSTA_LETTER).value = "cliente";
                            if (isFishMode) { // Se o modo Fish estiver ativo, envia para o webhook
                                await sendToN8NWebhook(fileHeader, row.values);
                            }
                        }
                    }

                    const countCliente = lote.length - countDisponivel;
                    log(`Resultados do Lote: ${countDisponivel} disponível(is), ${countCliente} cliente(s).`);
                    log(`💾 Salvando progresso do lote ${i + 1} na planilha...`);

                    const tempFilePath = path.join(path.dirname(filePath), `${path.basename(filePath, ".xlsx")}_temp.xlsx`);
                    await workbook.xlsx.writeFile(tempFilePath);
                    fs.unlinkSync(filePath);
                    fs.renameSync(tempFilePath, filePath);

                    log(`✅ Progresso salvo com sucesso.`);
                    sucesso = true;

                } catch (err) {
                    retries++;
                    const retryDelayMs = getRetryDelayMs();
                    log(`❌ Erro no processamento do lote (tentativa ${retries}/${MAX_RETRIES}): ${err.message}.`);
                    if (retries < MAX_RETRIES) {
                        log(`Tentando novamente em ${retryDelayMs / 60000} minutos...`);
                        if (cancelCurrentApiTask) break;
                        await sleep(retryDelayMs);
                    } else {
                        log(`Máximo de tentativas atingido para este lote. Pulando para o próximo.`);
                    }
                }
            }
            const successDelayMs = getSuccessDelayMs();
            if (sucesso && i < lotes.length - 1) {
                if (cancelCurrentApiTask) break;
                log(`Aguardando ${successDelayMs / 60000} minutos antes do próximo lote...`);
                await sleep(successDelayMs);
            }
        }
        if (!cancelCurrentApiTask) {
            let collectedClients = { header: null, rows: [] };
            if (extractClients && !isFishMode) { // Só extrai no final se NÃO for modo FISH
                log(`\nExtraindo dados de 'cliente' do arquivo...`);
                // O objeto 'worksheet' já foi atualizado em memória durante o processamento dos lotes
                collectedClients.header = worksheet.getRow(1).values;
                worksheet.eachRow((row, rowNum) => {
                    if (rowNum > 1) {
                        const status = row.getCell(COLUNA_RESPOSTA_LETTER).value;
                        if (status === 'cliente') {
                            collectedClients.rows.push(row.values);
                        }
                    }
                });
                log(`Encontrados ${collectedClients.rows.length} registros de 'cliente' para extração.`);
            }

            if (removeClients) {
                log(`\nProcessamento da API concluído para ${path.basename(filePath)}. Iniciando limpeza final (remoção de clientes)...`);
                
                const finalWorksheet = workbook.worksheets[0];
                const newWorkbook = new ExcelJS.Workbook();
                const newWorksheet = newWorkbook.addWorksheet('Disponiveis');
                
                // Copia o cabeçalho
                newWorksheet.getRow(1).values = finalWorksheet.getRow(1).values;

                let keptRows = 0;
                finalWorksheet.eachRow((row, rowNum) => {
                    if (rowNum > 1) { // Pula o cabeçalho
                        const status = row.getCell(COLUNA_RESPOSTA_LETTER).value;
                        if (status === 'disponível') {
                            newWorksheet.addRow(row.values);
                            keptRows++;
                        }
                    }
                });

                // NOVO: Limpa a coluna de resposta (C) no arquivo final.
                log('Limpando a coluna de resposta (C) no arquivo final...');
                newWorksheet.eachRow((row) => {
                    row.getCell(COLUNA_RESPOSTA_LETTER).value = null; 
                });

                await newWorkbook.xlsx.writeFile(filePath);
                log(`✅ Limpeza final concluída. ${keptRows} registros 'disponível' foram mantidos no arquivo.`);
            } else {
                log(`\n✅ Processamento da API concluído para ${path.basename(filePath)}. O arquivo foi salvo com todos os resultados (disponível/cliente).`);
            }
            return { success: true, clientData: collectedClients };
        }
        return { success: false, clientData: { header: null, rows: [] } }; // In case of cancellation
    } catch (error) {
        log(`❌ Erro fatal ao processar o arquivo ${path.basename(filePath)}: ${error.message}`);
        console.error(error);
        return { success: false, clientData: { header: null, rows: [] } };
    }
}


// #################################################################
// #           NOVA LÓGICA - PIPELINE DE RELACIONAMENTO            #
// #################################################################

// --- Constantes para a nova aba (copiadas do server.js) ---
const NOME_PLANILHA_PRINCIPAL = 'Sheet1';
const NOME_PLANILHA_RELACIONAMENTO = 'C6 - Relacionamento';
const NOME_PLANILHA_SUPERVISORES = 'supervisores';

// --- Funções utilitárias (copiadas do server.js) ---
const norm = v => (v === null || v === undefined) ? '' : String(v).trim();
const normKey = v => norm(v).toUpperCase();
const normCnpjKey = v => norm(v).replace(/\D/g, ''); // mantém só números

const excelDateToJSDate = (serial) => {
    if (typeof serial !== 'number' || isNaN(serial)) return null;
    const utc_days = Math.floor(serial - 25569);
    const utc_value = utc_days * 86400;
    return new Date(utc_value * 1000);
};

// --- Função principal do pipeline (adaptada do server.js) ---
async function runFullPipeline(filePaths, modo, event) {
    const log = (msg) => {
        console.log(`[Relacionamento] ${msg}`);
        if (event && event.sender) {
            event.sender.send("relacionamento-log", msg);
        }
    };

    try {
        log('Iniciando pipeline completo...');

        // --- Ler arquivos a partir dos caminhos recebidos ---
        log(`Lendo arquivo de relatório: ${path.basename(filePaths.relatorio)}`);
        const relWb = xlsx.read(await fsp.readFile(filePaths.relatorio));
        const relFirstSheet = relWb.Sheets[relWb.SheetNames[0]];
        const relDataAoA = xlsx.utils.sheet_to_json(relFirstSheet, { header: 1, defval: null });

        if (relDataAoA.length === 0) {
            log('Relatório vazio. Abortando.');
            return { success: false };
        }

        // --- Inserir 4 colunas em branco a partir da coluna C (índice 2) ---
        log('Inserindo 4 colunas em branco (C, D, E, F) e renomeando cabeçalhos...');
        const dataComNovasColunas = relDataAoA.map((row) => {
            const newRow = [...row];
            while (newRow.length < 2) newRow.push(null);
            newRow.splice(2, 0, null, null, null, null);
            return newRow;
        });

        const headerRow = dataComNovasColunas[0];
        headerRow[2] = 'fase';
        headerRow[3] = 'responsavel';
        headerRow[4] = 'Supervisor';
        headerRow[5] = 'faturamento';

        // --- Montar workbook inicial 'elegiveis_auto' em memória ---
        const elegiveisWb = xlsx.utils.book_new();
        const elegiveisWs = xlsx.utils.aoa_to_sheet(dataComNovasColunas, { cellDates: true });
        xlsx.utils.book_append_sheet(elegiveisWb, elegiveisWs, NOME_PLANILHA_PRINCIPAL);
        log('Planilha principal criada em memória.');

        // --- Ler Bitrix ---
        log(`Lendo arquivo Bitrix: ${path.basename(filePaths.bitrix)}`);
        const bitrixWb = xlsx.read(await fsp.readFile(filePaths.bitrix));
        const bitrixWs = bitrixWb.Sheets[bitrixWb.SheetNames[0]];
        const bitrixDataAoA = xlsx.utils.sheet_to_json(bitrixWs, { header: 1, defval: null });

        const idxB = xlsx.utils.decode_col('B');
        const idxE = xlsx.utils.decode_col('E');
        const idxH = xlsx.utils.decode_col('H');

        const bitrixRows = [];
        bitrixRows.push(['CNPJ', 'Fase', 'Responsavel']);
        for (let i = 1; i < bitrixDataAoA.length; i++) {
            const r = bitrixDataAoA[i];
            const cnpjVal = r[idxH];
            const faseVal = r[idxB];
            const respVal = r[idxE];
            if (cnpjVal === null || cnpjVal === undefined || String(cnpjVal).trim() === '') continue;
            bitrixRows.push([cnpjVal, faseVal, respVal]);
        }
        const relacionamentoWs = xlsx.utils.aoa_to_sheet(bitrixRows);
        xlsx.utils.book_append_sheet(elegiveisWb, relacionamentoWs, NOME_PLANILHA_RELACIONAMENTO);
        log(`Planilha "${NOME_PLANILHA_RELACIONAMENTO}" adicionada.`);

        // --- Ler Arquivo TIME ---
        log(`Lendo arquivo de time: ${path.basename(filePaths.time)}`);
        const timeWb = xlsx.read(await fsp.readFile(filePaths.time));
        const timeWs = timeWb.Sheets[timeWb.SheetNames[0]];
        const timeDataJson = xlsx.utils.sheet_to_json(timeWs, { defval: '' });

        const timeHeaders = Object.keys(timeDataJson[0] || {});
        const hConsultor = timeHeaders.find(h => h && h.toUpperCase().includes('CONSULTOR')) || timeHeaders[0];
        const hEquipe = timeHeaders.find(h => h && h.toUpperCase().includes('EQUIPE')) || timeHeaders[1] || timeHeaders[0];

        const supervisoresRows = [['Consultor', 'Equipe']];
        for (const row of timeDataJson) {
            supervisoresRows.push([row[hConsultor] || '', row[hEquipe] || '']);
        }
        const supervisoresWs = xlsx.utils.aoa_to_sheet(supervisoresRows);
        xlsx.utils.book_append_sheet(elegiveisWb, supervisoresWs, NOME_PLANILHA_SUPERVISORES);
        log(`Planilha "${NOME_PLANILHA_SUPERVISORES}" adicionada.`);

        // --- Ler Arquivo CONTATOSBITRIX ---
        log(`Lendo arquivo Contatos Bitrix: ${path.basename(filePaths.contatos)}`);
        const contatosWb = xlsx.read(await fsp.readFile(filePaths.contatos));
        const contatosWs = contatosWb.Sheets[contatosWb.SheetNames[0]];
        const contatosDataJson = xlsx.utils.sheet_to_json(contatosWs, { defval: '' });

        const mapFaturamento = {};
        const contatosHeaders = Object.keys(contatosDataJson[0] || {});
        const hCnpjContatos = contatosHeaders.find(h => h && h.toUpperCase().includes('CNPJ')) || contatosHeaders[0];
        const hFaturamento = contatosHeaders[1]; // Coluna B

        for (const row of contatosDataJson) {
            const cnpjKey = normCnpjKey(row[hCnpjContatos]);
            if (cnpjKey) mapFaturamento[cnpjKey] = row[hFaturamento];
        }
        log('Mapa de faturamento criado.');

        // --- Filtrar dados ---
        log('Convertendo planilha principal para JSON para aplicar filtros...');
        const elegiveisWsJson = xlsx.utils.sheet_to_json(elegiveisWs, { defval: null });
        if (elegiveisWsJson.length === 0) {
            log('Aviso: a planilha principal gerada está vazia. Abortando.');
            return { success: false };
        }

        const findHeader = (headers, primaryName, fallbackColumnLetter) => {
            let header = headers.find(h => h && h.trim().toUpperCase() === primaryName.toUpperCase());
            if (header) return header;
            const colIndex = xlsx.utils.decode_col(fallbackColumnLetter);
            if (headers[colIndex]) {
                log(`AVISO: Coluna "${primaryName}" não encontrada pelo nome. Usando fallback '${fallbackColumnLetter}' (${headers[colIndex]}).`);
                return headers[colIndex];
            }
            return null;
        };

        const headersRel = Object.keys(elegiveisWsJson[0]);
        const colElegivel = findHeader(headersRel, 'FL_ELEGIVEL_VENDA_C6PAY', 'AK');
        const colDataAprovacao = findHeader(headersRel, 'DT_APROVACAO_PAY', 'AM');

        let dadosFiltrados = [];
        log(`Modo de processamento selecionado: ${modo}`);
        log(`Total de linhas antes do filtro: ${elegiveisWsJson.length}`);

        if (modo === 'relacionamento') {
            log('Aplicando filtros para o modo "Relacionamento"...'); // MODO RELACIONAMENTO
            const colNivelAnterior = findHeader(headersRel, 'NIVEL_ANTERIOR', 'CE');
            const colAlvoAtivacao = findHeader(headersRel, 'ALVO_ATIVACAO', 'CD');
            const colTipoPessoa = findHeader(headersRel, 'TIPO_PESSOA', 'H');
            const colStatusCC = findHeader(headersRel, 'STATUS_CC', 'Y');

            if (!colNivelAnterior || !colAlvoAtivacao || !colTipoPessoa || !colStatusCC) {
                log('Erro: não foi possível localizar todas as colunas de filtro para o modo Relacionamento. Abortando.');
                return { success: false };
            } 

            const filtro1 = elegiveisWsJson.filter(row => String(row[colTipoPessoa]).toUpperCase() === 'PJ');
            log(`Após filtro TIPO_PESSOA = "PJ": ${filtro1.length}`);
            const filtro2 = filtro1.filter(row => String(row[colStatusCC]).toUpperCase() === 'LIBERADA');
            log(`Após filtro STATUS_CC = "LIBERADA": ${filtro2.length}`);
            const filtro3 = filtro2.filter(row => row[colNivelAnterior] === 0 || row[colNivelAnterior] === '0');
            log(`Após filtro NIVEL_ANTERIOR = "0": ${filtro3.length}`);
            dadosFiltrados = filtro3.filter(row => String(row[colAlvoAtivacao]).toUpperCase() === 'QUALQUER NÍVEL');
            log(`Após filtro ALVO_ATIVACAO = "QUALQUER NÍVEL": ${dadosFiltrados.length}`);

        } else { // Modo Máquina (Padrão)
            log('Aplicando filtros para o modo "Máquina"...'); // MODO MÁQUINA
            const colTipoPessoa = findHeader(headersRel, 'TIPO_PESSOA', 'H');
            const colStatusCC = findHeader(headersRel, 'STATUS_CC', 'Y');

            if (!colElegivel || !colTipoPessoa || !colDataAprovacao || !colStatusCC) {
                log('Erro: não foi possível localizar todas as colunas de filtro para o modo Padrão. Abortando.');
                return { success: false };
            }
            const filtro1 = elegiveisWsJson.filter(row => row[colElegivel] === 1 || row[colElegivel] === '1');
            log(`Após filtro FL_ELEGIVEL_VENDA_C6PAY = "1": ${filtro1.length}`);
            const filtro2 = filtro1.filter(row => String(row[colTipoPessoa]).toUpperCase() === 'PJ');
            log(`Após filtro TIPO_PESSOA = "PJ": ${filtro2.length}`);
            const filtro3 = filtro2.filter(row => row[colDataAprovacao] === null || row[colDataAprovacao] === '' || typeof row[colDataAprovacao] === 'undefined');
            log(`Após filtro DT_APROVACAO_PAY vazia: ${filtro3.length}`);
            dadosFiltrados = filtro3.filter(row => String(row[colStatusCC]).toUpperCase() === 'LIBERADA');
            log(`Após filtro STATUS_CC = "LIBERADA": ${dadosFiltrados.length}`);
        }

        if (dadosFiltrados.length === 0) {
            log('Nenhuma linha restou após os filtros. Gerando arquivo com cabeçalhos apenas.');
            // Não precisa salvar o arquivo aqui, apenas retorna sucesso
            return { success: true };
        }

        // --- Montar mapas para Lookup ---
        log('Montando mapas de lookup...');
        const mapBitrix = {};
        for (let i = 1; i < bitrixRows.length; i++) {
            const r = bitrixRows[i];
            const rawCnpj = r[0];
            if (!rawCnpj) continue;
            const key = normCnpjKey(rawCnpj);
            mapBitrix[key] = { fase: norm(r[1]), responsavel: norm(r[2]) };
        }

        const mapTime = {};
        for (let i = 1; i < supervisoresRows.length; i++) {
            const [consultorRaw, equipeRaw] = supervisoresRows[i];
            const consultorKey = normKey(consultorRaw);
            if (!consultorKey) continue;
            if (!mapTime[consultorKey]) mapTime[consultorKey] = norm(equipeRaw);
        }

        // --- Preencher colunas ---
        log('Executando lookups e preenchendo colunas nas linhas filtradas...');
        let countCnpjNotFound = 0;
        let countRespNotFound = 0;
        let countFaturamentoNotFound = 0;
        const dadosComLookups = dadosFiltrados.map((row) => {
            const possibleCnpjKeys = Object.keys(row).filter(k => k && k.toUpperCase().includes('CNPJ'));
            let rawCnpjValue = '';
            if (possibleCnpjKeys.length > 0) {
                rawCnpjValue = row[possibleCnpjKeys[0]];
            } else {
                rawCnpjValue = row['CNPJ'] || row['cnpj'] || '';
            }
            const cnpjKey = normCnpjKey(rawCnpjValue);

            let fase = 'Não encontrado';
            let responsavel = 'Não encontrado';
            let supervisor = 'Não encontrado';
            let faturamento = 'Não encontrado';

            if (cnpjKey && mapBitrix[cnpjKey]) {
                fase = mapBitrix[cnpjKey].fase || 'Não encontrado';
                responsavel = mapBitrix[cnpjKey].responsavel || 'Não encontrado';
            } else {
                countCnpjNotFound++;
            }

            const respKey = normKey(responsavel);
            if (respKey && mapTime[respKey]) {
                supervisor = mapTime[respKey];
            } else {
                if (responsavel !== 'Não encontrado') countRespNotFound++;
            }

            if (cnpjKey && mapFaturamento[cnpjKey] !== undefined) {
                faturamento = mapFaturamento[cnpjKey];
            } else {
                countFaturamentoNotFound++;
            }

            return {
                ...row,
                'fase': fase,
                'responsavel': responsavel,
                'Supervisor': supervisor,
                'faturamento': faturamento
            };
        });

        log(`Lookups concluídos. CNPJs não encontrados: ${countCnpjNotFound}. Responsáveis sem supervisor: ${countRespNotFound}. CNPJs sem faturamento: ${countFaturamentoNotFound}.`);

        // --- Se modo for 'relacionamento', filtrar e renomear ---
        let dadosFinaisParaSheet = dadosComLookups;
        if (modo === 'relacionamento') {
            log('Modo relacionamento: Filtrando e renomeando colunas para a saída final...');
            dadosFinaisParaSheet = dadosComLookups.map(row => {
                const findKey = (obj, name) => Object.keys(obj).find(k => k.toLowerCase() === name.toLowerCase());

                const keyCpfCnpj = findKey(row, 'cd_cpf_cnpj_cliente');
                const keyNomeCliente = findKey(row, 'nome_cliente');
                const keyTelefone = findKey(row, 'telefone_master');
                const keyEmail = findKey(row, 'email');
                const keyCashIn = findKey(row, 'vl_cash_in_mtd');
                const keyFaixaFaturamento = findKey(row, 'qual a faixa de faturamento mensal da sau empresa'); // Nome da coluna original
                const keyDataBase = findKey(row, 'data_base');
                const keyLimiteConta = findKey(row, 'limite_conta');
                const keyDtContaCriada = findKey(row, 'dt_conta_criada');
                const keyChavesPix = findKey(row, 'chaves_pix_forte');
                const keyLimiteCartao = findKey(row, 'limite_cartao');

                const cpfAsNumber = row[keyCpfCnpj] ? Number(String(row[keyCpfCnpj]).replace(/\D/g, '')) : null;
                const foneAsNumber = row[keyTelefone] ? Number(String(row[keyTelefone]).replace(/\D/g, '')) : null;
                const dataBaseAsDate = excelDateToJSDate(row[keyDataBase]);
                const dtContaCriadaAsDate = excelDateToJSDate(row[keyDtContaCriada]);

                return {
                    'DATA_BASE': dataBaseAsDate,
                    'SUPERVISOR': row['Supervisor'],
                    'RESPONSÁVEL': row['responsavel'],
                    'CPF': cpfAsNumber,
                    'livre1': row['fase'], // Fase
                    'nome': row[keyNomeCliente], // NOME_CLIENTE
                    'fone1': foneAsNumber, // TELEFONE_MASTER
                    'chave': row[keyEmail], // EMAIL
                    'livre2': row[keyCashIn], // VL_CASH_IN_MTD
                    'livre3': row[keyFaixaFaturamento] || row['faturamento'], // Fallback
                    'LIMITE_CONTA': row[keyLimiteConta],
                    'DT_CONTA_CRIADA': dtContaCriadaAsDate,
                    'CHAVES_PIX_FORTE': row[keyChavesPix],
                    'LIMITE_CARTAO': row[keyLimiteCartao]
                };
            });
        }

        // --- Salvar o arquivo final ---
        const { canceled, filePath: savePath } = await dialog.showSaveDialog(mainWindow, {
            title: "Salvar Relatório Final",
            defaultPath: `elegiveis_auto_${modo}_${Date.now()}.xlsx`,
            filters: [{ name: "Excel", extensions: ["xlsx"] }]
        });

        if (canceled || !savePath) {
            log("Salvamento cancelado pelo usuário.");
            return { success: true }; // O processo foi bem-sucedido, mas o usuário não salvou.
        }

        log(`Salvando arquivo final em: ${savePath}`);
        const finalSheet = xlsx.utils.json_to_sheet(dadosFinaisParaSheet, { skipHeader: false, cellDates: true });
        
        // Se for modo relacionamento, aplica formatação de número e data
        if (modo === 'relacionamento') {
             // Formatar colunas (Exemplo: D=CPF, G=fone1 como Número; A=DATA_BASE, L=DT_CONTA_CRIADA como Data)
            // A biblioteca 'xlsx' não suporta formatação complexa de célula como a 'exceljs'
            // Vamos garantir que as datas sejam objetos Date (já feito) e números sejam números (já feito)
            // O ExcelJS seria necessário para aplicar formatação de string (ex: "0" para números)
            // Mas para o 'xlsx', converter para tipo Number e Date já é o melhor que podemos fazer.
            log("Formatos de data e número aplicados para o modo relacionamento.");
        }

        const finalWb = xlsx.utils.book_new();
        xlsx.utils.book_append_sheet(finalWb, finalSheet, NOME_PLANILHA_PRINCIPAL);
        xlsx.utils.book_append_sheet(finalWb, relacionamentoWs, NOME_PLANILHA_RELACIONAMENTO);
        xlsx.utils.book_append_sheet(finalWb, supervisoresWs, NOME_PLANILHA_SUPERVISORES);
        
        // Escreve o arquivo final
        xlsx.writeFile(finalWb, savePath);

        log(`Arquivo salvo com sucesso. Pipeline completo finalizado.`);
        shell.showItemInFolder(savePath);
        return { success: true };

    } catch (err) {
        log(`Erro no pipeline: ${err.message}`);
        log(err.stack);
        return { success: false, error: err };
    }
}

// --- Handler do IPCMain para a nova aba ---
ipcMain.on('run-relacionamento-pipeline', async (event, filePaths, modo) => {
    if (!isAdmin()) {
        event.sender.send("relacionamento-log", "❌ Acesso negado.");
        event.sender.send("relacionamento-finished", false);
        return;
    }

    const log = (msg) => event.sender.send("relacionamento-log", msg);

    // Validação dos caminhos
    for (const key in filePaths) {
        if (!filePaths[key] || !fs.existsSync(filePaths[key])) {
            log(`❌ ERRO: Arquivo "${key}" não encontrado em: ${filePaths[key]}`);
            event.sender.send("relacionamento-finished", false);
            return;
        }
    }

    // Executa a função
    const result = await runFullPipeline(filePaths, modo, event);
    
    // Envia o resultado de volta
    event.sender.send("relacionamento-finished", result.success);
});