console.log('--- MAIN.JS CARREGADO - VERSÃO NOVA (NEON DB) ---');
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
const { Pool } = require('pg'); // NOVO: Driver do PostgreSQL

autoUpdater.logger = require("electron-log");
autoUpdater.logger.transports.file.level = "info";

const store = new Store();

// #################################################################
// #           CONFIGURAÇÃO DO BANCO DE DADOS (NEON DB)            #
// #################################################################

// IMPORTANTE: Substitua pela sua connection string do Neon DB.
// Ela se parece com: "postgresql://user:password@host:port/dbname?sslmode=require"
const NEON_CONNECTION_STRING = "postgresql://neondb_owner:npg_ki2aKHqlnFY9@ep-quiet-night-ac2uu9kc-pooler.sa-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require";

// Pool de conexões para otimizar o acesso ao banco
const pool = new Pool({
    connectionString: NEON_CONNECTION_STRING,
});

// SQL para criar as tabelas necessárias no seu banco de dados Neon
/*
-- Tabela para a funcionalidade de "Enriquecimento" (Fornecido por você)
CREATE TABLE IF NOT EXISTS empresas (
    id SERIAL PRIMARY KEY,
    cnpj TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS telefones (
    id SERIAL PRIMARY KEY,
    empresa_id INTEGER NOT NULL,
    numero TEXT NOT NULL,
    CONSTRAINT fk_empresa
        FOREIGN KEY(empresa_id) 
        REFERENCES empresas(id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_empresas_cnpj ON empresas(cnpj);
CREATE INDEX IF NOT EXISTS idx_telefones_empresa_id ON telefones(empresa_id);

-- Tabela para a funcionalidade de "Limpeza Local" (histórico de CNPJs)
CREATE TABLE IF NOT EXISTS limpeza_cnpjs (
    id SERIAL PRIMARY KEY,
    cnpj TEXT NOT NULL UNIQUE,
    batch_id TEXT,
    adicionado_em TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_limpeza_cnpjs_cnpj ON limpeza_cnpjs(cnpj);
CREATE INDEX IF NOT EXISTS idx_limpeza_cnpjs_batch_id ON limpeza_cnpjs(batch_id);


-- Tabela para a funcionalidade "Auto Raiz" e "Alimentar Raiz"
CREATE TABLE IF NOT EXISTS raiz_cnpjs (
    id SERIAL PRIMARY KEY,
    cnpj TEXT NOT NULL UNIQUE,
    fonte TEXT,
    lote_id TEXT,
    adicionado_em TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_raiz_cnpjs_cnpj ON raiz_cnpjs(cnpj);

*/

// Função para testar a conexão com o banco na inicialização
async function testDbConnection() {
    try {
        await pool.query('SELECT NOW()');
        console.log("✅ Conexão com o banco de dados Neon DB estabelecida com sucesso.");
    } catch (error) {
        console.error("❌ ERRO FATAL: Falha ao conectar ao Neon DB.", error);
        dialog.showErrorBox("Erro Crítico de Conexão", `Não foi possível conectar ao banco de dados: ${error.message}. A aplicação será encerrada.`);
        if (app) {
          app.quit();
        }
    }
}


// #################################################################
// #           SISTEMA DE LOGIN E PERMISSÕES (Sem alterações)      #
// #################################################################

const users = {
    'Pablo': { password: 'Vasco@2025', role: 'admin' },
    'Felipe': { password: 'Flamengo@2025', role: 'admin' },
    'Davi': { password: '080472Fr*', role: 'admin' },
    'Mayko': { password: '123456', role: 'limited' },
    'Bruna': { password: '123456', role: 'limited' },
    'Laiane': { password: '123456', role: 'limited' },
    'Waleska': { password: '123456', role: 'limited' },
    'Gomes': { password: '123456', role: 'limited' },
    'Tatiane': { password: '123456', role: 'limited' },
    'Natallia': { password: '123456', role: 'limited' },
};

let mainWindow;
let loginWindow;
let currentUser = null; 

function createLoginWindow() {
    loginWindow = new BrowserWindow({
        width: 420,
        height: 550,
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

ipcMain.handle('login-attempt', async (event, username, password, rememberMe) => {
    const user = users[username];
    if (user && user.password === password) {
        currentUser = { username: username, role: user.role };
        
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
    if (mainWindow) {
        mainWindow.close();
    }
    if (!loginWindow) {
        createLoginWindow();
    }
});

const isAdmin = () => {
    return currentUser && currentUser.role === 'admin';
};


// #################################################################
// #           LÓGICA DE INICIALIZAÇÃO (COM MODIFICAÇÕES)          #
// #################################################################

// Variável para armazenar o histórico de CNPJs da limpeza em memória
let storedCnpjs = new Set();

// NOVO: Carrega os CNPJs do histórico da tabela 'limpeza_cnpjs'
async function loadStoredCnpjs() {
    if (!isAdmin()) return;
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
        webPreferences: {
            nodeIntegration: false,
            contextIsolation: true,
            preload: path.join(__dirname, "preload.js")
        }
    });
    mainWindow.loadFile("index.html");
    
    mainWindow.webContents.on("did-finish-load", () => {
        if (currentUser) {
            mainWindow.webContents.send('user-info', currentUser);
        }
        if (isAdmin()) { 
            loadStoredCnpjs(); // Carrega o histórico de CNPJs
        }
        autoUpdater.checkForUpdatesAndNotify();
    });

    mainWindow.on('closed', () => {
        mainWindow = null;
    });
}

app.whenReady().then(async () => {
    // Testa a conexão com o banco antes de tudo
    await testDbConnection(); 

    const savedCredentials = store.get('credentials');

    if (savedCredentials && savedCredentials.username && savedCredentials.password) {
        const { username, password } = savedCredentials;
        const user = users[username];

        if (user && user.password === password) {
            currentUser = { username, role: user.role };
            createMainWindow();
        } else {
            const win = createLoginWindow();
            win.webContents.on('did-finish-load', () => {
                win.webContents.send('auto-login-failed', 'Credenciais salvas inválidas. Faça o login novamente.');
            });
        }
    } else {
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

// --- FUNÇÕES GERAIS E DE ARQUIVOS (Sem grandes alterações) ---

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
    if (!isAdmin()) return 0;
    try {
        const result = await pool.query('SELECT COUNT(*) FROM empresas;');
        return parseInt(result.rows[0].count, 10);
    } catch (error) {
        console.error("Erro ao contar CNPJs enriquecidos:", error);
        return 0;
    }
});

ipcMain.handle("download-enriched-data", async () => {
    if (!isAdmin()) return { success: false, message: "Acesso negado." };
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
            return [row.cnpj, ...Array.from({ length: maxPhones }, (_, i) => phones[i] || "")];
        });

        const workbook = new ExcelJS.Workbook();
        const worksheet = workbook.addWorksheet("Dados Enriquecidos");
        worksheet.addRow(headers);
        worksheet.addRows(data);
        await workbook.xlsx.writeFile(filePath);
        return { success: true, message: `Arquivo salvo com sucesso: ${filePath}` };
    } catch (error) {
        console.error("Erro ao baixar dados enriquecidos:", error);
        return { success: false, message: `Erro ao gerar arquivo: ${error.message}` };
    }
});

ipcMain.on("start-db-load", async (event, { masterFiles }) => {
    if (!isAdmin()) {
        event.sender.send("enrichment-log", "❌ Acesso negado.");
        event.sender.send("db-load-finished");
        return;
    }
    const log = (msg) => event.sender.send("enrichment-log", msg);
    const progress = (current, total, fileName, cnpjsProcessed) => event.sender.send("db-load-progress", { current, total, fileName, cnpjsProcessed });
    
    log(`--- Iniciando Carga para o Banco de Dados de Enriquecimento ---`);
    let totalCnpjsProcessed = 0;

    const saveChunkToDb = async (dataMap, filePath) => {
        if (dataMap.size === 0) return;
        const client = await pool.connect();
        try {
            await client.query('BEGIN');

            const uniqueCnpjs = Array.from(dataMap.keys());
            // 1. Insere todos os CNPJs novos na tabela 'empresas'
            const insertEmpresasQuery = `INSERT INTO empresas (cnpj) SELECT unnest($1::text[]) ON CONFLICT (cnpj) DO NOTHING`;
            await client.query(insertEmpresasQuery, [uniqueCnpjs]);

            // 2. Busca os IDs de todos os CNPJs (novos e existentes) do lote
            const getEmpresasQuery = `SELECT id, cnpj FROM empresas WHERE cnpj = ANY($1::text[])`;
            const result = await client.query(getEmpresasQuery, [uniqueCnpjs]);
            const empresaIdMap = new Map(result.rows.map(row => [row.cnpj, row.id]));

            // 3. Prepara os dados para a tabela 'telefones'
            const phoneValues = [];
            for (const [cnpj, phones] of dataMap.entries()) {
                const empresaId = empresaIdMap.get(cnpj);
                if (empresaId) {
                    const uniquePhones = [...new Set(phones)].filter(p => String(p).replace(/\D/g, '').length >= 8);
                    uniquePhones.forEach(phone => phoneValues.push({ empresa_id: empresaId, numero: phone }));
                }
            }

            // 4. Insere os telefones em lotes para evitar sobrecarga
            if (phoneValues.length > 0) {
                 const insertTelefonesQuery = `INSERT INTO telefones (empresa_id, numero) SELECT (d.v->>'empresa_id')::int, d.v->>'numero' FROM jsonb_array_elements($1::jsonb) d(v) ON CONFLICT DO NOTHING`;
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
                // (O código de leitura do Excel permanece o mesmo)
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
                if (cnpjsToUpdate.size > 0) await saveChunkToDb(cnpjsToUpdate, filePath);

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

async function runEnrichmentProcess({ filesToEnrich, strategy, backup }, log, progress, onFinish) {
    if (!isAdmin()){ log("❌ Acesso negado."); if(onFinish) onFinish(); return; }

    log("--- Iniciando Processo de Enriquecimento por Lotes ---");
    let totalEnrichedRowsOverall = 0, totalProcessedRowsOverall = 0, totalNotFoundInDbOverall = 0;
    const BATCH_SIZE = 2000;
    try {
        for (const fileObj of filesToEnrich) {
            const { path: filePath, id } = fileObj;
            const startTime = Date.now();
            log(`\nProcessando arquivo: ${path.basename(filePath)}`);
            progress(id, 0, null);
            if (backup) { const p = path.parse(filePath); fs.copyFileSync(filePath, path.join(p.dir, `${p.name}.backup_enrich_${Date.now()}${p.ext}`)); log(`Backup criado.`); }
            try {
                // (Código de leitura e identificação de colunas do Excel permanece o mesmo)
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
                    
                    // NOVO: Busca dados de enriquecimento no PostgreSQL
                    const enrichmentDataForBatch = new Map();
                    const cnpjKeys = Array.from(cnpjsInBatch.keys());
                    if (cnpjKeys.length > 0) {
                        const query = `
                            SELECT e.cnpj, array_agg(t.numero) as telefones
                            FROM empresas e
                            LEFT JOIN telefones t ON e.id = t.empresa_id
                            WHERE e.cnpj = ANY($1::text[])
                            GROUP BY e.id, e.cnpj;
                        `;
                        const result = await pool.query(query, [cnpjKeys]);
                        result.rows.forEach(row => enrichmentDataForBatch.set(row.cnpj, row.telefones || []));
                    }
                    
                    log(`Lote ${currentBatchNum}/${totalBatches}: ${enrichmentDataForBatch.size} CNPJs encontrados no BD. Atualizando planilha...`);

                    // (O resto da lógica de atualização da planilha permanece o mesmo)
                    for (const [cnpj, { row }] of cnpjsInBatch.entries()) { let rowWasEnriched = false; if (enrichmentDataForBatch.has(cnpj)) { const phonesFromDb = enrichmentDataForBatch.get(cnpj); const existingPhones = phoneCols.map(idx => row.getCell(idx).value).filter(Boolean); const shouldProcess = (strategy === "overwrite") || (strategy === "append" && existingPhones.length < phoneCols.length) || (strategy === "ignore" && existingPhones.length === 0); if (shouldProcess) { rowWasEnriched = true; if (strategy === "overwrite") phoneCols.forEach(idx => row.getCell(idx).value = null); let phonesToWrite = [...phonesFromDb]; phoneCols.forEach(idx => { if (strategy === "append" && row.getCell(idx).value) return; if (phonesToWrite.length > 0) row.getCell(idx).value = phonesToWrite.shift(); }); } } else { if (cnpj) notFoundInFile++; } row.getCell(statusCol).value = rowWasEnriched ? "Enriquecido" : "Pobre"; if (rowWasEnriched) enrichedInFile++; }
                    
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
    if (!isAdmin()) { event.sender.send("enrichment-log", "❌ Acesso negado."); event.sender.send("enrichment-finished"); return; }
    await runEnrichmentProcess(options, (msg) => event.sender.send("enrichment-log", msg), (id, pct, eta) => event.sender.send("enrichment-progress", { id, progress: pct, eta }), () => event.sender.send("enrichment-finished"));
});

// --- FUNÇÕES DA ABA MONITORAMENTO, LOGIN, ETC (Sem alterações relevantes ao DB) ---
// (Estas seções foram omitidas para brevidade, pois não interagem com o Firebase/Neon)
ipcMain.handle('fetch-monitoring-report', async (event, { reportUrl, operatorTimesParams }) => { if (!currentUser) { return { success: false, message: 'Acesso negado. Faça o login.' }; } let mainReportResult; try { const response = await axios.get(reportUrl, { timeout: 4000000, headers: { 'User-Agent': 'PostmanRuntime/7.44.1' } }); if (response.status === 200) { const data = (typeof response.data === 'string' && response.data.includes("Nenhum registro encontrado")) ? [] : response.data; mainReportResult = { success: true, data: data, operatorTimesData: null }; } else { return { success: false, message: `A API principal retornou um status inesperado: ${response.status}` }; } } catch (error) { console.error("Erro ao buscar relatório de monitoramento:", error.message); return { success: false, message: `Falha na comunicação com a API principal: ${error.message}` }; } if (mainReportResult.success && operatorTimesParams) { const { data_inicio, data_fim, operador_id, grupo_operador_id } = operatorTimesParams; const baseUrl = 'http://mbfinance.fastssl.com.br/api/relatorio/operador_tempos.php'; const url = `${baseUrl}?data_inicial=${data_inicio}&data_final=${data_fim}&operador_id=${operador_id}&grupo_operador_id=${grupo_operador_id}&servico_id=&operador_ativo=`; try { const timesResponse = await axios.get(url, { timeout: 30000 }); if (timesResponse.status === 200) { mainReportResult.operatorTimesData = timesResponse.data; } else { console.error(`API de tempos retornou status ${timesResponse.status}`); } } catch (error) { console.error('[DEBUG MAIN] ERRO na chamada da API de tempos:', error.message); } } return mainReportResult; });
ipcMain.handle('download-recording', async (event, url, fileName) => { if (!mainWindow) { return { success: false, message: 'Janela principal não encontrada.' }; } const { canceled, filePath } = await dialog.showSaveDialog(mainWindow, { title: 'Salvar Gravação', defaultPath: fileName, filters: [ { name: 'Áudio MP3', extensions: ['mp3'] } ] }); if (canceled || !filePath) { return { success: true, message: 'Download cancelado pelo usuário.' }; } try { const response = await axios({ method: 'get', url: url, responseType: 'stream', headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36' } }); const writer = fs.createWriteStream(filePath); response.data.pipe(writer); return new Promise((resolve, reject) => { writer.on('finish', () => resolve({ success: true, message: `Gravação salva em: ${filePath}` })); writer.on('error', (err) => { console.error("Erro ao salvar o arquivo:", err); reject({ success: false, message: `Falha ao salvar o arquivo: ${err.message}` }); }); }); } catch (error) { console.error("Erro no download da gravação:", error); let errorMessage = error.message; if (error.response && error.response.status === 403) { errorMessage = "Acesso negado (403 Forbidden). Verifique a URL ou permissões no servidor."; } return { success: false, message: `Erro ao baixar a gravação: ${errorMessage}` }; } });
async function runPhoneAdjustment(filePath, event, backup) { if (!isAdmin()) { event.sender.send("log", "❌ Acesso negado: Permissão de administrador necessária."); return; } const log = (msg) => event.sender.send("log", msg); if (!filePath || !fs.existsSync(filePath)) { log(`❌ Erro: Arquivo para ajuste de fones não encontrado em: ${filePath}`); return; } log(`\n--- Iniciando Ajuste de Fones para: ${path.basename(filePath)} ---`); try { if (backup) { const p = path.parse(filePath); const backupPath = path.join(p.dir, `${p.name}.backup_fones_${Date.now()}${p.ext}`); fs.copyFileSync(filePath, backupPath); log(`Backup do arquivo criado em: ${backupPath}`); } const workbook = new ExcelJS.Workbook(); await workbook.xlsx.readFile(filePath); const worksheet = workbook.worksheets[0]; const phoneColumns = []; worksheet.getRow(1).eachCell({ includeEmpty: true }, (cell, colNumber) => { if (cell.value && typeof cell.value === "string" && cell.value.trim().toLowerCase().startsWith("fone")) { phoneColumns.push(colNumber); } }); phoneColumns.sort((a, b) => a - b); if (phoneColumns.length === 0) { log("⚠️ Nenhuma coluna \"fone\" encontrada. Ajuste pulado."); return; } log(`Ajustando ${phoneColumns.length} colunas de telefone...`); let processedRows = 0; worksheet.eachRow((row, rowNumber) => { if (rowNumber === 1) return; const phoneValuesInRow = phoneColumns.map(colNumber => row.getCell(colNumber).value).filter(v => v !== null && v !== undefined && String(v).trim() !== ""); phoneColumns.forEach((colNumber, index) => { row.getCell(colNumber).value = index < phoneValuesInRow.length ? phoneValuesInRow[index] : null; }); processedRows++; }); await workbook.xlsx.writeFile(filePath); log(`✅ Ajuste de fones concluído. ${processedRows} linhas processadas.`); } catch (err) { log(`❌ Erro catastrófico durante o ajuste de fones: ${err.message}`); console.error(err); } }


// --- FUNÇÃO PARA ALIMENTAR A BASE RAIZ (Refatorada para PostgreSQL) ---
ipcMain.on("feed-root-database", async (event, filePaths) => {
    if (!isAdmin()) { log("❌ Acesso negado."); event.sender.send("root-feed-finished"); return; }
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
    if (!isAdmin()) { return { success: false, message: "Acesso negado." }; }
    if (storedCnpjs.size === 0) { dialog.showMessageBox(mainWindow, { type: "info", title: "Aviso", message: "Nenhum CNPJ armazenado para salvar." }); return { success: false, message: "Nenhum CNPJ armazenado para salvar." }; }
    const { canceled, filePath } = await dialog.showSaveDialog(mainWindow, { title: "Salvar CNPJs Armazenados", defaultPath: `cnpjs_armazenados_${Date.now()}.xlsx`, filters: [ { name: "Excel Files", extensions: ["xlsx"] } ] });
    if (canceled || !filePath) { return { success: false, message: "Operação de salvar cancelada." }; }
    try { const data = Array.from(storedCnpjs).map(cnpj => [cnpj]); const worksheet = XLSX.utils.aoa_to_sheet([ ["cpf"], ...data ]); const workbook = XLSX.utils.book_new(); XLSX.utils.book_append_sheet(workbook, worksheet, "CNPJs"); XLSX.writeFile(workbook, filePath); dialog.showMessageBox(mainWindow, { type: "info", title: "Sucesso", message: `Arquivo salvo com sucesso em: ${filePath}` }); return { success: true, message: `Arquivo salvo com sucesso em: ${filePath}` }; } catch (err) { console.error("Erro ao salvar Excel:", err); dialog.showMessageBox(mainWindow, { type: "error", title: "Erro", message: `Erro ao salvar arquivo: ${err.message}` }); return { success: false, message: `Erro ao salvar arquivo: ${err.message}` }; }
});

ipcMain.handle("delete-batch", async (event, batchId) => {
    if (!isAdmin()) { return { success: false, message: "Acesso negado." }; }
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
            // (Lógica de carregar raiz de arquivo local permanece a mesma)
            if (!args.rootFile || !fs.existsSync(args.rootFile)) { return log(`❌ Arquivo raiz não encontrado: ${args.rootFile}`); } const rootIdx = letterToIndex(args.rootCol); const wbRoot = await readSpreadsheet(args.rootFile); const sheetRoot = wbRoot.Sheets[wbRoot.SheetNames[0]]; const rowsRoot = XLSX.utils.sheet_to_json(sheetRoot, { header: 1 }).map(r => r[rootIdx]).filter(v => v).map(v => String(v).trim()); rowsRoot.forEach(item => rootSet.add(item)); log(`Lista raiz do arquivo carregada com ${rootSet.size} valores.`);
        }
        log(`Histórico de CNPJs em memória com ${storedCnpjs.size} registros.`);
        if (args.checkDb) log("Opção \"Consultar Banco de Dados\" está ATIVADA."); if (args.saveToDb) log("Opção \"Salvar no Banco de Dados\" está ATIVADA."); if (args.autoAdjust) log("Opção \"Ajustar Fones Pós-Limpeza\" está ATIVADA.");

        const allNewCnpjs = new Set();
        for (const fileObj of args.cleanFiles) {
            const newlyFoundInFile = await processFile(fileObj, rootSet, args.destCol, event, args.backup, args.checkDb, args.saveToDb, storedCnpjs);
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
            
            // Inserção em massa no PostgreSQL
            const query = `
                INSERT INTO limpeza_cnpjs (cnpj, batch_id)
                SELECT d.cnpj, $2 FROM unnest($1::text[]) AS d(cnpj)
                ON CONFLICT (cnpj) DO NOTHING;
            `;
            const result = await pool.query(query, [cnpjsArray, batchId]);
            event.sender.send("upload-progress", { current: 1, total: 1 }); // Simplificado para um único lote de envio

            // Atualiza o cache local
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

// A função processFile permanece a mesma, pois sua lógica interna não interage com o DB.
async function processFile(fileObj, rootSet, destCol, event, backup, checkDb, saveToDb, cnpjsHistory) { const file = fileObj.path; const id = fileObj.id; const log = (msg) => event.sender.send("log", msg); const progress = (pct) => event.sender.send("progress", { id, progress: pct }); log(`\nProcessando arquivo de limpeza: ${path.basename(file)}...`); if (!fs.existsSync(file)) return new Set(); if (backup) { const p = path.parse(file); const bkp = path.join(p.dir, `${p.name}.backup_${Date.now()}${p.ext}`); fs.copyFileSync(file, bkp); log(`Backup criado: ${bkp}`); } const wb = await readSpreadsheet(file); const sheet = wb.Sheets[wb.SheetNames[0]]; const data = XLSX.utils.sheet_to_json(sheet, { header: 1 }); if (data.length <= 1) { log(`⚠️ Arquivo vazio ou sem dados: ${file}`); return new Set(); } const header = data[0]; const destColIdx = letterToIndex(destCol); const cpfColIdx = header.findIndex(h => String(h).trim().toLowerCase() === "cpf"); if (cpfColIdx === -1) { log(`❌ ERRO: A coluna \"cpf\" não foi encontrada no arquivo ${path.basename(file)}. Pulando este arquivo.`); return new Set(); } const foneIdxs = header.reduce((acc, cell, i) => { if (typeof cell === "string" && cell.trim().toLowerCase().startsWith("fone")) acc.push(i); return acc; }, []); const cleaned = [header]; let removedByRoot = 0; let removedDuplicates = 0; let cleanedPhones = 0; const totalRows = data.length - 1; const newCnpjsInThisFile = new Set(); for (let i = 1; i < data.length; i++) { const row = data[i]; const key = row[destColIdx] ? String(row[destColIdx]).trim() : ""; const cnpj = row[cpfColIdx] ? String(row[cpfColIdx]).trim().replace(/\D/g, "") : ""; if (checkDb && cnpj && cnpjsHistory.has(cnpj)) { removedDuplicates++; continue; } if (key && rootSet.has(key)) { removedByRoot++; continue; } foneIdxs.forEach(idx => { const v = row[idx] ? String(row[idx]).trim() : ""; if (/^\d{10}$/.test(v)) { row[idx] = ""; cleanedPhones++; } }); cleaned.push(row); if (saveToDb && cnpj && !cnpjsHistory.has(cnpj)) { newCnpjsInThisFile.add(cnpj); } if (i % 2000 === 0) { progress(Math.floor((i / totalRows) * 100)); await new Promise(resolve => setImmediate(resolve)); } } const newWB = XLSX.utils.book_new(); XLSX.utils.book_append_sheet(newWB, XLSX.utils.aoa_to_sheet(cleaned), wb.SheetNames[0]); writeSpreadsheet(newWB, file); progress(100); log(`Arquivo: ${path.basename(file)}\n • Clientes repetidos (BD): ${removedDuplicates}\n • Removidos pela Raiz: ${removedByRoot}\n • Fones limpos: ${cleanedPhones}\n • Total final: ${cleaned.length - 1}`); return newCnpjsInThisFile; }

ipcMain.on("start-db-only-cleaning", async (event, { filesToClean, saveToDb }) => {
    if (!isAdmin()) { event.sender.send("log", "❌ Acesso negado."); return; }
    const log = (msg) => event.sender.send("log", msg);
    const batchId = `batch-${Date.now()}`;
    log(`--- Iniciando Limpeza Apenas pelo Banco de Dados para ${filesToClean.length} arquivo(s) ---`);
    if (saveToDb) log(`Opção \"Salvar no Banco de Dados\" ATIVADA. ID do Lote: ${batchId}`);
    log(`Usando ${storedCnpjs.size} CNPJs do histórico em memória.`);
    const allNewCnpjs = new Set();
    // A lógica de limpeza de arquivos é a mesma, o que muda é como salvamos no final
    for (const filePath of filesToClean) { log(`\nProcessando: ${path.basename(filePath)}`); try { const wb = await readSpreadsheet(filePath); const sheet = wb.Sheets[wb.SheetNames[0]]; const data = XLSX.utils.sheet_to_json(sheet, { header: 1 }); if (data.length <= 1) { log(`⚠️ Arquivo vazio ou sem dados: ${filePath}`); continue; } const header = data[0]; const cpfColIdx = header.findIndex(h => String(h).trim().toLowerCase() === "cpf"); if (cpfColIdx === -1) { log(`❌ ERRO: A coluna \"cpf\" não foi encontrada em ${path.basename(filePath)}. Pulando.`); continue; } let removedCount = 0; const cleaned = [header]; for (let i = 1; i < data.length; i++) { const row = data[i]; const cnpj = row[cpfColIdx] ? String(row[cpfColIdx]).trim().replace(/\D/g, "") : ""; if (cnpj && storedCnpjs.has(cnpj)) { removedCount++; continue; } cleaned.push(row); if (saveToDb && cnpj) { allNewCnpjs.add(cnpj); } } const newWB = XLSX.utils.book_new(); XLSX.utils.book_append_sheet(newWB, XLSX.utils.aoa_to_sheet(cleaned), wb.SheetNames[0]); writeSpreadsheet(newWB, filePath); log(`✅ Arquivo ${path.basename(filePath)} concluído. Removidos: ${removedCount}. Total final: ${cleaned.length - 1}`); } catch (err) { log(`❌ Erro ao processar ${path.basename(filePath)}: ${err.message}`); console.error(err); } }
    
    // Lógica de salvamento no PostgreSQL
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
ipcMain.on("start-merge", async (event, files) => {
    if (!isAdmin()) {
        event.sender.send("log", "❌ Acesso negado: Permissão de administrador necessária.");
        return;
    }
    const log = (msg) => event.sender.send("log", msg);
    if (!files || files.length < 2) {
        log("❌ Erro: Por favor, selecione pelo menos dois arquivos para mesclar.");
        dialog.showErrorBox("Erro de Mesclagem", "Você precisa selecionar no mínimo dois arquivos para a mesclagem.");
        return;
    }
    log(`\n--- Iniciando Mesclagem de ${files.length} arquivos ---`);
    try {
        const { canceled, filePath: savePath } = await dialog.showSaveDialog(mainWindow, {
            title: "Salvar Arquivo Mesclado",
            defaultPath: `mesclado_${Date.now()}.xlsx`,
            filters: [{ name: "Planilhas Excel", extensions: ["xlsx"] }]
        });
        if (canceled || !savePath) {
            log("Operação de mesclagem cancelada pelo usuário.");
            return;
        }
        log(`Arquivo de destino: ${savePath}`);
        let allDataRows = [];
        let totalRows = 0;
        const firstFilePath = files[0];
        log(`Lendo arquivo base: ${path.basename(firstFilePath)}`);
        const firstWb = await readSpreadsheet(firstFilePath);
        const firstWs = firstWb.Sheets[firstWb.SheetNames[0]];
        const firstFileData = XLSX.utils.sheet_to_json(firstWs, { header: 1, defval: "" });
        allDataRows.push(...firstFileData);
        totalRows += firstFileData.length;
        log(`Adicionadas ${firstFileData.length} linhas (com cabeçalho) do arquivo base.`);
        for (let i = 1; i < files.length; i++) {
            const filePath = files[i];
            log(`Lendo arquivo para anexar: ${path.basename(filePath)}`);
            const wb = await readSpreadsheet(filePath);
            const ws = wb.Sheets[wb.SheetNames[0]];
            const fileData = XLSX.utils.sheet_to_json(ws, { header: 1, defval: "" }).slice(1);
            if (fileData.length > 0) {
                allDataRows.push(...fileData);
                totalRows += fileData.length;
                log(`Adicionadas ${fileData.length} linhas de dados de ${path.basename(filePath)}.`);
            } else {
                log(`⚠️ Arquivo ${path.basename(filePath)} não continha dados além do cabeçalho.`);
            }
        }
        log(`\nTotal de linhas a serem escritas: ${totalRows}. Criando o arquivo final...`);
        const newWorkbook = XLSX.utils.book_new();
        const newWorksheet = XLSX.utils.aoa_to_sheet(allDataRows);
        XLSX.utils.book_append_sheet(newWorkbook, newWorksheet, "Mesclado");
        writeSpreadsheet(newWorkbook, savePath);
        log(`✅ Mesclagem concluída com sucesso! O arquivo foi salvo em: ${savePath}`);
        dialog.showMessageBox(mainWindow, { type: "info", title: "Sucesso", message: `Arquivos mesclados com sucesso!\n\nO resultado foi salvo em:\n${savePath}` });
    } catch (err) {
        log(`❌ Erro catastrófico durante a mesclagem: ${err.message}`);
        console.error(err);
        dialog.showErrorBox("Erro de Mesclagem", `Ocorreu um erro inesperado: ${err.message}`);
    }
});

ipcMain.on("start-adjust-phones", async (event, args) => {
    if (!isAdmin()) {
        event.sender.send("log", "❌ Acesso negado: Permissão de administrador necessária.");
        return;
    }
    const log = (msg) => event.sender.send("log", msg);
    log(`\n--- Iniciando Ajuste de Fones para ${path.basename(args.filePath)} ---`);
    await runPhoneAdjustment(args.filePath, event, args.backup);
    log(`\n✅ Ajuste de fones concluído para o arquivo.`);
});

let apiQueue = { pending: [], processing: null, completed: [] };
let isApiQueueRunning = false;

ipcMain.on("add-files-to-api-queue", (event, filePaths) => {
    if (!isAdmin()) return;
    apiQueue.pending.push(...filePaths);
    apiQueue.pending = [... new Set(apiQueue.pending)];
    event.sender.send("api-queue-update", apiQueue);
});

ipcMain.on("start-api-queue", (event, { keyMode }) => {
    if (!isAdmin()) return;
    if (isApiQueueRunning) return;
    isApiQueueRunning = true;
    processNextInApiQueue(event, keyMode);
});

ipcMain.on("reset-api-queue", (event) => {
    if (!isAdmin()) return;
    apiQueue = { pending: [], processing: null, completed: [] };
    isApiQueueRunning = false;
    event.sender.send("api-queue-update", apiQueue);
    event.sender.send("api-log", "Fila e status reiniciados.");
});

async function processNextInApiQueue(event, keyMode) {
    if (apiQueue.pending.length === 0) {
        event.sender.send("api-log", "\n✅ Fila de processamento concluída.");
        apiQueue.processing = null;
        isApiQueueRunning = false;
        event.sender.send("api-queue-update", apiQueue);
        return;
    }
    apiQueue.processing = apiQueue.pending.shift();
    event.sender.send("api-queue-update", apiQueue);
    event.sender.send("api-log", `--- Iniciando processamento de: ${path.basename(apiQueue.processing)} ---`);
    await runApiConsultation(apiQueue.processing, keyMode, (msg) => event.sender.send("api-log", msg), (current, total) => event.sender.send("api-progress", { current, total }));
    apiQueue.completed.push(apiQueue.processing);
    apiQueue.processing = null;
    event.sender.send("api-queue-update", apiQueue);
    processNextInApiQueue(event, keyMode);
}

async function runApiConsultation(filePath, keyMode, log, progress) {
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
    const BATCH_SIZE = 20000;
    const RETRY_MS = 6 * 60 * 1000;
    const DELAY_SUCESSO_MS = 3 * 60 * 1000;
    const MAX_RETRIES = 3;
    const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));
    const normalizeCnpj = (cnpj) => (String(cnpj).replace(/\D/g, "")).padStart(14, "0");
    try {
        log(`Iniciando processo com o modo de chave: '${keyMode}'.`);
        const workbook = new ExcelJS.Workbook();
        await workbook.xlsx.readFile(filePath);
        const worksheet = workbook.worksheets[0];
        const COLUNA_CNPJ = "cpf";
        let cnpjColNumber = -1;
        worksheet.getRow(1).eachCell({
            includeEmpty: true
        }, (cell, colNumber) => {
            if (cell.value && String(cell.value).trim().toLowerCase() === COLUNA_CNPJ) {
                cnpjColNumber = colNumber;
            }
        });
        if (cnpjColNumber === -1)
            throw new Error(`A coluna "${COLUNA_CNPJ}" não foi encontrada.`);

        const COLUNA_RESPOSTA_LETTER = "C";
        const registros = [];
        worksheet.eachRow({
            includeEmpty: false
        }, (row, rowNum) => {
            if (rowNum > 1) {
                const cnpjCell = row.getCell(cnpjColNumber);
                const cnpjValue = cnpjCell.value;
                const respostaCell = row.getCell(COLUNA_RESPOSTA_LETTER);
                if (!respostaCell.value) {
                    registros.push({
                        cnpj: normalizeCnpj(cnpjValue || ""),
                        rowNum
                    });
                }
            }
        });
        if (registros.length === 0) {
            log("✅ Nenhum registro novo para consultar neste arquivo.");
            return;
        }
        log(`Encontrados ${registros.length} registros novos para processar.`);
        const lotes = [];
        for (let i = 0; i < registros.length; i += BATCH_SIZE) {
            lotes.push(registros.slice(i, i + BATCH_SIZE));
        }
        for (let i = 0; i < lotes.length; i++) {
            const lote = lotes[i];
            log(`\n=== Processando Lote ${i + 1}/${lotes.length} (${lote.length} registros) ===`);
            progress(i + 1, lotes.length);
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
            let sucesso = false;
            let retries = 0;
            while (!sucesso && retries < MAX_RETRIES) {
                try {
                    log("Gerando token de acesso...");
                    const tokenParams = new URLSearchParams({
                        grant_type: "client_credentials",
                        client_id: currentCreds.CLIENT_ID,
                        client_secret: currentCreds.CLIENT_SECRET
                    });
                    const tokenResp = await axios.post(TOKEN_URL, tokenParams.toString(), {
                        headers: {
                            "Content-Type": "application/x-www-form-urlencoded"
                        },
                        timeout: 30000
                    });
                    const token = tokenResp.data.access_token;
                    log("Consultando API...");
                    const cnpjArray = lote.map(r => r.cnpj);
                    const consultaResp = await axios.post(CONSULTA_URL, {
                        CNPJ: cnpjArray
                    }, {
                        headers: {
                            Authorization: `Bearer ${token}`,
                            "Content-Type": "application/json"
                        },
                        timeout: 30000
                    });
                    const key = Object.keys(consultaResp.data).find(k => k.toLowerCase().includes("cnpj") && Array.isArray(consultaResp.data[k]));
                    const encontrados = key ? new Set(consultaResp.data[key].map(normalizeCnpj)) : new Set();
                    log(`Atualizando planilha em memória...`);
                    let countDisponivel = 0;
                    lote.forEach(({ cnpj, rowNum }) => {
                        if (encontrados.has(cnpj)) {
                            worksheet.getCell(`${COLUNA_RESPOSTA_LETTER}${rowNum}`).value = "disponível";
                            countDisponivel++;
                        } else {
                            worksheet.getCell(`${COLUNA_RESPOSTA_LETTER}${rowNum}`).value = "cliente";
                        }
                    });
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
                    log(`❌ Erro no processamento do lote (tentativa ${retries}/${MAX_RETRIES}): ${err.message}.`);
                    if (retries < MAX_RETRIES) {
                        log(`Tentando novamente em ${RETRY_MS / 60000} minutos...`);
                        await sleep(RETRY_MS);
                    } else {
                        log(`Máximo de tentativas atingido para este lote. Pulando para o próximo.`);
                    }
                }
            }
            if (i < lotes.length - 1) {
                log(`Aguardando ${DELAY_SUCESSO_MS / 60000} minutos antes do próximo lote...`);
                await sleep(DELAY_SUCESSO_MS);
            }
        }
        log(`\n🎉 Arquivo ${path.basename(filePath)} processado e salvo.`);
    } catch (error) {
        log(`❌ Erro fatal ao processar o arquivo ${path.basename(filePath)}: ${error.message}`);
        console.error(error);
    }
}