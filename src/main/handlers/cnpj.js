/**
 * Handlers de fila de API (consulta CNPJ C6/IM), locks de chave,
 * modo FISH, agendamento e e-mail de erro.
 */
const { ipcMain, dialog, shell } = require('electron');
const path = require('path');
const fs = require('fs');
const ExcelJS = require('exceljs');
const axios = require('axios');
const nodemailer = require('nodemailer');
const Store = require('electron-store');
const store = new Store();

const state = require('../state');
const { logSystemAction } = require('../database/connection');
const { getApiCredentials } = require('../keyfile');

// #################################################################
// #           ESTADO LOCAL DA FILA DE API                        #
// #################################################################

let apiQueue = { pending: [], processing: null, completed: [], cancelled: [], clientHeader: null, clientRows: [] };
let isApiQueueRunning = false;
let cancelCurrentApiTask = false;
let apiHeartbeatInterval = null;
let currentLockedKeys = [];
let isApiQueuePaused = false;
let fishScheduleTimer = null;
let currentApiOptions = { keyMode: 'chave1', removeClients: true };
let apiTimingSettings = {
    delayBetweenBatches: null,
    retryDelay: null
};
let fishModeFilePath = null;

// Getter/setter para auth.js usar no fechamento da janela
function getCurrentLockedKeys() { return currentLockedKeys; }
function setCurrentLockedKeys(keys) { currentLockedKeys = keys; }

const isAdmin = () => state.currentUser && state.currentUser.role === 'admin';

// #################################################################
// #           FUNÇÕES DE CONTROLE DE LOCKS                       #
// #################################################################

async function acquireApiLock(keysNeeded, username, mode) {
    if (!state.pool) return { success: true };

    try {
        const checkQuery = `
            SELECT key_name, username, status
            FROM api_locks
            WHERE key_name = ANY($1::text[])
            AND status = 'Em uso'
            AND last_heartbeat > NOW() - INTERVAL '2 minutes'
        `;
        const checkResult = await state.pool.query(checkQuery, [keysNeeded]);

        if (checkResult.rows.length > 0) {
            const lock = checkResult.rows[0];
            if (lock.username !== username) {
                return { success: false, lockedBy: lock.username, key: lock.key_name };
            }
        }

        const upsertQuery = `
            INSERT INTO api_locks (key_name, username, status, last_heartbeat, key_label, lock_mode)
            VALUES ($1, $2, 'Em uso', NOW(), $3, $4)
            ON CONFLICT (key_name)
            DO UPDATE SET
                username = EXCLUDED.username,
                status = 'Em uso',
                last_heartbeat = NOW(),
                key_label = EXCLUDED.key_label,
                lock_mode = EXCLUDED.lock_mode;
        `;

        for (const key of keysNeeded) {
            const label = key === 'c6' ? 'Chave 1 (C6)' : 'Chave 2 (IM)';
            await state.pool.query(upsertQuery, [key, username, label, mode]);
        }

        return { success: true };
    } catch (err) {
        console.error("Erro ao adquirir lock de API:", err);
        return { success: true, warning: err.message };
    }
}

async function releaseApiLock(keysToRelease) {
    if (!state.pool || !keysToRelease || keysToRelease.length === 0) return;
    try {
        await state.pool.query("UPDATE api_locks SET status = 'Livre' WHERE key_name = ANY($1::text[])", [keysToRelease]);
    } catch (err) {
        console.error("Erro ao liberar lock de API:", err);
    }
}

async function heartbeatApiLock(keysToMaintain) {
    if (!state.pool || !keysToMaintain || keysToMaintain.length === 0) return;
    try {
        await state.pool.query("UPDATE api_locks SET last_heartbeat = NOW(), status = 'Em uso' WHERE key_name = ANY($1::text[])", [keysToMaintain]);
    } catch (err) {
        console.error("Erro no heartbeat do lock:", err);
    }
}

// #################################################################
// #           E-MAIL DE ERRO                                     #
// #################################################################

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
                    Usuário que agendou: ${state.currentUser ? state.currentUser.username : 'desconhecido'}
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

// #################################################################
// #           SAVE COLLECTED CLIENTS                             #
// #################################################################

async function saveCollectedClients(event) {
    const log = (msg) => event.sender.send("api-log", msg);

    if (currentApiOptions.isFishMode || !currentApiOptions.extractClients || apiQueue.clientRows.length === 0) {
        return;
    }

    log(`\n--- Iniciando salvamento do arquivo consolidado de clientes (${apiQueue.clientRows.length} registros) ---`);

    try {
        const { canceled, filePath } = await dialog.showSaveDialog(state.mainWindow, {
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

// #################################################################
// #           CONSULTA API C6                                    #
// #################################################################

async function runApiConsultation(filePath, options, log, progress, fishPath) {
    const { keyMode, removeClients, isFishMode, extractClients, connectors, splitMode } = options;
    const resolvedConnectors = connectors && connectors.length > 0 ? connectors : (options.connector ? [options.connector] : ['RESGATE']);
    let fishClientIndex = 0;
    const _kf = getApiCredentials();
    if (!_kf) {
        log('❌ Licença de API não carregada. Importe o arquivo .mbkey na tela de login.');
        return;
    }
    const credentials = {
        c6: {
            CLIENT_ID: _kf.c6.clientId,
            CLIENT_SECRET: _kf.c6.clientSecret,
            name: "Chave 1 (Padrão)"
        },
        im: {
            CLIENT_ID: _kf.im.clientId,
            CLIENT_SECRET: _kf.im.clientSecret,
            name: "Chave 2 (Alternativa)"
        }
    };
    const TOKEN_URL = "https://crm-leads-p.c6bank.info/querie-partner/token";
    const CONSULTA_URL = "https://crm-leads-p.c6bank.info/querie-partner/client/avaliable";

    const BATCH_SIZE_SINGLE = 20000;
    const BATCH_SIZE_DUAL = 40000;

    const getRetryDelayMs = () => (parseFloat(apiTimingSettings.retryDelay) || 2) * 60 * 1000;
    const getSuccessDelayMs = () => (parseFloat(apiTimingSettings.delayBetweenBatches) || 2) * 60 * 1000;
    const MAX_RETRIES = 5;

    const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));
    const normalizeCnpj = (cnpj) => (String(cnpj).replace(/\D/g, "")).padStart(14, "0");

    const sendToN8NWebhook = async (header, rowData, connector) => {
        const N8N_WEBHOOK_URL = 'https://n8n.upscales.com.br/webhook/2ccead38-deb8-48d0-9f44-0edccafcc026';
        if (!rowData) return;

        const headerMap = {};
        header.forEach((h, index) => {
            if (h) headerMap[String(h).toLowerCase()] = index;
        });

        const params = {};
        params.nome = rowData[headerMap['nome']] || '';
        params.cpf = rowData[headerMap['cpf']] || rowData[headerMap['cnpj']] || '';
        params.chave = rowData[headerMap['chave']] || '';
        if (connector) params.conector = connector;

        for (const key in headerMap) {
            if (key.startsWith('fone')) {
                const phoneValue = rowData[headerMap[key]];
                if (phoneValue) {
                    params[key] = phoneValue;
                }
            }
        }

        const filteredParams = Object.fromEntries(
            Object.entries(params).filter(([_, v]) => v !== null && v !== '' && v !== undefined)
        );

        const queryString = new URLSearchParams(filteredParams).toString();
        const finalUrl = `${N8N_WEBHOOK_URL}?${queryString}`;

        try {
            await axios.get(finalUrl, { timeout: 15000 });
            log(`🐟 FISH: Cliente ${params.cpf || 'sem CPF'} enviado [conector: ${connector || 'N/A'}].`);
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

        let cnpjColNumber = -1;
        let fileHeader = worksheet.getRow(1).values;

        const foneColIndices = [];
        fileHeader.forEach((h, idx) => {
            if (h && String(h).toLowerCase().startsWith('fone')) foneColIndices.push(idx);
        });

        worksheet.getRow(1).eachCell({ includeEmpty: true }, (cell, colNumber) => {
            const val = cell.value ? String(cell.value).trim().toLowerCase() : "";
            if (val === "cpf" || val === "cnpj") cnpjColNumber = colNumber;
        });

        if (cnpjColNumber === -1) throw new Error(`A coluna "cpf" ou "cnpj" não foi encontrada.`);

        let statusColNumber = -1;
        worksheet.getRow(1).eachCell({ includeEmpty: true }, (cell, colNumber) => {
            if (cell.value && String(cell.value).trim().toUpperCase() === "STATUS_API_TEMP") {
                statusColNumber = colNumber;
            }
        });
        if (statusColNumber === -1) {
            statusColNumber = worksheet.columnCount + 1;
            worksheet.getCell(1, statusColNumber).value = "STATUS_API_TEMP";
        }

        const registros = [];
        worksheet.eachRow({ includeEmpty: false }, (row, rowNum) => {
            if (rowNum > 1) {
                const cnpjCell = row.getCell(cnpjColNumber);
                const respostaCell = row.getCell(statusColNumber);
                if (!respostaCell.value && cnpjCell.value) {
                    registros.push({ cnpj: normalizeCnpj(cnpjCell.value), rowNum });
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
                return { status: 'cancelled', clientData: { header: null, rows: [] } };
            }
            if (isApiQueuePaused) {
                log("Processamento PAUSADO. Aguardando para retomar...");
                while (isApiQueuePaused) {
                    await sleep(1000);
                    if (cancelCurrentApiTask) break;
                }
                if (cancelCurrentApiTask) {
                    log("Processamento do arquivo cancelado enquanto pausado.");
                    return { status: 'cancelled', clientData: { header: null, rows: [] } };
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

                    const currentKeyMode = options.keyMode;
                    if (currentKeyMode === 'dupla') {
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

                        res1.value.forEach(cnpj => encontrados.add(cnpj));
                        res2.value.forEach(cnpj => encontrados.add(cnpj));

                    } else {
                        let currentCreds;
                        if (currentKeyMode === "intercalar") {
                            currentCreds = i % 2 === 0 ? credentials.c6 : credentials.im;
                            log(`Usando credenciais intercaladas: ${currentCreds.name}`);
                        } else if (currentKeyMode === "chave2") {
                            currentCreds = credentials.im;
                        } else {
                            currentCreds = credentials.c6;
                        }
                        if (currentKeyMode !== "intercalar" && i === 0) {
                            log(`Usando credenciais fixas: ${currentCreds.name}`);
                        }
                        encontrados = await performApiCall(lote.map(r => r.cnpj), currentCreds);
                    }

                    log(`Atualizando planilha em memória...`);
                    let countDisponivel = 0;

                    for (const { cnpj, rowNum } of lote) {
                        const row = worksheet.getRow(rowNum);
                        if (encontrados.has(cnpj)) {
                            row.getCell(statusColNumber).value = "disponível";
                            countDisponivel++;
                        } else {
                            row.getCell(statusColNumber).value = "cliente";
                            if (isFishMode) {
                                const hasFone = foneColIndices.some(idx => {
                                    const v = row.values[idx];
                                    return v !== null && v !== '' && v !== undefined;
                                });
                                if (hasFone) {
                                    const activeConnector = splitMode && resolvedConnectors.length >= 2
                                        ? resolvedConnectors[fishClientIndex++ % resolvedConnectors.length]
                                        : resolvedConnectors[0];
                                    await sendToN8NWebhook(fileHeader, row.values, activeConnector);
                                }
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
            if (sucesso) {
                if (i < lotes.length - 1) {
                    if (cancelCurrentApiTask) break;
                    log(`Aguardando ${successDelayMs / 60000} minutos antes do próximo lote...`);
                    await sleep(successDelayMs);
                }
            }
        }
        if (!cancelCurrentApiTask) {
            let collectedClients = { header: null, rows: [] };
            if (extractClients && !isFishMode) {
                log(`\nExtraindo dados de 'cliente' do arquivo...`);
                collectedClients.header = worksheet.getRow(1).values;
                worksheet.eachRow((row, rowNum) => {
                    if (rowNum > 1) {
                        const status = row.getCell(statusColNumber).value;
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

                newWorksheet.getRow(1).values = finalWorksheet.getRow(1).values;

                let keptRows = 0;
                finalWorksheet.eachRow((row, rowNum) => {
                    if (rowNum > 1) {
                        const status = row.getCell(statusColNumber).value;
                        if (status === 'disponível') {
                            newWorksheet.addRow(row.values);
                            keptRows++;
                        }
                    }
                });

                log('Removendo coluna de controle temporária...');
                newWorksheet.spliceColumns(statusColNumber, 1);

                await newWorkbook.xlsx.writeFile(filePath);
                log(`✅ Limpeza final concluída. ${keptRows} registros 'disponível' foram mantidos no arquivo.`);
            } else {
                log(`\n✅ Processamento da API concluído para ${path.basename(filePath)}. O arquivo foi salvo com todos os resultados (disponível/cliente).`);
            }
            return { success: true, status: 'completed', clientData: collectedClients };
        }
        return { status: 'cancelled', clientData: { header: null, rows: [] } };
    } catch (error) {
        log(`❌ Erro fatal ao processar o arquivo ${path.basename(filePath)}: ${error.message}`);
        console.error(error);
        return { success: false, clientData: { header: null, rows: [] } };
    }
}

// #################################################################
// #           PROCESSAMENTO DA FILA                              #
// #################################################################

async function processNextInApiQueue(event) {
    if (!isApiQueueRunning) {
        apiQueue.processing = null;
        event.sender.send("api-queue-update", { ...apiQueue, isPaused: isApiQueuePaused });
        event.sender.send("api-log", "\nFila de processamento interrompida.");
        return;
    }

    if (isApiQueuePaused) {
        event.sender.send("api-log", "Fila PAUSADA. Aguardando para retomar...");
        return;
    }

    if (apiQueue.pending.length === 0) {
        event.sender.send("api-log", "\n✅ Fila de processamento concluída.");
        apiQueue.processing = null;
        isApiQueueRunning = false;

        if (apiHeartbeatInterval) clearInterval(apiHeartbeatInterval);
        releaseApiLock(currentLockedKeys);
        currentLockedKeys = [];

        if (currentApiOptions.extractClients && !currentApiOptions.isFishMode) {
            await saveCollectedClients(event);
        }

        event.sender.send("api-queue-update", { ...apiQueue, isPaused: isApiQueuePaused });
        return;
    }

    apiQueue.processing = apiQueue.pending.shift();
    event.sender.send("api-queue-update", { ...apiQueue, isPaused: isApiQueuePaused });
    event.sender.send("api-log", `--- Iniciando processamento de: ${path.basename(apiQueue.processing)} ---`);

    const result = await runApiConsultation(
        apiQueue.processing,
        currentApiOptions,
        (msg) => event.sender.send("api-log", msg),
        (current, total) => event.sender.send("api-progress", { current, total }),
        fishModeFilePath
    );

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
        cancelCurrentApiTask = false;
    } else {
        if (result && result.success) {
            apiQueue.completed.push(apiQueue.processing);
        } else {
            apiQueue.cancelled.push(apiQueue.processing);
        }
    }

    apiQueue.processing = null;
    event.sender.send("api-queue-update", { ...apiQueue, isPaused: isApiQueuePaused });

    if (!isApiQueuePaused) {
        processNextInApiQueue(event);
    }
}

// #################################################################
// #           AGENDAMENTO FISH                                   #
// #################################################################

async function runScheduledFishCleanup(schedule) {
    console.log(`[AGENDADOR] Iniciando execução agendada: ${new Date().toLocaleString()}`);
    const log = (msg) => {
        if (state.mainWindow && state.mainWindow.webContents) {
            state.mainWindow.webContents.send("api-log", `[AGENDADO] ${msg}`);
        }
        console.log(`[AGENDADO] ${msg}`);
    };

    try {
        apiQueue.pending.push(...schedule.files);
        apiQueue.pending = [...new Set(apiQueue.pending)];
        if (state.mainWindow) state.mainWindow.webContents.send("api-queue-update", { ...apiQueue, isPaused: isApiQueuePaused });

        isApiQueueRunning = true;
        isApiQueuePaused = false;
        currentApiOptions = schedule.apiOptions;
        cancelCurrentApiTask = false;

        log(`Iniciando processamento de ${schedule.files.length} arquivo(s) agendados.`);
        await processNextInApiQueue({ sender: state.mainWindow.webContents });

        log("✅ Execução agendada concluída com sucesso.");

    } catch (error) {
        log(`❌ ERRO CRÍTICO na execução agendada: ${error.message}`);
        console.error("[AGENDADOR] Erro:", error);
        await sendErrorEmail("Falha na Execução Agendada FISH", `Erro: ${error.message}\n\nStack: ${error.stack}`);
    } finally {
        store.delete('fish-schedule');
        if (state.mainWindow) state.mainWindow.webContents.send('fish-schedule-update', null);
    }
}

// #################################################################
// #           REGISTRO DE HANDLERS                               #
// #################################################################

function register() {
    ipcMain.on('set-api-delays', (event, settings) => {
        apiTimingSettings = settings;
        event.sender.send("api-log", `⚙️ Configurações de tempo atualizadas: Delay entre Lotes: ${settings.delayBetweenBatches || 'Padrão'}, Delay de Retentativa: ${settings.retryDelay || 'Padrão'}`);
    });

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
            event.sender.send("api-log", "\n⏸️ Fila de processamento PAUSADA. O processamento será retomado do ponto atual.");
            event.sender.send("api-queue-update", { ...apiQueue, isPaused: isApiQueuePaused });
        }
    });

    ipcMain.on("resume-api-queue", (event) => {
        if (!isAdmin()) return;
        if (isApiQueueRunning && isApiQueuePaused) {
            isApiQueuePaused = false;
            event.sender.send("api-log", "\n▶️ Fila de processamento RETOMADA.");
            event.sender.send("api-queue-update", { ...apiQueue, isPaused: isApiQueuePaused });
            if (!apiQueue.processing) {
                processNextInApiQueue(event);
            }
        }
    });

    ipcMain.on("set-api-key-mode", (event, keyMode) => {
        if (!isAdmin()) return;
        currentApiOptions.keyMode = keyMode;
        event.sender.send("api-log", `⚙️ Modo de chave alterado para: ${keyMode} (aplicado no próximo lote)`);
    });

    ipcMain.on("start-api-queue", async (event, options) => {
        if (!isAdmin()) return;
        if (isApiQueueRunning) return;
        currentApiOptions = options;
        isApiQueueRunning = true;
        isApiQueuePaused = false;
        logSystemAction(state.currentUser.username, 'Limpeza API', `Iniciou fila API. Modo: ${options.keyMode}. Fish: ${options.isFishMode}`);
        apiQueue.clientHeader = null;
        apiQueue.clientRows = [];
        fishModeFilePath = null;

        let keysNeeded = [];
        if (options.keyMode === 'chave1') keysNeeded = ['c6'];
        else if (options.keyMode === 'chave2') keysNeeded = ['im'];
        else if (options.keyMode === 'dupla' || options.keyMode === 'intercalar') keysNeeded = ['c6', 'im'];

        const lockResult = await acquireApiLock(keysNeeded, state.currentUser.username, options.keyMode);

        if (!lockResult.success) {
            isApiQueueRunning = false;
            event.sender.send("api-queue-update", { ...apiQueue, isPaused: isApiQueuePaused });
            const msg = `⚠️ A chave '${lockResult.key}' está com status "Em uso" pelo usuário: ${lockResult.lockedBy}.`;
            event.sender.send("api-log", msg);
            event.sender.send("api-lock-error", msg);
            return;
        }

        currentLockedKeys = keysNeeded;

        if (options.isFishMode) {
            const splitInfo = options.splitMode && options.connectors?.length >= 2
                ? ` | Divisão ativa entre: ${options.connectors.join(' e ')}`
                : ` | Conector: ${(options.connectors?.[0] || options.connector || 'RESGATE')}`;
            event.sender.send("api-log", `🐟 Modo FISH ativado. Clientes serão enviados para o webhook N8N.${splitInfo}`);
        }

        cancelCurrentApiTask = false;
        event.sender.send("api-queue-update", { ...apiQueue, isPaused: isApiQueuePaused });

        if (apiHeartbeatInterval) clearInterval(apiHeartbeatInterval);
        apiHeartbeatInterval = setInterval(() => {
            heartbeatApiLock(currentLockedKeys);
        }, 120000);

        processNextInApiQueue(event);
    });

    ipcMain.on('schedule-fish-cleanup', (event, scheduleOptions) => {
        if (fishScheduleTimer) clearTimeout(fishScheduleTimer);

        store.set('fish-schedule', scheduleOptions);

        const formattedDate = new Date(scheduleOptions.startTime).toLocaleString('pt-BR');
        logSystemAction(state.currentUser.username, 'Agendamento API', `Agendou limpeza para ${formattedDate}. Arquivos: ${scheduleOptions.files.length}`);

        event.sender.send('api-log', `✅ Agendamento FISH confirmado para ${formattedDate}.`);
        state.mainWindow.webContents.send('fish-schedule-update', scheduleOptions);

        const delay = new Date(scheduleOptions.startTime).getTime() - Date.now();

        if (delay > 0) {
            fishScheduleTimer = setTimeout(() => runScheduledFishCleanup(scheduleOptions), delay);
        }
    });

    ipcMain.on('cancel-fish-schedule', (event) => {
        if (fishScheduleTimer) clearTimeout(fishScheduleTimer);
        store.delete('fish-schedule');

        logSystemAction(state.currentUser.username, 'Agendamento API', 'Cancelou agendamento.');

        event.sender.send('api-log', `❌ Agendamento cancelado pelo usuário.`);
        state.mainWindow.webContents.send('fish-schedule-update', null);
    });

    ipcMain.on("reset-api-queue", (event) => {
        if (!isAdmin()) return;
        isApiQueueRunning = false;
        cancelCurrentApiTask = true;
        apiQueue = { pending: [], processing: null, completed: [], cancelled: [], clientHeader: null, clientRows: [] };
        isApiQueuePaused = false;
        fishModeFilePath = null;

        if (apiHeartbeatInterval) clearInterval(apiHeartbeatInterval);
        releaseApiLock(currentLockedKeys);
        currentLockedKeys = [];

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
}

module.exports = { register, releaseApiLock, getCurrentLockedKeys, setCurrentLockedKeys };
