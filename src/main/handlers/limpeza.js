/**
 * Handlers da aba Limpeza Local:
 * start-cleaning, start-db-only-cleaning, organize-daily-sheet,
 * start-adjust-phones, start-merge, split-list, feed-root-database,
 * save-stored-cnpjs-to-excel, delete-batch, update-blocklist
 */
const { ipcMain, dialog, shell } = require('electron');
const path = require('path');
const fs = require('fs');
const XLSX = require('xlsx');
const ExcelJS = require('exceljs');
const axios = require('axios');

const state = require('../state');
const { PROHIBITED_CNAES, queryWithRetry, logSystemAction } = require('../database/connection');
const { readSpreadsheet, writeSpreadsheet, letterToIndex } = require('./files');
const cache = require('../database/cache');

const isAdmin = () => state.currentUser && state.currentUser.role === 'admin';

// --- AJUSTE DE FONES ---
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
        logSystemAction(state.currentUser.username, 'Ajuste Fones', `Ajustou fones em: ${path.basename(filePath)}`);
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
                        cell.value = null;
                    }
                } else {
                    cell.value = null;
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

// --- PROCESSAMENTO DE ARQUIVO DE LIMPEZA ---
async function processFile(fileObj, rootSet, options, event, cnpjsHistory) {
    const file = fileObj.path;
    const id = fileObj.id;
    const logBuffer = [];
    const log = (msg) => logBuffer.push(msg);
    const progress = (pct) => event.sender.send("progress", { id, progress: pct });
    const { backup, checkDb, saveToDb, checkBlocklist, removeLandlines } = options;

    const cleanClientName = (name) => {
        if (!name || typeof name !== 'string') return name;
        return name.replace(/^[\d.\- ]+|[\d.\- ]+$/g, '').trim();
    };

    if (!fs.existsSync(file)) return { newCnpjs: new Set(), logs: [`❌ Arquivo não encontrado: ${path.basename(file)}`] };

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
        log(`⚠️ Arquivo vazio ou sem dados: ${path.basename(file)}`);
        return { newCnpjs: new Set(), logs: logBuffer };
    }

    const header = data[0];
    const cpfColIdx = header.findIndex(h => ["cpf", "cnpj"].includes(String(h).trim().toLowerCase()));
    const nomeColIdx = header.findIndex(h => String(h).trim().toLowerCase() === "nome");
    const cnaeColIdx = header.findIndex(h => ["cnae", "livre3"].includes(String(h).trim().toLowerCase()));
    const foneIdxs = header.reduce((acc, cell, i) => {
        if (typeof cell === "string" && /^fone([1-9]|1[0-9])$/.test(cell.trim().toLowerCase())) {
            acc.push(i);
        }
        return acc;
    }, []);

    if (cpfColIdx === -1) {
        log(`❌ ERRO: A coluna "cpf" ou "cnpj" não foi encontrada em ${path.basename(file)}. Pulando este arquivo.`);
        return { newCnpjs: new Set(), logs: logBuffer };
    }
    if (nomeColIdx === -1) {
        log(`⚠️ AVISO: Nenhuma coluna "nome" encontrada em ${path.basename(file)}. A limpeza de nomes será ignorada para este arquivo.`);
    }
    if (foneIdxs.length === 0 && checkBlocklist) {
        log(`⚠️ AVISO: A verificação de blocklist está ativa, mas nenhuma coluna 'fone' (fone1 a fone16) foi encontrada.`);
    }
    if (cnaeColIdx === -1) {
        log(`⚠️ AVISO: Nenhuma coluna "cnae" ou "livre3" encontrada em ${path.basename(file)}. A verificação de CNAE será ignorada para este arquivo.`);
    }

    const cleaned = [header];
    let removedByRoot = 0;
    let removedDuplicates = 0;
    let removedByCnae = 0;
    let removedByBlocklist = 0;
    let removedDdiCount = 0;
    let cleanedPhones = 0;
    const newCnpjsInThisFile = new Set();

    for (let i = 1; i < data.length; i++) {
        const row = data[i];
        const key = row[cpfColIdx] ? String(row[cpfColIdx]).trim().replace(/\D/g, "") : "";
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

        if (!options.isAutoRoot) {
            foneIdxs.forEach(idx => {
                let phoneValue = row[idx] ? String(row[idx]).trim() : "";
                if (phoneValue) {
                    phoneValue = phoneValue.replace(/\D/g, '');
                    if (phoneValue.startsWith("55") && phoneValue.length > 2) {
                        phoneValue = phoneValue.substring(2);
                        removedDdiCount++;
                    }
                    row[idx] = phoneValue ? Number(phoneValue) : null;
                }
            });
        }
        if (removeLandlines) {
            foneIdxs.forEach(idx => {
                const v = row[idx] ? String(row[idx]).trim() : "";
                if (/^\d{10}$/.test(v)) { row[idx] = null; cleanedPhones++; }
            });
        }

        if (nomeColIdx !== -1 && row[nomeColIdx]) {
            row[nomeColIdx] = cleanClientName(row[nomeColIdx]);
        }

        cleaned.push(row);
        if (saveToDb && cnpj && !cnpjsHistory.has(cnpj)) {
            newCnpjsInThisFile.add(cnpj);
        }
    }

    if (!checkBlocklist || foneIdxs.length === 0) {
        const finalWB = XLSX.utils.book_new();
        XLSX.utils.book_append_sheet(finalWB, XLSX.utils.aoa_to_sheet(cleaned), wb.SheetNames[0]);
        writeSpreadsheet(finalWB, file);
        progress(100);
        log(`✅ ${path.basename(file)}\n   • Repetidos (BD): ${removedDuplicates} | Pela Raiz: ${removedByRoot} | CNAE: ${removedByCnae}\n   • DDIs removidos: ${removedDdiCount} | Fones fixos: ${cleanedPhones}\n   • Total final: ${cleaned.length - 1}`);
        return { newCnpjs: newCnpjsInThisFile, logs: logBuffer };
    }

    log(`Verificando blocklist para ${cleaned.length - 1} linhas (lotes de 30k)...`);

    const finalCleaned = [header];
    const dataToVerify = cleaned.slice(1);
    const BATCH_SIZE = 30000;

    for (let i = 0; i < dataToVerify.length; i += BATCH_SIZE) {
        const batch = dataToVerify.slice(i, i + BATCH_SIZE);
        const phonesInBatch = new Set();
        batch.forEach(row => {
            foneIdxs.forEach(foneIdx => {
                const v = row[foneIdx] ? String(row[foneIdx]).replace(/\D/g, "").trim() : "";
                if (v) phonesInBatch.add(v);
            });
        });
        const blocked = new Set();
        if (phonesInBatch.size > 0) {
            const { rows } = await queryWithRetry(
                'SELECT telefone FROM blocklist WHERE telefone = ANY($1::text[])',
                [Array.from(phonesInBatch)],
                3,
                log
            );
            rows.forEach(r => blocked.add(r.telefone));
        }
        for (const row of batch) {
            const isBlocked = foneIdxs.some(foneIdx => {
                const v = row[foneIdx] ? String(row[foneIdx]).replace(/\D/g, "").trim() : "";
                return v && blocked.has(v);
            });
            if (isBlocked) { removedByBlocklist++; } else { finalCleaned.push(row); }
        }
        progress(Math.floor(((i + batch.length) / dataToVerify.length) * 100));
        await new Promise(resolve => setImmediate(resolve));
    }

    const newWB = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(newWB, XLSX.utils.aoa_to_sheet(finalCleaned), wb.SheetNames[0]);
    writeSpreadsheet(newWB, file);
    progress(100);

    log(`✅ ${path.basename(file)}\n   • Repetidos (BD): ${removedDuplicates} | Pela Raiz: ${removedByRoot} | Blocklist: ${removedByBlocklist} | CNAE: ${removedByCnae}\n   • DDIs removidos: ${removedDdiCount} | Fones fixos: ${cleanedPhones}\n   • Total final: ${finalCleaned.length - 1}`);

    return { newCnpjs: newCnpjsInThisFile, logs: logBuffer };
}

// --- MESCLAGEM ---
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
        const { canceled, filePath: savePath } = await dialog.showSaveDialog(state.mainWindow, {
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

            for (let rowNum = 2; rowNum <= inputWorksheet.rowCount; rowNum++) {
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
                    if (cnpj) seenCnpjs.add(cnpj);
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

function register() {
    ipcMain.handle("save-stored-cnpjs-to-excel", async (event) => {
        if (!isAdmin() || !state.pool) { return { success: false, message: "Acesso negado ou conexão com BD inativa." }; }
        const storedCnpjs = cache.getStoredCnpjs();
        if (storedCnpjs.size === 0) { dialog.showMessageBox(state.mainWindow, { type: "info", title: "Aviso", message: "Nenhum CNPJ armazenado para salvar." }); return { success: false, message: "Nenhum CNPJ armazenado para salvar." }; }
        const { canceled, filePath } = await dialog.showSaveDialog(state.mainWindow, { title: "Salvar CNPJs Armazenados", defaultPath: `cnpjs_armazenados_${Date.now()}.xlsx`, filters: [{ name: "Excel Files", extensions: ["xlsx"] }] });
        if (canceled || !filePath) { return { success: false, message: "Operação de salvar cancelada." }; }
        try {
            const data = Array.from(storedCnpjs).map(cnpj => [cnpj]);
            const worksheet = XLSX.utils.aoa_to_sheet([["cpf"], ...data]);
            const workbook = XLSX.utils.book_new();
            XLSX.utils.book_append_sheet(workbook, worksheet, "CNPJs");
            XLSX.writeFile(workbook, filePath);
            dialog.showMessageBox(state.mainWindow, { type: "info", title: "Sucesso", message: `Arquivo salvo com sucesso em: ${filePath}` });
            return { success: true, message: `Arquivo salvo com sucesso em: ${filePath}` };
        } catch (err) {
            console.error("Erro ao salvar Excel:", err);
            dialog.showMessageBox(state.mainWindow, { type: "error", title: "Erro", message: `Erro ao salvar arquivo: ${err.message}` });
            return { success: false, message: `Erro ao salvar arquivo: ${err.message}` };
        }
    });

    ipcMain.handle("delete-batch", async (event, batchId) => {
        if (!isAdmin() || !state.pool) { return { success: false, message: "Acesso negado ou conexão com BD inativa." }; }
        const log = (msg) => event.sender.send("log", msg);
        if (!batchId) { return { success: false, message: "ID do lote inválido." }; }
        log(`Buscando e excluindo documentos do lote "${batchId}" no Neon DB...`);
        try {
            logSystemAction(state.currentUser.username, 'Excluir Lote', `Excluiu lote ${batchId}`);
            const result = await state.pool.query('DELETE FROM limpeza_cnpjs WHERE batch_id = $1 RETURNING cnpj', [batchId]);
            const deletedCount = result.rowCount;
            if (deletedCount === 0) {
                return { success: false, message: `Nenhum CNPJ encontrado para o lote "${batchId}".` };
            }
            const storedCnpjs = cache.getStoredCnpjs();
            result.rows.forEach(row => storedCnpjs.delete(row.cnpj));
            log(`Total de CNPJs no cache local agora: ${storedCnpjs.size}`);
            return { success: true, message: `✅ ${deletedCount} CNPJs do lote "${batchId}" foram excluídos com sucesso!` };
        } catch (err) {
            console.error("Erro ao excluir lote do Neon DB:", err);
            return { success: false, message: `❌ Erro ao excluir lote: ${err.message}` };
        }
    });

    ipcMain.handle("update-blocklist", async (event, backup) => {
        if (!isAdmin()) { return { success: false, message: "Acesso negado." }; }
        const log = (msg) => event.sender.send("log", msg);
        try {
            logSystemAction(state.currentUser.username, 'Update Blocklist', 'Atualizou blocklist local.');
            const blocklistPath = "G:\\Meu Drive\\Marketing\\!Campanhas\\URA - Automatica\\Limpeza de base\\bases para a raiz\\Blocklist.xlsx";
            const rootPath = "G:\\Meu Drive\\Marketing\\!Campanhas\\URA - Automatica\\Limpeza de base\\raiz_att.xlsx";
            if (backup) {
                const timestamp = Date.now();
                const bkp = path.join(path.dirname(rootPath), `${path.basename(rootPath, path.extname(rootPath))}.backup_${timestamp}${path.extname(rootPath)}`);
                fs.copyFileSync(rootPath, bkp);
                log(`Backup da raiz criado em: ${bkp}`);
            }
            const wbBlock = await readSpreadsheet(blocklistPath);
            const dataBlock = XLSX.utils.sheet_to_json(wbBlock.Sheets[wbBlock.SheetNames[0]], { header: 1 }).flat().filter(v => v);
            const wbRoot = await readSpreadsheet(rootPath);
            const dataRoot = XLSX.utils.sheet_to_json(wbRoot.Sheets[wbRoot.SheetNames[0]], { header: 1 }).flat().filter(v => v);
            const merged = Array.from(new Set([...dataRoot, ...dataBlock])).map(v => [v]);
            const newWB = XLSX.utils.book_new();
            XLSX.utils.book_append_sheet(newWB, XLSX.utils.aoa_to_sheet(merged), wbRoot.SheetNames[0]);
            writeSpreadsheet(newWB, rootPath);
            return { success: true, message: "Raiz atualizada com blocklist com sucesso." };
        } catch (err) { return { success: false, message: err.message }; }
    });

    ipcMain.on("start-cleaning", async (event, args) => {
        if (!isAdmin()) { event.sender.send("log", "❌ Acesso negado."); return; }
        const log = (msg) => event.sender.send("log", msg);
        const { isAutoRoot, rootFile, checkDb, saveToDb, checkBlocklist, removeLandlines, autoAdjust } = args;

        if ((isAutoRoot || checkDb || saveToDb || checkBlocklist) && !state.pool) {
            return log("❌ ERRO: Uma ou mais opções de Banco de Dados estão ativadas, mas a conexão com o BD falhou ou não foi configurada.");
        }

        try {
            const batchId = `batch-${Date.now()}`;
            logSystemAction(state.currentUser.username, 'Limpeza Local', `Iniciou limpeza de ${args.cleanFiles.length} arquivos.`);
            if (args.saveToDb) log(`Este lote de salvamento terá o ID: ${batchId}`);
            const rootSet = new Set();
            if (args.isAutoRoot) {
                log("Auto Raiz ATIVADO. Carregando lista raiz do Banco de Dados...");
                const result = await queryWithRetry('SELECT cnpj FROM raiz_cnpjs', [], 3, log);
                result.rows.forEach(row => rootSet.add(row.cnpj));
                log(`✅ Raiz do BD carregada. Total de CNPJs na raiz: ${rootSet.size}.`);
            } else if (args.rootFile) {
                if (!fs.existsSync(args.rootFile)) { return log(`❌ Arquivo raiz não encontrado: ${args.rootFile}`); }
                const wbRoot = await readSpreadsheet(args.rootFile);
                const sheetRoot = wbRoot.Sheets[wbRoot.SheetNames[0]];
                const dataRoot = XLSX.utils.sheet_to_json(sheetRoot, { header: 1 });

                let rootIdx = -1;
                if (dataRoot.length > 0) {
                    const headerRoot = dataRoot[0];
                    rootIdx = headerRoot.findIndex(h => {
                        const val = String(h || '').trim().toLowerCase();
                        return val === 'cpf' || val === 'cnpj';
                    });
                }

                if (rootIdx === -1) {
                    return log(`❌ Arquivo raiz inválido: coluna 'cpf' ou 'cnpj' não encontrada em ${path.basename(args.rootFile)}.`);
                }
                log(`✅ Coluna raiz detectada: "${dataRoot[0][rootIdx]}" (índice ${rootIdx})`);

                const rowsRoot = dataRoot.map(r => r[rootIdx]).filter(v => v).map(v => String(v).trim().replace(/\D/g, "")).filter(v => v);
                rowsRoot.forEach(item => rootSet.add(item));
                log(`Lista raiz do arquivo carregada com ${rootSet.size} valores.`);
            } else {
                log("⚠️ Nenhuma lista raiz (arquivo ou Auto Raiz) foi fornecida. A verificação PROCV será ignorada.");
            }

            const storedCnpjs = cache.getStoredCnpjs();
            log(`Histórico de CNPJs em memória com ${storedCnpjs.size} registros.`);
            if (args.checkDb) log("Opção \"Consultar Banco de Dados\" está ATIVADA.");
            if (args.checkBlocklist) log(`Opção "Verificar Blocklist" está ATIVADA (consulta via BD).`);
            if (args.saveToDb) log("Opção \"Salvar no Banco de Dados\" está ATIVADA.");
            if (args.autoAdjust) log("Opção \"Ajustar Fones Pós-Limpeza\" está ATIVADA.");
            if (args.removeLandlines) log("Opção \"Remover Fones Fixos\" está ATIVADA.");
            log(`FILTRO DE CNAE PROIBIDO: ATIVADO (Padrão).`);

            const allNewCnpjs = new Set();
            const CONCURRENCY = 2;
            for (let i = 0; i < args.cleanFiles.length; i += CONCURRENCY) {
                const chunk = args.cleanFiles.slice(i, i + CONCURRENCY);
                const names = chunk.map(f => path.basename(f.path)).join(' e ');
                log(`\n⏳ PROCESSANDO ${names}... aguarde.`);

                const chunkResults = await Promise.all(
                    chunk.map(fileObj => processFile(fileObj, rootSet, args, event, storedCnpjs))
                );

                for (let j = 0; j < chunkResults.length; j++) {
                    const { newCnpjs, logs: fileLogs } = chunkResults[j];
                    fileLogs.forEach(msg => log(msg));
                    if (args.saveToDb && newCnpjs.size > 0) {
                        newCnpjs.forEach(cnpj => allNewCnpjs.add(cnpj));
                    }
                    if (args.autoAdjust) {
                        await runPhoneAdjustment(chunk[j].path, event, false);
                    }
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
                const result = await queryWithRetry(query, [cnpjsArray, batchId], 3, log);
                event.sender.send("upload-progress", { current: 1, total: 1 });
                const storedCnpjs = cache.getStoredCnpjs();
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

    ipcMain.on("start-db-only-cleaning", async (event, { filesToClean, saveToDb }) => {
        if (!isAdmin() || !state.pool) { event.sender.send("log", "❌ Acesso negado ou conexão com BD inativa."); return; }
        const log = (msg) => event.sender.send("log", msg);
        const batchId = `batch-${Date.now()}`;
        logSystemAction(state.currentUser.username, 'Limpeza BD Only', `Iniciou limpeza de ${filesToClean.length} arquivos.`);
        log(`--- Iniciando Limpeza Apenas pelo Banco de Dados para ${filesToClean.length} arquivo(s) ---`);
        if (saveToDb) log(`Opção \"Salvar no Banco de Dados\" ATIVADA. ID do Lote: ${batchId}`);
        const storedCnpjs = cache.getStoredCnpjs();
        log(`Usando ${storedCnpjs.size} CNPJs do histórico em memória.`);
        const allNewCnpjs = new Set();
        for (const filePath of filesToClean) {
            log(`\nProcessando: ${path.basename(filePath)}`);
            try {
                const wb = await readSpreadsheet(filePath);
                const sheet = wb.Sheets[wb.SheetNames[0]];
                const data = XLSX.utils.sheet_to_json(sheet, { header: 1 });
                if (data.length <= 1) { log(`⚠️ Arquivo vazio ou sem dados: ${filePath}`); continue; }
                const header = data[0];
                const cpfColIdx = header.findIndex(h => ["cpf", "cnpj"].includes(String(h).trim().toLowerCase()));
                if (cpfColIdx === -1) { log(`❌ ERRO: A coluna \"cpf\" ou \"cnpj\" não foi encontrada em ${path.basename(filePath)}. Pulando.`); continue; }
                let removedCount = 0;
                const cleaned = [header];
                for (let i = 1; i < data.length; i++) {
                    const row = data[i];
                    const cnpj = row[cpfColIdx] ? String(row[cpfColIdx]).trim().replace(/\D/g, "") : "";
                    if (cnpj && storedCnpjs.has(cnpj)) { removedCount++; continue; }
                    cleaned.push(row);
                    if (saveToDb && cnpj) { allNewCnpjs.add(cnpj); }
                }
                const newWB = XLSX.utils.book_new();
                XLSX.utils.book_append_sheet(newWB, XLSX.utils.aoa_to_sheet(cleaned), wb.SheetNames[0]);
                writeSpreadsheet(newWB, filePath);
                log(`✅ Arquivo ${path.basename(filePath)} concluído. Removidos: ${removedCount}. Total final: ${cleaned.length - 1}`);
            } catch (err) {
                log(`❌ Erro ao processar ${path.basename(filePath)}: ${err.message}`);
                console.error(err);
            }
        }

        if (saveToDb && allNewCnpjs.size > 0) {
            log(`\nEnviando ${allNewCnpjs.size} novos CNPJs para o banco de dados...`);
            const cnpjsArray = Array.from(allNewCnpjs);
            const query = `INSERT INTO limpeza_cnpjs (cnpj, batch_id) SELECT d.cnpj, $2 FROM unnest($1::text[]) AS d(cnpj) ON CONFLICT (cnpj) DO NOTHING;`;
            const result = await queryWithRetry(query, [cnpjsArray, batchId], 3, log);
            event.sender.send("upload-progress", { current: 1, total: 1 });
            cnpjsArray.forEach(cnpj => storedCnpjs.add(cnpj));
            log(`✅ ${result.rowCount} novos registros adicionados. Total agora: ${storedCnpjs.size}.`);
            log(`✅ ID do Lote salvo: ${batchId}`);
        }
        log("\n--- Limpeza Apenas pelo Banco de Dados finalizada. ---");
    });

    ipcMain.on('organize-daily-sheet', async (event, filePaths, organizationType, options = {}) => {
        const log = (msg) => event.sender.send("log", msg);
        const files = Array.isArray(filePaths) ? filePaths : [filePaths];

        const cleanClientName = (name) => {
            if (!name || typeof name !== 'string') return name;
            return name.replace(/^[\d.\- ]+|[\d.\- ]+$/g, '').trim();
        };

        log(`--- Iniciando Organização (${organizationType}) da Planilha Diária ---`);
        logSystemAction(state.currentUser.username, 'Organizador', `Organizou ${files.length} arquivos. Tipo: ${organizationType}`);

        // --- LÓGICA PARA WHATSAPP ---
        if (organizationType === 'whatsapp') {
            const { removeBlocklist, tagMode, manualTag, scheduleDate, scheduleTime, sector, useApi, filename } = options;

            const processedFiles = [];
            const allCnpjsToConsult = new Set();

            log(`Lendo ${files.length} arquivo(s) para processamento...`);

            for (const filePath of files) {
                try {
                    const workbook = new ExcelJS.Workbook();
                    await workbook.xlsx.readFile(filePath);
                    const worksheet = workbook.worksheets[0];

                    const headerRow = worksheet.getRow(1);
                    const headerMap = {};
                    headerRow.eachCell({ includeEmpty: true }, (cell, colNumber) => {
                        if (cell.value) {
                            const normalizedHeader = String(cell.value).trim().toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g, "");
                            headerMap[normalizedHeader] = colNumber;
                        }
                    });

                    const findColumn = (variations) => {
                        for (const v of variations) {
                            const normalizedV = v.toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g, "");
                            for (const h in headerMap) {
                                if (h.includes(normalizedV)) return headerMap[h];
                            }
                        }
                        return null;
                    };

                    const findAllColumns = (variations) => {
                        const cols = [];
                        for (const h in headerMap) {
                            for (const v of variations) {
                                const normalizedV = v.toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g, "");
                                if (h.includes(normalizedV)) {
                                    cols.push(headerMap[h]);
                                    break;
                                }
                            }
                        }
                        return cols;
                    };

                    const colNome = findColumn(['nome do negocio', 'nome do negócio', 'nome', 'razao social', 'razão social', 'cliente']);
                    const colTel = findColumn(['telefone celular', 'telefone', 'celular', 'fone1', 'fone']);
                    const colEmail = findColumn(['e-mail', 'email', 'mail', 'chave']);
                    const cnpjCols = findAllColumns(['cnpj', 'cpf', 'documento']);

                    if (!colTel) {
                        log("❌ ERRO: Coluna de Telefone não encontrada.");
                        return;
                    }

                    let tagValue = '';
                    if (tagMode === 'filename') {
                        tagValue = path.basename(filePath).replace(/\.[^/.]+$/, '');
                    } else if (tagMode === 'manual') {
                        tagValue = manualTag;
                    } else if (tagMode === 'scheduled') {
                        tagValue = `${scheduleDate} ${sector} - ${scheduleTime}h`;
                    }

                    const outputRows = [];
                    const phonesToCheck = new Set();

                    worksheet.eachRow((row, rowNumber) => {
                        if (rowNumber === 1) return;

                        let rawPhone = row.getCell(colTel).value;
                        if (!rawPhone) return;

                        let phone = String(rawPhone).replace(/\D/g, '');

                        if (phone.length >= 10 && phone.length <= 11) {
                            phone = '55' + phone;
                        } else if (phone.length > 11 && !phone.startsWith('55')) {
                            phone = '55' + phone;
                        }

                        let cnpjVal = '';
                        if (cnpjCols.length > 0) {
                            for (const col of cnpjCols) {
                                const rawCnpj = row.getCell(col).value;
                                const val = (rawCnpj && typeof rawCnpj === 'object') ? (rawCnpj.text || rawCnpj.result || '') : rawCnpj;
                                if (val) { cnpjVal = val; break; }
                            }
                        }

                        if (phone) {
                            const rowData = {
                                Nome: colNome ? cleanClientName(row.getCell(colNome).value) : '',
                                Telefone: phone,
                                CountryCode: '',
                                Tags: tagValue,
                                Email: colEmail ? row.getCell(colEmail).value : '',
                                Cnpj: cnpjVal ? String(cnpjVal).replace(/\D/g, '') : ''
                            };
                            outputRows.push(rowData);
                            if (removeBlocklist) phonesToCheck.add(phone);
                            if (useApi && rowData.Cnpj && rowData.Cnpj.length >= 11) {
                                allCnpjsToConsult.add(rowData.Cnpj);
                            }
                        }
                    });

                    processedFiles.push({ filePath, outputRows, phonesToCheck });
                } catch (err) { log(`❌ Erro ao ler arquivo ${path.basename(filePath)}: ${err.message}`); }
            }

            let availableCnpjs = null;

            if (useApi && allCnpjsToConsult.size > 0) {
                log(`\n--- OTIMIZAÇÃO API ---`);
                log(`Consolidando CNPJs de ${files.length} arquivos... Total único: ${allCnpjsToConsult.size}`);

                const cnpjsArray = Array.from(allCnpjsToConsult);
                availableCnpjs = new Set();

                const credentials = {
                    CLIENT_ID: process.env.IM_CLIENT_ID,
                    CLIENT_SECRET: process.env.IM_CLIENT_SECRET
                };
                const TOKEN_URL = "https://crm-leads-p.c6bank.info/querie-partner/token";
                const CONSULTA_URL = "https://crm-leads-p.c6bank.info/querie-partner/client/avaliable";

                let apiSuccess = false;

                while (!apiSuccess) {
                    try {
                        log("Autenticando na API (Chave 2)...");
                        const tokenParams = new URLSearchParams({ grant_type: "client_credentials", client_id: credentials.CLIENT_ID, client_secret: credentials.CLIENT_SECRET });
                        const tokenResp = await axios.post(TOKEN_URL, tokenParams.toString(), { headers: { "Content-Type": "application/x-www-form-urlencoded" }, timeout: 30000 });
                        const token = tokenResp.data.access_token;

                        log(`Consultando API em lotes...`);
                        const BATCH_API = 20000;
                        for (let i = 0; i < cnpjsArray.length; i += BATCH_API) {
                            const batch = cnpjsArray.slice(i, i + BATCH_API);
                            log(`Enviando lote ${Math.ceil(i/BATCH_API)+1}/${Math.ceil(cnpjsArray.length/BATCH_API)}...`);
                            const consultaResp = await axios.post(CONSULTA_URL, { CNPJ: batch }, { headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" }, timeout: 60000 });
                            const key = Object.keys(consultaResp.data).find(k => k.toLowerCase().includes("cnpj") && Array.isArray(consultaResp.data[k]));
                            if (key && consultaResp.data[key]) {
                                consultaResp.data[key].forEach(c => availableCnpjs.add(String(c).replace(/\D/g, "").padStart(14, "0")));
                            }
                        }
                        apiSuccess = true;
                        log(`Consulta API concluída. ${availableCnpjs.size} CNPJs disponíveis encontrados.`);

                    } catch (err) {
                        log(`❌ Erro na API: ${err.message}`);
                        const response = await dialog.showMessageBox(state.mainWindow, {
                            type: 'error',
                            buttons: ['Pular Etapa API', 'Tentar Novamente (1min)'],
                            defaultId: 1,
                            title: 'Erro na Limpeza API',
                            message: `Erro ao consultar API: ${err.message}\n\nDeseja tentar novamente ou pular a limpeza API (mantendo todos)?`
                        });

                        if (response.response === 0) {
                            log("Usuário pulou a etapa API.");
                            availableCnpjs = null;
                            apiSuccess = true;
                        } else {
                            log("Aguardando 1 minuto...");
                            await new Promise(resolve => setTimeout(resolve, 60000));
                        }
                    }
                }
            } else if (useApi) {
                log("Nenhum CNPJ encontrado nos arquivos para consulta API.");
            }

            for (const pFile of processedFiles) {
                const { filePath, outputRows, phonesToCheck } = pFile;
                const dir = path.dirname(filePath);
                const name = path.parse(filePath).name;

                let finalName = name;
                if (filename && typeof filename === 'string' && filename.trim().length > 0) {
                    if (processedFiles.length > 1) {
                        finalName = `${filename.trim()}_${name}`;
                    } else {
                        finalName = filename.trim();
                    }
                }
                if (!finalName.toLowerCase().endsWith('.xlsx')) finalName += '.xlsx';
                const savePath = path.join(dir, finalName);

                log(`\nFinalizando: ${name}...`);

                const blockedPhones = new Set();
                if (removeBlocklist && phonesToCheck.size > 0 && state.pool) {
                    const query = 'SELECT telefone FROM blocklist WHERE telefone = ANY($1::text[])';
                    const { rows } = await state.pool.query(query, [Array.from(phonesToCheck)]);
                    rows.forEach(r => blockedPhones.add(r.telefone));
                }

                let finalRows = outputRows.filter(r => !blockedPhones.has(r.Telefone));

                if (availableCnpjs !== null) {
                    const beforeCount = finalRows.length;
                    finalRows = finalRows.filter(r => !r.Cnpj || availableCnpjs.has(r.Cnpj.padStart(14, "0")));
                    log(`API: ${beforeCount - finalRows.length} removidos. Blocklist: ${blockedPhones.size} removidos.`);
                }

                const newWb = new ExcelJS.Workbook();
                const newWs = newWb.addWorksheet('Whatsapp');
                newWs.columns = [
                    { header: 'Nome', key: 'Nome', width: 30 },
                    { header: 'Telefone', key: 'Telefone', width: 20 },
                    { header: 'country code', key: 'CountryCode', width: 15 },
                    { header: 'Tags', key: 'Tags', width: 30 },
                    { header: 'E-mail', key: 'Email', width: 30 },
                    { header: 'Cnpj', key: 'Cnpj', width: 20 }
                ];
                newWs.addRows(finalRows);
                await newWb.xlsx.writeFile(savePath);
                log(`✅ Salvo: ${path.basename(savePath)} (${finalRows.length} linhas)`);
            }

            log(`\n🎉 Processo em lote finalizado!`);
            if (processedFiles.length > 0) shell.showItemInFolder(path.dirname(processedFiles[0].filePath));
            return;
        }

        // --- LÓGICA PARA CADÊNCIA ---
        if (organizationType === 'cadencia') {
            const filePath = files[0];
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

                    const newFilePath = path.join(dir, `${originalName}_${sheetName.replace(/[^a-zA-Z0-9]/g, '_')}_organizado.csv`);

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
                        return fallback;
                    };

                    const dataForCsv = [];
                    worksheet.eachRow((row, rowNumber) => {
                        if (rowNumber > 1 && row.values.length > 1) {
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
                                nome = row.getCell('F').value;
                                cpf = row.getCell('B').value;
                                fone1 = row.getCell('L').value;
                                chave = row.getCell('M').value;
                            }

                            if (nome || cpf) {
                                newRowData = {
                                    nome: cleanClientName(nome) || '',
                                    cpf: cpf ? String(cpf).replace(/\D/g, '') : '',
                                    fone1: fone1 ? String(fone1).replace(/\D/g, '') : '',
                                    chave: chave || '',
                                    livre7: 'C6'
                                };
                            }
                            if (newRowData) {
                                dataForCsv.push(newRowData);
                            }
                        }
                    });

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
                        await csvWorkbook.csv.writeFile(newFilePath, { formatterOptions: { delimiter: ';' } });
                        log(`✅ Aba "${sheetName}" concluída. ${dataForCsv.length} linhas salvas em: ${path.basename(newFilePath)}`);
                    } else {
                        log(`⚠️ Nenhum dado processado para a aba "${sheetName}". Arquivo não foi gerado.`);
                    }
                }

                if (processedSheetCount > 0) {
                    log(`\n--- ✅ Processo de separação por abas finalizado com sucesso! ---`);
                    shell.showItemInFolder(dir);
                } else {
                    log(`⚠️ Nenhuma aba encontrada no arquivo.`);
                }

            } catch (error) {
                log(`❌ ERRO GERAL ao separar planilhas por aba: ${error.message}`);
                console.error("Erro detalhado na separação:", error);
            }
            return;
        }

        // --- LÓGICA ANTIGA (bernardo, empresaAqui, olos, relacionamento) ---
        const filePath = files[0];
        const dir = path.dirname(filePath);
        const originalName = path.parse(filePath).name;
        let newFilePath = path.join(dir, `${originalName}_organizado.xlsx`);

        if (organizationType === 'olos') {
            newFilePath = path.join(dir, `reversaprincipal.${originalName}.xlsx`);
        }

        let writer;

        try {
            const writerOptions = { filename: newFilePath, useStyles: true, useSharedStrings: true };
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
                    { header: 'livre1', key: 'livre1', width: 20 },
                    { header: 'chave', key: 'chave', width: 30 },
                    { header: 'livre2', key: 'livre2', width: 20 },
                    { header: 'livre3', key: 'livre3', width: 45 },
                    { header: 'fone1', key: 'fone1', width: 15, style: { numFmt: '0' } }
                ];
            } else if (organizationType === 'olos') {
                newWorksheet.columns = [
                    { header: 'nome', key: 'nome', width: 40 },
                    { header: 'CNPJ', key: 'cpf', width: 20, style: { numFmt: '0' } },
                    { header: 'livre1', key: 'livre1', width: 15 },
                    { header: 'EMAIL', key: 'chave', width: 30 },
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
            }

            const reader = new ExcelJS.stream.xlsx.WorkbookReader(filePath);
            const headerMap = {};
            let processedRows = 0;
            const BATCH_LOG_INTERVAL = 20000;
            let useHeaderMapping = true;

            const fallbackMapping = {
                empresaAqui: { nome: 'B', cnpj: 'A', tel1: 'E', tel2: 'F', email: 'G', cnae: 'H', data: 'L' },
                relacionamento: { cpf: 'B', livre1: 'C', nome: 'E', fone1: 'G', chave: 'H', livre2: 'I', livre3: 'Q' }
            };

            reader.read();

            reader.on('worksheet', worksheet => {
                worksheet.on('row', row => {
                    if (row.number === 1) {
                        row.values.forEach((value, index) => {
                            if (value) headerMap[String(value).trim().toLowerCase()] = index;
                        });

                        let requiredCols;
                        if (organizationType === 'bernardo' || organizationType === 'olos') {
                            requiredCols = ['razao_social', 'cnpj_pk', 'data_inicio_atividade_formatado', 'correiro_eletronico', 'cnae_fiscal_principal', 'telefone_1_formatado', 'telefone_2_formatado'];
                        } else {
                            requiredCols = ['razao', 'cnpj', 'data inicio ativ.', 'e-mail', 'cnae principal', 'telefone 1', 'telefone 2'];
                        }
                        if (organizationType === 'relacionamento') {
                            requiredCols = ['cd_cpf_cnpj_cliente', 'fase', 'nome_cliente', 'telefone_master', 'email', 'vl_cash_in_mtd', 'qual a faixa de faturamento mensal da sua empresa?'];
                        }

                        const allHeadersFound = requiredCols.every(col => {
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
                        } else if (organizationType === 'olos') {
                            const getValue = (colName) => {
                                const colIndex = headerMap[colName.toLowerCase()];
                                return colIndex ? row.getCell(colIndex).value : null;
                            };
                            let yearStr = '';
                            const rawDate = getValue('data_inicio_atividade_formatado');
                            if (rawDate instanceof Date) {
                                yearStr = String(rawDate.getFullYear());
                            } else if (rawDate) {
                                const match = String(rawDate).match(/\d{4}/);
                                if (match) yearStr = match[0];
                                else yearStr = String(rawDate);
                            }
                            newRowData = {
                                nome: cleanClientName(getValue('razao_social')),
                                cpf: getValue('cnpj_pk') ? Number(String(getValue('cnpj_pk')).replace(/\D/g, '')) : null,
                                livre1: yearStr,
                                chave: getValue('correiro_eletronico'),
                                livre3: getValue('cnae_fiscal_principal') ? Number(String(getValue('cnae_fiscal_principal')).replace(/\D/g, '')) : null,
                                livre5: 'OLOS', livre7: 'FLEX',
                                fone1: getValue('telefone_1_formatado') ? Number(String(getValue('telefone_1_formatado')).replace(/\D/g, '')) : null,
                                fone2: getValue('telefone_2_formatado') ? Number(String(getValue('telefone_2_formatado')).replace(/\D/g, '')) : null
                            };
                        } else if (organizationType === 'empresaAqui') {
                            let razao, cnpj, dataInicio, email, cnae, tel1, tel2;
                            if (useHeaderMapping) {
                                const findHeaderIndex = (possibleNames) => {
                                    for (const name of possibleNames) {
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
                            } else {
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
                                livre1: dataInicio, chave: email,
                                livre3: cnae ? Number(String(cnae).replace(/\D/g, '')) : null,
                                livre5: null, livre7: 'C6',
                                fone1: tel1 ? Number(String(tel1).replace(/\D/g, '')) : null,
                                fone2: tel2 ? Number(String(tel2).replace(/\D/g, '')) : null
                            };
                        } else if (organizationType === 'relacionamento') {
                            let cpf, fase, nome, fone1, email, cashIn, faturamento;
                            if (useHeaderMapping) {
                                const findHeaderIndex = (possibleNames) => {
                                    for (const name of possibleNames) {
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
                    try { await writer.commit(); resolve(); } catch (e) { reject(e); }
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
        logSystemAction(state.currentUser.username, 'Mesclagem', `Mesclou ${files.length} arquivos. Estratégia: ${options.strategy}`);
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
            dialog.showMessageBox(state.mainWindow, { type: "info", title: "Sucesso", message: finalMessage });
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
        log(`\n--- Iniciando Ajuste de Fones para ${path.basename(args.filePath)} ---\n`);
        await runPhoneAdjustment(args.filePath, event, args.backup);
        log(`\n✅ Ajuste de fones concluído para o arquivo.\n`);
    });

    ipcMain.on("split-list", async (event, { filePath, linesPerSplit }) => {
        if (!isAdmin()) {
            event.sender.send("log", "❌ Acesso negado: Permissão de administrador necessária.");
            return;
        }
        const log = (msg) => event.sender.send("log", msg);
        log(`\n--- Iniciando Divisão de Lista para ${path.basename(filePath)} ---\n`);
        logSystemAction(state.currentUser.username, 'Divisão Lista', `Dividiu arquivo ${path.basename(filePath)}`);

        try {
            const wb = await readSpreadsheet(filePath);
            const sheet = wb.Sheets[wb.SheetNames[0]];
            const data = XLSX.utils.sheet_to_json(sheet, { header: 1, defval: null });

            if (data.length <= 1) {
                log(`⚠️ Arquivo ${path.basename(filePath)} vazio ou sem dados.\n`);
                return;
            }

            const header = data[0];
            const rows = data.slice(1);
            const totalRows = rows.length;
            const numFiles = Math.ceil(totalRows / linesPerSplit);
            const baseName = path.basename(filePath, path.extname(filePath));
            const outputDir = path.dirname(filePath);

            log(`Total de ${totalRows} linhas de dados. Será dividido em ${numFiles} arquivo(s) com ${linesPerSplit} linhas cada.\n`);

            for (let i = 0; i < numFiles; i++) {
                const chunk = rows.slice(i * linesPerSplit, (i + 1) * linesPerSplit);
                const newWb = XLSX.utils.book_new();
                const newSheet = XLSX.utils.aoa_to_sheet([header, ...chunk]);
                XLSX.utils.book_append_sheet(newWb, newSheet, 'Dados');

                const newFilePath = path.join(outputDir, `${baseName}_parte${i + 1}.xlsx`);
                writeSpreadsheet(newWb, newFilePath);
                log(`✅ Parte ${i + 1} salva em: ${path.basename(newFilePath)}\n`);
            }

            log(`\n--- Divisão de Lista concluída com sucesso! ---\n`);
            event.sender.send("log", `🎉 Arquivos divididos salvos em: ${outputDir}`);
            shell.showItemInFolder(path.join(outputDir, `${baseName}_parte1.xlsx`));

        } catch (error) {
            log(`❌ ERRO ao dividir a lista: ${error.message}\n`);
            console.error("Erro detalhado na divisão:", error);
        }
    });

    ipcMain.on("feed-root-database", async (event, filePaths) => {
        if (!isAdmin() || !state.pool) { event.sender.send("log", "❌ Acesso negado ou conexão com BD inativa."); event.sender.send("root-feed-finished"); return; }
        const log = (msg) => event.sender.send("log", msg);
        log(`--- Iniciando Alimentação da Base Raiz ---`);
        logSystemAction(state.currentUser.username, 'Alimentar Raiz', `Iniciou alimentação com ${filePaths.length} arquivos.`);

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
                const result = await state.pool.query(query, [cnpjChunk, sourceFile, batchId]);
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
                const workbook = new ExcelJS.Workbook();
                await workbook.xlsx.readFile(filePath);
                const worksheet = workbook.worksheets[0];
                if (!worksheet || worksheet.rowCount <= 1) { log(`⚠️ Arquivo ${fileName} está vazio ou não possui dados. Pulando.`); continue; }
                let cnpjColIdx = -1;
                worksheet.getRow(1).eachCell((cell, colNumber) => {
                    const header = String(cell.value || "").trim().toLowerCase();
                    if (header === 'cpf' || header === 'cnpj') cnpjColIdx = colNumber;
                });
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
}

module.exports = { register, runPhoneAdjustment };
