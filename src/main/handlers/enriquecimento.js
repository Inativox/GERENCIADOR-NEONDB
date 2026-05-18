/**
 * Handlers da aba de Enriquecimento: carga de BD, download e enriquecimento de planilhas.
 */
const { ipcMain, dialog } = require('electron');
const path = require('path');
const fs = require('fs');
const ExcelJS = require('exceljs');

const state = require('../state');
const { logSystemAction } = require('../database/connection');

const isAdmin = () => state.currentUser && state.currentUser.role === 'admin';

// #################################################################
// #           FUNÇÕES UTILITÁRIAS                                #
// #################################################################

function formatEta(totalSeconds) {
    if (!isFinite(totalSeconds) || totalSeconds < 0) return "Calculando...";
    const m = Math.floor(totalSeconds / 60);
    const s = Math.floor(totalSeconds % 60);
    return `${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`;
}

async function saveChunkToDb(dataMap, filePath, year, log) {
    if (dataMap.size === 0) return;
    const client = await state.pool.connect();
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
    } catch (error) {
        await client.query('ROLLBACK');
        if (log) log(`❌ ERRO no lote do arquivo ${path.basename(filePath)}: ${error.message}`);
    } finally {
        client.release();
    }
}

async function runEnrichmentProcess({ filesToEnrich, strategy, backup, year, batchSize, usePadrao, useAllDb, fillCpf }, log, progress, onFinish) {
    if (!isAdmin() || !state.pool) {
        log("❌ Acesso negado ou conexão com BD inativa.");
        if (onFinish) onFinish();
        return;
    }

    const BATCH_SIZE = batchSize || 20000;
    const anosDeBusca = [];

    log(`--- Iniciando Processo de Enriquecimento ---`);
    log(`Tamanho do Lote: ${BATCH_SIZE.toLocaleString('pt-BR')} registros.`);
    log(`Modo: Todo Banco de Dados`);
    logSystemAction(state.currentUser.username, 'Enriquecimento', `Iniciou enriquecimento. Estratégia: ${strategy}. Arquivos: ${filesToEnrich.length}`);
    let totalEnrichedRowsOverall = 0, totalProcessedRowsOverall = 0, totalNotFoundInDbOverall = 0;
    try {
        for (const fileObj of filesToEnrich) {
            const { path: filePath, id } = fileObj;
            const startTime = Date.now();
            log(`\nProcessando arquivo: ${path.basename(filePath)}`);
            progress(id, 0, null);
            if (backup) {
                const p = path.parse(filePath);
                fs.copyFileSync(filePath, path.join(p.dir, `${p.name}.backup_enrich_${Date.now()}${p.ext}`));
                log(`Backup criado.`);
            }
            try {
                const workbook = new ExcelJS.Workbook();
                await workbook.xlsx.readFile(filePath);
                const worksheet = workbook.worksheets[0];
                let cnpjCol = -1, statusCol = -1, livre6Col = -1;
                const phoneCols = [];
                worksheet.getRow(1).eachCell((cell, colNum) => {
                    const h = String(cell.value || "").trim().toLowerCase();
                    if (h === "cpf" || h === "cnpj") cnpjCol = colNum;
                    else if (h.startsWith("fone")) phoneCols.push(colNum);
                    else if (h === "status") statusCol = colNum;
                    else if (h === "livre6") livre6Col = colNum;
                });
                phoneCols.sort((a, b) => a - b);
                if (cnpjCol === -1) { log(`❌ ERRO: Coluna 'cpf'/'cnpj' não encontrada. Pulando.`); continue; }
                if (fillCpf && livre6Col === -1) {
                    livre6Col = worksheet.columnCount + 1;
                    worksheet.getCell(1, livre6Col).value = "livre6";
                }
                if (statusCol === -1) {
                    statusCol = worksheet.columnCount + 1;
                    worksheet.getCell(1, statusCol).value = "status";
                }

                const totalRows = worksheet.rowCount - 1;
                let enrichedInFile = 0, notFoundInFile = 0;
                const totalBatches = Math.ceil((worksheet.rowCount - 1) / BATCH_SIZE);
                log(`Arquivo possui ${totalRows} linhas, divididas em ${totalBatches} lote(s).`);

                for (let i = 2; i <= worksheet.rowCount; i += BATCH_SIZE) {
                    const currentBatchNum = Math.floor((i - 2) / BATCH_SIZE) + 1;
                    const cnpjsInBatch = new Map();
                    const endIndex = Math.min(i + BATCH_SIZE - 1, worksheet.rowCount);
                    for (let j = i; j <= endIndex; j++) {
                        const row = worksheet.getRow(j);
                        const cnpj = String(row.getCell(cnpjCol).text || "").replace(/\D/g, "").trim();
                        if (cnpj) cnpjsInBatch.set(cnpj, { rowNum: j, row: row });
                    }
                    if (cnpjsInBatch.size === 0) continue;

                    log(`Lote ${currentBatchNum}/${totalBatches}: Processando ${cnpjsInBatch.size} CNPJs...`);

                    const enrichmentDataForBatch = new Map();
                    const cnpjKeys = Array.from(cnpjsInBatch.keys());
                    if (cnpjKeys.length > 0) {
                        let queryText = `
                            SELECT
                                e.cnpj,
                                (SELECT array_agg(p) FROM socios s, unnest(s.telefones) AS p WHERE s.empresa_id = e.id) AS socio_phones,
                                (SELECT array_agg(s.cpf) FROM socios s WHERE s.empresa_id = e.id) AS socio_cpfs,
                                array_agg(t.numero ORDER BY t.id) AS empresa_phones
                            FROM empresas e
                            LEFT JOIN telefones t ON e.id = t.empresa_id
                            WHERE e.cnpj = ANY($1::text[])
                        `;
                        const queryParams = [cnpjKeys];

                        if (!useAllDb) {
                            queryText += ` AND e.ano = ANY($2::integer[])`;
                            queryParams.push(anosDeBusca);
                        }

                        queryText += ` GROUP BY e.id, e.cnpj;`;

                        const result = await state.pool.query(queryText, queryParams);
                        result.rows.forEach(dbRow => {
                            const socioPhones = dbRow.socio_phones || [];
                            const empresaPhones = (dbRow.empresa_phones || []).filter(Boolean);
                            const combinedPhones = [...new Set([...socioPhones, ...empresaPhones])];
                            const socioCpfs = dbRow.socio_cpfs || [];

                            if (combinedPhones.length > 0 || (fillCpf && socioCpfs.length > 0)) {
                                enrichmentDataForBatch.set(dbRow.cnpj, {
                                    phones: combinedPhones,
                                    cpfs: socioCpfs
                                });
                            }
                        });
                    }

                    log(`Lote ${currentBatchNum}/${totalBatches}: ${enrichmentDataForBatch.size} CNPJs encontrados no BD. Atualizando planilha...`);

                    for (const [cnpj, { row }] of cnpjsInBatch.entries()) {
                        let rowWasEnriched = false;
                        if (enrichmentDataForBatch.has(cnpj)) {
                            const { phones: phonesFromDb, cpfs: cpfsFromDb } = enrichmentDataForBatch.get(cnpj);
                            const existingPhones = phoneCols.map(idx => String(row.getCell(idx).value || '').trim()).filter(Boolean);
                            const shouldProcess = (strategy === "overwrite") || (strategy === "append" && existingPhones.length < phoneCols.length) || (strategy === "ignore" && existingPhones.length === 0);

                            if (shouldProcess && phonesFromDb && phonesFromDb.length > 0) {
                                rowWasEnriched = true;

                                let finalPhones = [];
                                if (strategy === "overwrite") {
                                    finalPhones = [...new Set(phonesFromDb)];
                                } else if (strategy === "append") {
                                    finalPhones = [...new Set([...existingPhones, ...phonesFromDb])];
                                } else {
                                    finalPhones = [...new Set(phonesFromDb)];
                                }

                                phoneCols.forEach(idx => {
                                    row.getCell(idx).value = null;
                                });

                                finalPhones.slice(0, phoneCols.length).forEach((phone, index) => {
                                    const numericPhoneString = String(phone).replace(/\D/g, '');
                                    if (numericPhoneString) {
                                        const cell = row.getCell(phoneCols[index]);
                                        cell.value = Number(numericPhoneString);
                                        cell.numFmt = '0';
                                    }
                                });
                            }

                            if (fillCpf && livre6Col !== -1 && cpfsFromDb && cpfsFromDb.length > 0) {
                                row.getCell(livre6Col).value = cpfsFromDb[0];
                                rowWasEnriched = true;
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

// #################################################################
// #           REGISTRO DE HANDLERS                               #
// #################################################################

function register() {
    ipcMain.handle("get-enriched-cnpj-count", async () => {
        if (!isAdmin() || !state.pool) return 0;
        try {
            const result = await state.pool.query('SELECT COUNT(*) FROM empresas;');
            return parseInt(result.rows[0].count, 10);
        } catch (error) {
            console.error("Erro ao contar CNPJs enriquecidos:", error);
            return 0;
        }
    });

    ipcMain.handle("download-enriched-data", async () => {
        if (!isAdmin() || !state.pool) return { success: false, message: "Acesso negado ou conexão com BD inativa." };
        try {
            const { canceled, filePath } = await dialog.showSaveDialog(state.mainWindow, {
                title: "Salvar Dados Enriquecidos",
                defaultPath: `dados_enriquecidos_${Date.now()}.xlsx`,
                filters: [{ name: "Excel Files", extensions: ["xlsx"] }]
            });
            if (canceled || !filePath) return { success: false, message: "Download cancelado." };

            const query = `
                SELECT e.cnpj, array_agg(t.numero ORDER BY t.id) as telefones
                FROM empresas e
                LEFT JOIN telefones t ON e.id = t.empresa_id
                GROUP BY e.id, e.cnpj
                ORDER BY e.id;
            `;
            const { rows } = await state.pool.query(query);

            if (rows.length === 0) return { success: false, message: "Nenhum dado encontrado." };
            logSystemAction(state.currentUser.username, 'Download Enriquecidos', 'Baixou dados enriquecidos.');

            const maxPhones = rows.reduce((max, row) => Math.max(max, row.telefones ? row.telefones.length : 0), 0);
            const headers = ["cpf", ...Array.from({ length: maxPhones }, (_, i) => `fone${i + 1}`)];

            const data = rows.map(row => {
                const phones = row.telefones || [];
                const processedPhones = Array.from({ length: maxPhones }, (_, i) => {
                    const phone = phones[i];
                    if (!phone) return null;
                    const numericPhone = Number(String(phone).replace(/\D/g, ''));
                    return isNaN(numericPhone) ? phone : numericPhone;
                });
                return [row.cnpj, ...processedPhones];
            });

            const workbook = new ExcelJS.Workbook();
            const worksheet = workbook.addWorksheet("Dados Enriquecidos");
            worksheet.addRow(headers);
            worksheet.addRows(data);

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
        if (!isAdmin() || !state.pool) {
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
        logSystemAction(state.currentUser.username, 'Carga BD', `Iniciou carga de dados. Ano: ${year}. Arquivos: ${masterFiles.length}`);
        let totalCnpjsProcessed = 0;

        try {
            await state.pool.query(`
                CREATE TABLE IF NOT EXISTS socios (
                    id SERIAL PRIMARY KEY,
                    empresa_id INTEGER NOT NULL REFERENCES empresas(id) ON DELETE CASCADE,
                    cpf TEXT NOT NULL,
                    nome TEXT NOT NULL DEFAULT 'Desconhecido',
                    telefones TEXT[] DEFAULT '{}',
                    UNIQUE (empresa_id, cpf)
                );
            `);
            await state.pool.query(`ALTER TABLE socios ADD COLUMN IF NOT EXISTS nome TEXT NOT NULL DEFAULT 'Desconhecido';`);
            await state.pool.query(`ALTER TABLE socios ADD COLUMN IF NOT EXISTS telefones TEXT[] DEFAULT '{}';`);
            await state.pool.query(`DROP TABLE IF EXISTS telefones_socios;`);
        } catch (err) {
            log(`❌ Erro ao inicializar tabelas de sócios: ${err.message}`);
            event.sender.send("db-load-finished");
            return;
        }

        // phoneDataMap: cnpj -> string[]  (telefones diretos da empresa)
        // socioDataMap: cnpj -> { cpf, phones[] }[]
        const saveChunk = async (phoneDataMap, socioDataMap, filePath) => {
            const allCnpjs = new Set([...phoneDataMap.keys(), ...socioDataMap.keys()]);
            if (allCnpjs.size === 0) return;

            const client = await state.pool.connect();
            try {
                await client.query('BEGIN');

                const uniqueCnpjs = Array.from(allCnpjs);
                await client.query(
                    `INSERT INTO empresas (cnpj, ano) SELECT unnest($1::text[]), $2 ON CONFLICT (cnpj) DO UPDATE SET ano = EXCLUDED.ano;`,
                    [uniqueCnpjs, year]
                );
                const { rows: empresaRows } = await client.query(`SELECT id, cnpj FROM empresas WHERE cnpj = ANY($1::text[])`, [uniqueCnpjs]);
                const empresaIdMap = new Map(empresaRows.map(row => [row.cnpj, row.id]));

                // Telefones diretos da empresa (comportamento original)
                const phoneValues = [];
                for (const [cnpj, phones] of phoneDataMap.entries()) {
                    const empresaId = empresaIdMap.get(cnpj);
                    if (!empresaId) continue;
                    const uniquePhones = [...new Set(phones)].filter(p => String(p).replace(/\D/g, '').length >= 8);
                    uniquePhones.forEach(phone => phoneValues.push({ empresa_id: empresaId, numero: String(phone) }));
                }
                if (phoneValues.length > 0) {
                    await client.query(
                        `INSERT INTO telefones (empresa_id, numero) SELECT (d.v->>'empresa_id')::int, d.v->>'numero' FROM jsonb_array_elements($1::jsonb) d(v) ON CONFLICT (empresa_id, numero) DO NOTHING`,
                        [JSON.stringify(phoneValues)]
                    );
                }

                // Sócios com telefones inline
                for (const [cnpj, socios] of socioDataMap.entries()) {
                    const empresaId = empresaIdMap.get(cnpj);
                    if (!empresaId) continue;

                    for (const { cpf, nome, phones } of socios) {
                        if (!cpf) continue;
                        const uniquePhones = [...new Set(phones)].filter(p => String(p).replace(/\D/g, '').length >= 8);
                        await client.query(
                            `INSERT INTO socios (empresa_id, cpf, nome, telefones)
                             VALUES ($1, $2, $3, $4)
                             ON CONFLICT (empresa_id, cpf) DO UPDATE SET nome = EXCLUDED.nome, telefones = EXCLUDED.telefones`,
                            [empresaId, cpf, nome, uniquePhones]
                        );
                    }
                }

                await client.query('COMMIT');
                totalCnpjsProcessed += allCnpjs.size;
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
                    const workbook = new ExcelJS.Workbook();
                    await workbook.xlsx.readFile(filePath);
                    const worksheet = workbook.worksheets[0];
                    if (!worksheet || worksheet.rowCount === 0) { log(`⚠️ Arquivo ${fileName} vazio ou inválido. Pulando.`); continue; }

                    const headerMap = new Map();
                    worksheet.getRow(1).eachCell({ includeEmpty: true }, (cell, colNum) => headerMap.set(colNum, String(cell.value || "").trim().toLowerCase()));

                    let cnpjColIdx = -1;
                    const phoneColIdxs = [];
                    const socioColMap = new Map(); // socioNum -> { cpfColIdx, nomeColIdx, phoneColIdxs[] }

                    for (const [colNum, header] of headerMap.entries()) {
                        if (header === "cpf" || header === "cnpj") {
                            cnpjColIdx = colNum;
                            continue;
                        }
                        const cpfSocioMatch = header.match(/^cpf_socio_(\d+)$/);
                        if (cpfSocioMatch) {
                            const n = parseInt(cpfSocioMatch[1]);
                            if (!socioColMap.has(n)) socioColMap.set(n, { cpfColIdx: -1, nomeColIdx: -1, phoneColIdxs: [] });
                            socioColMap.get(n).cpfColIdx = colNum;
                            continue;
                        }
                        const nomeSocioMatch = header.match(/^nome_socio_(\d+)$/);
                        if (nomeSocioMatch) {
                            const n = parseInt(nomeSocioMatch[1]);
                            if (!socioColMap.has(n)) socioColMap.set(n, { cpfColIdx: -1, nomeColIdx: -1, phoneColIdxs: [] });
                            socioColMap.get(n).nomeColIdx = colNum;
                            continue;
                        }
                        const celularSocioMatch = header.match(/^celular\d+_socio_(\d+)$/);
                        if (celularSocioMatch) {
                            const n = parseInt(celularSocioMatch[1]);
                            if (!socioColMap.has(n)) socioColMap.set(n, { cpfColIdx: -1, nomeColIdx: -1, phoneColIdxs: [] });
                            socioColMap.get(n).phoneColIdxs.push(colNum);
                            continue;
                        }
                        if (/^(fone|telefone|celular)/.test(header)) {
                            phoneColIdxs.push(colNum);
                        }
                    }

                    // Log de diagnóstico: mostra todas as colunas do cabeçalho para identificar nomes exatos
                    const allHeaders = [...headerMap.values()].filter(Boolean);
                    const suspectHeaders = allHeaders.filter(h => h.includes("socio") || h.includes("cpf") || h.includes("celular") || h.includes("fone"));
                    if (suspectHeaders.length > 0) log(`Colunas encontradas no cabeçalho: ${suspectHeaders.join(" | ")}`);

                    if (cnpjColIdx === -1) { log(`❌ ERRO: Coluna CNPJ/CPF não encontrada. Pulando.`); continue; }
                    if (phoneColIdxs.length === 0 && socioColMap.size === 0) { log(`❌ ERRO: Nenhuma coluna de telefone ou sócio encontrada. Pulando.`); continue; }

                    for (const [, data] of socioColMap.entries()) data.phoneColIdxs.sort((a, b) => a - b);

                    log(`Colunas detectadas — CNPJ: col ${cnpjColIdx} | Fones diretos: ${phoneColIdxs.length} | Sócios: ${socioColMap.size}`);

                    let phoneDataMap = new Map();
                    let socioDataMap = new Map();

                    for (let i = 2; i <= worksheet.rowCount; i++) {
                        const row = worksheet.getRow(i);
                        const cnpj = String(row.getCell(cnpjColIdx).value || "").replace(/\D/g, "").trim();
                        if (cnpj.length < 8) continue;

                        if (phoneColIdxs.length > 0) {
                            const phones = phoneColIdxs.map(idx => String(row.getCell(idx).value || "").trim()).filter(Boolean);
                            if (phones.length > 0) phoneDataMap.set(cnpj, [...(phoneDataMap.get(cnpj) || []), ...phones]);
                        }

                        if (socioColMap.size > 0) {
                            const sociosForRow = [];
                            for (const [, { cpfColIdx, nomeColIdx, phoneColIdxs: socioPhoneCols }] of socioColMap.entries()) {
                                if (cpfColIdx === -1) continue;
                                const cpfRaw = String(row.getCell(cpfColIdx).value || "").replace(/\D/g, "").trim();
                                if (cpfRaw.length < 10) continue;
                                const cpf = cpfRaw.padStart(11, '0'); // restaura zero à esquerda perdido pelo Excel
                                const nome = (nomeColIdx !== -1 ? String(row.getCell(nomeColIdx).value || "").trim() : "") || "Desconhecido";
                                const phones = socioPhoneCols.map(idx => String(row.getCell(idx).value || "").trim()).filter(Boolean);
                                sociosForRow.push({ cpf, nome, phones });
                            }
                            if (sociosForRow.length > 0) {
                                socioDataMap.set(cnpj, [...(socioDataMap.get(cnpj) || []), ...sociosForRow]);
                            }
                        }

                        if (i % 5000 === 0) {
                            await saveChunk(phoneDataMap, socioDataMap, filePath);
                            phoneDataMap = new Map();
                            socioDataMap = new Map();
                            progress(fileIndex + 1, masterFiles.length, fileName, totalCnpjsProcessed);
                        }
                    }

                    if (phoneDataMap.size > 0 || socioDataMap.size > 0) {
                        await saveChunk(phoneDataMap, socioDataMap, filePath);
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

    ipcMain.on("start-enrichment", async (event, options) => {
        if (!isAdmin() || !state.pool) {
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

    // Stub: preload.js expõe este canal mas não há implementação no main.js original
    ipcMain.handle("prepare-enrichment-files", async (event, options) => {
        return { success: false, message: "Não implementado." };
    });
}

module.exports = { register };
