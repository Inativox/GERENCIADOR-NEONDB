/**
 * Handlers da aba Limpeza Local:
 * start-cleaning, feed-root-database
 */
const { ipcMain } = require('electron');
const path = require('path');
const fs = require('fs');
const XLSX = require('xlsx');
const ExcelJS = require('exceljs');


const state = require('../state');
const { PROHIBITED_CNAES, queryWithRetry, logSystemAction } = require('../database/connection');
const { readSpreadsheet, writeSpreadsheet } = require('./files');
const { normalizarTelefone, phoneIndices, createCrossListContext, cruzarECompactar, commitCrossList } = require('../limpezaTelefones');



const isAdmin = () => state.currentUser && state.currentUser.role === 'admin';
let cleaningSender = null;

// --- PROCESSAMENTO DE ARQUIVO DE LIMPEZA ---
async function processFile(fileObj, rootSet, options, event, crossList = createCrossListContext()) {
    const file = fileObj.path;
    const id = fileObj.id;
    const logBuffer = [];
    const log = (msg) => logBuffer.push(msg);
    const progress = (pct) => event.sender.send("progress", { id, progress: pct });
    const { backup, checkBlocklist, removeLandlines, checkNumerosInvalidos, fillLivre5, cleaningDate } = options;

    const cleanClientName = (name) => {
        if (!name || typeof name !== 'string') return name;
        return name.replace(/^[\d.\- ]+|[\d.\- ]+$/g, '').trim();
    };

    if (!fs.existsSync(file)) return { processed: false, logs: [`❌ Arquivo não encontrado: ${path.basename(file)}`] };

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
        return { processed: false, logs: logBuffer };
    }

    const header = data[0];
    const cpfColIdx = header.findIndex(h => ["cpf", "cnpj"].includes(String(h).trim().toLowerCase()));
    const nomeColIdx = header.findIndex(h => String(h).trim().toLowerCase() === "nome");
    const cnaeColIdx = header.findIndex(h => ["cnae", "livre3"].includes(String(h).trim().toLowerCase()));
    const livre5ColIdx = header.findIndex(h => String(h).trim().toLowerCase() === "livre5");
    const foneIdxs = phoneIndices(header);

    if (cpfColIdx === -1) {
        log(`❌ ERRO: A coluna "cpf" ou "cnpj" não foi encontrada em ${path.basename(file)}. Pulando este arquivo.`);
        return { processed: false, logs: logBuffer };
    }
    if (nomeColIdx === -1) {
        log(`⚠️ AVISO: Nenhuma coluna "nome" encontrada em ${path.basename(file)}. A limpeza de nomes será ignorada para este arquivo.`);
    }
    if (foneIdxs.length === 0 && checkBlocklist) {
        log(`⚠️ AVISO: A verificação de blocklist está ativa, mas nenhuma coluna 'fone' (fone1 a fone16) foi encontrada.`);
    }
    //verificacao se as colunas fone da planilha estao presentes. adicionado por enzo. 
    if(foneIdxs.length === 0 && checkNumerosInvalidos){
     log(`⚠️ AVISO: A verificação de números inválidos está ativa, mas nenhuma coluna 'fone' (fone1 a fone16) foi encontrada.`);
    }
    if (cnaeColIdx === -1) {
        log(`⚠️ AVISO: Nenhuma coluna "cnae" ou "livre3" encontrada em ${path.basename(file)}. A verificação de CNAE será ignorada para este arquivo.`);
    }
    const livre5Value = `${path.parse(file).name} | ${cleaningDate}`;
    if (fillLivre5 && livre5ColIdx === -1) {
        log(`⚠️ AVISO: ${path.basename(file)} não possui a coluna "livre5". O preenchimento com nome e data foi ignorado.`);
    } else if (fillLivre5) {
        log(`🏷️ A coluna "livre5" de ${path.basename(file)} será preenchida com "${livre5Value}".`);
    }

    const cleaned = [header];
    let removedByRoot = 0;
    let removedByCnae = 0;
    let removedByBlocklist = 0;
    let removedInvalidPhonesNumber = 0; // adicionado por enzo
    let removedDdiCount = 0;
    let cleanedPhones = 0;
    let dirtyPhones = 0;

    for (let i = 1; i < data.length; i++) {
        if (i % 5000 === 0) await new Promise(resolve => setImmediate(resolve));
        const row = data[i];
        const key = row[cpfColIdx] ? String(row[cpfColIdx]).trim().replace(/\D/g, "") : "";

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
            const raw = row[idx];
            const { phone, ddiRemoved } = normalizarTelefone(raw);
            if (raw !== null && raw !== undefined && String(raw).trim() && !phone) dirtyPhones++;
            if (ddiRemoved) removedDdiCount++;
            row[idx] = phone ? Number(phone) : null;
        });
        if (removeLandlines) {
            foneIdxs.forEach(idx => {
                const v = row[idx] ? String(row[idx]).trim() : "";
                if (/^\d{10}$/.test(v)) { row[idx] = null; cleanedPhones++; }
            });
        }

        if (nomeColIdx !== -1 && row[nomeColIdx]) {
            row[nomeColIdx] = cleanClientName(row[nomeColIdx]);
        }

        if (fillLivre5 && livre5ColIdx !== -1) {
            row[livre5ColIdx] = livre5Value;
        }

        cleaned.push(row);
    }

    const saveAdjusted = rows => {
        const result = cruzarECompactar(header, rows, foneIdxs, cpfColIdx, crossList);
        const output = XLSX.utils.book_new();
        XLSX.utils.book_append_sheet(output, XLSX.utils.aoa_to_sheet(result.rows), wb.SheetNames[0]);
        writeSpreadsheet(output, file);
        // Só reserva as chaves depois de a planilha ter sido gravada.
        commitCrossList(crossList, result);
        progress(100);
        if (!foneIdxs.length) log('Sem colunas fone: ajuste e exclusão de linhas sem contato não foram aplicados.');
        log(`✅ ${path.basename(file)}\n   • Pela Raiz: ${removedByRoot} | CNAE: ${removedByCnae} | Blocklist: ${removedByBlocklist}\n   • CNPJs repetidos: ${result.stats.repeatedDocuments} | Telefones repetidos: ${result.stats.repeatedPhones}\n   • Telefones sujos: ${dirtyPhones} | Números inválidos (BD): ${removedInvalidPhonesNumber}\n   • Linhas sem telefone: ${result.stats.withoutPhones} | Fones fixos: ${cleanedPhones} | DDIs removidos: ${removedDdiCount}\n   • Total final: ${result.rows.length - 1}`);
        return { processed: true, logs: logBuffer };
    };

    if ((!checkBlocklist && !checkNumerosInvalidos) || foneIdxs.length === 0) {
        return saveAdjusted(cleaned.slice(1));
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
                if (v) {
                    phonesInBatch.add(v);
                    phonesInBatch.add(`55${v}`);
                }
            });
        });
        const blocked = new Set();
        //condicao nao verifica o checkBlockList se o toogle estiver desativado vai remover os blocklist de qualquer jeito. para corrigir isso devemos colocar (if (checkBlocklist && phonesInBatch.size > 0)) tornado a utilizacao do toggle sendo feito de forma correta. essa correcao foi adicionada por enzo.
        if (checkBlocklist && phonesInBatch.size > 0) {
            const { rows } = await queryWithRetry(
                'SELECT telefone FROM blocklist WHERE telefone = ANY($1::text[])',
                [Array.from(phonesInBatch)],
                3,
                log
            );
            rows.forEach(r => blocked.add(normalizarTelefone(r.telefone).phone));
        }

        //esse bloco de codigo verifica se tem numeros invalidos e numeros no lote faz uma query de leitura da tabela telefones_invalidos
        // e adiciona no set. adicionado por enzo.
        const invalidPhones = new Set()
        if(checkNumerosInvalidos && phonesInBatch.size > 0){
        const {rows} = await queryWithRetry(
        'SELECT telefone from telefones_invalidos WHERE telefone = ANY($1::text[])',
        [Array.from(phonesInBatch)], 3, log
        );
        rows.forEach(r => invalidPhones.add(normalizarTelefone(r.telefone).phone))
        
        }

        //esse bloco de codigo verifica se o numero esta bloqueado entao remove a linha da planilha ja numero invalidos remove apenas a celula. parte de numeros invalidos adicionado por enzo.
        for (const row of batch) {
            const isBlocked = foneIdxs.some(foneIdx => {
                const v = row[foneIdx] ? String(row[foneIdx]).replace(/\D/g, "").trim() : "";
                return v && blocked.has(v);
            });
            
            if (isBlocked) { removedByBlocklist++; continue; }
            //codigo abaixo remove apenas a celula do numero de telefone invalido.
            let hadInvalidPhone =  false;
            foneIdxs.forEach(foneIdx =>{
            const v = row[foneIdx] ? String(row[foneIdx]).replace(/\D/g, "").trim() : "";
            if(v && invalidPhones.has(v)){
            row[foneIdx] = null;
            hadInvalidPhone = true
            }
            });
            if(hadInvalidPhone){
            removedInvalidPhonesNumber++
            }

            finalCleaned.push(row); 
        }
        progress(Math.floor(((i + batch.length) / dataToVerify.length) * 100));
        await new Promise(resolve => setImmediate(resolve));
    }

    return saveAdjusted(finalCleaned.slice(1));
}

function register() {
    ipcMain.on("start-cleaning", async (event, args) => {
        const send = (channel, payload) => {
            if (!event.sender.isDestroyed?.()) event.sender.send(channel, payload);
        };
        const log = message => send('log', message);
        if (state.flowManager?.isBusy()) {
            send('cleaning-finished', { success: false, message: 'Aguarde o fluxo em execução terminar.' });
            log('Aguarde o fluxo em execução terminar antes da limpeza local.');
            return;
        }
        if (!isAdmin()) {
            log('Acesso negado. Permissão de administrador necessária.');
            send('cleaning-finished', { success: false });
            return;
        }
        if (cleaningSender) {
            log('Uma limpeza já está em andamento. Aguarde a conclusão do lote.');
            if (cleaningSender !== event.sender) send('cleaning-finished', { success: false });
            return;
        }
        cleaningSender = event.sender;
        let success = false;
        let processados = 0;
        let pulados = 0;
        try {
            // A exceção depende da sessão autenticada, nunca de dados enviados pela tela.
            args = { ...args, checkBlocklist: state.currentUser.username !== 'Davi' || args?.checkBlocklist === true };
            if (!Array.isArray(args?.cleanFiles) || args.cleanFiles.length === 0 ||
                args.cleanFiles.some(file => typeof file?.path !== 'string' || !file.path)) {
                throw new Error('Selecione ao menos um arquivo válido para limpar.');
            }
            const selectedPaths = new Set();
            for (const file of args.cleanFiles) {
                const resolved = path.resolve(file.path);
                const key = process.platform === 'win32' ? resolved.toLowerCase() : resolved;
                if (selectedPaths.has(key)) throw new Error(`Arquivo repetido no lote: ${path.basename(file.path)}. Selecione cada lista apenas uma vez.`);
                selectedPaths.add(key);
            }
            if ((args.isAutoRoot || args.checkBlocklist || args.checkNumerosInvalidos) && !state.pool) {
                throw new Error('Conecte ao banco para usar a raiz automática ou os filtros de telefone.');
            }
            const cleaningDate = new Intl.DateTimeFormat('pt-BR', {
                timeZone: 'America/Sao_Paulo'
            }).format(new Date());
            const cleaningOptions = { ...args, cleaningDate };
            const crossList = createCrossListContext();
            log('Cruzamento do lote ativo: mantém a primeira ocorrência de CNPJ/telefone. Ajuste de fones obrigatório.');
            logSystemAction(state.currentUser.username, 'Limpeza Local', `Iniciou limpeza de ${args.cleanFiles.length} arquivos.`);
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

            if (args.checkBlocklist) log(`Opção "Verificar Blocklist" está ATIVADA (consulta via BD).`);
            if(args.checkNumerosInvalidos) log(`Opção "Verificar Números Inválidos" está ATIVADA (consulta via BD).`)
            if (args.fillLivre5) log(`Opção "Preencher Livre5" está ATIVADA. Data da limpeza: ${cleaningDate}.`);
            if (args.removeLandlines) log("Opção \"Remover Fones Fixos\" está ATIVADA.");
            log(`FILTRO DE CNAE PROIBIDO: ATIVADO (Padrão).`);

            const fileEvent = { sender: { send } };
            for (let i = 0; i < args.cleanFiles.length; i++) {
                const file = args.cleanFiles[i];
                log(`\nPROCESSANDO ${i + 1}/${args.cleanFiles.length}: ${path.basename(file.path)}`);
                const { processed, logs: fileLogs } = await processFile(file, rootSet, cleaningOptions, fileEvent, crossList);
                fileLogs.forEach(log);
                if (processed) {
                    processados++;
                } else {
                    pulados++;
                }
            }
            success = processados > 0 && pulados === 0;
            log(`\n${success ? '✅ Processo concluído para todos os arquivos.' : 'Limpeza finalizada com avisos.'} Processados: ${processados} | Pulados: ${pulados}.`);
        } catch (err) {
            log(`❌ Erro inesperado no processo de limpeza: ${err.message}`);
            console.error(err);
        } finally {
            cleaningSender = null;
            send('cleaning-finished', { success, processados, pulados });
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

module.exports = { register, processFile, isCleaning: () => Boolean(cleaningSender) };
