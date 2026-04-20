/**
 * Handlers da aba de Relacionamento: pipeline de elegíveis e divisão por responsável.
 */
const { ipcMain, dialog, shell } = require('electron');
const path = require('path');
const fs = require('fs');
const fsp = require('fs').promises;
const XLSX = require('xlsx');
const ExcelJS = require('exceljs');

const state = require('../state');
const { logSystemAction } = require('../database/connection');

const isAdmin = () => state.currentUser && state.currentUser.role === 'admin';

// #################################################################
// #           CONSTANTES E UTILITÁRIOS                           #
// #################################################################

const NOME_PLANILHA_PRINCIPAL = 'Sheet1';
const NOME_PLANILHA_RELACIONAMENTO = 'C6 - Relacionamento';
const NOME_PLANILHA_SUPERVISORES = 'supervisores';

const norm = v => (v === null || v === undefined) ? '' : String(v).trim();
const normKey = v => norm(v).toUpperCase();
const normCnpjKey = v => norm(v).replace(/\D/g, '');

const excelDateToJSDate = (serial) => {
    if (typeof serial !== 'number' || isNaN(serial)) return null;
    const utc_days = Math.floor(serial - 25569);
    const utc_value = utc_days * 86400;
    return new Date(utc_value * 1000);
};

// #################################################################
// #           PIPELINE PRINCIPAL                                 #
// #################################################################

async function runFullPipeline(filePaths, modo, event) {
    const log = (msg) => {
        console.log(`[Relacionamento] ${msg}`);
        if (event && event.sender) {
            event.sender.send("relacionamento-log", msg);
        }
    };

    try {
        log('Iniciando pipeline completo...');
        logSystemAction(state.currentUser.username, 'Relacionamento', `Iniciou pipeline relacionamento. Modo: ${modo}`);

        log(`Lendo arquivo de relatório: ${path.basename(filePaths.relatorio)}`);
        const relWb = XLSX.read(await fsp.readFile(filePaths.relatorio));
        const relFirstSheet = relWb.Sheets[relWb.SheetNames[0]];
        const relDataAoA = XLSX.utils.sheet_to_json(relFirstSheet, { header: 1, defval: null });

        if (relDataAoA.length === 0) {
            log('Relatório vazio. Abortando.');
            return { success: false };
        }

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

        const elegiveisWb = XLSX.utils.book_new();
        const elegiveisWs = XLSX.utils.aoa_to_sheet(dataComNovasColunas, { cellDates: true });
        XLSX.utils.book_append_sheet(elegiveisWb, elegiveisWs, NOME_PLANILHA_PRINCIPAL);
        log('Planilha principal criada em memória.');

        log(`Lendo arquivo Bitrix: ${path.basename(filePaths.bitrix)}`);
        const bitrixWb = XLSX.read(await fsp.readFile(filePaths.bitrix));
        const bitrixWs = bitrixWb.Sheets[bitrixWb.SheetNames[0]];
        const bitrixDataAoA = XLSX.utils.sheet_to_json(bitrixWs, { header: 1, defval: null });

        const idxB = XLSX.utils.decode_col('B');
        const idxE = XLSX.utils.decode_col('E');
        const idxH = XLSX.utils.decode_col('H');

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
        const relacionamentoWs = XLSX.utils.aoa_to_sheet(bitrixRows);
        XLSX.utils.book_append_sheet(elegiveisWb, relacionamentoWs, NOME_PLANILHA_RELACIONAMENTO);
        log(`Planilha "${NOME_PLANILHA_RELACIONAMENTO}" adicionada.`);

        log(`Lendo arquivo de time: ${path.basename(filePaths.time)}`);
        const timeWb = XLSX.read(await fsp.readFile(filePaths.time));
        const timeWs = timeWb.Sheets[timeWb.SheetNames[0]];
        const timeDataJson = XLSX.utils.sheet_to_json(timeWs, { defval: '' });

        const timeHeaders = Object.keys(timeDataJson[0] || {});
        const hConsultor = timeHeaders.find(h => h && h.toUpperCase().includes('CONSULTOR')) || timeHeaders[0];
        const hEquipe = timeHeaders.find(h => h && h.toUpperCase().includes('EQUIPE')) || timeHeaders[1] || timeHeaders[0];

        const supervisoresRows = [['Consultor', 'Equipe']];
        for (const row of timeDataJson) {
            supervisoresRows.push([row[hConsultor] || '', row[hEquipe] || '']);
        }
        const supervisoresWs = XLSX.utils.aoa_to_sheet(supervisoresRows);
        XLSX.utils.book_append_sheet(elegiveisWb, supervisoresWs, NOME_PLANILHA_SUPERVISORES);
        log(`Planilha "${NOME_PLANILHA_SUPERVISORES}" adicionada.`);

        const mapFaturamento = {};
        if (filePaths.contatos && fs.existsSync(filePaths.contatos)) {
            log(`Lendo arquivo Contatos Bitrix: ${path.basename(filePaths.contatos)}`);
            const contatosWb = XLSX.read(await fsp.readFile(filePaths.contatos));
            const contatosWs = contatosWb.Sheets[contatosWb.SheetNames[0]];
            const contatosDataJson = XLSX.utils.sheet_to_json(contatosWs, { defval: '' });

            if (contatosDataJson.length > 0) {
                const contatosHeaders = Object.keys(contatosDataJson[0] || {});
                const hCnpjContatos = contatosHeaders.find(h => h && h.toUpperCase().includes('CNPJ')) || contatosHeaders[0];
                const hFaturamento = contatosHeaders[1];

                for (const row of contatosDataJson) {
                    const cnpjKey = normCnpjKey(row[hCnpjContatos]);
                    if (cnpjKey) mapFaturamento[cnpjKey] = row[hFaturamento];
                }
                log('Mapa de faturamento criado.');
            } else {
                log('⚠️ Arquivo de Contatos está vazio. O faturamento não será preenchido.');
            }
        } else {
            log('⚠️ Arquivo de Contatos não fornecido. O faturamento não será preenchido.');
        }

        log('Convertendo planilha principal para JSON para aplicar filtros...');
        const elegiveisWsJson = XLSX.utils.sheet_to_json(elegiveisWs, { defval: null });
        if (elegiveisWsJson.length === 0) {
            log('Aviso: a planilha principal gerada está vazia. Abortando.');
            return { success: false };
        }

        const findHeader = (headers, primaryName, fallbackColumnLetter) => {
            let header = headers.find(h => h && h.trim().toUpperCase() === primaryName.toUpperCase());
            if (header) return header;
            const colIndex = XLSX.utils.decode_col(fallbackColumnLetter);
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
            log('Aplicando filtros para o modo "Relacionamento"...');
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

        } else {
            log('Aplicando filtros para o modo "Máquina"...');
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
            return { success: true };
        }

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
                const keyFaixaFaturamento = findKey(row, 'qual a faixa de faturamento mensal da sau empresa');
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
                    'livre1': row['fase'],
                    'nome': row[keyNomeCliente],
                    'fone1': foneAsNumber,
                    'chave': row[keyEmail],
                    'livre2': row[keyCashIn],
                    'livre3': row[keyFaixaFaturamento] || row['faturamento'],
                    'LIMITE_CONTA': row[keyLimiteConta],
                    'DT_CONTA_CRIADA': dtContaCriadaAsDate,
                    'CHAVES_PIX_FORTE': row[keyChavesPix],
                    'LIMITE_CARTAO': row[keyLimiteCartao]
                };
            });
        }

        const { canceled, filePath: savePath } = await dialog.showSaveDialog(state.mainWindow, {
            title: "Salvar Relatório Final",
            defaultPath: `elegiveis_auto_${modo}_${Date.now()}.xlsx`,
            filters: [{ name: "Excel", extensions: ["xlsx"] }]
        });

        if (canceled || !savePath) {
            log("Salvamento cancelado pelo usuário.");
            return { success: true };
        }

        log(`Salvando arquivo final em: ${savePath}`);
        const finalSheet = XLSX.utils.json_to_sheet(dadosFinaisParaSheet, { skipHeader: false, cellDates: true });

        if (modo === 'relacionamento') {
            log("Formatos de data e número aplicados para o modo relacionamento.");
        }

        const finalWb = XLSX.utils.book_new();
        XLSX.utils.book_append_sheet(finalWb, finalSheet, NOME_PLANILHA_PRINCIPAL);
        XLSX.utils.book_append_sheet(finalWb, relacionamentoWs, NOME_PLANILHA_RELACIONAMENTO);
        XLSX.utils.book_append_sheet(finalWb, supervisoresWs, NOME_PLANILHA_SUPERVISORES);

        XLSX.writeFile(finalWb, savePath);

        log(`Arquivo salvo com sucesso. Pipeline completo finalizado.`);
        shell.showItemInFolder(savePath);
        return { success: true };

    } catch (err) {
        log(`Erro no pipeline: ${err.message}`);
        log(err.stack);
        return { success: false, error: err };
    }
}

// #################################################################
// #           REGISTRO DE HANDLERS                               #
// #################################################################

function register() {
    ipcMain.on('run-relacionamento-pipeline', async (event, filePaths, modo) => {
        if (!isAdmin()) {
            event.sender.send("relacionamento-log", "❌ Acesso negado.");
            event.sender.send("relacionamento-finished", false);
            return;
        }

        const result = await runFullPipeline(filePaths, modo, event);
        event.sender.send("relacionamento-finished", result.success);
    });

    ipcMain.on('split-by-responsible', async (event, filePath) => {
        const log = (msg) => event.sender.send('split-by-responsible-log', msg);

        if (!fs.existsSync(filePath)) {
            log('❌ Arquivo não encontrado.');
            event.sender.send('split-by-responsible-finished', { success: false, message: 'Arquivo não encontrado.' });
            return;
        }

        log(`Iniciando leitura do arquivo: ${path.basename(filePath)}`);
        log('Isso pode demorar um pouco dependendo do tamanho da lista...');
        logSystemAction(state.currentUser.username, 'Divisão Responsável', `Dividiu arquivo ${path.basename(filePath)}`);

        try {
            const workbook = new ExcelJS.Workbook();
            await workbook.xlsx.readFile(filePath);
            const worksheet = workbook.getWorksheet(1);

            if (!worksheet) {
                throw new Error('O arquivo Excel não possui nenhuma aba.');
            }

            log(`Arquivo carregado. Total de linhas: ${worksheet.rowCount}`);

            let respColIndex = 3;
            let respColFound = false;

            const headerRow = worksheet.getRow(1);
            headerRow.eachCell((cell, colNumber) => {
                if (cell.value && String(cell.value).toUpperCase().includes('RESPONSA') || String(cell.value).toUpperCase().includes('RESPONSÁVEL')) {
                    respColIndex = colNumber;
                    respColFound = true;
                    log(`Coluna RESPONSÁVEL identificada no índice: ${colNumber} (${cell.value})`);
                }
            });

            if (!respColFound) {
                log(`⚠️ Cabeçalho 'RESPONSÁVEL' não encontrado. Usando Coluna C (Índice 3) como padrão.`);
            }

            const groups = {};

            log('Analisando linhas e separando por responsável...');

            let rowCount = 0;
            worksheet.eachRow((row, rowNumber) => {
                if (rowNumber === 1) return;

                const cellValue = row.getCell(respColIndex).value;
                let respName = cellValue ? String(cellValue).trim() : 'Sem Responsável';

                const safeName = respName.replace(/[^a-zA-Z0-9\-_ ]/g, '').trim() || 'Desconhecido';

                if (!groups[safeName]) {
                    groups[safeName] = [];
                }
                groups[safeName].push(row);
                rowCount++;
            });

            const responsibleNames = Object.keys(groups);
            log(`Encontrados ${responsibleNames.length} responsáveis diferentes.`);

            const outputDir = path.join(path.dirname(filePath), path.basename(filePath, path.extname(filePath)) + '_Divididos');
            if (!fs.existsSync(outputDir)) {
                fs.mkdirSync(outputDir);
            }

            for (const respName of responsibleNames) {
                log(`Gerando arquivo para: ${respName} (${groups[respName].length} linhas)...`);

                const newWb = new ExcelJS.Workbook();
                const newWs = newWb.addWorksheet('Lista');

                const newHeaderRow = newWs.getRow(1);
                headerRow.eachCell({ includeEmpty: true }, (cell, colNumber) => {
                    const newCell = newHeaderRow.getCell(colNumber);
                    newCell.value = cell.value;
                    newCell.style = JSON.parse(JSON.stringify(cell.style));
                    const colWidth = worksheet.getColumn(colNumber).width;
                    if (colWidth) {
                        newWs.getColumn(colNumber).width = colWidth;
                    }
                });
                newHeaderRow.height = headerRow.height;
                newHeaderRow.commit();

                const rowsComponents = groups[respName];
                for (const srcRow of rowsComponents) {
                    const newRow = newWs.addRow([]);

                    srcRow.eachCell({ includeEmpty: true }, (cell, colNumber) => {
                        const newCell = newRow.getCell(colNumber);
                        newCell.value = cell.value;
                        newCell.style = JSON.parse(JSON.stringify(cell.style));
                    });
                    newRow.height = srcRow.height;
                    newRow.commit();
                }

                const savePath = path.join(outputDir, `${respName}.xlsx`);
                await newWb.xlsx.writeFile(savePath);
            }

            log(`🎉 Processo concluído! Os arquivos estão na pasta: ${outputDir}`);
            shell.showItemInFolder(outputDir);
            event.sender.send('split-by-responsible-finished', { success: true, message: 'Divisão concluída com sucesso!' });

        } catch (err) {
            console.error(err);
            log(`❌ Erro crítico: ${err.message}`);
            event.sender.send('split-by-responsible-finished', { success: false, message: `Erro: ${err.message}` });
        }
    });
}

module.exports = { register };
