/**
 * Handlers da aba Blocklist: alimentação, verificação, estatísticas e divisão de CSV.
 */
const { ipcMain, shell } = require('electron');
const path = require('path');
const fs = require('fs');
const ExcelJS = require('exceljs');
const nodemailer = require('nodemailer');
const { parse } = require('csv-parse');

const state = require('../state');
const { logSystemAction } = require('../database/connection');

const isAdmin = () => state.currentUser && state.currentUser.role === 'admin';

// #################################################################
// #           E-MAIL DE ATUALIZAÇÃO                              #
// #################################################################

async function sendBlocklistUpdateEmail(totalNewPhones, finalTotalCount) {
    const transporter = nodemailer.createTransport({
        host: process.env.SMTP_HOST || "smtp.gmail.com",
        port: parseInt(process.env.SMTP_PORT, 10) || 465,
        secure: (process.env.SMTP_PORT || "465") === "465",
        auth: {
            user: process.env.SMTP_USER,
            pass: process.env.SMTP_PASS,
        },
    });

    const now = new Date();
    const formattedDate = now.toLocaleDateString('pt-BR', { day: '2-digit', month: '2-digit', year: 'numeric' });
    const formattedTime = now.toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit' });

    const mailOptions = {
        from: `"Gerenciador de Bases" <${process.env.SMTP_USER}>`,
        to: "tatiane@mbfinance.com.br",
        cc: "rodrigo.gadelha@mbfinance.com.br",
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
                    Processo executado por: ${state.currentUser ? state.currentUser.username : 'desconhecido'}
                </p>
            </div>
        `,
    };

    return transporter.sendMail(mailOptions);
}

// #################################################################
// #           REGISTRO DE HANDLERS                               #
// #################################################################

function register() {
    ipcMain.handle("add-numbers-to-blocklist", async (_event, numbers) => {
        if (!isAdmin() || !state.pool) return { success: false, message: "Acesso negado ou conexão com BD inativa." };
        if (!Array.isArray(numbers) || numbers.length === 0) return { success: false, message: "Nenhum número fornecido." };
        try {
            const query = `
                INSERT INTO blocklist (telefone)
                SELECT unnest($1::text[])
                ON CONFLICT (telefone) DO NOTHING;
            `;
            const result = await state.pool.query(query, [numbers]);
            const added = result.rowCount;
            logSystemAction(state.currentUser.username, 'Adicionar Manualmente à Blocklist', `${added} de ${numbers.length} números adicionados.`);
            return { success: true, added, total: numbers.length };
        } catch (e) {
            return { success: false, message: e.message };
        }
    });

    ipcMain.on("feed-blocklist", async (event, { filePaths, sendEmail }) => {
        if (!isAdmin() || !state.pool) {
            event.sender.send("blocklist-log", "❌ Acesso negado ou conexão com BD inativa.");
            return;
        }
        const log = (msg) => event.sender.send("blocklist-log", msg);
        log(`--- Iniciando Alimentação da Blocklist ---`);
        logSystemAction(state.currentUser.username, 'Alimentar Blocklist', `Iniciou alimentação com ${filePaths.length} arquivos.`);

        const DB_BATCH_SIZE = 50000;
        let totalNewPhonesAdded = 0;

        const processChunk = async (phoneChunk) => {
            if (phoneChunk.size === 0) return;
            try {
                const query = `
                    INSERT INTO blocklist (telefone)
                    SELECT unnest($1::text[])
                    ON CONFLICT (telefone) DO NOTHING;
                `;
                const result = await state.pool.query(query, [Array.from(phoneChunk)]);
                const newCount = result.rowCount;
                if (newCount > 0) {
                    log(`✅ Lote salvo. ${newCount} novos telefones adicionados à blocklist.`);
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
            const finalCountResult = await state.pool.query('SELECT COUNT(*) FROM blocklist;');
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

    ipcMain.handle("get-blocklist-stats", async () => {
        if (!isAdmin() || !state.pool) {
            return { success: false, message: "Acesso negado ou conexão com BD inativa.", data: { total: 0, addedToday: 0 } };
        }
        try {
            const [totalResult, todayResult] = await Promise.all([
                state.pool.query('SELECT COUNT(*) FROM blocklist;'),
                state.pool.query("SELECT COUNT(*) FROM blocklist WHERE data_adicao >= current_date;")
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

    ipcMain.handle("check-blocklist-numbers", async (event, numbers) => {
        if (!isAdmin() || !state.pool) {
            return { success: false, message: "Acesso negado ou conexão com BD inativa." };
        }
        if (!numbers || numbers.length === 0) {
            return { success: false, message: "Nenhum número fornecido para verificação." };
        }

        try {
            const query = `
                SELECT
                    telefone,
                    to_char(data_adicao, 'DD/MM/YYYY HH24:MI:SS') as data_formatada
                FROM blocklist
                WHERE telefone = ANY($1::text[])
            `;
            const result = await state.pool.query(query, [numbers]);

            const foundNumbersMap = new Map(result.rows.map(row => [row.telefone, row.data_formatada]));
            const notFoundNumbers = numbers.filter(num => !foundNumbersMap.has(num));
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

    ipcMain.on("split-large-csv", async (event, { filePath, linesPerSplit }) => {
        const log = (msg) => event.sender.send("blocklist-log", msg);

        if (!fs.existsSync(filePath)) {
            log(`❌ ERRO: O arquivo de entrada não foi encontrado em: ${filePath}`);
            return;
        }

        log(`--- Iniciando divisão do arquivo: ${path.basename(filePath)} ---`);
        log(`⚙️  Configuração: ${linesPerSplit.toLocaleString('pt-BR')} linhas por arquivo.`);
        logSystemAction(state.currentUser.username, 'Divisão CSV', `Dividiu CSV ${path.basename(filePath)} em partes de ${linesPerSplit}`);

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
            }).filter(r => r.telefone);

            worksheet.addRows(cleanedRows);

            await workbook.csv.writeFile(outputFilePath, { formatterOptions: { delimiter: ';' } });
            log(`✅ Arquivo salvo: ${path.basename(outputFilePath)}`);
        };

        for await (const row of inputStream.pipe(parser)) {
            rowsForCurrentFile.push(row);
            lineCounter++;
            if (lineCounter % 100000 === 0) log(`... ${lineCounter.toLocaleString('pt-BR')} linhas processadas`);
            if (rowsForCurrentFile.length >= linesPerSplit) {
                await saveChunkToCsv(rowsForCurrentFile, fileCounter);
                rowsForCurrentFile = [];
                fileCounter++;
            }
        }
        if (rowsForCurrentFile.length > 0) {
            await saveChunkToCsv(rowsForCurrentFile, fileCounter);
        }
        log(`\n\n🎉 Processo concluído! Total de ${lineCounter.toLocaleString('pt-BR')} linhas divididas em ${fileCounter} arquivo(s).`);
        shell.showItemInFolder(outputDir);
    });
}

module.exports = { register };
