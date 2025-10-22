const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse');
const ExcelJS = require('exceljs');

// --- CONFIGURAÇÕES ---
// Altere o caminho para o seu arquivo CSV de entrada
const INPUT_CSV_PATH = 'C:\\Users\\dabra\\Downloads\\Não Perturbe Callix - 17.10.25 (1).csv'; 
// O nome base para os arquivos de saída
const OUTPUT_FILE_BASE_NAME = 'telefones_csv_parte';
// Quantidade de linhas por arquivo Excel
const LINES_PER_FILE = 1000000; 
// ---------------------

/**
 * Função principal que orquestra o processo de divisão e conversão.
 */
async function processLargeCsv() {
    if (!fs.existsSync(INPUT_CSV_PATH)) {
        console.error(`❌ ERRO: O arquivo de entrada não foi encontrado em: ${INPUT_CSV_PATH}`);
        return;
    }

    console.log(`--- Iniciando processamento do arquivo: ${path.basename(INPUT_CSV_PATH)} ---`);
    console.log(`⚙️  Configuração: ${LINES_PER_FILE.toLocaleString('pt-BR')} linhas por arquivo.`);

    const inputStream = fs.createReadStream(INPUT_CSV_PATH);
    const parser = parse({
        delimiter: ',',
        from_line: 1 //
    });

    let fileCounter = 1;
    let lineCounter = 0;
    let rowsForCurrentFile = [];
    const outputDir = path.dirname(INPUT_CSV_PATH);

    // Função para salvar um lote de linhas em um arquivo CSV
    const saveChunkToCsv = async (rows, partNumber) => {
        if (rows.length === 0) return;

        const outputFilePath = path.join(outputDir, `${OUTPUT_FILE_BASE_NAME}_${partNumber}.csv`);
        console.log(`\n⏳ Gerando arquivo: ${path.basename(outputFilePath)} com ${rows.length.toLocaleString('pt-BR')} linhas...`);

        const workbook = new ExcelJS.Workbook();
        const worksheet = workbook.addWorksheet('Telefones');

        worksheet.columns = [
            { header: 'telefone', key: 'telefone', width: 20 }
        ];
        
        const cleanedRows = rows.map(row => {
            // MODIFICADO: Agora pega o valor da segunda coluna (índice 1)
            // e remove qualquer caractere que não seja um dígito.
            // Isso lida com o formato "BLOCKLIST,31988401762".
            const cleanedPhone = String(row[1] || '').replace(/\D/g, '');
            return { telefone: cleanedPhone };
        });
        worksheet.addRows(cleanedRows);
        
        // Escreve o arquivo CSV, usando ';' como delimitador para melhor compatibilidade com Excel (PT-BR)
        await workbook.csv.writeFile(outputFilePath, {
            formatterOptions: {
                delimiter: ';'
            }
        });
        console.log(`✅ Arquivo salvo: ${path.basename(outputFilePath)}`);
    };

    // Processa o stream de dados
    for await (const row of inputStream.pipe(parser)) {
        rowsForCurrentFile.push(row);
        lineCounter++;

        // A cada 100.000 linhas, mostra um progresso no console
        if (lineCounter % 100000 === 0) {
            process.stdout.write(`\r📄 Linhas processadas: ${lineCounter.toLocaleString('pt-BR')}`);
        }

        // Se atingir o limite, salva o arquivo e reinicia os contadores
        if (rowsForCurrentFile.length >= LINES_PER_FILE) {
            await saveChunkToCsv(rowsForCurrentFile, fileCounter);
            rowsForCurrentFile = [];
            fileCounter++;
        }
    }

    // Garante que o último lote de linhas seja salvo
    if (rowsForCurrentFile.length > 0) {
        await saveChunkToCsv(rowsForCurrentFile, fileCounter);
    }

    console.log(`\n\n🎉 Processo concluído! Total de ${lineCounter.toLocaleString('pt-BR')} linhas divididas em ${fileCounter} arquivo(s).`);
    console.log(`📂 Os arquivos foram salvos em: ${outputDir}`);
}

// Inicia a execução do script
processLargeCsv().catch(err => {
    console.error('\n❌ Ocorreu um erro fatal durante o processamento:', err);
});
