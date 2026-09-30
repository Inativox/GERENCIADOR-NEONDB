/** Benchmark sintético local; todos os arquivos ficam em uma pasta temporária. */
const fs = require('node:fs/promises');
const path = require('node:path');
const os = require('node:os');
const { monitorEventLoopDelay, performance } = require('node:perf_hooks');
const ExcelJS = require('exceljs');
const { runLimpezaColunas } = require('../src/main/limpezaColunasJob');

async function main() {
    const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'columns-benchmark-'));
    try {
        const input = path.join(directory, 'sintetico.xlsx');
        const rows = 50000;
        const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({ filename: input, useStyles: false, useSharedStrings: false });
        const sheet = workbook.addWorksheet('Base');
        sheet.addRow(['Nome do Negócio', 'CNPJ', 'Telefone Celular']).commit();
        for (let index = 0; index < rows; index++) sheet.addRow([`Empresa ${index}`, String(10000000000000 + index), '5521998364849']).commit();
        await workbook.commit();
        const delay = monitorEventLoopDelay({ resolution: 10 });
        let heartbeats = 0;
        delay.enable();
        const timer = setInterval(() => { heartbeats++; }, 10);
        const started = performance.now();
        let result;
        try { result = await runLimpezaColunas([input], () => {}); }
        finally { clearInterval(timer); delay.disable(); }
        if (result.processados !== 1) throw new Error('A planilha sintética não foi processada.');
        console.log(JSON.stringify({ rows, durationMs: Math.round(performance.now() - started), mainLoopHeartbeats: heartbeats, mainLoopP99Ms: Number((delay.percentile(99) / 1e6).toFixed(1)), mainLoopMaxMs: Number((delay.max / 1e6).toFixed(1)) }, null, 2));
    } finally {
        await fs.rm(directory, { recursive: true, force: true });
    }
}

main().catch(error => { console.error(error.message); process.exitCode = 1; });
