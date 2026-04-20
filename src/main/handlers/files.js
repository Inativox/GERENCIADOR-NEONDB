/**
 * Handlers de seleção de arquivos, leitura/escrita de planilhas.
 */
const { ipcMain, dialog, shell } = require('electron');
const path = require('path');
const fs = require('fs');
const fsp = require('fs').promises;
const XLSX = require('xlsx');

const state = require('../state');

function letterToIndex(letter) {
    return letter.toUpperCase().charCodeAt(0) - 65;
}

async function readSpreadsheet(filePath) {
    try {
        if (path.extname(filePath).toLowerCase() === ".csv") {
            const data = await fsp.readFile(filePath, "utf8");
            return XLSX.read(data, { type: "string", cellDates: true });
        } else {
            const buffer = await fsp.readFile(filePath);
            return XLSX.read(buffer, { type: 'buffer', cellDates: true });
        }
    } catch (e) {
        console.error(`Erro ao ler planilha: ${filePath}`, e);
        throw new Error(`Não foi possível ler o arquivo ${path.basename(filePath)}. Verifique se o caminho está correto e se você tem permissão.`);
    }
}

function writeSpreadsheet(workbook, filePath) {
    XLSX.writeFile(workbook, filePath);
}

function register() {
    ipcMain.on('open-path', (event, filePath) => {
        shell.openPath(filePath).catch(err => {
            const msg = `ERRO: Não foi possível abrir o arquivo em ${filePath}`;
            console.error("Falha ao abrir o caminho:", err);
            event.sender.send("log", msg);
            event.sender.send("automation-log", msg);
        });
    });

    ipcMain.handle("select-file", async (event, { title, multi }) => {
        const { canceled, filePaths } = await dialog.showOpenDialog(state.mainWindow, {
            title: title,
            properties: [multi ? "multiSelections" : "openFile", "openFile"],
            filters: [{ name: "Planilhas", extensions: ["xlsx", "xls", "csv"] }]
        });
        return canceled ? null : filePaths;
    });

    ipcMain.handle("show-save-dialog", async (event, options) => {
        const result = await dialog.showSaveDialog(state.mainWindow, options);
        return result;
    });
}

module.exports = { letterToIndex, readSpreadsheet, writeSpreadsheet, register };
