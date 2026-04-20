/**
 * Cache em memória para CNPJs e telefones da blocklist.
 * Evita consultas repetidas ao BD durante o processo de limpeza.
 */
const state = require('../state');
const { ipcMain } = require('electron');

let storedCnpjs = new Set();
let blocklistPhones = new Set();

async function loadStoredCnpjs() {
    const isAdmin = () => state.currentUser && state.currentUser.role === 'admin';

    if (!isAdmin() || !state.pool) {
        if (state.mainWindow && isAdmin()) {
            state.mainWindow.webContents.send("log", "⚠️ A conexão com o BD não está ativa. Histórico de CNPJs não carregado.");
        }
        return;
    }

    try {
        const result = await state.pool.query('SELECT cnpj FROM limpeza_cnpjs');
        storedCnpjs = new Set(result.rows.map(row => row.cnpj));
        console.log(`${storedCnpjs.size} CNPJs carregados do Neon DB.`);
        if (state.mainWindow) {
            state.mainWindow.webContents.send("log", `✅ Sincronização com o BD concluída. ${storedCnpjs.size} CNPJs carregados.`);
        }
    } catch (err) {
        console.error("Falha ao carregar CNPJs do Neon DB:", err);
        if (state.mainWindow) {
            state.mainWindow.webContents.send("log", `❌ ERRO ao carregar histórico do BD: ${err.message}`);
        }
    }
}

async function loadBlocklistPhones() {
    if (!state.pool) return;
    try {
        const result = await state.pool.query('SELECT telefone FROM blocklist');
        blocklistPhones = new Set(result.rows.map(row => row.telefone));
        console.log(`${blocklistPhones.size} telefones carregados da blocklist em memória.`);
        if (state.mainWindow) {
            state.mainWindow.webContents.send("log", `✅ Cache blocklist: ${blocklistPhones.size.toLocaleString('pt-BR')} números em memória.`);
        }
    } catch (err) {
        console.error("Falha ao carregar blocklist em memória:", err.message);
        if (state.mainWindow) {
            state.mainWindow.webContents.send("log", `⚠️ Cache blocklist não carregado: ${err.message}. Verificação por BD como fallback.`);
        }
    }
}

function register() {
    ipcMain.handle('refresh-blocklist-cache', async () => {
        if (!state.pool) return { success: false, size: 0 };
        await loadBlocklistPhones();
        return { success: true, size: blocklistPhones.size };
    });
}

module.exports = {
    storedCnpjs,
    blocklistPhones,
    loadStoredCnpjs,
    loadBlocklistPhones,
    getStoredCnpjs: () => storedCnpjs,
    getBlocklistPhones: () => blocklistPhones,
    setStoredCnpjs: (set) => { storedCnpjs = set; },
    register,
};
