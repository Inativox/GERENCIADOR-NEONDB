/**
 * Cache em memória para CNPJs já usados na limpeza.
 * Evita consultas repetidas ao BD durante o processo de limpeza.
 */
const state = require('../state');
const { ipcMain } = require('electron');

let storedCnpjs = new Set();

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

// A blocklist saiu do Neon na 1.8.26 e é consultada no BigQuery por lote: não há mais cópia em memória.
// O canal antigo continua respondendo, agora com o total da lista.
function register() {
    ipcMain.handle('refresh-blocklist-cache', async () => {
        const service = state.bqReceitaService;
        if (!service?.configured?.()) return { success: false, size: 0 };
        try {
            const { total } = await require('../blocklistBq').stats(service.client());
            return { success: true, size: total };
        } catch {
            return { success: false, size: 0 };
        }
    });
}

module.exports = {
    storedCnpjs,
    loadStoredCnpjs,
    getStoredCnpjs: () => storedCnpjs,
    setStoredCnpjs: (set) => { storedCnpjs = set; },
    register,
};
