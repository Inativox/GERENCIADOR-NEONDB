/**
 * Configuração do pool de conexão com o banco de dados,
 * retry automático e lista de CNAEs proibidos.
 */
const { Pool } = require('pg');
const Store = require('electron-store');
const store = new Store();

const state = require('../state');

// #################################################################
// #           LISTA DE CNAES PROIBIDOS                           #
// #################################################################
const PROHIBITED_CNAES = new Set([
    '8299704', '8299706', '9002702', '9200301', '9200302', '9200399',
    '9329803', '9329804', '9491000', '9492800', '9529106', '9609204',
    '1210700', '1220401', '1220402', '1220403', '1220499', '2092401',
    '2442300', '2550101', '2550102', '3211602', '3211603', '4520005',
    '4681801', '4681802', '4681803', '4681804', '4681805', '4732600',
    '4782202', '4783101', '4783102', '4789009', '6434400', '6440900',
    '6491300', '6619399', '7912100', '8422100', '9420100', '9430800',
    '724301', '729404', '893200', '899101', '899102', '899103',
    '899199', '9499500', '9493600', '220906', '5590601', '9411100',
    '8720401', '9412099', '8711504', '7911200'
]);

async function initializePool(connectionString, windowToLog) {
    if (state.pool) {
        await state.pool.end();
        console.log("Pool de conexões anterior encerrado.");
    }

    if (!connectionString) {
        console.log("Chave de conexão não fornecida. A inicialização do pool foi ignorada.");
        if (windowToLog) windowToLog.webContents.send("log", "⚠️ Chave de conexão do BD não configurada. Funções do BD desabilitadas.");
        state.pool = null;
        return;
    }

    state.pool = new Pool({
        connectionString: connectionString,
        max: 10,
        idleTimeoutMillis: 30000,
        connectionTimeoutMillis: 15000,
        keepAlive: true,
        keepAliveInitialDelayMillis: 10000,
    });

    try {
        await state.pool.query('SELECT NOW()');
        console.log("✅ Conexão com o banco de dados estabelecida com sucesso.");

        await state.pool.query(`
            CREATE TABLE IF NOT EXISTS api_locks (
                key_name TEXT PRIMARY KEY,
                username TEXT NOT NULL,
                status TEXT DEFAULT 'Livre',
                last_heartbeat TIMESTAMP DEFAULT NOW(),
                key_label TEXT,
                lock_mode TEXT
            );
        `);
        await state.pool.query(`ALTER TABLE api_locks ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'Livre';`);
        await state.pool.query(`ALTER TABLE api_locks ADD COLUMN IF NOT EXISTS key_label TEXT;`);
        await state.pool.query(`ALTER TABLE api_locks ADD COLUMN IF NOT EXISTS lock_mode TEXT;`);

        await state.pool.query(`
            CREATE TABLE IF NOT EXISTS system_logs (
                id SERIAL PRIMARY KEY,
                username TEXT,
                action TEXT,
                details TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            );
        `);

        if (windowToLog) windowToLog.webContents.send("log", "✅ Conexão com o Banco de Dados estabelecida com sucesso.");
    } catch (error) {
        console.error("❌ Falha ao estabelecer conexão com o banco de dados:", error.message);
        if (windowToLog) windowToLog.webContents.send("log", `❌ ERRO DE CONEXÃO BD: ${error.message}. Funções do BD podem não funcionar.`);
        state.pool = null;
        throw error;
    }
}

// #################################################################
// #           QUERY COM RETRY AUTOMÁTICO                         #
// #################################################################
const RETRYABLE_PG_CODES = new Set([
    '08000', '08003', '08006', '08001', '08004',
    '40001', '40P01',
    '57P03', '53300',
]);

async function queryWithRetry(sql, params = [], maxRetries = 3, logFn = null) {
    let lastError;
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            if (!state.pool) throw new Error('Pool de conexão não disponível.');
            return await state.pool.query(sql, params);
        } catch (err) {
            lastError = err;
            const isRetryable =
                RETRYABLE_PG_CODES.has(err.code) ||
                ['ECONNRESET', 'ECONNREFUSED', 'ETIMEDOUT', 'EPIPE', 'ENOTFOUND'].includes(err.code) ||
                /connection|timeout|terminating|broken pipe|reset/i.test(err.message);

            if (!isRetryable || attempt === maxRetries) throw err;

            const delay = 1500 * attempt;
            const msg = `⚠️ Erro de BD na tentativa ${attempt}/${maxRetries} (${err.code || err.message.slice(0, 60)}). Reconectando em ${delay / 1000}s...`;
            if (logFn) logFn(msg);
            console.warn(`[queryWithRetry] ${msg}`);

            await new Promise(r => setTimeout(r, delay));

            try {
                const savedCs = store.get('dbConnectionString');
                if (savedCs) await initializePool(savedCs, null);
            } catch (reconnErr) {
                console.warn('[queryWithRetry] Falha ao reconectar pool:', reconnErr.message);
            }
        }
    }
    throw lastError;
}

// --- FUNÇÃO DE LOG DO SISTEMA (AUDIT) ---
async function logSystemAction(username, action, details) {
    if (!state.pool) return;
    try {
        const user = username || (state.currentUser ? state.currentUser.username : 'Desconhecido');
        const now = new Date();
        state.pool.query('INSERT INTO system_logs (username, action, details, created_at) VALUES ($1, $2, $3, $4)', [user, action, details, now])
            .catch(err => console.error("Erro ao inserir log no BD:", err.message));
    } catch (err) {
        console.error("Erro ao tentar registrar log:", err.message);
    }
}

module.exports = {
    PROHIBITED_CNAES,
    initializePool,
    queryWithRetry,
    logSystemAction,
};
