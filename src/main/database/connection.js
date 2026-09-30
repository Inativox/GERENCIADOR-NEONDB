/**
 * Configuração do pool de conexão com o banco de dados,
 * retry automático e lista de CNAEs proibidos.
 */
const { Pool } = require('pg');

const state = require('../state');

// #################################################################
// #           LISTA DE CNAES PROIBIDOS                           #
// #################################################################
const PROHIBITED_CNAES = new Set([
    '114800',  '220906',  '724301',  '729404',  '893200',
    '899101',  '899102',  '899103',  '899199',  '1111902',
    '1210700', '1220401', '1220402', '1220403', '1220499',
    '2092401', '2442300', '2550101', '2550102', '3211602',
    '3211603', '4520005', '4623104', '4636201', '4681801',
    '4681802', '4681803', '4681804', '4681805', '4731800',
    '4732600', '4782202', '4783101', '4783102', '4789009',
    '5590601', '6434400', '6440900', '6491300', '6619399',
    '7911200', '7912100', '8299704', '8299706', '8422100',
    '8711504', '8720401', '8730101', '8730102', '9002702',
    '9200301', '9200302', '9200399', '9329803', '9329804',
    '9411100', '9412099', '9420100', '9430800', '9491000',
    '9492800', '9493600', '9499500', '9529106', '9609204',
]);

let initialization = Promise.resolve();
let poolGeneration = 0;

function assertGeneration(generation) {
    if (generation !== poolGeneration) {
        const error = new Error('Inicialização do banco cancelada após encerramento da sessão.');
        error.code = 'DB_INITIALIZATION_CANCELLED';
        throw error;
    }
}

function closePool() {
    // Invalida também candidatos privados e inicializações ainda na fila.
    poolGeneration++;
    const previous = state.pool;
    state.pool = null;
    return previous ? previous.end().catch(error => console.warn('Falha ao encerrar pool:', error.message)) : Promise.resolve();
}

function notify(windowToLog, message) {
    if (!windowToLog || windowToLog.isDestroyed?.() || windowToLog.webContents.isDestroyed?.()) return;
    windowToLog.webContents.send('log', message);
}

// Serializa mudanças explícitas de configuração, nunca os retries de queries.
function initializePool(connectionString, windowToLog) {
    const generation = poolGeneration;
    const pending = initialization.then(() => initializePoolNow(connectionString, windowToLog, generation));
    initialization = pending.catch(() => {});
    return pending;
}

async function initializePoolNow(connectionString, windowToLog, generation) {
    assertGeneration(generation);

    if (!connectionString) {
        console.log("Chave de conexão não fornecida. A inicialização do pool foi ignorada.");
        notify(windowToLog, '⚠️ Chave de conexão do BD não configurada. Funções do BD desabilitadas.');
        const previous = state.pool;
        state.pool = null;
        if (previous) await previous.end();
        return;
    }

    const candidate = new Pool({
        connectionString: connectionString,
        max: 10,
        idleTimeoutMillis: 30000,
        connectionTimeoutMillis: 15000,
        keepAlive: true,
        keepAliveInitialDelayMillis: 10000,
    });
    // pg remove o cliente quebrado; o pool pode criar outra conexão na próxima query.
    candidate.on('error', (error) => {
        console.error('Erro em conexão ociosa do banco:', error.code || error.message);
        notify(state.mainWindow, '⚠️ Uma conexão do banco foi interrompida. Novas consultas tentarão conectar novamente.');
    });

    try {
        await candidate.query('SELECT NOW()');
        assertGeneration(generation);
        console.log("✅ Conexão com o banco de dados estabelecida com sucesso.");

        await candidate.query(`
            CREATE TABLE IF NOT EXISTS api_locks (
                key_name TEXT PRIMARY KEY,
                username TEXT NOT NULL,
                status TEXT DEFAULT 'Livre',
                last_heartbeat TIMESTAMP DEFAULT NOW(),
                key_label TEXT,
                lock_mode TEXT
            );
        `);
        assertGeneration(generation);
        await candidate.query(`ALTER TABLE api_locks ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'Livre';`);
        await candidate.query(`ALTER TABLE api_locks ADD COLUMN IF NOT EXISTS key_label TEXT;`);
        await candidate.query(`ALTER TABLE api_locks ADD COLUMN IF NOT EXISTS lock_mode TEXT;`);

        await candidate.query(`
            CREATE TABLE IF NOT EXISTS system_logs (
                id SERIAL PRIMARY KEY,
                username TEXT,
                action TEXT,
                details TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            );
        `);
        assertGeneration(generation);

    } catch (error) {
        console.error("❌ Falha ao estabelecer conexão com o banco de dados:", error.message);
        await candidate.end().catch(err => console.warn('Falha ao liberar pool inválido:', err.message));
        notify(windowToLog, '❌ Não foi possível conectar ao banco. Verifique a configuração e a conexão de rede.');
        throw error;
    }
    const previous = state.pool;
    state.pool = candidate;
    if (previous) previous.end().catch(err => console.warn('Falha ao encerrar pool anterior:', err.message));
    notify(windowToLog, '✅ Conexão com o Banco de Dados estabelecida com sucesso.');
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
            const msg = `⚠️ Erro de BD na tentativa ${attempt}/${maxRetries} (${err.code || err.message.slice(0, 60)}). Nova tentativa em ${delay / 1000}s...`;
            if (logFn) logFn(msg);
            console.warn(`[queryWithRetry] ${msg}`);

            await new Promise(r => setTimeout(r, delay));

            // O próprio pg substitui conexões perdidas. Encerrar o pool aqui
            // interromperia consultas e transações de outras tarefas.
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
    closePool,
    queryWithRetry,
    logSystemAction,
};
