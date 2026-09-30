const fs = require('fs');
const path = require('path');
const dotenv = require('dotenv');

const ENV_KEYS = new Set(['SMTP_HOST', 'SMTP_PORT', 'SMTP_USER', 'SMTP_PASS', 'API_KEY', 'C6_CLIENT_ID', 'C6_CLIENT_SECRET', 'IM_CLIENT_ID', 'IM_CLIENT_SECRET']);

function validateBundle(bundle) {
    if (!bundle || bundle.version !== 1 || !bundle.users || Array.isArray(bundle.users) || !Object.keys(bundle.users).length) throw new Error('Arquivo de acesso inválido ou sem usuários.');
    for (const [username, user] of Object.entries(bundle.users)) {
        if (!username.trim() || ['__proto__', 'constructor', 'prototype'].includes(username) || !user || typeof user.password !== 'string' || !user.password || !['admin', 'limited', 'master'].includes(user.role)) throw new Error('Usuário ou perfil inválido no arquivo de acesso.');
    }
    const env = bundle.env || {};
    if (typeof env !== 'object' || Array.isArray(env) || Object.entries(env).some(([key, value]) => !ENV_KEYS.has(key) || typeof value !== 'string')) throw new Error('Configuração de ambiente inválida.');
    if (bundle.keyFile !== undefined && (typeof bundle.keyFile !== 'string' || !bundle.keyFile.trim())) throw new Error('Licença de API inválida.');
    return { version: 1, users: bundle.users, env, ...(bundle.keyFile ? { keyFile: bundle.keyFile } : {}) };
}

function createPrivateConfig({ app, projectRoot, environment = process.env }) {
    const directory = path.join(app.getPath('userData'), 'private');
    const configPath = path.join(directory, 'access.json');
    let issue = '';
    const managedEnvironment = new Map();
    const read = () => fs.existsSync(configPath) ? validateBundle(JSON.parse(fs.readFileSync(configPath, 'utf8'))) : null;
    function importBundle(bundle) {
        const validated = validateBundle(bundle);
        fs.mkdirSync(directory, { recursive: true });
        const temporary = `${configPath}.tmp`;
        fs.writeFileSync(temporary, JSON.stringify(validated), { mode: 0o600 });
        fs.renameSync(temporary, configPath);
        issue = '';
        applyEnvironment(validated.env);
    }
    function applyEnvironment(env) {
        for (const [key, value] of managedEnvironment) {
            if (environment[key] === value) delete environment[key];
        }
        managedEnvironment.clear();
        for (const [key, value] of Object.entries(env)) {
            if (ENV_KEYS.has(key) && environment[key] === undefined) {
                environment[key] = value;
                managedEnvironment.set(key, value);
            }
        }
    }
    function initialize() {
        try {
            if (!app.isPackaged) {
                const localEnv = path.join(projectRoot, '.env');
                if (fs.existsSync(localEnv)) applyEnvironment(dotenv.parse(fs.readFileSync(localEnv)));
                return;
            }
            if (!fs.existsSync(configPath)) {
                const roots = [app.getPath('appData'), environment.ProgramData].filter(Boolean);
                for (const root of roots) {
                    const legacy = path.join(root, 'MB Finance', 'Gerenciador de Bases', 'legacy-app.asar');
                    const usersPath = path.join(legacy, 'users.json');
                    if (!fs.existsSync(usersPath)) continue;
                    const envPath = path.join(legacy, '.env');
                    const licensePath = path.join(legacy, 'chave-api.mbkey');
                    const legacyEnv = fs.existsSync(envPath) ? dotenv.parse(fs.readFileSync(envPath)) : {};
                    importBundle({
                        version: 1, users: JSON.parse(fs.readFileSync(usersPath, 'utf8')),
                        env: Object.fromEntries(Object.entries(legacyEnv).filter(([key]) => ENV_KEYS.has(key))),
                        ...(fs.existsSync(licensePath) ? { keyFile: fs.readFileSync(licensePath, 'utf8') } : {})
                    });
                    break;
                }
            }
            const config = read();
            if (config) applyEnvironment(config.env);
        } catch {
            issue = 'Não foi possível carregar a configuração local. Importe novamente o arquivo de acesso da empresa.';
        }
    }
    function loadUsers() {
        try {
            if (!app.isPackaged) return JSON.parse(fs.readFileSync(path.join(projectRoot, 'users.json'), 'utf8'));
            return read()?.users || {};
        } catch { return {}; }
    }
    function keyFilePath() {
        const config = read();
        if (!config?.keyFile) return null;
        const file = path.join(directory, 'api.mbkey');
        fs.writeFileSync(file, config.keyFile, { mode: 0o600 });
        return file;
    }
    return { initialize, importBundle, loadUsers, keyFilePath, hasAccess: () => Object.keys(loadUsers()).length > 0, status: () => ({ configured: Object.keys(loadUsers()).length > 0, message: issue }) };
}

module.exports = { createPrivateConfig, validateBundle };
