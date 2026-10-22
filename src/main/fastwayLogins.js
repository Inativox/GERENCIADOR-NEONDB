/**
 * Login Fastway que cada usuário escolheu salvar neste computador (aba Importar Mailing).
 * Fica cifrado pela proteção do Windows (safeStorage/DPAPI): só abre na mesma conta do Windows.
 */
const KEY = 'fastway_logins';

function createFastwayLogins({ store, safeStorage }) {
    const all = () => {
        const saved = store.get(KEY, {});
        return saved && typeof saved === 'object' && !Array.isArray(saved) ? saved : {};
    };
    const canSave = () => {
        try { return safeStorage.isEncryptionAvailable(); } catch { return false; }
    };

    function get(owner) {
        const saved = all();
        if (!Object.hasOwn(saved, owner) || typeof saved[owner] !== 'string' || !canSave()) return null;
        try {
            const { login, password } = JSON.parse(safeStorage.decryptString(Buffer.from(saved[owner], 'base64')));
            return typeof login === 'string' && login && typeof password === 'string' && password ? { login, password } : null;
        } catch { return null; }
    }

    function save(owner, { login, password }) {
        if (!canSave()) throw new Error('A proteção do Windows não está disponível para salvar o login.');
        const sealed = safeStorage.encryptString(JSON.stringify({ login, password })).toString('base64');
        store.set(KEY, { ...all(), [owner]: sealed });
    }

    function forget(owner) {
        const { [owner]: _removed, ...rest } = all();
        store.set(KEY, rest);
    }

    // O que a tela pode saber: se existe login salvo e qual é. A senha nunca sai do main.
    function summary(owner) {
        const saved = get(owner);
        return { saved: Boolean(saved), login: saved?.login || null, canSave: canSave() };
    }

    return { get, save, forget, summary };
}

module.exports = { createFastwayLogins };
