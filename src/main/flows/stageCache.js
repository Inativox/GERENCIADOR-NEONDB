'use strict';
const fs = require('node:fs/promises');
const path = require('node:path');
const { randomUUID } = require('node:crypto');
const { open } = require('lmdb');

/** Output is synced before its cursor and dedup keys are committed together. */
async function openStageCache({ jobDir, stage, fingerprint, version, temporary, target }) {
    const directory = path.join(jobDir, 'stage-cache', stage);
    await fs.mkdir(directory, { recursive: true });
    const db = open({ path: directory, encoding: 'json' });
    const identity = `${fingerprint}:${version}`;
    let state = db.get('state');
    if (state && state.identity !== identity) { await db.clearAsync(); state = null; }
    if (!state) {
        state = { id: randomUUID(), identity, rows: 0, outputBytes: 0, inputOffset: 0, processed: 0, cursor: '', counts: {} };
        await fs.writeFile(temporary, '', { mode: 0o600 });
        await db.put('state', state);
    } else {
        try { await fs.access(temporary); }
        catch (error) {
            if (error.code !== 'ENOENT') { await db.close(); throw error; }
            // Recover a crash between rename and the completed-stage checkpoint.
            try { await fs.rename(target, temporary); }
            catch { await db.close(); throw new Error('O arquivo do cache está indisponível. Preserve o histórico antes de iniciar outra execução.'); }
        }
        const size = (await fs.stat(temporary)).size;
        if (size < state.outputBytes) { await db.close(); throw new Error('O arquivo do cache está incompleto. Preserve o histórico antes de iniciar outra execução.'); }
        await fs.truncate(temporary, state.outputBytes);
    }
    let pending = new Set();
    return {
        state,
        has(kind, value) { const key = `${kind}:${value}`; return pending.has(key) || db.get(key) === true; },
        remember(kind, value) { pending.add(`${kind}:${value}`); },
        async save(next) {
            const file = await fs.open(temporary, 'r+');
            try { await file.sync(); } finally { await file.close(); }
            const saved = { ...state, ...next, identity };
            // The transaction atomically saves dedup reservations and the cursor.
            db.transactionSync(() => {
                for (const key of pending) db.putSync(key, true);
                db.putSync('state', saved);
            });
            state = saved;
            this.state = saved;
            pending = new Set();
        },
        async close() { await db.close(); },
    };
}
module.exports = { openStageCache };
