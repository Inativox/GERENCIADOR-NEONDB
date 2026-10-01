'use strict';
const { randomUUID } = require('node:crypto');
const failure = () => Object.assign(new Error('Não foi possível reservar as chaves da API. Confira a conexão do Gerenciador e tente novamente.'), { code: 'FLOW_VALIDATION' });

/** Shared by manual queue and flows. A transaction owns all requested keys or none. */
function createApiLocks({ getPool }) {
    return {
        async acquire(keys, username, mode) {
            if (!keys.length || keys.some(key => !['c6', 'im'].includes(key)) || !username) throw failure();
            const pool = getPool();
            if (!pool) throw failure();
            const token = `${mode}:${randomUUID()}`;
            let client, committed = false;
            try {
                client = await pool.connect();
                await client.query('BEGIN');
                await client.query("SET LOCAL statement_timeout = '15s'");
                await client.query("SET LOCAL lock_timeout = '5s'");
                await client.query('LOCK TABLE api_locks IN SHARE ROW EXCLUSIVE MODE');
                const { rows } = await client.query(`SELECT key_name, username, status, last_heartbeat,
                    EXTRACT(EPOCH FROM (last_heartbeat + INTERVAL '1 minute')) * 1000 AS next_allowed_at
                    FROM api_locks WHERE key_name = ANY($1::text[])
                    AND status = 'Em uso' AND last_heartbeat > NOW() - INTERVAL '2 minutes'`, [keys]);
                if (rows.length) {
                    throw Object.assign(new Error(`A chave ${rows[0].key_name === 'c6' ? 'C6' : 'IM'} está em uso. Aguarde a outra execução terminar e retome.`), { code: 'FLOW_VALIDATION', lockedBy: rows[0].username, key: rows[0].key_name });
                }
                const previous = await client.query(`SELECT EXTRACT(EPOCH FROM (last_heartbeat + INTERVAL '1 minute')) * 1000 AS next_allowed_at
                    FROM api_locks WHERE key_name = ANY($1::text[])`, [keys]);
                const nextAllowedAt = Math.max(0, ...previous.rows.map(row => Number(row.next_allowed_at || 0)));
                for (const key of keys) await client.query(`INSERT INTO api_locks(key_name, username, status, last_heartbeat, key_label, lock_mode)
                    VALUES ($1, $2, 'Em uso', NOW(), $3, $4)
                    ON CONFLICT(key_name) DO UPDATE SET username = EXCLUDED.username, status = 'Em uso',
                    last_heartbeat = NOW(), key_label = EXCLUDED.key_label, lock_mode = EXCLUDED.lock_mode`,
                [key, username, key === 'c6' ? 'Chave 1 (C6)' : 'Chave 2 (IM)', token]);
                await client.query('COMMIT'); committed = true;
                let lost = false, released = false, heartbeat, lastVerifiedAt = Date.now();
                const lease = {
                    keys, token, nextAllowedAt,
                    async assert() {
                        if (Date.now() - lastVerifiedAt >= 110000) lost = true;
                        if (lost || released) throw Object.assign(new Error('A reserva das chaves da API foi perdida. Retome a execução para reservar novamente.'), { code: 'FLOW_VALIDATION' });
                    },
                    async heartbeat() {
                        if (released) return;
                        try {
                            const result = await pool.query({ text: `UPDATE api_locks SET last_heartbeat = NOW()
                                WHERE key_name = ANY($1::text[]) AND lock_mode = $2 AND status = 'Em uso'
                                AND last_heartbeat > NOW() - INTERVAL '2 minutes'`, values: [keys, token], query_timeout: 15000 });
                            if (result.rowCount !== keys.length) lost = true;
                            else lastVerifiedAt = Date.now();
                        } catch { lost = true; }
                        await lease.assert();
                    },
                    async release() {
                        if (released) return;
                        released = true; clearInterval(heartbeat);
                        // Ownership prevents an expired session from releasing a replacement.
                        await pool.query({ text: `UPDATE api_locks SET status = 'Livre', last_heartbeat = NOW()
                            WHERE key_name = ANY($1::text[]) AND lock_mode = $2`, values: [keys, token], query_timeout: 15000 }).catch(() => {});
                    },
                };
                heartbeat = setInterval(() => { void lease.heartbeat().catch(() => {}); }, 20000);
                heartbeat.unref?.();
                return lease;
            } catch (error) {
                if (error.code === 'FLOW_VALIDATION') throw error;
                throw failure();
            } finally {
                if (client) {
                    if (!committed) await client.query('ROLLBACK').catch(() => {});
                    client.release();
                }
            }
        },
    };
}
module.exports = { createApiLocks };
