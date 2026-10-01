'use strict';
const fs = require('node:fs/promises');
const path = require('node:path');
const crypto = require('node:crypto');
const { setTimeout: wait } = require('node:timers/promises');
const { cnpjText } = require('../documentos');

const DELAY_MS = 60000;
const BATCH_SIZE = 40000;
const MAX_ATTEMPTS = 5;
const TOKEN_URL = 'https://crm-leads-p.c6bank.info/querie-partner/token';
const CONSULT_URL = 'https://crm-leads-p.c6bank.info/querie-partner/client/avaliable';
function failure(message, retryable = false) {
    return Object.assign(new Error(message), { code: 'FLOW_VALIDATION', retryable });
}
function cancelled(signal) {
    if (signal?.aborted) throw Object.assign(new Error('Execução cancelada.'), { code: 'FLOW_CANCELLED' });
}
function document(value) {
    if (typeof value === 'number' && (!Number.isSafeInteger(value) || value < 0)) return '';
    let text = String(value ?? '').trim();
    if (/^\d{1,14}$/.test(text)) text = text.padStart(14, '0');
    return cnpjText(text);
}
function createApiClient({ credentials, http = require('axios') }) {
    for (const key of ['c6', 'im']) {
        if (!credentials?.[key]?.clientId || !credentials?.[key]?.clientSecret) {
            throw failure('Importe uma licença de API válida com as duas chaves C6/IM na tela de login.');
        }
    }
    return {
        async consult(documents, key, signal) {
            cancelled(signal);
            if (!['c6', 'im'].includes(key) || !documents.length || documents.length > 20000 || documents.some(value => !cnpjText(value))) {
                throw failure('Lote de CNPJs inválido para a consulta API.');
            }
            try {
                const credential = credentials[key];
                const form = new URLSearchParams({ grant_type: 'client_credentials', client_id: credential.clientId, client_secret: credential.clientSecret });
                const auth = await http.post(TOKEN_URL, form.toString(), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 30000, signal });
                if (typeof auth.data?.access_token !== 'string' || !auth.data.access_token) throw failure('A API não retornou um token válido. Confira a licença.');
                cancelled(signal);
                const response = await http.post(CONSULT_URL, { CNPJ: documents }, { headers: { Authorization: `Bearer ${auth.data.access_token}`, 'Content-Type': 'application/json' }, timeout: 30000, signal });
                cancelled(signal);
                const data = response.data;
                if (!data || typeof data !== 'object' || Array.isArray(data)) throw failure('Resposta inválida da API. Nenhum CNPJ deste lote foi classificado.');
                const fields = Object.keys(data).filter(name => /cnpj/i.test(name) && Array.isArray(data[name]));
                if (fields.length !== 1) throw failure('Resposta inválida da API. Nenhum CNPJ deste lote foi classificado.');
                const requested = new Set(documents), available = new Set();
                for (const value of data[fields[0]]) {
                    const cnpj = document(value);
                    if (!cnpj || !requested.has(cnpj)) throw failure('A API retornou CNPJs inesperados. Nenhum CNPJ deste lote foi classificado.');
                    available.add(cnpj);
                }
                return available;
            } catch (error) {
                cancelled(signal);
                if (error.code === 'FLOW_VALIDATION') throw error;
                const status = error.response?.status;
                if (status === 401 || status === 403) throw failure('A API recusou a licença. Verifique ou importe novamente as chaves C6/IM.');
                if (status && status !== 429 && status < 500) throw failure('A API recusou o lote de CNPJs. Confira o suporte a esses documentos antes de retomar.');
                throw failure('A consulta API falhou temporariamente. Verifique a conexão e tente retomar.', true);
            }
        },
    };
}
async function atomic(file, value) {
    await fs.writeFile(file + '.tmp', JSON.stringify(value), { mode: 0o600 });
    await fs.rename(file + '.tmp', file);
}
async function read(file) {
    try { return JSON.parse(await fs.readFile(file, 'utf8')); }
    catch (error) { if (error.code === 'ENOENT') return null; throw error; }
}

/** Per-key durable results allow the successful half to survive a failed round. */
async function runApiStage({ jobDir, source, batches, emit, signal, counts, update, acquire,
    now = Date.now, sleep = (ms, signal) => wait(ms, undefined, { signal }), batchSize = BATCH_SIZE }) {
    if (!Number.isInteger(batchSize) || batchSize < 2 || batchSize > BATCH_SIZE) throw failure('Tamanho de lote API inválido.');
    const directory = path.join(jobDir, 'api-results');
    await fs.mkdir(directory, { recursive: true });
    const pacingFile = path.join(directory, 'pacing.json');
    let pacing = await read(pacingFile), lease;
    let nextAllowedAt = Number(pacing?.nextAllowedAt || 0), round = 0;
    for (const key of ['apiConsulted', 'apiAvailable', 'apiClients', 'apiBatches']) counts[key] = 0;
    async function pause() {
        cancelled(signal);
        const remaining = nextAllowedAt - now();
        if (remaining > 0) {
            update({ log: `API: aguardando ${Math.ceil(remaining / 1000)} segundos para a próxima consulta.` });
            await sleep(remaining, signal);
        }
        cancelled(signal);
    }
    try {
        for await (const rows of batches(source, signal, batchSize)) {
            cancelled(signal);
            const middle = Math.ceil(rows.length / 2);
            const parts = [rows.slice(0, middle), rows.slice(middle)].map((records, index) => ({ records, key: index === 0 ? 'c6' : 'im',
                file: path.join(directory, `${String(round).padStart(8, '0')}-${index}.json`),
                fingerprint: crypto.createHash('sha256').update(JSON.stringify(records)).digest('hex') })).filter(part => part.records.length);
            for (const part of parts) {
                part.result = await read(part.file);
                const requested = new Set(part.records.map(row => row.cnpj));
                if (part.result && (part.result.fingerprint !== part.fingerprint || !Array.isArray(part.result.available) || part.result.rows !== part.records.length
                    || part.result.available.some(value => !requested.has(value)) || new Set(part.result.available).size !== part.result.available.length)) {
                    throw failure('O resultado salvo da API não corresponde ao lote. Inicie uma nova execução.');
                }
                if (part.result) nextAllowedAt = Math.max(nextAllowedAt, Number(part.result.nextAllowedAt || 0));
            }
            for (let attempt = 1; parts.some(part => !part.result); attempt++) {
                if (!lease) {
                    if (!acquire) throw failure('A sessão da API não está disponível. Reabra o aplicativo e verifique a licença.');
                    lease = await acquire(signal);
                    nextAllowedAt = Math.max(nextAllowedAt, Number(lease.nextAllowedAt || 0));
                }
                await pause();
                await lease.assert?.();
                cancelled(signal);
                // If the process dies in-flight, account for token + query timeouts before retrying.
                await atomic(pacingFile, { nextAllowedAt: now() + 120000 });
                update({ log: `API: lote ${round + 1}, chave dupla, tentativa ${attempt}.` });
                const pending = parts.filter(part => !part.result);
                const results = await Promise.allSettled(pending.map(async part => {
                    const available = await lease.consult(part.records.map(row => row.cnpj), part.key, signal);
                    cancelled(signal);
                    await lease.assert?.();
                    const requested = new Set(part.records.map(row => row.cnpj));
                    if (!(available instanceof Set) || [...available].some(value => !requested.has(value))) throw failure('Resposta inválida da API. Nenhum CNPJ deste lote foi classificado.');
                    const result = { fingerprint: part.fingerprint, rows: part.records.length, available: [...available], checkedAt: new Date(now()).toISOString(), nextAllowedAt: now() + DELAY_MS };
                    await atomic(part.file, result);
                    part.result = result;
                }));
                nextAllowedAt = now() + DELAY_MS;
                await atomic(pacingFile, { nextAllowedAt });
                cancelled(signal);
                const rejected = results.filter(result => result.status === 'rejected');
                if (rejected.length) {
                    const permanent = rejected.find(result => !result.reason?.retryable);
                    if (permanent) throw permanent.reason;
                    if (attempt >= MAX_ATTEMPTS) throw failure('A API falhou após 5 tentativas. Retome a execução para continuar dos lotes confirmados.');
                    update({ log: 'API: falha temporária. Aguardando 1 minuto; resultados confirmados serão preservados.' });
                }
            }
            for (const part of parts) {
                const available = new Set(part.result.available);
                counts.apiConsulted += part.records.length;
                counts.apiAvailable += available.size;
                counts.apiClients += part.records.length - available.size;
                for (const row of part.records) if (available.has(row.cnpj)) await emit({ ...row, status_api: 'disponivel' });
            }
            counts.apiBatches++;
            round++;
            update({ log: `API: lote ${round} confirmado. Disponíveis: ${counts.apiAvailable}; clientes: ${counts.apiClients}.` });
        }
    } finally { if (lease) await lease.release(); }
}
module.exports = { createApiClient, runApiStage, DELAY_MS, BATCH_SIZE };
