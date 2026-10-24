const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const axios = require('axios');
const { cnpjText, cnpjCheckDigits } = require('../documentos');
const { credentialPaths, authRequired, runGcloud } = require('./bqAuth');
const ENDPOINT = 'https://bigquery.googleapis.com/bigquery/v2';
const TOKEN_ENDPOINT = 'https://oauth2.googleapis.com/token';
const SCOPE = 'https://www.googleapis.com/auth/bigquery';

// All available historic membership, not only the latest deal/category.
const ROOT_SQL = String.raw`WITH versions AS (
 SELECT id, SAFE_CAST(JSON_VALUE(payload,'$.CATEGORY_ID') AS INT64) AS category_id,
 REGEXP_REPLACE(UPPER(JSON_VALUE(payload,'$.UF_CRM_1637254536351')),r'[^0-9A-Z]','') AS document,
 ingested_at FROM \`mbtech-bronze.bitrix.deals\`
), membership AS (
 SELECT DISTINCT id, category_id FROM versions WHERE category_id IN UNNEST(@pipelines)
 UNION DISTINCT
 SELECT deal_id, category_id FROM \`mbtech-bronze.bitrix.stage_history\` WHERE category_id IN UNNEST(@pipelines)
)
SELECT DISTINCT v.document FROM membership m JOIN versions v USING(id)
WHERE LENGTH(v.document) BETWEEN 12 AND 14 ORDER BY v.document`.replace(/\\`/g, '`');

function restoreCnpj(value) {
    const text = cnpjText(value);
    if (text && /[A-Z]/.test(text)) return cnpjCheckDigits(text) ? text : '';
    if (typeof value === 'string' && /[A-Z]/i.test(value)) return '';
    const digits = String(value || '').replace(/\D/g, '');
    if (/^\d{14}$/.test(digits) && !/^(\d)\1+$/.test(digits)) return digits;
    if (!/^\d{12,13}$/.test(digits)) return '';
    const padded = digits.padStart(14, '0');
    if (/^(\d)\1+$/.test(padded)) return '';
    const check = size => {
        let sum = 0, weight = size - 7;
        for (let i = 0; i < size; i++) { sum += Number(padded[i]) * weight--; if (weight < 2) weight = 9; }
        const remainder = sum % 11; return remainder < 2 ? 0 : 11 - remainder;
    };
    return check(12) === Number(padded[12]) && check(13) === Number(padded[13]) ? padded : '';
}
function existingCredentials(keyFile, environment = process.env) {
    const { source } = credentialPaths(keyFile, environment);
    if (!source) return null;
    const content = JSON.parse(fs.readFileSync(source, 'utf8'));
    if (!['service_account', 'authorized_user'].includes(content.type)) throw new Error('Tipo de acesso BQ não suportado. Importe uma credencial local ou configure ADC.');
    return content;
}
const RESULT_ATTEMPTS = 3;
const RANGE_ROWS = 20000;
const RESULT_RETRY_DELAYS_MS = [1000, 4000];
function bytesLimitExceeded(details) {
    const reasons = [details?.reason, ...(details?.errors || []).map(item => item?.reason)];
    return reasons.includes('bytesBilledLimitExceeded');
}
function pause(ms, signal) {
    return new Promise(resolve => {
        const timer = setTimeout(resolve, ms);
        signal?.addEventListener?.('abort', () => { clearTimeout(timer); resolve(); }, { once: true });
    });
}
function createBqClient({ keyFile, project = 'mbtech-bronze', location = 'southamerica-east1', http = axios, credentialsProvider = existingCredentials, commandRunner, retryDelaysMs = RESULT_RETRY_DELAYS_MS, sleep = pause } = {}) {
    let token, validUntil = 0, mode = 'gcloud';
    const accessToken = async () => {
        if (token && Date.now() < validUntil) return token;
        try {
            const credential = credentialsProvider(keyFile);
            const { source, adc } = credentialPaths(keyFile);
            mode = credential?.type === 'service_account' ? 'service_account' : credential ? (source && path.resolve(source) !== path.resolve(adc) ? 'imported' : 'adc') : 'gcloud';
            if (credential) {
                let body;
                if (credential.type === 'service_account') {
                    const now = Math.floor(Date.now() / 1000);
                    const encode = value => Buffer.from(JSON.stringify(value)).toString('base64url');
                    const unsigned = `${encode({ alg: 'RS256', typ: 'JWT' })}.${encode({ iss: credential.client_email, scope: SCOPE, aud: TOKEN_ENDPOINT, iat: now, exp: now + 3600 })}`;
                    const signature = crypto.sign('RSA-SHA256', Buffer.from(unsigned), credential.private_key).toString('base64url');
                    body = new URLSearchParams({ grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer', assertion: unsigned + '.' + signature });
                } else {
                    body = new URLSearchParams({ grant_type: 'refresh_token', client_id: credential.client_id, client_secret: credential.client_secret, refresh_token: credential.refresh_token });
                }
                const response = await http.post(TOKEN_ENDPOINT, body.toString(), { timeout: 20000, headers: { 'Content-Type': 'application/x-www-form-urlencoded' } });
                token = response.data.access_token;
                validUntil = Date.now() + Math.max(0, Number(response.data.expires_in || 3600) - 60) * 1000;
            } else {
                const response = await runGcloud(['auth', 'print-access-token', '--quiet'], { commandRunner });
                token = response.stdout.trim(); validUntil = Date.now() + 45 * 60000;
            }
            if (!token || /\s/.test(token)) throw new Error();
            return token;
        } catch (error) {
            token = null; validUntil = 0;
            const oauthError = error.response?.data?.error;
            const cliError = String(error.stderr || '');
            if (['invalid_grant', 'invalid_rapt'].includes(oauthError) || (mode === 'gcloud' && /invalid_grant|invalid_rapt|reauthentication|reauth|revoked|expired|gcloud auth login|no active account|no.*account.*selected/i.test(cliError))) throw authRequired(mode);
            throw new Error('Acesso ao BigQuery indisponível. Verifique a conexão e a credencial local ou o Google Cloud CLI desta máquina.');
        }
    };
    const request = async (method, url, { data, signal, params } = {}, retried = false) => {
        if (signal?.aborted) throw new Error('Consulta BQ cancelada.');
        const authorization = await accessToken();
        try { return (await http.request({ method, url: ENDPOINT + url, data, params, signal, timeout: 35000, headers: { Authorization: 'Bearer ' + authorization } })).data; }
        catch (error) {
            if (signal?.aborted) throw new Error('Consulta BQ cancelada.');
            if (error.response?.status === 401) {
                token = null; validUntil = 0;
                if (!retried) return request(method, url, { data, signal, params }, true);
                throw authRequired(mode);
            }
            if (error.response?.status === 403) throw Object.assign(new Error('Sua conta Google não tem permissão para consultar o BQ. Verifique o acesso aos dados e ao projeto de consultas.'), { code: 'BQ_FORBIDDEN' });
            if (bytesLimitExceeded(error.response?.data?.error)) throw Object.assign(new Error('A consulta BQ passou do limite de bytes configurado.'), { code: 'BQ_BYTES_LIMIT' });
            // 5xx or no response at all (network drop, timeout): transient, may be retried.
            const status = error.response?.status;
            const retryable = status >= 500 || (!error.response && error.code !== 'ERR_CANCELED');
            throw Object.assign(new Error('Falha ao consultar o BQ. Verifique conexão, schema e limite de bytes.'), { retryable });
        }
    };
    // Result-page GETs only: up to RESULT_ATTEMPTS tries on transient errors, never on 400/401/403 or abort.
    const getWithRetry = async (url, options) => {
        for (let attempt = 1; ; attempt++) {
            try { return await request('GET', url, options); } catch (error) {
                if (!error.retryable || attempt >= RESULT_ATTEMPTS || options.signal?.aborted) throw error;
                await sleep(retryDelaysMs[Math.min(attempt - 1, retryDelaysMs.length - 1)], options.signal);
                if (options.signal?.aborted) throw new Error('Consulta BQ cancelada.');
            }
        }
    };
    const toObjects = (fields, rows) => rows.map(row => Object.fromEntries((row.f || []).map((cell, index) => [fields[index]?.name ?? String(index), cell?.v ?? null])));
    const cancelJob = jobId => request('POST', `/projects/${project}/jobs/${jobId}/cancel`, { params: { location } }).catch(() => {});
    // A session keeps temporary tables between queries; its jobs run one at a time.
    const sessionFields = (session, createSession) => ({ ...(createSession ? { createSession: true } : {}), ...(session ? { connectionProperties: [{ key: 'session_id', value: session }] } : {}) });
    // Result pages read by row offset, `parallel` at a time, and delivered one at a time as they arrive:
    // at most `parallel` pages wait in memory. Each answer stops near 10 MB, so a short page asks again for the rest.
    const fetchRanges = async ({ jobId, fields, from, total, parallel, signal, onPage }, state) => {
        const pending = [];
        for (let start = from; start < total; start += RANGE_ROWS) pending.push([start, Math.min(RANGE_ROWS, total - start)]);
        let chain = Promise.resolve(), failed = false;
        const deliver = async rows => {
            const previous = chain; let release;
            chain = new Promise(resolve => { release = resolve; });
            try { await previous; if (!failed) await onPage(rows); state.rows += rows.length; } finally { release(); }
        };
        const worker = async () => {
            try {
                while (pending.length && !failed) {
                    if (signal?.aborted) throw new Error('Consulta BQ cancelada.');
                    const [start, size] = pending.shift();
                    const page = await getWithRetry(`/projects/${project}/queries/${jobId}`, { signal, params: { location, startIndex: String(start), maxResults: size } });
                    const got = (page.rows || []).length;
                    if (!got) throw new Error('O BQ devolveu uma página vazia. Tente novamente.');
                    if (got < size) pending.unshift([start + got, size - got]);
                    await deliver(toObjects(fields, page.rows));
                }
            } catch (error) { failed = true; throw error; }
        };
        await Promise.all(Array.from({ length: parallel }, worker));
    };
    const runPages = async ({ query, params, signal, pageSize, onPage, maxBytesBilled, jobTimeoutMs, waitMs, session, createSession, parallel = 1 }, state) => {
        const body = { query, useLegacySql: false, location, parameterMode: 'NAMED', queryParameters: params, maximumBytesBilled: String(maxBytesBilled), timeoutMs: 10000, maxResults: pageSize, ...sessionFields(session, createSession) };
        // Let submission finish so an aborted local request does not lose the remote job ID.
        let result = await request('POST', `/projects/${project}/queries`, { data: { ...body, jobTimeoutMs: String(jobTimeoutMs) } });
        const started = Date.now();
        state.reference = result.jobReference;
        state.session = result.sessionInfo?.sessionId || session;
        while (true) {
            if (signal?.aborted) throw new Error('Consulta BQ cancelada.');
            if (result.errors?.length) {
                if (result.errors.some(item => bytesLimitExceeded(item))) throw Object.assign(new Error('A consulta BQ passou do limite de bytes configurado.'), { code: 'BQ_BYTES_LIMIT' });
                throw new Error('A consulta BQ falhou. Verifique o schema e as permissões.');
            }
            if (!result.jobComplete) {
                if (!state.reference?.jobId || Date.now() - started > waitMs) throw new Error('Consulta BQ demorou demais. Tente novamente.');
                result = await getWithRetry(`/projects/${project}/queries/${state.reference.jobId}`, { signal, params: { location, timeoutMs: 10000, maxResults: pageSize } });
                continue;
            }
            const rows = toObjects(result.schema?.fields || [], result.rows || []);
            state.rows += rows.length; state.bytes = Number(result.totalBytesProcessed || state.bytes);
            await onPage(rows);
            if (!result.pageToken) break;
            if (parallel > 1 && Number(result.totalRows) > state.rows) {
                await fetchRanges({ jobId: state.reference.jobId, fields: result.schema?.fields || [], from: state.rows, total: Number(result.totalRows), parallel, signal, onPage }, state);
                break;
            }
            result = await getWithRetry(`/projects/${project}/queries/${state.reference.jobId}`, { signal, params: { location, pageToken: result.pageToken, maxResults: pageSize } });
        }
    };
    const queryPages = async ({ query, params = [], signal, pageSize = 50000, onPage = async () => {}, maxBytesBilled = '1000000000', jobTimeoutMs = '180000', waitMs = 180000, session, createSession = false, parallel = 1 } = {}) => {
        if (signal?.aborted) throw new Error('Consulta BQ cancelada.');
        const state = { rows: 0, bytes: 0, reference: undefined, session: undefined };
        let complete = false;
        try {
            await runPages({ query, params, signal, pageSize, onPage, maxBytesBilled, jobTimeoutMs, waitMs, session, createSession, parallel }, state);
            complete = true;
            return { rows: state.rows, jobId: state.reference?.jobId, bytes: state.bytes, ...(state.session ? { session: state.session } : {}) };
        } catch (error) {
            if (error.code === 'BQ_FORBIDDEN') throw Object.assign(new Error('Sem acesso à Receita no BigQuery. Use a fonte Neon neste fluxo ou peça acesso ao dataset receita.'), { code: 'FLOW_VALIDATION' });
            throw error;
        } finally {
            if (!complete && state.reference?.jobId) await cancelJob(state.reference.jobId);
        }
    };
    const loadRoot = async (pipelines, { signal, onProgress = () => {} } = {}) => {
        if (!Array.isArray(pipelines) || !pipelines.length || pipelines.some(id => !Number.isSafeInteger(id) || id < 1)) throw new Error('Pipelines inválidos para raiz BQ.');
        const queryParameters = [{ name: 'pipelines', parameterType: { type: 'ARRAY', arrayType: { type: 'INT64' } }, parameterValue: { arrayValues: pipelines.map(value => ({ value: String(value) })) } }];
        const estimate = await request('POST', `/projects/${project}/queries`, { data: { query: ROOT_SQL, useLegacySql: false, location, parameterMode: 'NAMED', queryParameters, maximumBytesBilled: '1000000000', dryRun: true }, signal });
        const bytes = Number(estimate.totalBytesProcessed || 0);
        if (bytes > 1000000000) throw new Error('Consulta da raiz excede o limite de 1 GB. Revise a fonte antes de executar.');
        if (signal?.aborted) throw new Error('Consulta BQ cancelada.');
        const documents = new Set(); let skipped = 0, restored = 0, jobId;
        const onPage = async rows => {
            for (const row of rows) {
                const raw = row.document ?? Object.values(row)[0];
                const normalized = restoreCnpj(raw);
                if (normalized) { documents.add(normalized); if (String(raw).length < 14) restored++; } else skipped++;
                if (documents.size > 1000000) throw new Error('A raiz BQ excede 1 milhão de documentos. Revise a fonte antes de executar.');
            }
            onProgress({ documents: documents.size });
        };
        try {
            ({ jobId } = await queryPages({ query: ROOT_SQL, params: queryParameters, signal, pageSize: 10000, onPage }));
        } catch (error) {
            if (error.code === 'FLOW_VALIDATION') throw Object.assign(new Error('Sua conta Google não tem permissão para consultar o BQ. Verifique o acesso aos dados e ao projeto de consultas.'), { code: 'BQ_FORBIDDEN' });
            throw error;
        }
        if (!documents.size) {
            if (jobId) await cancelJob(jobId);
            throw new Error('A raiz BQ não retornou documentos utilizáveis. A limpeza foi interrompida.');
        }
        return { documents: [...documents], info: { source: 'bq', pipelines, queriedAt: new Date().toISOString(), count: documents.size, skipped, restored, estimatedBytes: bytes, jobId, coverage: 'histórico disponível de negócios e fases; documentos ausentes não podem ser recuperados' } };
    };
    // Ends a session and drops its temporary tables.
    const abortSession = async session => {
        try { await request('POST', `/projects/${project}/queries`, { data: { query: 'CALL BQ.ABORT_SESSION()', useLegacySql: false, location, timeoutMs: 10000, ...sessionFields(session) } }); }
        catch { /* An unreachable session still expires by itself. */ }
    };
    return { loadRoot, queryPages, abortSession, test: async () => {
        await request('GET', '/projects/mbtech-bronze/datasets/bitrix/tables/deals');
        await request('GET', '/projects/mbtech-bronze/datasets/bitrix/tables/stage_history');
        const estimate = await request('POST', `/projects/${project}/queries`, { data: { query: ROOT_SQL, useLegacySql: false, location, dryRun: true, parameterMode: 'NAMED', queryParameters: [{ name: 'pipelines', parameterType: { type: 'ARRAY', arrayType: { type: 'INT64' } }, parameterValue: { arrayValues: [{ value: '90' }] } }] } });
        if (Number(estimate.totalBytesProcessed || 0) > 1000000000) throw new Error('Consulta da raiz excede o limite de 1 GB. Revise a fonte antes de executar.');
        return { success: true, message: 'Acesso BQ e consulta histórica Bitrix validados sem gerar listas.', estimatedBytes: Number(estimate.totalBytesProcessed || 0) };
    } };
}
module.exports = { ROOT_SQL, restoreCnpj, createBqClient, existingCredentials };
