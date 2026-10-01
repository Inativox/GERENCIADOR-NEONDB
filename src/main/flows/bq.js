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
function createBqClient({ keyFile, project = 'mbtech-bronze', location = 'southamerica-east1', http = axios, credentialsProvider = existingCredentials, commandRunner } = {}) {
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
            throw new Error('Falha ao consultar o BQ. Verifique conexão, schema e limite de bytes.');
        }
    };
    const loadRoot = async (pipelines, { signal, onProgress = () => {} } = {}) => {
        if (!Array.isArray(pipelines) || !pipelines.length || pipelines.some(id => !Number.isSafeInteger(id) || id < 1)) throw new Error('Pipelines inválidos para raiz BQ.');
        const body = { query: ROOT_SQL, useLegacySql: false, location, parameterMode: 'NAMED', queryParameters: [{ name: 'pipelines', parameterType: { type: 'ARRAY', arrayType: { type: 'INT64' } }, parameterValue: { arrayValues: pipelines.map(value => ({ value: String(value) })) } }], maximumBytesBilled: '1000000000', timeoutMs: 10000, maxResults: 10000 };
        const estimate = await request('POST', `/projects/${project}/queries`, { data: { ...body, dryRun: true }, signal });
        const bytes = Number(estimate.totalBytesProcessed || 0);
        if (bytes > 1000000000) throw new Error('Consulta da raiz excede o limite de 1 GB. Revise a fonte antes de executar.');
        if (signal?.aborted) throw new Error('Consulta BQ cancelada.');
        let reference;
        let complete = false;
        try {
        // Let submission finish so an aborted local request does not lose the remote job ID.
        // The server also bounds its execution if the network fails before returning the ID.
        let result = await request('POST', `/projects/${project}/queries`, { data: { ...body, jobTimeoutMs: '180000' } });
        reference = result.jobReference;
        const documents = new Set(); let skipped = 0, restored = 0;
        const started = Date.now();
        while (true) {
            if (signal?.aborted) {
                throw new Error('Consulta BQ cancelada.');
            }
            if (result.errors?.length) throw new Error('A consulta da raiz BQ falhou. Verifique o schema e as permissões.');
            if (!result.jobComplete) {
                if (!reference?.jobId || Date.now() - started > 180000) throw new Error('Consulta da raiz BQ demorou demais. Tente novamente.');
                result = await request('GET', `/projects/${project}/queries/${reference.jobId}`, { signal, params: { location, timeoutMs: 10000, maxResults: 10000 } });
                continue;
            }
            for (const row of result.rows || []) {
                const raw = row.f?.[0]?.v;
                const normalized = restoreCnpj(raw);
                if (normalized) { documents.add(normalized); if (String(raw).length < 14) restored++; } else skipped++;
                if (documents.size > 1000000) throw new Error('A raiz BQ excede 1 milhão de documentos. Revise a fonte antes de executar.');
            }
            onProgress({ documents: documents.size });
            if (!result.pageToken) break;
            result = await request('GET', `/projects/${project}/queries/${reference.jobId}`, { signal, params: { location, pageToken: result.pageToken, maxResults: 10000 } });
        }
        if (!documents.size) throw new Error('A raiz BQ não retornou documentos utilizáveis. A limpeza foi interrompida.');
        complete = true;
        return { documents: [...documents], info: { source: 'bq', pipelines, queriedAt: new Date().toISOString(), count: documents.size, skipped, restored, estimatedBytes: bytes, jobId: reference?.jobId, coverage: 'histórico disponível de negócios e fases; documentos ausentes não podem ser recuperados' } };
        } finally {
            if (!complete && reference?.jobId) await request('POST', `/projects/${project}/jobs/${reference.jobId}/cancel`, { params: { location } }).catch(() => {});
        }
    };
    return { loadRoot, test: async () => {
        await request('GET', '/projects/mbtech-bronze/datasets/bitrix/tables/deals');
        await request('GET', '/projects/mbtech-bronze/datasets/bitrix/tables/stage_history');
        const estimate = await request('POST', `/projects/${project}/queries`, { data: { query: ROOT_SQL, useLegacySql: false, location, dryRun: true, parameterMode: 'NAMED', queryParameters: [{ name: 'pipelines', parameterType: { type: 'ARRAY', arrayType: { type: 'INT64' } }, parameterValue: { arrayValues: [{ value: '90' }] } }] } });
        if (Number(estimate.totalBytesProcessed || 0) > 1000000000) throw new Error('Consulta da raiz excede o limite de 1 GB. Revise a fonte antes de executar.');
        return { success: true, message: 'Acesso BQ e consulta histórica Bitrix validados sem gerar listas.', estimatedBytes: Number(estimate.totalBytesProcessed || 0) };
    } };
}
module.exports = { ROOT_SQL, restoreCnpj, createBqClient, existingCredentials };
