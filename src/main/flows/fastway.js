'use strict';

const fs = require('node:fs');
const fsp = fs.promises;
const os = require('node:os');
const path = require('node:path');
const axios = require('axios');
const FormData = require('form-data');
const { parse } = require('csv-parse');
const { finished } = require('node:stream/promises');

const ENDPOINT = 'https://mbfinance2.fastssl.com.br/api/mailingup/index.php';
const MAX_ROWS = 250000;
const REQUIRED_COLUMNS = ['nome', 'cpf', 'fone1'];
const clean = value => String(value ?? '').replace(/[;"\r\n]/g, match => match === ';' ? ',' : ' ').trim();
const safeError = message => Object.assign(new Error(message), { code: 'FLOW_VALIDATION' });

/** Checks a manually chosen CSV before upload, so the Fastway never receives a file it would reject. */
async function inspectCsv(file) {
    const input = fs.createReadStream(file);
    const parser = parse({ bom: true, delimiter: ';', skip_empty_lines: true, relax_quotes: true });
    input.on('error', error => parser.destroy(error));
    input.pipe(parser);
    let header = null, rows = 0;
    try {
        for await (const record of parser) {
            if (header) { if (++rows > MAX_ROWS) throw safeError(`A Fastway aceita até ${MAX_ROWS.toLocaleString('pt-BR')} registros por arquivo.`); continue; }
            header = record.map(value => value.trim().toLowerCase());
            if (header.length === 1 && header[0].includes(',')) throw safeError('O CSV precisa ser separado por ponto e vírgula (;).');
            const missing = REQUIRED_COLUMNS.filter(column => !header.includes(column));
            if (missing.length) throw safeError(`O cabeçalho precisa ter as colunas nome, cpf e fone1. Falta: ${missing.join(', ')}.`);
        }
    } catch (error) {
        if (String(error.code).startsWith('CSV_')) throw safeError(`Não foi possível ler a linha ${error.lines} do CSV. Confira se ela tem as mesmas colunas do cabeçalho.`);
        throw error;
    } finally { input.destroy(); }
    if (!rows) throw safeError('O CSV está vazio ou sem registros.');
    return { rows };
}

async function apiCsv(source) {
    const target = path.join(os.tmpdir(), `gerenciador-fastway-${process.pid}-${Date.now()}-${Math.random().toString(16).slice(2)}.csv`);
    const output = fs.createWriteStream(target, { flags: 'wx', mode: 0o600 });
    const input = fs.createReadStream(source);
    const parser = parse({ bom: true, delimiter: ';', skip_empty_lines: true, relax_quotes: true });
    input.on('error', error => parser.destroy(error));
    input.pipe(parser);
    const done = finished(output);
    done.catch(() => {});
    try {
        let rows = 0;
        for await (const record of parser) {
            if (rows === 0) await write(output, '\uFEFF');
            await write(output, record.map(clean).join(';') + '\r\n');
            rows++;
        }
        if (rows < 2) throw new Error('O CSV Fastway está vazio ou sem registros.');
        await new Promise(resolve => output.end(resolve));
        await done;
        return { path: target, rows: rows - 1 };
    } catch (error) {
        input.destroy(); parser.destroy(); output.destroy(); await fsp.rm(target, { force: true });
        throw error;
    }
}
// The Fastway explains its own HTTP codes (such as 481) in the body; keep a short plain-text excerpt without the password.
function answerExcerpt(data, password) {
    const text = (typeof data === 'string' ? data : JSON.stringify(data ?? '')).replace(/<[^>]*>/g, ' ').split(password).join('***').replace(/\s+/g, ' ').trim();
    return text && text !== '""' ? ` (${text.slice(0, 200)})` : '';
}
function write(stream, value) {
    return new Promise((resolve, reject) => stream.write(value, error => error ? reject(error) : resolve()));
}

async function upload({ file, campaignId, credentials, signal }) {
    if (!credentials?.login || !credentials?.password) throw new Error('Configure as credenciais Fastway nos acessos do fluxo.');
    if (!/^[1-9]\d{0,8}$/.test(String(campaignId || ''))) throw new Error('Informe um ID de serviço Fastway válido no fluxo.');
    const converted = await apiCsv(file);
    try {
        if (converted.rows > MAX_ROWS) throw new Error(`A Fastway aceita até ${MAX_ROWS.toLocaleString('pt-BR')} registros por arquivo.`);
        const form = new FormData();
        form.append('login', credentials.login);
        form.append('senha', credentials.password);
        form.append('CampanhaId', String(campaignId));
        form.append('uploading', '42');
        form.append('auto_ddd', 'off');
        form.append('operacaoId', '0');
        form.append('json', 'S');
        form.append('arquivo', fs.createReadStream(converted.path), { filename: path.basename(file), contentType: 'text/csv' });
        const response = await axios.post(ENDPOINT, form, { headers: form.getHeaders(), maxBodyLength: Infinity, maxContentLength: Infinity, timeout: 30 * 60 * 1000, signal });
        const data = response.data;
        if (typeof data === 'string') {
            let parsed;
            try { parsed = JSON.parse(data); } catch { throw new Error('A Fastway retornou uma resposta inesperada. Verifique o painel antes de tentar novamente.'); }
            if (/erro|error|falh|inv[aá]lid|negad/i.test(JSON.stringify(parsed))) throw new Error('A Fastway recusou o mailing. Verifique o serviço, o layout e as credenciais.');
        } else if (data?.success === false || data?.erro || data?.error) {
            throw new Error('A Fastway recusou o mailing. Verifique o serviço, o layout e as credenciais.');
        }
        return { rows: converted.rows, file: path.basename(file) };
    } catch (error) {
        if (error.code === 'FLOW_CANCELLED' || error.name === 'AbortError' || signal?.aborted) throw Object.assign(new Error('Envio cancelado.'), { code: 'FLOW_CANCELLED' });
        if (error.response) throw safeError(`A Fastway respondeu HTTP ${error.response.status}${answerExcerpt(error.response.data, credentials.password)}. Confirme no painel se o mailing foi recebido antes de retomar para evitar duplicidade.`);
        if (error.code === 'FLOW_VALIDATION') throw error;
        throw safeError('Não foi possível confirmar a resposta da Fastway. Confira o painel antes de retomar, pois o servidor pode ter recebido o arquivo.');
    } finally { await fsp.rm(converted.path, { force: true }); }
}

module.exports = { upload, inspectCsv, MAX_ROWS };
