const { randomUUID } = require('crypto');
const { PROHIBITED_CNAES } = require('../database/connection');
const MAX_ROWS = Number.MAX_SAFE_INTEGER;
const OPERATIONS = [
    { id: 'c6', name: 'C6 — Abertura', pipelines: [90] },
    { id: 'santander', name: 'Santander', pipelines: [119] },
    { id: 'pagbank', name: 'PagBank', pipelines: [34] },
    { id: 'mercadopago', name: 'Mercado Pago — Hunter', pipelines: [58] },
];
function defaults() {
    return { id: '', revision: 0, name: 'Novo fluxo', operation: 'c6', pipelines: [90],
        generation: { limit: 100000, uf: [], cidade: [], bairro: [], cnaes: [], naturezas: [], dateFrom: '', dateTo: '', mei: 'all', phone: 'with', email: 'all', situacoes: ['02'] },
        enrichment: { enabled: false, strategy: 'append', fillCpf: false },
        api: { enabled: true, keyMode: 'dupla', delayMs: 60000 },
        cleaning: { enabled: true, rootSource: 'bq', rootFile: '', blocklist: true, invalidPhones: false, removeLandlines: false, fillLivre5: false, prohibitedCnaes: [...PROHIBITED_CNAES].map(value => value.padStart(7, '0')) },
        output: { formatId: 'padrao', csv: false, rowsPerFile: 100000, includeSituacao: false } };
}
function text(value, max = 200) { if (typeof value !== 'string' || value.length > max || value.includes('\0')) throw new Error('Texto inválido na configuração do fluxo.'); return value.trim(); }
function option(value, allowed) { if (!allowed.includes(value)) throw new Error('Opção inválida na configuração do fluxo.'); return value; }
function number(value, min, max) { if (!Number.isSafeInteger(value) || value < min || value > max) throw new Error(`Informe um número inteiro entre ${min} e ${max}.`); return value; }
function list(value, pattern, max = 100) {
    if (!Array.isArray(value) || value.length > max) throw new Error('Lista de filtros inválida.');
    return [...new Set(value.map(item => { const result = text(item, 80); if (!pattern.test(result)) throw new Error('Valor inválido na lista de filtros.'); return result; }))];
}
function date(value) { const result = text(value, 10); const parsed = new Date(result); if (result && (!/^\d{4}-\d{2}-\d{2}$/.test(result) || !Number.isFinite(parsed.getTime()) || parsed.toISOString().slice(0, 10) !== result)) throw new Error('Data inválida.'); return result; }
function locations(value) { return list(typeof value === 'string' ? value.split(',').map(item => item.trim()).filter(Boolean) : value, /^.+$/u, 500); }
function validateFlow(input, existing, { resolveFormat = require('./formats').getFormat } = {}) {
    if (!input || typeof input !== 'object') throw new Error('Fluxo inválido.');
    const base = defaults();
    const g = { ...base.generation, ...input.generation }, e = { ...base.enrichment, ...input.enrichment }, c = { ...base.cleaning, ...input.cleaning }, o = { ...base.output, ...input.output };
    const operation = option(input.operation || base.operation, OPERATIONS.map(item => item.id));
    const name = text(input.name || '', 100); if (!name) throw new Error('Dê um nome ao fluxo.');
    const id = existing?.id || randomUUID();
    let pipelines = input.pipelines || OPERATIONS.find(item => item.id === operation).pipelines;
    if (!Array.isArray(pipelines) || !pipelines.length || pipelines.length > 20) throw new Error('Informe os pipelines da operação.');
    pipelines = [...new Set(pipelines.map(value => number(value, 1, 1000000)))];
    const from = date(g.dateFrom), to = date(g.dateTo); if (from && to && from > to) throw new Error('A data inicial deve preceder a final.');
    const situacoes = list(g.situacoes, /^(01|02|03|04|08)$/); if (!situacoes.length) throw new Error('Escolha ao menos uma situação da Receita.');
    const rootSource = option(c.rootSource, ['bq', 'neon', 'file', 'none']);
    const rootFile = text(c.rootFile || '', 2048);
    if (rootSource === 'file' && c.enabled !== false && !/\.(xlsx|csv)$/i.test(rootFile)) throw new Error('Selecione um arquivo raiz XLSX ou CSV.');
    resolveFormat(o.formatId, { includeSituacao: o.includeSituacao !== false });
    return { id, revision: (existing?.revision || 0) + 1, name, operation, pipelines,
        generation: { limit: g.limit == null || g.limit === '' ? null : number(g.limit, 1, MAX_ROWS), uf: list(g.uf, /^[A-Z]{2}$/i).map(value => value.toUpperCase()), cidade: locations(g.cidade), bairro: locations(g.bairro), cnaes: list(g.cnaes, /^\d{1,7}$/, 500), naturezas: list(g.naturezas, /^\d{1,4}$/, 500), dateFrom: from, dateTo: to,
            mei: option(g.mei, ['all', 'yes', 'no']), phone: option(g.phone, ['all', 'with', 'without']), email: option(g.email, ['all', 'with', 'without']), situacoes },
        enrichment: { enabled: e.enabled === true, strategy: option(e.strategy, ['append', 'overwrite', 'ignore']), fillCpf: e.fillCpf === true },
        api: { enabled: operation === 'c6' && (input.api?.enabled == null || input.api.enabled === true), keyMode: 'dupla', delayMs: 60000 },
        cleaning: { enabled: c.enabled !== false, rootSource, rootFile, blocklist: c.blocklist !== false, invalidPhones: c.invalidPhones === true, removeLandlines: c.removeLandlines === true, fillLivre5: c.fillLivre5 === true, prohibitedCnaes: list(c.prohibitedCnaes, /^\d{1,7}$/, 500) },
        output: { formatId: text(o.formatId, 80), csv: o.csv === true, rowsPerFile: number(o.rowsPerFile, 1, 1000000), includeSituacao: o.includeSituacao !== false } };
}
function effectiveFlow(flow, user, { resolveFormat = require('./formats').getFormat } = {}) {
    const snapshot = JSON.parse(JSON.stringify(flow));
    snapshot.api = { enabled: flow.operation === 'c6' && (flow.api?.enabled == null || flow.api.enabled === true), keyMode: 'dupla', delayMs: 60000 };
    snapshot.cleaning.blocklist = user.username !== 'Davi' || snapshot.cleaning.blocklist;
    snapshot.output.formatSnapshot = resolveFormat(snapshot.output.formatId, { includeSituacao: snapshot.output.includeSituacao, fillCpf: snapshot.enrichment.enabled && snapshot.enrichment.fillCpf });
    return snapshot;
}
module.exports = { defaults, validateFlow, effectiveFlow, OPERATIONS, MAX_ROWS };
