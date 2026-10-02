'use strict';

const fs = require('node:fs');
const fsp = fs.promises;
const path = require('node:path');
const crypto = require('node:crypto');
const { records, entries } = require('./jsonl');
const { openStageCache } = require('./stageCache');
const { createPackedWriter, writeBuffered } = require('./packedJsonl');
const { finished } = require('node:stream/promises');
const ExcelJS = require('exceljs');
const { readOnlyPoolOptions } = require('./postgres');
const { normalizarDocumento } = require('../limpezaTelefones');
const { normalizarTelefoneFluxo, variantesTelefoneFluxo, telefoneParaGeracao } = require('./telefones');
const { PROHIBITED_CNAES } = require('../database/connection');

const BATCH_SIZE = 2000;
const STAGES = ['generation', 'enrichment', 'api', 'cleaning', 'export'];
function abort(signal) {
    if (signal?.aborted) {
        const error = new Error('Execução cancelada.');
        error.code = 'FLOW_CANCELLED';
        throw error;
    }
}
function friendly(stage, error) {
    if (error.code === 'FLOW_CANCELLED' || error.name === 'AbortError') {
        const cancelled = new Error('Execução cancelada.');
        cancelled.code = 'FLOW_CANCELLED';
        return cancelled;
    }
    if (error.code === 'FLOW_VALIDATION') return error;
    if (['ENOSPC', 'EDQUOT', 'FLOW_DISK_FULL'].includes(error.code)) {
        return Object.assign(new Error('Não há espaço disponível para continuar. Libere espaço no disco do cache ou da saída e retome do último lote confirmado.'), { code: 'FLOW_DISK_FULL' });
    }
    const messages = {
        generation: 'Não foi possível gerar a base da Receita. Verifique a conexão e os filtros.',
        enrichment: 'Não foi possível enriquecer a base. Verifique a conexão e o esquema do banco.',
        cleaning: 'Não foi possível concluir a limpeza. Verifique a raiz e os filtros de telefone.',
        api: 'Não foi possível validar a disponibilidade na API. Retome dos lotes confirmados; nenhum arquivo final foi gerado.',
        export: 'Não foi possível salvar os arquivos finais. Verifique a pasta e o espaço disponível.'
    };
    const result = new Error(messages[stage] || 'Não foi possível executar o fluxo.');
    result.code = 'FLOW_FAILED';
    return result;
}
function validation(message) {
    const error = new Error(message);
    error.code = 'FLOW_VALIDATION';
    return error;
}
async function atomicJson(file, data) {
    const temporary = `${file}.tmp`;
    const handle = await fsp.open(temporary, 'w', 0o600);
    try { await handle.writeFile(JSON.stringify(data)); await handle.sync(); }
    finally { await handle.close(); }
    await fsp.rename(temporary, file);
}
async function readJson(file) {
    try { return JSON.parse(await fsp.readFile(file, 'utf8')); }
    catch (error) { if (error.code === 'ENOENT') return null; throw error; }
}
async function write(stream, data) {
    await writeBuffered(stream, data);
}
async function close(stream) { stream.end(); await finished(stream); }
async function* batches(file, signal, batchSize = BATCH_SIZE, startOffset = 0) {
    let batch = [];
    for await (const { row, offset } of entries(file, signal, startOffset)) {
        batch.push(row);
        batch.inputOffset = offset;
        if (batch.length === batchSize) { yield batch; batch = []; }
    }
    if (batch.length) yield batch;
}
function canonical(row) {
    row = { ...row };
    const phones = Array.isArray(row.phones) ? [...row.phones] : [];
    for (const key of ['telefone_principal', 'telefone_secundario', 'telefone1', 'telefone2']) {
        if (row[key]) { row[key] = telefoneParaGeracao(row[key]); phones.push(row[key]); }
    }
    for (const key of Object.keys(row).filter(key => /^fone\d+$/.test(key)).sort((a, b) => Number(a.slice(4)) - Number(b.slice(4)))) {
        if (row[key]) { row[key] = telefoneParaGeracao(row[key]); phones.push(row[key]); }
    }
    return { ...row, cnpj: normalizarDocumento(row.cnpj || row.cpf), phones: [...new Set(phones.filter(Boolean).map(telefoneParaGeracao))] };
}
function normalizedPhones(values) {
    return [...new Set((values || []).map(value => normalizarTelefoneFluxo(value).phone).filter(Boolean))];
}
function generationFilters(generation) {
    // The editor persists single city/neighborhood strings; Receita supports lists.
    // Adapt only at the boundary, keeping the frozen editor snapshot unchanged.
    const list = value => typeof value === 'string' ? (value.trim() ? [value.trim()] : []) : value;
    return { ...generation, cidade: list(generation?.cidade) ?? [], bairro: list(generation?.bairro) ?? [] };
}
function safeCell(value) {
    if (value === null || value === undefined) return '';
    if (typeof value === 'object') value = JSON.stringify(value);
    const text = String(value);
    return /^[\s\uFEFF]*[=+@-]/u.test(text) || /^[\t\r\n]/u.test(text) ? `'${text}` : text;
}
function csvRow(values) { return values.map(value => `"${safeCell(value).replace(/"/g, '""')}"`).join(';') + '\r\n'; }

function outputFormat(flow, providers, checkpoint) {
    const formats = providers.getFormat && providers.mapOutputRow ? providers : require('./formats');
    const context = { operation: flow.operation, fillLivre5: flow.cleaning?.enabled !== false && flow.cleaning?.fillLivre5,
        jobName: flow.name, date: new Intl.DateTimeFormat('pt-BR', { timeZone: 'America/Sao_Paulo' }).format(new Date(checkpoint.createdAt)),
        includeSituacao: flow.output?.includeSituacao !== false, fillCpf: Boolean(flow.enrichment?.enabled && flow.enrichment?.fillCpf) };
    const format = flow.output?.formatSnapshot || (providers.getFormat || formats.getFormat)(flow.output?.formatId || 'padrao', context);
    if (!Array.isArray(format.colunas) || !format.colunas.length) throw validation('Formato de saída inválido.');
    const headers = format.colunas.map(column => typeof column === 'string' ? column : column.header);
    const slots = format.colunas.map(require('./layouts').phoneSlot).filter(Boolean);
    const phoneCapacity = slots.length ? Math.max(...slots) : 0;
    if (!phoneCapacity) throw validation('O formato selecionado não possui colunas de telefone.');
    return { context, format, headers, phoneCapacity, map: providers.mapOutputRow || formats.mapOutputRow };
}

async function queryEnrichment(pool, documents) {
    const { rows } = await pool.query(`SELECT e.cnpj,
        (SELECT array_agg(p ORDER BY s.id, n.ordinality) FROM socios s,
            unnest(s.telefones) WITH ORDINALITY AS n(p, ordinality) WHERE s.empresa_id = e.id) AS socio_phones,
        (SELECT array_agg(s.cpf ORDER BY s.id) FROM socios s WHERE s.empresa_id = e.id) AS socio_cpfs,
        array_agg(t.numero ORDER BY t.id) AS empresa_phones
        FROM empresas e LEFT JOIN telefones t ON e.id = t.empresa_id
        WHERE e.cnpj = ANY($1::text[]) GROUP BY e.id, e.cnpj ORDER BY e.cnpj`, [documents]);
    return rows;
}
async function queryPhones(pool, kind, phones) {
    // SQL identifiers come exclusively from this fixed allowlist.
    const table = kind === 'blocklist' ? 'blocklist' : 'telefones_invalidos';
    const { rows } = await pool.query(`SELECT telefone FROM ${table} WHERE telefone = ANY($1::text[])`, [phones]);
    return rows.map(row => row.telefone);
}

/** Resumes only complete immutable JSONL stages. Connections never enter checkpoints. */
async function runFlow({ flow, user, jobDir, connections = {}, rootFile, signal, onUpdate = () => {}, providers = {}, cachePolicy = {}, processingPolicy = {} }) {
    const ownedPools = [];
    const pools = {};
    let stage = 'generation';
    let checkpoint;
    let stageProgress = null;
    // Batch size is a runtime setting; changing it must not invalidate saved cursors.
    const batchSize = processingPolicy.batchSize ?? BATCH_SIZE;
    if (!Number.isSafeInteger(batchSize) || batchSize < 1 || batchSize > 10000) throw validation('O lote de processamento deve estar entre 1 e 10.000 registros.');
    const checkpointPath = path.join(jobDir, 'checkpoint.json');
    async function poolFor(name) {
        if (pools[name]) return pools[name];
        const config = connections[name];
        if (!config) throw validation(`Configure a conexão ${name === 'receita' ? 'da Receita' : 'do enriquecimento'} para executar este fluxo.`);
        if (typeof config.query === 'function') return (pools[name] = config);
        const create = providers.createPool || (settings => new (require('pg').Pool)(settings));
        const pool = create(readOnlyPoolOptions(config));
        pool.on?.('error', () => {});
        ownedPools.push(pool);
        return (pools[name] = pool);
    }
    function update(data = {}) {
        // Notifications are advisory: an observer must never roll back a confirmed file.
        try {
            const pending = onUpdate({ stage, counts: { ...checkpoint.counts }, replaceCounts: true, progress: stageProgress, ...data });
            pending?.catch?.(() => {});
        } catch { /* The next update or main checkpoint reconciliation can recover. */ }
    }
    async function commit(name, extra) {
        abort(signal);
        if (name === 'cleaning') checkpoint.counts.cleaned = extra.rows;
        if (name === 'export') checkpoint.counts.exported = extra.outputs.filter(output => output.kind === 'xlsx').reduce((sum, output) => sum + output.rows, 0);
        const next = { ...checkpoint, stages: { ...checkpoint.stages, [name]: extra }, counts: { ...checkpoint.counts } };
        await atomicJson(checkpointPath, next);
        checkpoint = next;
        await retireConsumedStages(name);
        if (stageProgress?.stage === name) stageProgress = { ...stageProgress, complete: true, total: stageProgress.processed };
        update({ log: `Etapa ${name} concluída.` });
    }
    async function cleanupRetiredStages() {
        const directory = path.resolve(jobDir);
        for (const name of STAGES.slice(0, -1)) {
            if (!checkpoint.stages[name]?.retired) continue;
            // Only fixed stage names inside this job are eligible for deletion.
            for (const basename of [`${name}.jsonl`, `${name}.jsonl.pack`]) {
                for (const suffix of ['', '.tmp']) await fsp.rm(path.join(directory, basename + suffix), { force: true });
            }
            for (const target of [path.join(directory, 'stage-cache', name), ...(name === 'api' ? [path.join(directory, 'api-results')] : [])]) {
                const outputDirectory = path.resolve(flow.output?.directory || path.join(jobDir, 'outputs'));
                const relative = path.relative(target, outputDirectory);
                if (!relative || (!relative.startsWith('..' + path.sep) && relative !== '..' && !path.isAbsolute(relative))) continue;
                await fsp.rm(target, { recursive: true, force: true });
            }
        }
    }
    async function retireConsumedStages(completed) {
        if (!cachePolicy.prune) return;
        const boundary = STAGES.indexOf(completed);
        let changed = false;
        const stages = { ...checkpoint.stages };
        for (const name of STAGES.slice(0, boundary)) {
            if (stages[name] && !stages[name].retired) { stages[name] = { ...stages[name], retired: true }; changed = true; }
        }
        if (changed) {
            const next = { ...checkpoint, stages };
            await atomicJson(checkpointPath, next);
            checkpoint = next;
        }
        // Cleanup cannot invalidate an already confirmed stage or export.
        await cleanupRetiredStages().catch(() => update({ log: 'Etapa confirmada. A liberação de arquivos antigos será tentada novamente ao retomar.' }));
    }
    async function jsonlStage(name, produce) {
        const compressed = checkpoint.cacheFormat === 'packed-v1';
        const target = path.join(jobDir, `${name}.jsonl${compressed ? '.pack' : ''}`);
        const temporary = `${target}.tmp`;
        const cache = await openStageCache({ jobDir, stage: name, fingerprint: checkpoint.fingerprint, version: name === 'cleaning' ? 2 : 1, temporary, target });
        const total = name === 'generation' ? null : checkpoint.stages[name === 'enrichment' ? 'generation' : name === 'api' || !flow.api?.enabled ? 'enrichment' : 'api'].rows;
        if (cache.state.processed) checkpoint.counts = { ...cache.state.counts };
        stageProgress = { stage: name, processed: cache.state.processed, total, complete: false };
        update({ log: cache.state.processed ? `Retomando ${name} do último lote salvo (${cache.state.processed.toLocaleString('pt-BR')} registros).` : undefined });
        const output = fs.createWriteStream(temporary, { flags: 'a', mode: 0o600 });
        const completion = finished(output);
        completion.catch(() => {});
        let count = cache.state.rows, outputBytes = cache.state.outputBytes;
        const writer = compressed ? createPackedWriter(output, outputBytes) : null;
        cache.confirm = async position => {
            abort(signal);
            if (cachePolicy.prune && typeof fsp.statfs === 'function') {
                const disk = await fsp.statfs(jobDir);
                if (disk.bavail * disk.bsize < 128 * 1024 * 1024) throw Object.assign(new Error('Disco do cache sem espaço livre suficiente.'), { code: 'FLOW_DISK_FULL' });
            }
            if (writer) { await writer.flush(); outputBytes = writer.outputBytes; }
            await new Promise((resolve, reject) => output.write('', error => error ? reject(error) : resolve()));
            await cache.save({ ...position, rows: count, outputBytes, counts: { ...checkpoint.counts } });
            stageProgress = { stage: name, processed: cache.state.processed, total, complete: false };
            update();
        };
        try {
            await produce(async row => {
                abort(signal);
                if (writer) await writer.append(row);
                else { const line = JSON.stringify(row) + '\n'; await write(output, line); outputBytes += Buffer.byteLength(line); }
                count++;
            }, cache);
            await cache.confirm({ processed: cache.state.processed });
            await close(output);
            abort(signal);
            await fsp.rename(temporary, target);
            await commit(name, { file: target, rows: count, cacheId: cache.state.id, ...(name === 'generation' ? { phoneFormatVersion: 1 } : {}), ...(name === 'api' ? { sourceStage: 'enrichment' } : {}), ...(name === 'cleaning' ? { sourceStage: flow.api?.enabled ? 'api' : 'enrichment', contactFilterVersion: 2 } : {}) });
        } catch (error) {
            output.destroy();
            await completion.catch(() => {});
            throw error;
        } finally { await cache.close(); }
    }
    try {
        if (!flow || !user?.username) throw validation('Fluxo e sessão autenticada são obrigatórios.');
        if (flow.api?.enabled && flow.operation !== 'c6') throw validation('A validação de disponibilidade C6 só pode ser usada em fluxos C6.');
        const rowsPerFile = flow.output?.rowsPerFile ?? 100000;
        if (!Number.isSafeInteger(rowsPerFile) || rowsPerFile < 1 || rowsPerFile > 1000000) throw validation('Linhas por arquivo deve estar entre 1 e 1.000.000.');
        await fsp.mkdir(jobDir, { recursive: true });
        const fingerprint = crypto.createHash('sha256').update(JSON.stringify({ flow, owner: user.username, rootFile: rootFile || null })).digest('hex');
        checkpoint = await readJson(checkpointPath);
        if (checkpoint && checkpoint.fingerprint !== fingerprint) throw validation('A configuração do fluxo mudou. Inicie uma nova execução.');
        if (checkpoint?.stages.generation && checkpoint.stages.generation.phoneFormatVersion !== 1) throw validation('Esta execução foi gerada antes do ajuste de celulares na origem. Reinicie a geração para usar os telefones completos desde a primeira etapa.');
        if (flow.api?.enabled && ((checkpoint?.stages.api && checkpoint.stages.api.sourceStage !== 'enrichment') || (checkpoint?.stages.cleaning && checkpoint.stages.cleaning.sourceStage !== 'api'))) throw validation('Esta execução usava filtros antes da API. Inicie uma nova execução para aplicar os filtros somente no final.');
        if (!checkpoint) {
            checkpoint = { version: 1, id: crypto.randomUUID(), fingerprint, createdAt: new Date().toISOString(), stages: {}, counts: {}, ...(cachePolicy.compressed ? { cacheFormat: 'packed-v1' } : {}) };
            await atomicJson(checkpointPath, checkpoint);
        }
        if (checkpoint.stages.cleaning && checkpoint.stages.cleaning.contactFilterVersion !== 2) {
            // Keep the expensive source stages; the new blocklist rule needs a new final pass.
            delete checkpoint.stages.cleaning;
            delete checkpoint.stages.export;
            delete checkpoint.counts.cleaned;
            delete checkpoint.counts.exported;
            await atomicJson(checkpointPath, checkpoint);
        }
        abort(signal);
        await cleanupRetiredStages().catch(() => {});
        // Every retained checkpoint points to a stage file confirmed by atomic rename.
        for (const name of STAGES.slice(0, -1)) {
            if (checkpoint.stages[name] && !checkpoint.stages[name].retired) await fsp.access(checkpoint.stages[name].file);
        }
        if (!checkpoint.stages.generation) {
            stage = 'generation';
            checkpoint.counts = { generated: 0 };
            update({ status: 'running', log: 'Gerando base da Receita.' });
            const iterate = providers.iterateReceita || require('./receita').iterateReceita;
            const pool = providers.iterateReceita && !connections.receita ? undefined : await poolFor('receita');
            await jsonlStage(stage, async (emit, cache) => {
                const filters = generationFilters(flow.generation);
                if (filters.limit != null && cache.state.processed >= filters.limit) return;
                if (filters.limit != null) filters.limit -= cache.state.processed;
                for await (const batch of iterate({ pool, filters, afterCnpj: cache.state.cursor, batchSize, signal })) {
                    abort(signal);
                    for (const row of batch.rows) await emit(canonical(row));
                    checkpoint.counts.generated = (checkpoint.counts.generated || 0) + batch.rows.length;
                    await cache.confirm({ processed: cache.state.processed + batch.rows.length, cursor: batch.cursor || batch.rows.at(-1)?.cnpj || cache.state.cursor });
                }
            });
        }
        if (!checkpoint.stages.enrichment) {
            stage = 'enrichment';
            checkpoint.counts.enriched = 0;
            const { phoneCapacity } = outputFormat(flow, providers, checkpoint);
            update({ log: flow.enrichment?.enabled ? 'Enriquecendo contatos.' : 'Enriquecimento desativado.' });
            await jsonlStage(stage, async (emit, cache) => {
                for await (const batch of batches(checkpoint.stages.generation.file, signal, batchSize, cache.state.inputOffset)) {
                    let found = new Map();
                    if (flow.enrichment?.enabled) {
                        const documents = [...new Set(batch.map(row => row.cnpj).filter(Boolean))];
                        if (documents.length) {
                            const rows = providers.queryEnrichment ? await providers.queryEnrichment(documents, signal) : await queryEnrichment(await poolFor('enrichment'), documents);
                            found = new Map(rows.map(row => [normalizarDocumento(row.cnpj), row]));
                        }
                    }
                    abort(signal);
                    for (const row of batch) {
                        const data = found.get(row.cnpj);
                        let changed = false;
                        if (data) {
                            const incoming = normalizedPhones(data.phones || [...(data.socio_phones || []), ...(data.empresa_phones || [])]);
                            const existing = normalizedPhones(row.phones);
                            const strategy = flow.enrichment.strategy || 'append';
                            if (incoming.length && (strategy !== 'ignore' || !existing.length)) {
                                row.phones = strategy === 'append' ? [...new Set([...existing, ...incoming])] : incoming;
                                changed = strategy !== 'append' || row.phones.slice(0, phoneCapacity).some(phone => !existing.includes(phone));
                            }
                            const cpfs = data.cpfs || data.socio_cpfs || [];
                            if (flow.enrichment.fillCpf && cpfs.length) {
                                row.livre6 = normalizarDocumento(cpfs[0], 'cpf');
                                row.socio_cpfs = cpfs.map(cpf => normalizarDocumento(cpf, 'cpf'));
                                changed = true;
                            }
                        }
                        if (flow.enrichment?.enabled) row.status = changed ? 'Enriquecido' : 'Pobre';
                        if (changed) checkpoint.counts.enriched++;
                        await emit(row);
                    }
                    await cache.confirm({ processed: cache.state.processed + batch.length, inputOffset: batch.inputOffset });
                }
            });
        }
        if (flow.api?.enabled && !checkpoint.stages.api) {
            stage = 'api';
            update({ log: 'Validando disponibilidade C6 com chave dupla após enriquecimento e antes dos filtros finais.' });
            const { runApiStage } = require('./disponibilidadeApi');
            await jsonlStage(stage, (emit, cache) => runApiStage({ jobDir, source: checkpoint.stages.enrichment.file, batches, emit, signal, cache,
                counts: checkpoint.counts, update, acquire: providers.acquireApi,
                ...providers.apiTiming }));
        }
        if (!checkpoint.stages.cleaning) {
            stage = 'cleaning';
            // Counts from a failed attempt are reset; retained stages stay immutable.
            delete checkpoint.counts.ninthDigitAdded;
            for (const key of ['kept', 'removedRoot', 'removedCnae', 'removedBlocklist', 'blockedPhones', 'invalidPhones', 'landlines', 'dirtyPhones', 'ddiRemoved', 'repeatedDocuments', 'repeatedPhones', 'withoutPhones', 'withoutPhonesBeforeFilters', 'withoutPhonesAfterFilters', 'withoutPhonesRepeatedOnly', 'truncatedPhones']) checkpoint.counts[key] = 0;
            update({ log: 'Aplicando filtros finais, limpeza e cruzamento após enriquecimento e API quando ativada.' });
            const options = flow.cleaning || {};
            const enabled = options.enabled !== false;
            let root = new Set();
            if (enabled && options.rootSource && options.rootSource !== 'none') {
                const snapshot = typeof rootFile === 'string' ? await readJson(rootFile) : rootFile;
                if (!snapshot || !Array.isArray(snapshot.documents) || !snapshot.documents.length) throw validation('A raiz selecionada está vazia ou indisponível. Atualize a raiz antes de executar.');
                root = new Set(snapshot.documents.map(value => normalizarDocumento(value)).filter(Boolean));
                if (!root.size) throw validation('A raiz selecionada não contém documentos válidos.');
            }
            const prohibited = new Set(options.prohibitedCnaes === false ? [] : (Array.isArray(options.prohibitedCnaes) ? options.prohibitedCnaes : [...PROHIBITED_CNAES])
                .map(value => String(value).replace(/\D/g, '')).filter(Boolean).map(value => value.padStart(7, '0')));
            const blocklist = user.username !== 'Davi' || options.blocklist !== false;
            const { phoneCapacity } = outputFormat(flow, providers, checkpoint);
            await jsonlStage(stage, async (emit, cache) => {
                const input = flow.api?.enabled ? checkpoint.stages.api.file : checkpoint.stages.enrichment.file;
                for await (const batch of batches(input, signal, batchSize, cache.state.inputOffset)) {
                    for (const row of batch) {
                        row.phones = row.phones.map(value => {
                            const result = normalizarTelefoneFluxo(value, { ajustarNonoDigito: false });
                            if (!result.phone && value) checkpoint.counts.dirtyPhones++;
                            if (result.ddiRemoved) checkpoint.counts.ddiRemoved++;
                            return result.phone;
                        }).filter(Boolean);
                    }
                    const phones = [...new Set(batch.flatMap(row => row.phones))];
                    const variants = [...new Set(phones.flatMap(variantesTelefoneFluxo))];
                    const lookup = async kind => {
                        if (!variants.length) return new Set();
                        const values = providers.queryPhones ? await providers.queryPhones(kind, variants, signal) : await queryPhones(await poolFor('enrichment'), kind, variants);
                        return new Set(normalizedPhones(values));
                    };
                    const blocked = blocklist ? await lookup('blocklist') : new Set();
                    const invalid = enabled && options.invalidPhones ? await lookup('invalid') : new Set();
                    abort(signal);
                    for (const row of batch) {
                        if (root.has(row.cnpj)) { checkpoint.counts.removedRoot++; continue; }
                        const cnae = String(row.cnae || row.atividade_principal_cod || row.cnae_fiscal_principal || row.livre3 || '').replace(/\D/g, '');
                        if (enabled && cnae && prohibited.has(cnae.padStart(7, '0'))) { checkpoint.counts.removedCnae++; continue; }
                        const hadPhonesBeforeFilters = row.phones.length > 0;
                        row.phones = row.phones.filter(phone => {
                            if (blocked.has(phone)) { checkpoint.counts.blockedPhones++; return false; }
                            if (invalid.has(phone)) { checkpoint.counts.invalidPhones++; return false; }
                            if (enabled && options.removeLandlines && normalizarTelefoneFluxo(phone, { ajustarNonoDigito: false }).landline) { checkpoint.counts.landlines++; return false; }
                            return true;
                        });
                        if (row.cnpj && cache.has('document', row.cnpj)) { checkpoint.counts.repeatedDocuments++; continue; }
                        const unique = [];
                        for (const phone of row.phones) {
                            if (cache.has('phone', phone) || unique.includes(phone)) checkpoint.counts.repeatedPhones++;
                            else unique.push(phone);
                        }
                        if (!unique.length) {
                            checkpoint.counts.withoutPhones++;
                            const reason = !hadPhonesBeforeFilters ? 'withoutPhonesBeforeFilters' : !row.phones.length ? 'withoutPhonesAfterFilters' : 'withoutPhonesRepeatedOnly';
                            checkpoint.counts[reason]++;
                            continue;
                        }
                        // Only contacts that fit the selected final layout are reserved.
                        // Excess contacts remain available to later surviving companies.
                        const retained = unique.slice(0, phoneCapacity);
                        checkpoint.counts.truncatedPhones += unique.length - retained.length;
                        row.phones = retained;
                        if (typeof row.nome === 'string') row.nome = row.nome.replace(/^[\d.\- ]+|[\d.\- ]+$/g, '').trim();
                        if (typeof row.razao_social === 'string') row.razao_social = row.razao_social.replace(/^[\d.\- ]+|[\d.\- ]+$/g, '').trim();
                        await emit(row);
                        if (row.cnpj) cache.remember('document', row.cnpj);
                        retained.forEach(phone => cache.remember('phone', phone));
                        checkpoint.counts.kept++;
                    }
                    await cache.confirm({ processed: cache.state.processed + batch.length, inputOffset: batch.inputOffset });
                }
            });
        }
        stage = 'export';
        stageProgress = { stage, processed: checkpoint.stages.export ? checkpoint.stages.cleaning.rows : 0, total: checkpoint.stages.cleaning.rows, complete: Boolean(checkpoint.stages.export) };
        if (!checkpoint.stages.export) {
            update({ log: 'Salvando arquivos finais.' });
            await exportFiles({ checkpoint, flow, jobDir, signal, providers, rowsPerFile, commit, update, onProgress: processed => { stageProgress = { ...stageProgress, processed }; update(); } });
        }
        for (const output of checkpoint.stages.export.outputs) await fsp.access(output.path);
        const result = { status: checkpoint.stages.cleaning.rows ? 'completed' : 'empty', counts: checkpoint.counts, outputs: checkpoint.stages.export.outputs };
        update({ ...result, log: result.status === 'empty' ? 'Nenhum registro restou para exportar após os filtros do fluxo.' : 'Fluxo concluído.' });
        return result;
    } catch (error) {
        if (checkpoint) update({ status: friendly(stage, error).code === 'FLOW_CANCELLED' ? 'cancelled' : 'failed', log: friendly(stage, error).message });
        throw friendly(stage, error);
    } finally { await Promise.allSettled(ownedPools.map(pool => pool.end())); }
}

async function exportFiles({ checkpoint, flow, jobDir, signal, providers, rowsPerFile, commit, update, onProgress }) {
    const directory = path.resolve(flow.output?.directory || path.join(jobDir, 'outputs'));
    await fsp.mkdir(directory, { recursive: true });
    const pendingPath = path.join(jobDir, 'export-pending.json');
    const cachePath = path.join(jobDir, 'export-cache.json');
    const identity = `${checkpoint.fingerprint}:${checkpoint.stages.cleaning.cacheId}`;
    let saved = await readJson(cachePath);
    if (saved?.identity !== identity) saved = { identity, inputOffset: 0, processed: 0, outputs: [] };
    const confirmedPaths = new Set(saved.outputs.map(output => output.path));
    for (const output of saved.outputs) await fsp.access(output.path);
    const previous = await readJson(pendingPath);
    // Delete only our own pending files inside the chosen destination.
    for (const file of previous?.files || []) {
        if (!confirmedPaths.has(file) && path.dirname(path.resolve(file)) === directory && path.basename(file).startsWith(`fluxo_${checkpoint.id}_`)) await fsp.rm(file, { force: true });
    }
    const { context, format, map, headers } = outputFormat(flow, providers, checkpoint);
    const attempt = crypto.randomBytes(6).toString('hex');
    const files = [];
    const outputs = [...saved.outputs];
    let active;
    let part = saved.outputs.filter(output => output.kind === 'xlsx').length;
    let processed = saved.processed, inputOffset = saved.inputOffset;
    onProgress?.(processed);
    const remember = async file => { files.push(file); await atomicJson(pendingPath, { files }); };
    async function start() {
        const base = path.join(directory, `fluxo_${checkpoint.id}_${attempt}_${String(++part).padStart(3, '0')}`);
        const xlsx = `${base}.xlsx`;
        await remember(`${xlsx}.tmp`); await remember(xlsx);
        const stream = fs.createWriteStream(`${xlsx}.tmp`, { flags: 'wx' });
        const streamDone = finished(stream); streamDone.catch(() => {});
        const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({ stream, useStyles: true, useSharedStrings: false });
        const zipFailure = new Promise((_, reject) => workbook.zip.once('error', reject));
        zipFailure.catch(() => {});
        const worksheet = workbook.addWorksheet('Base');
        format.colunas.forEach((_, index) => { worksheet.getColumn(index + 1).numFmt = '@'; });
        worksheet.addRow(headers.map(safeCell)).commit();
        active = { xlsx, stream, streamDone, zipFailure, workbook, worksheet, rows: 0 };
        if (flow.output.csv) {
            const csv = `${base}.csv`;
            await remember(`${csv}.tmp`); await remember(csv);
            active.csv = csv;
            active.csvStream = fs.createWriteStream(`${csv}.tmp`, { flags: 'wx' });
            active.csvDone = finished(active.csvStream); active.csvDone.catch(() => {});
            await write(active.csvStream, '\uFEFF' + csvRow(headers));
        }
    }
    async function finishPart() {
        if (!active) return;
        active.worksheet.commit();
        await Promise.race([active.workbook.commit(), active.streamDone, active.zipFailure]);
        await active.streamDone;
        if (active.csvStream) await close(active.csvStream);
        for (const file of [active.xlsx, active.csv].filter(Boolean)) {
            const handle = await fsp.open(`${file}.tmp`, 'r+');
            try { await handle.sync(); } finally { await handle.close(); }
            await fsp.rename(`${file}.tmp`, file);
        }
        abort(signal);
        outputs.push({ path: active.xlsx, kind: 'xlsx', rows: active.rows });
        if (active.csv) outputs.push({ path: active.csv, kind: 'csv', rows: active.rows });
        await atomicJson(cachePath, { identity, inputOffset, processed, outputs });
        for (const output of outputs) confirmedPaths.add(output.path);
        active = null;
    }
    try {
        const input = checkpoint.stages.cleaning.file;
        for await (const entry of entries(input, signal, saved.inputOffset)) {
            const record = entry.row;
            if (!active) await start();
            const values = map(record, format, context).map(safeCell);
            active.worksheet.addRow(values).commit();
            if (active.csvStream) await write(active.csvStream, csvRow(values));
            active.rows++;
            processed++;
            inputOffset = entry.offset;
            if (active.rows >= rowsPerFile) { await finishPart(); update(); }
            if (processed % 1000 === 0) onProgress?.(processed);
            // Give cancellation and zip/output streams a turn even without CSV.
            if (active?.rows % 1000 === 0) await new Promise(resolve => setImmediate(resolve));
        }
        await finishPart();
        onProgress?.(processed);
        abort(signal);
        await commit('export', { outputs });
        // Once the final checkpoint is confirmed, a manifest cleanup failure must
        // not delete those now-final outputs. A remaining manifest is harmless.
        await fsp.rm(pendingPath, { force: true }).catch(() => {});
    } catch (error) {
        if (active) {
            active.workbook.zip?.abort?.();
            active.stream.destroy(); active.csvStream?.destroy();
            await Promise.allSettled([active.streamDone, active.csvDone].filter(Boolean));
        }
        for (const file of files) if (!confirmedPaths.has(file)) await fsp.rm(file, { force: true }).catch(() => {});
        throw error;
    }
}

module.exports = { runFlow, canonical, csvRow, safeCell, queryEnrichment, queryPhones };
