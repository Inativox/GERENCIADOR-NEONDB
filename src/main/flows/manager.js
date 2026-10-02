const fs = require('fs');
const path = require('path');
const { randomUUID } = require('crypto');
const { Worker } = require('worker_threads');
const { createUserStore } = require('./store');
const { effectiveFlow, validateFlow } = require('./config');
const { listFormats, getFormat, mapOutputRow } = require('./formats');
const { validateLayout } = require('./layouts');

function createFlowManager({ baseDirectory, getCacheDirectory = () => baseDirectory, getCacheDirectories = () => [getCacheDirectory()], getUser, resolveConnections, resolveRoot, resolveApiSession, onUpdate = () => {}, workerFactory = data => new Worker(path.join(__dirname, '../workers/flowWorker.js'), { workerData: data, resourceLimits: { maxOldGenerationSizeMb: 512 } }) }) {
    const stores = new Map(); let active = null;
    function user() { const current = getUser(); if (!current || current.role !== 'admin') throw new Error('Seu perfil não tem acesso à geração e limpeza de listas.'); return { username: current.username, role: current.role }; }
    function storeFor(owner) {
        if (!stores.has(owner)) {
            const store = createUserStore(baseDirectory, owner);
            for (const job of store.listJobs()) if (job.status === 'running') store.saveJob({ ...store.getJob(job.id), status: 'interrupted', error: 'Execução interrompida. Retome a partir da última etapa confirmada.', updatedAt: new Date().toISOString() });
            stores.set(owner, store);
        }
        return stores.get(owner);
    }
    function saveUpdate(context, update) {
        const job = context.job;
        if (update.stage) job.stage = String(update.stage).slice(0, 60);
        if (Object.prototype.hasOwnProperty.call(update, 'progress')) {
            const progress = update.progress;
            job.progress = progress && progress.stage === job.stage && Number.isSafeInteger(progress.processed) && progress.processed >= 0
                ? { stage: job.stage, processed: progress.processed, total: Number.isSafeInteger(progress.total) && progress.total >= 0 ? progress.total : null, complete: progress.complete === true }
                : null;
        }
        if (update.counts && typeof update.counts === 'object') job.counts = { ...(update.replaceCounts === true ? {} : job.counts), ...Object.fromEntries(Object.entries(update.counts).filter(([key, value]) => /^\w{1,60}$/.test(key) && Number.isFinite(value) && value >= 0)) };
        if (update.log) job.logs = [...job.logs, String(update.log).slice(0, 1500)].slice(-300);
        if (Array.isArray(update.outputs)) job.outputs = update.outputs.map(item => ({ path: String(item.path), kind: String(item.kind), rows: Number(item.rows) || 0 }));
        if (update.status) job.status = update.status;
        job.updatedAt = new Date().toISOString();
        context.store.saveJob(job);
        try { onUpdate(JSON.parse(JSON.stringify(job))); } catch { /* A closed window cannot stop a persisted job. */ }
    }
    const formatResolver = store => (id, context) => getFormat(id, context, store.listLayouts());
    async function run(context) {
        try {
            const connections = await resolveConnections(context.job.flowSnapshot);
            if (!context.job.rootFile || !fs.existsSync(context.job.rootFile)) {
                saveUpdate(context, { stage: 'root', log: 'Preparando a raiz e os acessos deste fluxo.' });
                const root = await resolveRoot(context.job.flowSnapshot, { signal: context.controller.signal, onProgress: () => {} });
                context.job.rootInfo = root.info;
                context.job.rootFile = path.join(context.job.jobDir, 'root.json');
                await fs.promises.writeFile(context.job.rootFile + '.tmp', JSON.stringify(root), { mode: 0o600 });
                await fs.promises.rename(context.job.rootFile + '.tmp', context.job.rootFile);
                saveUpdate(context, { log: `Raiz ${root.info.source}: ${root.info.count} documentos. Consulta ${root.info.queriedAt}.` });
            }
            if (context.controller.signal.aborted) throw new Error('Execução cancelada.');
            const current = user(); if (current.username !== context.job.owner) throw new Error('A sessão mudou. Retome usando o usuário que iniciou o fluxo.');
            const worker = workerFactory({ flow: context.job.flowSnapshot, user: current, jobDir: context.job.jobDir, connections, rootFile: context.job.rootFile });
            context.worker = worker;
            async function apiMessage(message) {
                try {
                    if (!context.job.flowSnapshot.api?.enabled || context.job.flowSnapshot.operation !== 'c6' || !resolveApiSession) throw new Error('A etapa API não está disponível nesta execução.');
                    if (message.action === 'acquire') {
                        if (context.apiLease || context.apiAcquiring) throw new Error('Já existe uma reserva de API nesta execução.');
                        context.apiAcquiring = true;
                        try {
                            const lease = await resolveApiSession(context.job.owner);
                            if (context.controller.signal.aborted || active !== context || getUser()?.username !== context.job.owner) {
                                await lease.release(); throw new Error('A execução ou a sessão foi encerrada.');
                            }
                            context.apiLease = lease;
                        } finally { context.apiAcquiring = false; }
                        worker.postMessage({ type: 'api-response', id: message.id, credentials: context.apiLease.credentials, nextAllowedAt: context.apiLease.nextAllowedAt });
                    } else if (message.action === 'assert') {
                        if (!context.apiLease) throw new Error('A reserva de API foi encerrada.');
                        await context.apiLease.assert();
                        worker.postMessage({ type: 'api-response', id: message.id });
                    } else if (message.action === 'release') {
                        await context.apiLease?.release(); context.apiLease = null;
                        worker.postMessage({ type: 'api-response', id: message.id });
                    } else throw new Error('Solicitação API inválida.');
                } catch (error) {
                    // Only intentionally friendly service errors may cross the process boundary.
                    const messageText = error.code === 'FLOW_VALIDATION' ? error.message : 'Não foi possível reservar ou manter as chaves da API. Retome a execução.';
                    try { worker.postMessage({ type: 'api-response', id: message.id, error: messageText }); } catch { /* Worker already exited. */ }
                }
            }
            await new Promise((resolve, reject) => {
                let finished = false;
                worker.on('message', message => {
                    if (finished) return;
                    try {
                        if (message.type === 'api-request') { void apiMessage(message); return; }
                        if (message.type === 'update') saveUpdate(context, message.data || {});
                        if (message.type === 'result') { saveUpdate(context, { ...message.data, status: message.data?.status || 'completed' }); finished = true; resolve(); }
                        if (message.type === 'error') { finished = true; reject(Object.assign(new Error(message.code === 'FLOW_CANCELLED' ? 'Execução cancelada.' : (message.message || 'Falha na execução do fluxo.')), { code: ['FLOW_DISK_FULL', 'FLOW_CANCELLED'].includes(message.code) ? message.code : '' })); }
                    } catch { finished = true; reject(new Error('Não foi possível salvar o histórico. Verifique o espaço disponível e retome a execução.')); }
                });
                worker.on('error', () => { if (!finished) { finished = true; reject(new Error('O processamento foi interrompido. Retome a etapa; reduza o volume se o problema persistir.')); } });
                worker.on('exit', code => { if (!finished) { finished = true; reject(new Error(`Worker interrompido (${code}). Retome a execução.`)); } });
            });
        } catch (error) {
            context.job.error = context.controller.signal.aborted ? 'Execução cancelada. Pode retomar a última etapa confirmada.' : error.message;
            context.job.errorCode = !context.controller.signal.aborted && ['BQ_AUTH_REQUIRED', 'FLOW_DISK_FULL'].includes(error.code) ? error.code : '';
            const status = context.controller.signal.aborted ? 'cancelled' : 'failed';
            try { saveUpdate(context, { status, log: context.job.error }); }
            catch { context.job.status = status; try { onUpdate(JSON.parse(JSON.stringify(context.job))); } catch { /* Best effort while disk is unavailable. */ } }
        } finally {
            clearTimeout(context.cancelTimer);
            await context.apiLease?.release().catch(() => {});
            context.apiLease = null;
            context.worker?.terminate().catch(() => {});
            if (active === context) active = null;
        }
    }
    function launch(store, job) {
        const context = { job, store, controller: new AbortController(), worker: null };
        saveUpdate(context, { status: 'running' }); active = context;
        setImmediate(() => { void run(context); });
        return JSON.parse(JSON.stringify(job));
    }
    function cancel(context) {
        context.controller.abort(); context.worker?.postMessage({ type: 'cancel' });
        if (context.worker && !context.cancelTimer) {
            context.cancelTimer = setTimeout(() => { context.worker?.terminate().catch(() => {}); }, 5000);
            context.cancelTimer.unref();
        }
    }
    return {
        bootstrap() {
            const current = user(), store = storeFor(current.username);
            for (const job of store.listJobs()) if (job.status === 'running' && active?.job.id !== job.id) store.saveJob({ ...store.getJob(job.id), status: 'interrupted', error: 'Execução interrompida. Retome a partir da última etapa confirmada.', updatedAt: new Date().toISOString() });
            return { user: current, flows: store.listFlows(), jobs: store.listJobs(), formats: listFormats(store.listLayouts()), cacheDirectory: getCacheDirectory() };
        },
        save(input) { const store = storeFor(user().username); const existing = input.id ? store.getFlow(input.id) : null; if (input.id && !existing) throw new Error('Fluxo não encontrado nesta conta.'); return store.saveFlow(validateFlow(input, existing, { resolveFormat: formatResolver(store) })); },
        saveLayout(input) {
            const store = storeFor(user().username), existing = input?.id ? store.getLayout(input.id) : null;
            if (input?.id && !existing) throw new Error('Layout próprio não encontrado nesta conta. Para alterar um modelo padrão, salve uma cópia.');
            if (input?.id && input.revision !== existing.revision) throw new Error('Este layout foi alterado. Reabra o editor para usar a revisão atual.');
            return store.saveLayout(validateLayout(input, existing));
        },
        deleteLayout(id) {
            const store = storeFor(user().username);
            if (!store.getLayout(id)) throw new Error('Layout próprio não encontrado nesta conta.');
            if (store.listFlows().some(flow => flow.output.formatId === id)) throw new Error('Este layout é usado por um fluxo. Escolha outro layout nesse fluxo e salve antes de excluir.');
            store.deleteLayout(id);
        },
        previewLayout(input) {
            user();
            const layout = validateLayout(input.layout);
            const format = getFormat(layout.id, { includeSituacao: input.includeSituacao !== false, fillCpf: input.fillCpf === true }, [layout]);
            const sample = { cnpj: '04252011000110', razao_social: 'Empresa Exemplo', nome_fantasia: 'Comércio Exemplo', data_abertura: '2020-01-15', email: 'contato@exemplo.test', phones: Array.from({ length: 10 }, (_, i) => `119876543${String(i + 10)}`), estado: 'SP', cidade: 'São Paulo', bairro: 'Centro', logradouro: 'Rua Exemplo', numero: '100', cep: '01001000', atividade_principal_cod: '4711302', atividade_principal: 'Comércio varejista', situacao_cadastral_cod: '02', situacao_cadastral: 'Ativa', situacao_cadastral_data: '2026-09-30', situacao_motivo: 'Sem motivo', nome_socio: 'Sócio Exemplo', cpf_socio: '***123456**' };
            return { headers: format.colunas.map(column => column.header), values: mapOutputRow(sample, format, { operation: input.operation, fillLivre5: input.fillLivre5 === true, jobName: 'Lista exemplo', date: '2026-09-30' }) };
        },
        delete(id) { const store = storeFor(user().username); if (active?.job.flowId === id) throw new Error('Aguarde ou cancele a execução antes de excluir o fluxo.'); if (!store.getFlow(id)) throw new Error('Fluxo não encontrado.'); store.deleteFlow(id); },
        start({ flowId, outputDirectory }) {
            const current = user(); if (active) throw new Error('Já existe um fluxo em execução. Aguarde ou cancele.');
            const store = storeFor(current.username), flow = store.getFlow(flowId);
            if (!flow) throw new Error('Salve e selecione um fluxo antes de executar.');
            if (!outputDirectory || !path.isAbsolute(outputDirectory) || !fs.statSync(outputDirectory).isDirectory()) throw new Error('Selecione uma pasta de saída válida.');
            const snapshot = effectiveFlow(flow, current, { resolveFormat: formatResolver(store) }); snapshot.output.directory = outputDirectory;
            const cacheRoot = getCacheDirectory();
            if (typeof cacheRoot !== 'string' || !path.isAbsolute(cacheRoot)) throw new Error('Pasta do cache inválida.');
            const account = path.basename(store.directory);
            const id = randomUUID(), directory = path.join(cacheRoot, account, 'jobs', id); fs.mkdirSync(directory, { recursive: true });
            const timestamp = new Date().toISOString();
            return launch(store, { id, flowId: flow.id, flowName: flow.name, owner: current.username, status: 'running', stage: 'root', createdAt: timestamp, updatedAt: timestamp, counts: {}, outputs: [], logs: [], error: '', flowSnapshot: snapshot, jobDir: directory });
        },
        resume(id) { const current = user(); if (active) throw new Error('Já existe um fluxo em execução.'); const store = storeFor(current.username), job = store.getJob(id); if (!job || !['failed', 'cancelled', 'interrupted'].includes(job.status)) throw new Error('Esta execução não pode ser retomada.'); if (job.cacheDiscarded) throw new Error('O cache desta execução foi apagado. Gere uma nova lista usando o fluxo salvo.'); job.error = ''; job.errorCode = ''; job.flowSnapshot.cleaning.blocklist = current.username !== 'Davi' || job.flowSnapshot.cleaning.blocklist; return launch(store, job); },
        async discardCache(id) {
            const store = storeFor(user().username), job = store.getJob(id);
            if (!job || active?.job.id === id || job.status === 'running') throw new Error('O cache de uma execução ativa não pode ser apagado.');
            const directory = path.resolve(job.jobDir);
            if (!/^[a-zA-Z0-9-]+$/.test(id) || path.basename(directory) !== id || path.basename(path.dirname(directory)) !== 'jobs') throw new Error('Pasta do cache inválida.');
            const allowed = [baseDirectory, ...getCacheDirectories()].map(root => path.resolve(root, path.basename(store.directory), 'jobs', id));
            if (!allowed.includes(directory)) throw new Error('Pasta do cache fora do armazenamento autorizado.');
            // Mark loss of resumability first; final output files are preserved.
            job.cacheDiscarded = true; job.cacheDiscardedAt = new Date().toISOString();
            store.saveJob(job);
            for (const name of ['generation', 'enrichment', 'api', 'cleaning']) {
                for (const extension of ['.jsonl', '.jsonl.tmp', '.jsonl.pack', '.jsonl.pack.tmp']) await fs.promises.rm(path.join(directory, name + extension), { force: true });
            }
            for (const name of ['stage-cache', 'api-results']) {
                const target = path.join(directory, name);
                const containsOutput = (job.outputs || []).some(output => {
                    const relative = path.relative(target, path.resolve(output.path));
                    return !relative || (!relative.startsWith('..' + path.sep) && relative !== '..' && !path.isAbsolute(relative));
                });
                if (!containsOutput) await fs.promises.rm(target, { recursive: true, force: true });
            }
            try { onUpdate(store.getJob(id)); } catch { /* Cleanup is already saved. */ }
            return store.getJob(id);
        },
        cancel(id) { const current = user(); if (!active || active.job.id !== id || active.job.owner !== current.username) throw new Error('Execução ativa não encontrada nesta conta.'); cancel(active); },
        cancelActive() { if (active) cancel(active); },
        isBusy: () => Boolean(active),
        output(id, filename) { const store = storeFor(user().username), job = store.getJob(id); if (!job || !['completed', 'empty'].includes(job.status) || !job.outputs.some(item => item.path === filename)) throw new Error('Arquivo não pertence a uma execução concluída desta conta.'); if (!fs.existsSync(filename)) throw new Error('Arquivo não está mais disponível nessa pasta.'); return filename; },
    };
}
module.exports = { createFlowManager };
