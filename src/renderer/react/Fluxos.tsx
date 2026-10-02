import { useCallback, useEffect, useRef, useState } from 'react';
import type { ReactNode } from 'react';
import { jsx, jsxs } from 'react/jsx-runtime';
import type { BqAuthStatus, CustomLayout, Flow, FlowAPI, FlowBootstrap, FlowFormat, FlowJob, FlowOperation, JobStatus, Operation } from './flowTypes';
import { LayoutEditor } from './LayoutEditor';
import { ReceitaSelect } from './ReceitaSelect';
import './fluxos.css';

const OPERATIONS: FlowOperation[] = [
    { id: 'c6', name: 'C6', pipelines: [90] }, { id: 'santander', name: 'Santander', pipelines: [119] },
    { id: 'pagbank', name: 'PagBank', pipelines: [34] }, { id: 'mercadopago', name: 'Mercado Pago · Hunter', pipelines: [58] },
];
const SITUACOES = [['01', 'Nula'], ['02', 'Ativa'], ['03', 'Suspensa'], ['04', 'Inapta'], ['08', 'Baixada']];
const STATUS: Record<JobStatus, string> = { running: 'Em execução', completed: 'Concluído', empty: 'Sem resultados', failed: 'Falhou', cancelled: 'Cancelado', interrupted: 'Interrompido' };
const STAGES: Record<string, string> = { queued: 'Na fila', preparing: 'Preparando', root: 'Raiz histórica', generation: 'Geração Receita', enrichment: 'Enriquecimento', cleaning: 'Limpeza', api: 'Disponibilidade C6', export: 'Exportação', output: 'Exportação', completed: 'Concluído', finalizing: 'Finalizando' };
const COUNTS: Record<string, string> = { apiConsulted: 'CNPJs consultados na API', apiAvailable: 'Disponíveis na API', apiClients: 'Clientes removidos pela API', apiBatches: 'Lotes API confirmados', generated: 'Gerados', enriched: 'Enriquecidos', cleaned: 'Após limpeza', kept: 'Mantidos', output: 'Na saída', exported: 'Exportados', removedRoot: 'Excluídos pela raiz', removedCnae: 'Excluídos por CNAE', removedBlocklist: 'Linhas excluídas pela blocklist (histórico)', blockedPhones: 'Telefones removidos pela blocklist', invalidPhones: 'Telefones inválidos no banco', landlines: 'Telefones fixos removidos', dirtyPhones: 'Telefones sujos removidos', ddiRemoved: 'DDIs normalizados', repeatedDocuments: 'CNPJs repetidos', repeatedPhones: 'Telefones repetidos', withoutPhones: 'Linhas descartadas sem telefone (total)', withoutPhonesBeforeFilters: 'Sem contato utilizável antes dos filtros', withoutPhonesAfterFilters: 'Sem contato após os filtros', withoutPhonesRepeatedOnly: 'Somente contatos já utilizados', ninthDigitAdded: 'Celulares com nono dígito corrigido', truncatedPhones: 'Contatos excedentes ao layout' };
const splitList = (text: string) => [...new Set(text.split(/[,;\s]+/).map(item => item.trim()).filter(Boolean))];
const copy = (flow: Flow): Flow => JSON.parse(JSON.stringify(flow));
const basename = (path: string) => path.split(/[\\/]/).pop() || path;
const dateLabel = (date: string) => { const value = new Date(date); return Number.isNaN(value.getTime()) ? 'Data indisponível' : value.toLocaleString('pt-BR'); };
const safeText = (value: unknown) => String(value ?? '').slice(0, 4000).replace(/postgres(?:ql)?:\/\/[^\s]+/gi, '[acesso protegido]').replace(/-----BEGIN[^]*?-----END[^\n]*-----/g, '[chave protegida]');

export function createFlow(operation: Operation = 'c6', defaults?: Partial<Flow>): Flow {
    return {
        id: '', name: '', operation, pipelines: OPERATIONS.find(item => item.id === operation)?.pipelines || [], revision: 0,
        generation: { limit: 100000, uf: [], cidade: [], bairro: [], cnaes: [], naturezas: [], dateFrom: '', dateTo: '', mei: 'all', phone: 'with', email: 'all', situacoes: ['02'], ...defaults?.generation },
        enrichment: { enabled: true, strategy: 'append', fillCpf: false, ...defaults?.enrichment },
        api: { enabled: operation === 'c6', keyMode: 'dupla', delayMs: 60000 },
        cleaning: { enabled: true, rootSource: 'bq', rootFile: '', blocklist: true, invalidPhones: true, removeLandlines: false, fillLivre5: false, prohibitedCnaes: [], ...defaults?.cleaning },
        output: { fileName: '', formatId: 'padrao', csv: false, rowsPerFile: 100000, includeSituacao: false, ...defaults?.output },
    };
}

function Field({ label, children, hint }: { label: string; children: ReactNode; hint?: string }) {
    return <label className="flow-field"><span>{label}</span>{children}{hint && <small>{hint}</small>}</label>;
}
function FlowProgress({ job }: { job: FlowJob }) {
    const render = (window as Window & { renderFlowProgress?: (job: FlowJob, jsxFactory: typeof jsx, jsxsFactory: typeof jsxs) => ReactNode }).renderFlowProgress;
    return render?.(job, jsx, jsxs) ?? null;
}
function RootInfo({ info }: { info: NonNullable<FlowJob['rootInfo']> }) {
    return <div className="flow-hint"><p>Raiz consultada: {safeText(info.source || 'Fonte registrada')} · {info.count ?? info.documents ?? info.rows ?? '—'} documentos{info.queriedAt ? ` · ${dateLabel(info.queriedAt)}` : ''}.</p>{info.pipelines?.length ? <p>Pipelines: {info.pipelines.join(', ')} · todas as fases do histórico disponível.</p> : null}{info.skipped || info.restored ? <p>Documentos descartados: {info.skipped || 0} · zeros recuperados com validação: {info.restored || 0}.</p> : null}{info.coverage && <p>{safeText(info.coverage)}</p>}</div>;
}
function Check({ label, checked, disabled, onChange }: { label: string; checked: boolean; disabled?: boolean; onChange: (value: boolean) => void }) {
    return <label className="flow-check"><input type="checkbox" checked={checked} disabled={disabled} onChange={event => onChange(event.target.checked)} /><span>{label}</span></label>;
}
function Section({ number, title, children, description }: { number: string; title: string; children: ReactNode; description?: string }) {
    return <section className="flow-section"><div className="flow-section-heading"><span>{number}</span><h3>{title}</h3></div>{description && <p className="flow-hint">{description}</p>}{children}</section>;
}

export function Fluxos() {
    const api = window.electronAPI as unknown as FlowAPI;
    const [bootstrap, setBootstrap] = useState<FlowBootstrap | null>(null);
    const [flows, setFlows] = useState<Flow[]>([]);
    const [jobs, setJobs] = useState<FlowJob[]>([]);
    const [draft, setDraft] = useState<Flow | null>(null);
    const [dirty, setDirty] = useState(false);
    const [pipelines, setPipelines] = useState('');
    const [lists, setLists] = useState({ prohibitedCnaes: '' });
    const [pane, setPane] = useState<'editor' | 'history'>('editor');
    const [error, setError] = useState('');
    const [notice, setNotice] = useState('');
    const [busy, setBusy] = useState('');
    const [folder, setFolder] = useState('');
    const [pendingSelection, setPendingSelection] = useState<Flow | 'new' | null>(null);
    const [deleteRequested, setDeleteRequested] = useState(false);
    const [selectedJob, setSelectedJob] = useState('');
    const [bqAuth, setBqAuth] = useState<BqAuthStatus>({ state: 'idle', message: '' });
    const [layoutEditor, setLayoutEditor] = useState<{ format: FlowFormat | null; asCopy: boolean } | null>(null);
    const dirtyRef = useRef(false);
    const draftRef = useRef<Flow | null>(null);
    const userRef = useRef('');
    const busyRef = useRef(false);
    const sessionRef = useRef(0);

    const loadDraft = useCallback((flow: Flow) => {
        const next = copy(flow);
        const locations = (value: unknown): string[] => Array.isArray(value) ? value : typeof value === 'string' ? value.split(',').map(item => item.trim()).filter(Boolean) : [];
        next.generation.cidade = locations(next.generation.cidade);
        next.generation.bairro = locations(next.generation.bairro);
        next.output.includeSituacao = false;
        next.api = { enabled: next.operation === 'c6' && (next.api?.enabled ?? true), keyMode: 'dupla', delayMs: 60000 };
        setDraft(next); draftRef.current = next;
        setPipelines(next.pipelines.join(', '));
        setLists({ prohibitedCnaes: next.cleaning.prohibitedCnaes.join(', ') });
        setDirty(false); dirtyRef.current = false;
        setPendingSelection(null); setDeleteRequested(false);
    }, []);

    const mergeJob = useCallback((job: FlowJob) => {
        if (!job?.id || !userRef.current || job.owner !== userRef.current) return;
        setJobs(previous => [job, ...previous.filter(item => item.id !== job.id)].sort((a, b) => b.createdAt.localeCompare(a.createdAt)).slice(0, 200));
    }, []);

    const refresh = useCallback(async () => {
        const result = await api.flowsBootstrap();
        if (!result.success || !result.user) {
            userRef.current = ''; sessionRef.current += 1; dirtyRef.current = false; draftRef.current = null;
            setBootstrap(null); setFlows([]); setJobs([]); setDraft(null);
            setBqAuth({ state: 'idle', message: '' });
            setLayoutEditor(null);
            throw new Error(result.message || 'Não foi possível carregar seus fluxos.');
        }
        const changedUser = userRef.current !== result.user.username;
        if (changedUser) {
            sessionRef.current += 1;
            dirtyRef.current = false; draftRef.current = null;
            setFolder(''); setSelectedJob('');
            setLayoutEditor(null);
        }
        userRef.current = result.user.username;
        setBootstrap(result);
        setBqAuth(result.access?.bqAuth || { state: 'idle', message: '' });
        setFlows(result.flows || []);
        setJobs((result.jobs || []).filter(job => job.owner === result.user!.username).slice(0, 200));
        if (!dirtyRef.current) {
            const selected = result.flows?.find(flow => flow.id === draftRef.current?.id);
            loadDraft(selected || result.flows?.[0] || createFlow('c6', result.defaults));
        }
    }, [api, loadDraft]);

    useEffect(() => {
        let active = true;
        let unsubscribe = () => {};
        let unsubscribeAuth = () => {};
        try {
            unsubscribe = api.onFlowUpdate(mergeJob);
            unsubscribeAuth = api.onFlowBqAuthUpdate?.(update => { if (update.owner === userRef.current) setBqAuth(update); }) || (() => {});
            refresh().catch(reason => { if (active) setError(safeText(reason instanceof Error ? reason.message : 'Falha ao carregar fluxos.')); });
        } catch { setError('Esta versão do aplicativo ainda não disponibiliza os fluxos.'); }
        // A tab may stay mounted while the login changes. Revalidate the owner before showing stored data.
        const revalidate = () => { refresh().catch(() => {}); };
        window.addEventListener('focus', revalidate);
        return () => { active = false; unsubscribe(); unsubscribeAuth(); window.removeEventListener('focus', revalidate); };
    }, [api, refresh, mergeJob]);

    function edit(next: Flow) { setDraft(next); draftRef.current = next; setDirty(true); dirtyRef.current = true; setDeleteRequested(false); }
    function generation(next: Partial<Flow['generation']>) { if (draft) edit({ ...draft, generation: { ...draft.generation, ...next } }); }
    function enrichment(next: Partial<Flow['enrichment']>) { if (draft) edit({ ...draft, enrichment: { ...draft.enrichment, ...next } }); }
    function cleaning(next: Partial<Flow['cleaning']>) { if (draft) edit({ ...draft, cleaning: { ...draft.cleaning, ...next } }); }
    function output(next: Partial<Flow['output']>) { if (draft) edit({ ...draft, output: { ...draft.output, ...next } }); }
    function editList(key: keyof typeof lists, value: string) {
        setLists(previous => ({ ...previous, [key]: value }));
        if (key === 'prohibitedCnaes') cleaning({ prohibitedCnaes: splitList(value) });
        else generation({ [key]: splitList(value) });
    }
    function selectFlow(next: Flow | 'new') {
        setPane('editor');
        if (dirty) { setPendingSelection(next); return; }
        loadDraft(next === 'new' ? createFlow(draft?.operation, bootstrap?.defaults) : next);
    }

    async function perform(key: string, action: () => Promise<void>) {
        if (busyRef.current) return;
        busyRef.current = true; setBusy(key); setError(''); setNotice('');
        try { await action(); }
        catch (reason) { setError(safeText(reason instanceof Error ? reason.message : 'Não foi possível concluir a ação. Tente novamente.')); }
        finally { busyRef.current = false; setBusy(''); }
    }
    const requireSuccess = (result: { success: boolean; message?: string }) => { if (!result.success) throw new Error(result.message || 'Não foi possível concluir a ação.'); };
    const operations = bootstrap?.operations?.length ? bootstrap.operations : OPERATIONS;
    const isDavi = bootstrap?.user?.username === 'Davi';
    const activeJob = jobs.find(job => job.status === 'running');
    const job = jobs.find(item => item.id === selectedJob) || jobs[0];
    const formats = bootstrap?.formats || [];
    const format = formats.find(item => item.id === draft?.output.formatId);
    const canConfigure = bootstrap?.user?.role?.toLowerCase() === 'admin' || isDavi;
    const canRenewBq = ['gcloud', 'adc'].includes(bootstrap?.access?.bqLoginMode || 'gcloud');
    const renewingBq = bqAuth.state === 'renewing';
    function savedLayout(layout: CustomLayout) {
        setBootstrap(previous => previous ? { ...previous, formats: [...(previous.formats || []).filter(item => item.id !== layout.id), layout] } : previous);
        if (draft) output({ formatId: layout.id });
        setLayoutEditor(null);
        setNotice('Layout salvo e selecionado. Salve o fluxo para usar esta configuração.');
    }
    function deletedLayout() {
        const id = layoutEditor?.format?.id;
        setBootstrap(previous => previous ? { ...previous, formats: (previous.formats || []).filter(item => item.id !== id) } : previous);
        if (draft?.output.formatId === id) output({ formatId: 'padrao' });
        setLayoutEditor(null);
        setNotice('Layout excluído. O histórico das execuções foi preservado.');
    }
    async function renewBq() {
        await perform('bq-login', async () => {
            const session = sessionRef.current;
            const result = await api.flowsRenewBq();
            if (session !== sessionRef.current) return;
            requireSuccess(result);
            await refresh();
            setNotice(result.message || 'Login Google renovado.');
        });
    }

    async function save() {
        if (!draft) return;
        await perform('save', async () => {
            const pipelineValues = splitList(pipelines);
            if (!draft.name.trim()) throw new Error('Informe um nome para o fluxo.');
            if (draft.name.trim().length > 100) throw new Error('Use até 100 caracteres no nome.');
            if (pipelineValues.some(value => !/^\d+$/.test(value) || Number(value) < 1 || !Number.isSafeInteger(Number(value)))) throw new Error('Pipelines devem ser números inteiros positivos, separados por vírgula.');
            if (draft.cleaning.rootSource === 'bq' && !pipelineValues.length) throw new Error('Informe ao menos um pipeline para consultar a raiz BQ.');
            if (draft.generation.limit !== null && (!Number.isSafeInteger(draft.generation.limit) || draft.generation.limit < 1)) throw new Error('Informe um limite inteiro positivo ou deixe vazio para gerar sem limite.');
            if (!Number.isInteger(draft.output.rowsPerFile) || draft.output.rowsPerFile < 1 || draft.output.rowsPerFile > 1000000) throw new Error('Informe de 1 a 1.000.000 linhas por arquivo.');
            if (!draft.generation.situacoes.length) throw new Error('Selecione ao menos uma situação cadastral.');
            if (draft.generation.uf.some(value => !/^[A-Z]{2}$/.test(value))) throw new Error('Informe cada UF com duas letras, separadas por vírgula.');
            if (draft.generation.dateFrom && draft.generation.dateTo && draft.generation.dateFrom > draft.generation.dateTo) throw new Error('A data inicial deve ser anterior ou igual à data final.');
            if (draft.generation.cnaes.some(value => !/^\d{7}$/.test(value))) throw new Error('CNAEs devem ter 7 dígitos, sem pontuação.');
            if (draft.generation.naturezas.some(value => !/^\d{4}$/.test(value))) throw new Error('Naturezas jurídicas devem ter 4 dígitos, sem pontuação.');
            if (draft.cleaning.prohibitedCnaes.some(value => !/^\d{7}$/.test(value))) throw new Error('CNAEs proibidos devem ter 7 dígitos, sem pontuação.');
            if (draft.cleaning.enabled && draft.cleaning.rootSource === 'file' && !draft.cleaning.rootFile.trim()) throw new Error('Selecione o arquivo da raiz.');
            const result = await api.flowsSave({ ...draft, name: draft.name.trim(), pipelines: pipelineValues.map(Number), cleaning: { ...draft.cleaning, blocklist: isDavi ? draft.cleaning.blocklist : true } });
            requireSuccess(result);
            if (result.flow) { loadDraft(result.flow); setFlows(previous => [result.flow!, ...previous.filter(item => item.id !== result.flow!.id)]); }
            else { dirtyRef.current = false; setDirty(false); await refresh(); }
            setNotice('Fluxo salvo. A próxima execução usará esta configuração.');
        });
    }

    async function start() {
        if (!draft?.id || dirty || activeJob) return;
        await perform('start', async () => {
            let destination = folder;
            if (!destination) {
                const selected = await api.flowsSelectFolder();
                if (selected.cancelled) return;
                requireSuccess(selected);
                destination = selected.path || '';
                if (!destination) throw new Error('Selecione uma pasta de saída.');
                setFolder(destination);
            }
            const result = await api.flowsStart({ flowId: draft.id, outputDirectory: destination });
            requireSuccess(result);
            if (result.job) { mergeJob(result.job); setSelectedJob(result.job.id); }
            setPane('history');
        });
    }

    return <div className="flows-app" aria-busy={!!busy}>
        <header className="flows-heading"><div><h1>Gerar listas</h1><p>Salve a configuração por operação e acompanhe cada etapa da geração.</p></div><div className="flow-view-switch" aria-label="Visualização"><button disabled={!!layoutEditor} aria-pressed={pane === 'editor'} onClick={() => setPane('editor')}>Fluxos</button><button disabled={!!layoutEditor} aria-pressed={pane === 'history'} onClick={() => setPane('history')}>Histórico <span>{jobs.length}</span></button></div></header>
        {error && <p className="flow-error" role="alert">{error}</p>}
        {notice && <p className="flow-notice" role="status">{notice}</p>}
        {bootstrap && bqAuth.state !== 'idle' && <div className={bqAuth.state === 'failed' ? 'flow-error' : 'flow-notice'} role={bqAuth.state === 'failed' ? 'alert' : 'status'}><p>{safeText(bqAuth.message)}</p>{bqAuth.state === 'ready' && <button disabled={!!busy} onClick={() => setPane('history')}>Ver histórico</button>}{bqAuth.state === 'failed' && canRenewBq && <button disabled={!!busy || !!activeJob} onClick={() => void renewBq()}>Renovar login Google</button>}</div>}
        {!bootstrap ? <div className="flow-empty">{error ? 'Reabra esta aba após configurar o acesso ao aplicativo.' : 'Carregando seus fluxos…'}<button disabled={!!busy} onClick={() => perform('refresh', refresh)}>Tentar novamente</button></div> : <>
        {pane === 'editor' ? <div className="flows-workspace">
            <aside className="flow-sidebar" aria-label="Fluxos salvos"><div className="flow-sidebar-heading"><h2>Meus fluxos</h2><span>{flows.length}</span></div><button className="flow-new" disabled={!!busy || !!layoutEditor} onClick={() => selectFlow('new')}>+ Novo fluxo</button><nav className="flow-presets">{flows.map(flow => <button key={flow.id} disabled={!!busy} aria-current={draft?.id === flow.id ? 'true' : undefined} onClick={() => selectFlow(flow)}><strong>{flow.name}</strong><span>{operations.find(item => item.id === flow.operation)?.name || OPERATIONS.find(item => item.id === flow.operation)?.name} · revisão {flow.revision}</span></button>)}</nav>{!flows.length && <p className="flow-hint">Crie seu primeiro fluxo. Você poderá reutilizar seus filtros em outras listas.</p>}<p className="flow-sidebar-foot">Os fluxos e o histórico pertencem a {bootstrap.user?.username}.</p></aside>
            {draft && <div className="flow-editor">
                <div className="flow-editor-heading"><div><h2>{draft.id ? draft.name : 'Novo fluxo'}</h2><span>{dirty ? 'Alterações ainda não salvas' : draft.id ? `Revisão ${draft.revision} salva` : 'Configure e salve para gerar'}</span></div><div className="flow-actions"><button disabled={!!busy || !!layoutEditor} onClick={() => { const next = { ...copy(draft), id: '', revision: 0, name: `${draft.name || 'Fluxo'} · cópia` }; loadDraft(next); edit(next); }}>Duplicar</button>{draft.id && <button className="flow-delete" disabled={!!busy || !!layoutEditor || activeJob?.flowId === draft.id} onClick={() => setDeleteRequested(true)}>Excluir</button>}</div></div>
                {pendingSelection && <div className="flow-confirm" role="alert"><p>Este fluxo tem alterações não salvas.</p><button onClick={() => { loadDraft(pendingSelection === 'new' ? createFlow(draft.operation, bootstrap.defaults) : pendingSelection); }}>Descartar alterações e continuar</button><button onClick={() => setPendingSelection(null)}>Continuar editando</button></div>}
                {deleteRequested && <div className="flow-confirm" role="alert"><p>Excluir o fluxo “{draft.name}”? O histórico das execuções será preservado.</p><button disabled={!!busy} onClick={() => perform('delete', async () => { requireSuccess(await api.flowsDelete(draft.id)); dirtyRef.current = false; draftRef.current = null; await refresh(); setNotice('Fluxo excluído.'); })}>Excluir fluxo</button><button onClick={() => setDeleteRequested(false)}>Manter fluxo</button></div>}
                {layoutEditor ? <LayoutEditor format={layoutEditor.format} asCopy={layoutEditor.asCopy} fields={bootstrap.layoutFields || []} flow={draft} api={api} onSaved={savedLayout} onClose={() => setLayoutEditor(null)} onDeleted={deletedLayout} /> : <form onSubmit={event => { event.preventDefault(); void save(); }}>
                <fieldset className="flow-form-fields" disabled={!!busy}>
                    <Section number="01" title="Nome e operação"><div className="flow-grid"><Field label="Nome do fluxo"><input value={draft.name} maxLength={100} placeholder="Ex.: C6 · comércio SP" onChange={event => edit({ ...draft, name: event.target.value })} /></Field><Field label="Operação"><select value={draft.operation} onChange={event => { const operation = event.target.value as Operation; const nextPipelines = operations.find(item => item.id === operation)?.pipelines || OPERATIONS.find(item => item.id === operation)?.pipelines || []; edit({ ...draft, operation, pipelines: nextPipelines, api: { enabled: operation === 'c6', keyMode: 'dupla', delayMs: 60000 } }); setPipelines(nextPipelines.join(', ')); }}>{operations.map(item => <option key={item.id} value={item.id}>{item.name || item.nome || item.label || item.id}</option>)}</select></Field></div></Section>
                    <Section number="02" title="Geração na Receita" description="A situação Ativa vem selecionada. Datas referem-se à abertura da empresa."><div className="flow-grid flow-grid--three">
                        <Field label="Limite de empresas" hint="Vazio = sem limite. Busca todas as empresas que atendem aos filtros."><input type="number" min={1} step={1} value={draft.generation.limit ?? ''} placeholder="Sem limite" onChange={event => generation({ limit: event.target.value === '' ? null : Number(event.target.value) })} /></Field>
                        <ReceitaSelect label="UFs" field="uf" values={draft.generation.uf} api={api} disabled={!!busy || !bootstrap.access?.receitaConfigured} onChange={uf => generation({ uf, cidade: [], bairro: [] })} />
                        <ReceitaSelect label="Cidades" field="cidade" values={draft.generation.cidade} uf={draft.generation.uf} api={api} disabled={!!busy || !bootstrap.access?.receitaConfigured} onChange={cidade => generation({ cidade, bairro: [] })} />
                        <ReceitaSelect label="Bairros" field="bairro" values={draft.generation.bairro} uf={draft.generation.uf} cidade={draft.generation.cidade} api={api} disabled={!!busy || !bootstrap.access?.receitaConfigured} onChange={bairro => generation({ bairro })} />
                        <Field label="Abertura a partir de"><input type="date" value={draft.generation.dateFrom} onChange={event => generation({ dateFrom: event.target.value })} /></Field>
                        <Field label="Abertura até"><input type="date" value={draft.generation.dateTo} onChange={event => generation({ dateTo: event.target.value })} /></Field>
                        <ReceitaSelect label="CNAEs" field="cnaes" values={draft.generation.cnaes} api={api} disabled={!!busy || !bootstrap.access?.receitaConfigured} onChange={cnaes => generation({ cnaes })} />
                        <ReceitaSelect label="Naturezas jurídicas" field="naturezas" values={draft.generation.naturezas} api={api} disabled={!!busy || !bootstrap.access?.receitaConfigured} onChange={naturezas => generation({ naturezas })} />
                        <Field label="MEI"><select value={draft.generation.mei} onChange={event => generation({ mei: event.target.value as Flow['generation']['mei'] })}><option value="all">Todos</option><option value="yes">Somente MEI</option><option value="no">Excluir MEI</option></select></Field>
                        <Field label="Telefone na Receita"><select value={draft.generation.phone} onChange={event => generation({ phone: event.target.value as Flow['generation']['phone'] })}><option value="all">Com ou sem telefone</option><option value="with">Com telefone</option><option value="without">Sem telefone</option></select></Field>
                        <Field label="E-mail na Receita"><select value={draft.generation.email} onChange={event => generation({ email: event.target.value as Flow['generation']['email'] })}><option value="all">Com ou sem e-mail</option><option value="with">Com e-mail</option><option value="without">Sem e-mail</option></select></Field>
                    </div><fieldset className="flow-situacoes"><legend>Situações cadastrais</legend><div className="flow-checks">{SITUACOES.map(([code, name]) => <Check key={code} label={`${code} · ${name}`} checked={draft.generation.situacoes.includes(code)} onChange={enabled => generation({ situacoes: enabled ? [...draft.generation.situacoes, code] : draft.generation.situacoes.filter(item => item !== code) })} />)}</div></fieldset></Section>
                    <Section number="03" title="Enriquecimento"><Check label="Enriquecer empresas e contatos" checked={draft.enrichment.enabled} onChange={enabled => enrichment({ enabled })} /><div className="flow-grid"><Field label="Telefones encontrados"><select disabled={!draft.enrichment.enabled} value={draft.enrichment.strategy} onChange={event => enrichment({ strategy: event.target.value as Flow['enrichment']['strategy'] })}><option value="append">Adicionar aos telefones existentes</option><option value="overwrite">Substituir os telefones existentes</option><option value="ignore">Preencher apenas quando estiver vazio</option></select></Field><Check label="Preencher CPF do sócio" disabled={!draft.enrichment.enabled} checked={draft.enrichment.fillCpf} onChange={fillCpf => enrichment({ fillCpf })} /></div></Section>
                    <Section number="04" title="Raiz e limpeza" description="Normalizar telefones, remover duplicados e descartar registros sem telefone são etapas obrigatórias."><div className="flow-grid"><Field label="Fonte da raiz histórica"><select value={draft.cleaning.rootSource} onChange={event => cleaning({ rootSource: event.target.value as Flow['cleaning']['rootSource'] })}><option value="bq">BigQuery · histórico Bitrix</option><option value="neon">Neon · raiz comercial</option><option value="file">Arquivo local</option><option value="none">Sem cruzamento de raiz</option></select></Field><Field label="Pipelines históricos" hint="Todas as fases. C6: 90 · Santander: 119 · PagBank: 34 · MP: 58."><input disabled={draft.cleaning.rootSource !== 'bq'} value={pipelines} placeholder="90, 119" onChange={event => { setPipelines(event.target.value); edit({ ...draft, pipelines: splitList(event.target.value).filter(value => /^\d+$/.test(value)).map(Number) }); }} /></Field></div>
                    {draft.cleaning.rootSource === 'file' && <div className="flow-file-picker"><Field label="Arquivo da raiz"><input value={draft.cleaning.rootFile} readOnly placeholder="Selecione um arquivo" /></Field><button type="button" onClick={() => perform('root-file', async () => { const paths = await api.selectFile({ title: 'Selecione a raiz histórica', multi: false }); if (paths?.[0]) cleaning({ rootFile: paths[0] }); })}>Selecionar arquivo</button></div>}
                    <div className="flow-policy"><Check label="Aplicar blocklist" checked={isDavi ? draft.cleaning.blocklist : true} disabled={!isDavi} onChange={blocklist => cleaning({ blocklist })} /><span>{isDavi ? 'A dispensa fica registrada na configuração deste fluxo.' : 'Obrigatória para seu usuário. Somente Davi pode dispensar.'}</span></div>
                    <Check label="Ativar filtros opcionais de limpeza" checked={draft.cleaning.enabled} onChange={enabled => cleaning({ enabled })} /><div className="flow-checks flow-optional"><Check label="Remover telefones inválidos" disabled={!draft.cleaning.enabled} checked={draft.cleaning.invalidPhones} onChange={invalidPhones => cleaning({ invalidPhones })} /><Check label="Remover telefones fixos" disabled={!draft.cleaning.enabled} checked={draft.cleaning.removeLandlines} onChange={removeLandlines => cleaning({ removeLandlines })} /><Check label="Preencher LIVRE5" disabled={!draft.cleaning.enabled} checked={draft.cleaning.fillLivre5} onChange={fillLivre5 => cleaning({ fillLivre5 })} /></div><p className="flow-hint">A raiz, os CNAEs e os filtros opcionais só são aplicados quando ativos. Os dados das etapas ficam salvos para retomada.</p><Field label="CNAEs proibidos" hint="7 dígitos por código. Aplicados com os filtros opcionais ativos."><input disabled={!draft.cleaning.enabled} value={lists.prohibitedCnaes} onChange={event => editList('prohibitedCnaes', event.target.value)} placeholder="Nenhum" /></Field></Section>
                    {draft.operation === 'c6' && <Section number="05" title="Disponibilidade na API" description="Consulta online após o enriquecimento, antes dos filtros finais e da exportação. As duas chaves verificam disponibilidade no C6."><div className="flow-checks"><Check label="Validar na Limpeza API antes de exportar" checked={draft.api?.enabled === true} onChange={enabled => edit({ ...draft, api: { enabled, keyMode: 'dupla', delayMs: 60000 } })} /></div>{draft.api?.enabled && <><p className="flow-hint">Chave dupla C6/IM · intervalo de 1 minuto entre lotes. Apenas CNPJs disponíveis entram na saída.</p>{bootstrap.access?.apiConfigured === false && <p className="flow-hint">Importe a licença com as duas chaves na tela de login antes de executar.</p>}</>}</Section>}
                    <Section number={draft.operation === 'c6' ? '06' : '05'} title="Arquivos de saída">
                        <Field label="Nome dos arquivos" hint="Sem extensão. Se vazio, usa o nome do fluxo."><input name="outputFileName" maxLength={100} value={draft.output.fileName || ''} onChange={event => output({ fileName: event.target.value })} placeholder="Ex.: lista rca" /></Field>
                        {draft.output.fileName?.trim() && <p className="flow-hint" aria-live="polite">Prévia: {draft.output.fileName.trim()} parte1.xlsx · {draft.output.fileName.trim()} parte2.xlsx{draft.output.csv ? ' · CSV com os mesmos nomes' : ''}</p>}
                        <div className="flow-grid"><Field label="Layout"><select value={draft.output.formatId} onChange={event => output({ formatId: event.target.value })}>{!formats.length && <option value="padrao">Padrão</option>}{formats.map(item => <option key={item.id} value={item.id}>{item.nome}{item.custom ? ' · próprio' : ''}</option>)}</select></Field><Field label="Linhas por arquivo" hint="Até 1.000.000. XLSX é sempre gerado."><input type="number" min={1} max={1000000} step={1} value={draft.output.rowsPerFile || ''} onChange={event => output({ rowsPerFile: Number(event.target.value) })} /></Field></div>{format && <p className="flow-layout-preview">Colunas: {format.colunas.map(column => typeof column === 'string' ? column : column.header).filter(Boolean).join(' · ')}</p>}<div className="flow-actions flow-layout-actions"><button type="button" disabled={!format || !bootstrap.layoutFields?.length} onClick={() => setLayoutEditor({ format: format || null, asCopy: !format?.custom })}>{format?.custom ? 'Editar layout' : 'Personalizar layout'}</button><button type="button" disabled={!bootstrap.layoutFields?.length} onClick={() => setLayoutEditor({ format: null, asCopy: false })}>Novo layout</button>{format?.custom && <button type="button" onClick={() => setLayoutEditor({ format, asCopy: true })}>Duplicar layout</button>}</div><div className="flow-checks"><Check label="Gerar CSV adicional" checked={draft.output.csv} onChange={csv => output({ csv })} /></div>
                    </Section>
                </fieldset>
                <div className="flow-run-summary"><span>{!draft.cleaning.enabled || draft.cleaning.rootSource === 'none' ? 'Modo cadência · sem raiz' : `Raiz: ${draft.cleaning.rootSource.toUpperCase()}`}</span><span>Blocklist: {!isDavi || draft.cleaning.blocklist ? 'ativa' : 'dispensada por Davi'}</span><span>Enriquecimento: {draft.enrichment.enabled ? 'ativo' : 'desligado'}</span><span>Saída: {draft.output.csv ? 'XLSX + CSV' : 'XLSX'}</span></div>
                <footer className="flow-run-bar"><div><button type="submit" className="flow-save" disabled={!!busy}>{busy === 'save' ? 'Salvando…' : 'Salvar fluxo'}</button><button type="button" className="flow-primary" disabled={!!busy || !draft.id || dirty || !!activeJob || !bootstrap.access?.receitaConfigured} onClick={start}>{busy === 'start' ? 'Iniciando…' : 'Gerar lista'}</button></div><span>{activeJob ? 'Há uma execução em andamento.' : dirty || !draft.id ? 'Salve o fluxo para iniciar.' : !bootstrap.access?.receitaConfigured ? 'Configure o banco da Receita no login.' : 'Cada execução preserva a revisão usada.'}</span><button type="button" disabled={!!busy} className="flow-folder" title={folder} onClick={() => perform('folder', async () => { const result = await api.flowsSelectFolder(); if (result.cancelled) return; requireSuccess(result); if (result.path) setFolder(result.path); })}>{folder ? `Pasta: ${basename(folder)}` : 'Escolher pasta de saída'}</button></footer>
                </form>}
            </div>}
        </div> : <section className="flow-history" aria-label="Histórico de execuções"><div className="flow-history-heading"><h2>Execuções</h2><button disabled={!!busy} onClick={() => perform('refresh', refresh)}>Atualizar</button></div>{!jobs.length ? <div className="flow-empty"><strong>Nenhuma execução registrada</strong><p>Salve um fluxo e gere sua primeira lista.</p></div> : <><div className="flow-history-table"><table><thead><tr><th>Fluxo</th><th>Início</th><th>Situação</th><th>Etapa</th></tr></thead><tbody>{jobs.map(item => <tr key={item.id} className={job?.id === item.id ? 'flow-selected-row' : ''}><td><button aria-pressed={job?.id === item.id} onClick={() => setSelectedJob(item.id)}>{item.flowName}</button></td><td>{dateLabel(item.createdAt)}</td><td><span className={`flow-status flow-status--${item.status}`}>{STATUS[item.status] || item.status}</span></td><td>{STAGES[item.stage] || safeText(item.stage)}</td></tr>)}</tbody></table></div>{job && <div className="flow-job-detail"><div className="flow-job-heading"><div><h3>{job.flowName}</h3><p>{STATUS[job.status]} · {STAGES[job.stage] || safeText(job.stage)} · atualizado {dateLabel(job.updatedAt)}</p></div><div className="flow-actions">{job.status === 'running' && <button disabled={!!busy} onClick={() => perform('cancel', async () => { const result = await api.flowsCancel(job.id); requireSuccess(result); if (result.job) mergeJob(result.job); setNotice('Cancelamento solicitado. Aguarde a etapa parar.'); })}>Cancelar execução</button>}{!job.cacheDiscarded && ['failed', 'cancelled', 'interrupted'].includes(job.status) && <button className="flow-primary" disabled={!!busy || !!activeJob || renewingBq} onClick={() => perform('resume', async () => { const result = await api.flowsResume(job.id); requireSuccess(result); if (result.job) mergeJob(result.job); })}>Retomar execução</button>}</div></div><FlowProgress job={job} /><div className="flow-job-counts">{[['generated', 'Gerados'], ['enriched', 'Enriquecidos'], ['cleaned', 'Após limpeza'], ['exported', 'Exportados']].map(([key, label]) => <div key={key}><span>{label}</span><strong>{job.counts?.[key] == null ? '—' : Number(job.counts[key]).toLocaleString('pt-BR')}</strong></div>)}</div><dl className="flow-other-counts">{Object.entries(job.counts || {}).filter(([key]) => !['generated', 'enriched', 'cleaned', 'exported'].includes(key)).map(([key, count]) => <div key={key}><dt>{COUNTS[key] || key}</dt><dd>{count == null ? 'Ainda não informado' : Number(count).toLocaleString('pt-BR')}</dd></div>)}</dl><p className="flow-hint">“—” significa etapa ainda não informada. Zero indica que a etapa contou nenhum registro.</p>{job.flowSnapshot && <p className="flow-hint">Configuração congelada: revisão {job.flowSnapshot.revision} · {OPERATIONS.find(item => item.id === job.flowSnapshot?.operation)?.name || 'Operação'} · raiz {job.flowSnapshot.cleaning?.rootSource || 'não informada'} · pipelines {job.flowSnapshot.pipelines?.join(', ') || 'nenhum'}</p>}{job.rootInfo && <RootInfo info={job.rootInfo} />}{job.error && <p className="flow-error" role="alert">{safeText(job.error)}</p>}{job.cacheDiscarded ? <p className="flow-hint">Cache apagado. Esta execução não pode ser retomada; os arquivos finais foram preservados.</p> : job.status !== 'running' && api.flowsDiscardCache && <button disabled={!!busy} onClick={() => { if (!window.confirm('Apagar o cache desta execução? Ela não poderá ser retomada. O histórico e os arquivos finais serão preservados.')) return; void perform('discard-cache', async () => { const result = await api.flowsDiscardCache!(job.id); requireSuccess(result); if (result.job) mergeJob(result.job); setNotice('Cache apagado. Histórico e arquivos finais preservados.'); }); }}>Apagar cache desta execução</button>}<h4>Arquivos</h4>{job.outputs?.length && ['completed', 'empty'].includes(job.status) ? <ul className="flow-outputs">{job.outputs.map(outputFile => <li key={outputFile.path}><div><strong>{basename(outputFile.path)}</strong><span>{outputFile.kind} · {outputFile.rows.toLocaleString('pt-BR')} linhas</span></div><button disabled={!!busy} onClick={() => perform('open', async () => { requireSuccess(await api.flowsOpenOutput({ jobId: job.id, path: outputFile.path })); })}>Abrir arquivo</button></li>)}</ul> : <p className="flow-hint">{job.status === 'empty' ? 'A execução terminou sem registros para exportar.' : 'Os arquivos finais aparecem após a conclusão.'}</p>}<h4>Atividade</h4><pre className="flow-logs" aria-label="Últimas mensagens da execução">{(job.logs || []).slice(-300).map(safeText).join('\n') || 'Aguardando mensagens da execução.'}</pre><p className="flow-hint">Últimas 300 mensagens. A retomada preserva os lotes e arquivos já confirmados desta execução.</p></div>}</>}</section>}
        <details className="flow-storage">
            <summary>Armazenamento do processamento</summary>
            <p className="flow-hint">Cache: {bootstrap.cacheDirectory || 'Pasta padrão do aplicativo'}</p>
            <p className="flow-hint">Novas execuções usam cache comprimido. Etapas já consumidas são liberadas; após a exportação, ficam os arquivos finais e o histórico. Execuções com erro mantêm os lotes necessários para retomar.</p>
            <button disabled={!!busy || !api.flowsSelectCacheFolder} onClick={() => perform('cache-folder', async () => { const result = await api.flowsSelectCacheFolder!(); if (result.cancelled) return; requireSuccess(result); await refresh(); setNotice('Pasta alterada para novas execuções. As retomadas continuam na pasta original.'); })}>Escolher pasta do cache</button>
        </details>
        <details className="flow-access">
            <summary>Acessos às fontes <span>Receita: {bootstrap.access?.receitaConfigured ? 'configurada' : 'pendente'} · BQ: {bootstrap.access?.bqConfigured ? 'configurado' : 'pendente'}</span></summary>
            <p className="flow-hint">Os acessos ficam no armazenamento privado do aplicativo. Credenciais salvas não são exibidas.</p>
            {canConfigure ? <div className="flow-access-grid">
                <div>
                    <h3>Bancos do aplicativo</h3>
                    <p className="flow-hint">Neon: {bootstrap.access?.neonConfigured ? 'acesso do login reutilizado para enriquecimento, raiz comercial e filtros.' : 'configure o banco do Gerenciador na tela de login.'}</p>
                    <p className="flow-hint">Receita: {bootstrap.access?.receitaConfigured ? 'acesso salvo reutilizado para geração e situação cadastral.' : 'configure o banco da Receita na tela de login.'}</p>
                    <p className="flow-hint">Para adicionar ou alterar os dois bancos, use “Salvar e testar” na tela de login.</p>
                </div>
                <div>
                    <h3>BigQuery</h3>
                    <p className="flow-hint">O token renova automaticamente. Se o login Google perder a validade, entre novamente pelo navegador.</p>
                    <div className="flow-actions">
                        <button disabled={!!busy || renewingBq || !!activeJob} onClick={() => perform('bq-access', async () => { const result = await api.flowsConfigureBq(); requireSuccess(result); await refresh(); setNotice(result.message || 'Acesso BQ configurado.'); })}>Importar chave BQ</button>
                        {canRenewBq && <button disabled={!!busy || renewingBq || !!activeJob} onClick={() => void renewBq()}>{renewingBq || busy === 'bq-login' ? 'Aguardando login…' : 'Renovar login Google'}</button>}
                        <button disabled={!!busy || renewingBq || !bootstrap.access?.bqConfigured} onClick={() => perform('bq-test', async () => { const result = await api.flowsTestBq(); requireSuccess(result); setNotice(result.message || 'Acesso ao BigQuery verificado.'); })}>{busy === 'bq-test' ? 'Verificando…' : 'Testar BQ'}</button>
                    </div>
                    {canRenewBq ? <Check label="Abrir login Google automaticamente quando expirar" checked={bootstrap.access?.bqAutoLogin !== false} disabled={!!busy || renewingBq} onChange={enabled => { void perform('bq-auto-login', async () => { requireSuccess(await api.flowsBqAutoLogin(enabled)); await refresh(); }); }} /> : <p className="flow-hint">Este acesso usa uma credencial importada. Se ela for revogada, importe uma nova chave BQ.</p>}
                </div>
            </div> : <p className="flow-hint">Peça a um administrador para configurar os acessos às fontes.</p>}
        </details>
        </>}
    </div>;
}
