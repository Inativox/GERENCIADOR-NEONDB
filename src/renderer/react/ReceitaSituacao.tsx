import { useEffect, useRef, useState } from 'react';
import type { ReactNode } from 'react';
import './fluxos.css';
import { AlertIcon, CheckIcon, FileIcon, SearchIcon, SpinnerIcon } from './icons';
type Job = { id: string; owner: string; name: string; status: string; message: string; output: string | null; csvOutput?: string | null; counts: { processed: number; found: number; notFound: number; invalid: number; removed?: number; mei?: number } };
type Options = { removeMei: boolean; csv: boolean };
// Mesmos valores de KEEP_OPTIONS em src/main/receitaSituacao.js. "outra" (código fora dos
// cinco da Receita) não tem caixa: acompanha o "Todos" e só sai quando algo é desmarcado.
type Keep = 'ativa' | 'baixada' | 'inapta' | 'suspensa' | 'nula' | 'outra' | 'nao_encontrado' | 'invalido';
// Tom de cada situação, pelas cores semânticas do tema: da ativa (verde) até a baixada (vermelho).
type Tone = 'ok' | 'warn' | 'alert' | 'stop' | 'none';
const KEEP: [Keep, string, Tone][] = [['ativa', 'Ativa', 'ok'], ['suspensa', 'Suspensa', 'warn'], ['inapta', 'Inapta', 'alert'], ['baixada', 'Baixada', 'stop'], ['nula', 'Nula', 'stop'], ['nao_encontrado', 'Não encontrado', 'none'], ['invalido', 'Documento inválido', 'none']];
const TONE_BY_CODE: Record<string, Tone> = { '01': 'stop', '02': 'ok', '03': 'warn', '04': 'alert', '08': 'stop' };
const ALL: Keep[] = [...KEEP.map(([value]) => value), 'outra'];
const PRESETS: [string, Keep[]][] = [['Todos', ALL], ['Só ativas', ['ativa']], ['Só baixadas', ['baixada']], ['Sem inaptas', ALL.filter(value => value !== 'inapta')], ['Só problemáticas', ['baixada', 'inapta', 'suspensa', 'nula']]];
const same = (a: Keep[], b: Keep[]) => a.length === b.length && a.every(value => b.includes(value));
const count = (value: number) => value.toLocaleString('pt-BR');
type Result = Record<string, string | null>;
type Response = { success: boolean; message?: string; configured?: boolean; job?: Job | null; file?: { id: string; name: string }; found?: boolean; result?: Result | null };
type API = {
    receitaSituacaoState(): Promise<Response>; receitaSituacaoFile(): Promise<Response>; receitaSituacaoOne(cnpj: string): Promise<Response>;
    receitaSituacaoStart(fileId: string, keep: Keep[] | null, options: Options): Promise<Response>; receitaSituacaoCancel(): Promise<Response>; receitaSituacaoOpen(): Promise<Response>;
    onReceitaSituacaoUpdate(callback: (job: Job) => void): () => void;
};
// `aside` sits at the right of the heading; the Importar Mailing tab passes its section switch there.
export function ReceitaSituacao({ aside }: { aside?: ReactNode }) {
    const api = window.electronAPI as unknown as API;
    const [configured, setConfigured] = useState(false), [file, setFile] = useState<Response['file']>(), [job, setJob] = useState<Job | null>(null);
    const [cnpj, setCnpj] = useState(''), [result, setResult] = useState<Result | null>(null), [consulted, setConsulted] = useState(false), [busy, setBusy] = useState(false), [message, setMessage] = useState('');
    const [keep, setKeep] = useState<Keep[]>(ALL), [options, setOptions] = useState<Options>({ removeMei: false, csv: false });
    const toggle = (value: Keep) => setKeep(previous => { const rest = previous.filter(item => item !== value && item !== 'outra'), next = previous.includes(value) ? rest : [...rest, value]; return next.length === KEEP.length ? ALL : next; });
    const locked = useRef(false);
    const running = job?.status === 'running' || job?.status === 'cancelling';
    useEffect(() => {
        let mounted = true;
        const refresh = () => api.receitaSituacaoState().then(response => { if (!mounted) return; if (!response.success) { setMessage(response.message || 'Acesso indisponível.'); return; } setConfigured(Boolean(response.configured)); setJob(response.job || null); }).catch(() => { if (mounted) setMessage('Reabra o aplicativo para carregar a consulta da Receita.'); });
        void refresh();
        const off = api.onReceitaSituacaoUpdate(value => { if (mounted) setJob(value); });
        window.addEventListener('focus', refresh);
        return () => { mounted = false; off(); window.removeEventListener('focus', refresh); };
    }, [api]);
    async function action(run: () => Promise<Response>, complete?: (response: Response) => void) {
        if (locked.current) return; locked.current = true; setBusy(true); setMessage('');
        try { const response = await run(); if (!response.success) throw new Error(response.message || 'Não foi possível concluir a consulta.'); complete?.(response); }
        catch (reason) { setMessage(reason instanceof Error ? reason.message : 'Falha na consulta. Tente novamente.'); }
        finally { locked.current = false; setBusy(false); }
    }
    const blocked = busy || running;
    const missing = !file ? 'Falta selecionar a lista.' : !keep.length ? 'Marque ao menos uma situação.' : '';
    const run = {
        start: () => file && void action(() => api.receitaSituacaoStart(file.id, same(keep, ALL) ? null : keep, options), response => setJob(response.job || null)),
        cancel: () => void action(() => api.receitaSituacaoCancel(), () => setJob(previous => previous && { ...previous, status: 'cancelling', message: 'Cancelando consulta…' })),
        open: () => void action(() => api.receitaSituacaoOpen()),
    };
    return <div className="flows-app receita-situacao-app" aria-busy={busy}>
        <header className="flows-heading"><div><h1>Situação Receita</h1><p>Consulte CNPJs ou uma lista própria na Receita do BigQuery, sem depender de um fluxo de geração.</p></div>{aside}</header>
        {!configured && <p role="status" className="flow-notice">Entre com sua conta Google na tela de Fluxos (Acessos às fontes). O mesmo acesso ao BigQuery será usado nesta consulta.</p>}
        {message && <p role="alert" className="flow-notice flow-notice--error">{message}</p>}
        <QuickLookup cnpj={cnpj} result={result} consulted={consulted} disabled={blocked} ready={configured && Boolean(cnpj.trim())}
            onChange={value => { setCnpj(value); setConsulted(false); }} onSubmit={() => void action(() => api.receitaSituacaoOne(cnpj), response => { setResult(response.result || null); setConsulted(true); })} />
        <div className="receita-layout">
            <ListRules keep={keep} options={options} disabled={blocked} onToggle={toggle} onPreset={setKeep} onOptions={setOptions} />
            <aside className="receita-panel" aria-label="Lista para consultar">
                <FileSlot file={file} disabled={blocked} onChoose={() => void action(() => api.receitaSituacaoFile(), response => { if (response.file) setFile(response.file); })} />
                <RunArea job={job} running={running} busy={busy} ready={configured && !missing} missing={configured ? missing : ''} {...run} />
            </aside>
        </div>
    </div>;
}

type LookupProps = { cnpj: string; result: Result | null; consulted: boolean; disabled: boolean; ready: boolean; onChange(value: string): void; onSubmit(): void };
function QuickLookup({ cnpj, result, consulted, disabled, ready, onChange, onSubmit }: LookupProps) {
    return <section className="receita-quick" aria-labelledby="receita-quick-title">
        <form className="receita-quick-form" onSubmit={event => { event.preventDefault(); onSubmit(); }}>
            <h2 id="receita-quick-title">Consultar um CNPJ</h2>
            <label className="flow-field"><span>CNPJ</span><input value={cnpj} maxLength={30} placeholder="Com ou sem pontuação" spellCheck={false} disabled={disabled} onChange={event => onChange(event.target.value)} /></label>
            <button className="flow-primary" disabled={disabled || !ready}><SearchIcon />Consultar situação</button>
            <p className="receita-hint">A situação corresponde à última atualização da Receita no BigQuery. A consulta não atualiza a Receita em tempo real.</p>
        </form>
        {consulted && (result ? <LookupResult result={result} /> : <p role="status" className="receita-quick-empty">CNPJ não encontrado na Receita do BigQuery.</p>)}
    </section>;
}

function LookupResult({ result }: { result: Result }) {
    const code = result.situacao_cadastral_cod || '';
    const fields: [string, ReactNode, string?][] = [
        ['CNPJ', result.cnpj, 'receita-mono'], ['Razão social', result.razao_social, 'receita-result-name'],
        ['Situação', <span className="receita-badge" data-tone={TONE_BY_CODE[code] || 'none'}><b>{code}</b> · {result.situacao_cadastral}</span>],
        ['Data da situação', result.situacao_cadastral_data, 'receita-mono'], ['Motivo', result.situacao_motivo], ['Atualização na base', result.ultima_atualizacao, 'receita-mono'],
    ];
    return <dl className="receita-situacao-result">{fields.map(([label, value, className]) => <div key={label} className={className}><dt>{label}</dt><dd>{value || 'Não informado'}</dd></div>)}</dl>;
}

type RulesProps = { keep: Keep[]; options: Options; disabled: boolean; onToggle(value: Keep): void; onPreset(values: Keep[]): void; onOptions(update: (previous: Options) => Options): void };
function ListRules({ keep, options, disabled, onToggle, onPreset, onOptions }: RulesProps) {
    const kept = KEEP.filter(([value]) => keep.includes(value)).length;
    return <section className="receita-rules" aria-labelledby="receita-rules-title">
        <header className="receita-rules-heading"><h2 id="receita-rules-title">Consultar uma lista</h2><span>{kept === KEEP.length ? 'Todas as situações' : `${kept} de ${KEEP.length} situações`}</span></header>
        <div className="receita-group" role="group" aria-labelledby="receita-keep-label">
            <span id="receita-keep-label" className="receita-group-name">Manter no resultado</span>
            <div>
                <div className="receita-chips">{KEEP.map(([value, label, tone]) => <label key={value} className="receita-chip" data-tone={tone}>
                    <input type="checkbox" checked={keep.includes(value)} disabled={disabled} onChange={() => onToggle(value)} /><span className="receita-chip-dot" aria-hidden="true" />{label}
                </label>)}</div>
                <div className="receita-presets" role="group" aria-label="Atalhos"><span>Atalhos</span>{PRESETS.map(([label, values]) => <button key={label} type="button" aria-pressed={same(keep, values)} disabled={disabled} onClick={() => onPreset(values)}>{label}</button>)}</div>
            </div>
        </div>
        <div className="receita-group" role="group" aria-labelledby="receita-options-label">
            <span id="receita-options-label" className="receita-group-name">Opções</span>
            <div className="receita-situacao-switches">
                <Switch label="Remover MEI" hint="Tira do resultado os CNPJs que são MEI na Receita." checked={options.removeMei} disabled={disabled} onChange={removeMei => onOptions(previous => ({ ...previous, removeMei }))} />
                <Switch label="Gerar CSV adicional" hint="Salva também um .csv com as mesmas linhas, pronto para o Importar Mailing. A coluna CNPJ sai com o nome cpf." checked={options.csv} disabled={disabled} onChange={csv => onOptions(previous => ({ ...previous, csv }))} />
            </div>
        </div>
    </section>;
}

function FileSlot({ file, disabled, onChoose }: { file: Response['file']; disabled: boolean; onChoose(): void }) {
    return <section className="receita-panel-section">
        <h2>Lista</h2>
        {file
            ? <div className="receita-file"><FileIcon /><div><strong title={file.name}>{file.name}</strong><span>{/\.csv$/i.test(file.name) ? 'CSV' : 'XLSX · primeira aba'}</span></div><button disabled={disabled} onClick={onChoose}>Trocar</button></div>
            : <button className="receita-file-pick" disabled={disabled} onClick={onChoose}><FileIcon />Selecionar lista</button>}
        <p className="receita-hint">Selecione XLSX ou CSV com coluna CNPJ ou CPF (com os CNPJs). A primeira aba do XLSX será consultada.</p>
    </section>;
}

type RunProps = { job: Job | null; running: boolean; busy: boolean; ready: boolean; missing: string; start(): void; cancel(): void; open(): void };
function RunArea({ job, running, busy, ready, missing, start, cancel, open }: RunProps) {
    if (running && job) return <section className="receita-run">
        <JobStatus job={job} /><Counts job={job} />
        <div className="receita-job-actions"><button disabled={busy || job.status === 'cancelling'} onClick={cancel}>Cancelar consulta</button></div>
    </section>;
    return <section className="receita-run">
        <button className="flow-primary receita-run-button" disabled={busy || !ready} onClick={start}>Consultar lista</button>
        {missing ? <p className="receita-missing">{missing}</p>
            : !job && <p className="receita-hint">O resultado sai em XLSX na mesma pasta, com as colunas originais e os dados da situação acrescentados; as linhas das situações desmarcadas e, com a chave ligada, os MEI ficam de fora. O arquivo original é preservado.</p>}
        {job && <div className="receita-job" key={job.id}>
            <JobStatus job={job} /><Counts job={job} />
            {job.status === 'completed' && job.output && <div className="receita-job-actions"><button onClick={open}>Abrir resultado</button></div>}
        </div>}
    </section>;
}

function JobStatus({ job }: { job: Job }) {
    const state = job.status === 'completed' ? 'done' : job.status === 'running' || job.status === 'cancelling' ? 'running' : 'failed';
    return <div className={`receita-status receita-status--${state}`} role="status">
        {state === 'running' ? <SpinnerIcon /> : state === 'done' ? <CheckIcon /> : <AlertIcon />}
        <div><strong title={job.name}>{job.name}</strong><p>{job.message}</p></div>
    </div>;
}

function Counts({ job }: { job: Job }) {
    const entries: [string, number][] = [['Consultados', job.counts.processed], ['Encontrados', job.counts.found], ['Não encontrados', job.counts.notFound], ['Inválidos', job.counts.invalid]];
    if (job.counts.removed) entries.push(['Fora do resultado', job.counts.removed]);
    if (job.counts.mei) entries.push(['MEI removidos', job.counts.mei]);
    return <div className="flow-job-counts receita-counts">{entries.map(([label, value]) => <div key={label}><span>{label}</span><strong>{count(Number(value))}</strong></div>)}</div>;
}

// Mesma chave da Limpeza Local (estilos globais .toggle-switch em src/styles/index.css).
function Switch({ label, hint, checked, disabled, onChange }: { label: string; hint: string; checked: boolean; disabled: boolean; onChange: (value: boolean) => void }) {
    return <label className="toggle-switch receita-situacao-switch">
        <span className="switch"><input type="checkbox" checked={checked} disabled={disabled} onChange={event => onChange(event.target.checked)} /><span className="slider" /></span>
        <span className="toggle-label">{label}<small>{hint}</small></span>
    </label>;
}
