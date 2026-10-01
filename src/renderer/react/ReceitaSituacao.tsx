import { useEffect, useRef, useState } from 'react';
import './fluxos.css';
type Job = { id: string; owner: string; name: string; status: string; message: string; output: string | null; counts: { processed: number; found: number; notFound: number; invalid: number } };
type Result = Record<string, string | null>;
type Response = { success: boolean; message?: string; configured?: boolean; job?: Job | null; file?: { id: string; name: string }; found?: boolean; result?: Result | null };
type API = {
    receitaSituacaoState(): Promise<Response>; receitaSituacaoFile(): Promise<Response>; receitaSituacaoOne(cnpj: string): Promise<Response>;
    receitaSituacaoStart(fileId: string): Promise<Response>; receitaSituacaoCancel(): Promise<Response>; receitaSituacaoOpen(): Promise<Response>;
    onReceitaSituacaoUpdate(callback: (job: Job) => void): () => void;
};
export function ReceitaSituacao() {
    const api = window.electronAPI as unknown as API;
    const [configured, setConfigured] = useState(false), [file, setFile] = useState<Response['file']>(), [job, setJob] = useState<Job | null>(null);
    const [cnpj, setCnpj] = useState(''), [result, setResult] = useState<Result | null>(null), [consulted, setConsulted] = useState(false), [busy, setBusy] = useState(false), [message, setMessage] = useState('');
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
    return <div className="flows-app receita-situacao-app" aria-busy={busy}>
        <header className="flows-heading"><div><h1>Situação Receita</h1><p>Consulte CNPJs ou uma lista própria no banco da Receita, sem depender de um fluxo de geração.</p></div></header>
        <p className="flow-explanation">A situação corresponde à versão da base configurada. A consulta não atualiza a Receita em tempo real.</p>
        {!configured && <p role="status" className="flow-notice">Configure a fonte Receita em Gerar listas → Acessos às fontes.</p>}
        {message && <p role="alert" className="flow-notice flow-notice--error">{message}</p>}
        <div className="receita-situacao-grid">
            <section className="flow-editor"><header className="flow-editor-heading"><div><h2>Consultar um CNPJ</h2></div></header><div className="flow-section">
                <form onSubmit={event => { event.preventDefault(); void action(() => api.receitaSituacaoOne(cnpj), response => { setResult(response.result || null); setConsulted(true); }); }}>
                    <label className="flow-field"><span>CNPJ</span><input value={cnpj} maxLength={30} placeholder="Com ou sem pontuação" disabled={busy || running} onChange={event => { setCnpj(event.target.value); setConsulted(false); }} /></label>
                    <div className="flow-actions"><button className="flow-primary" disabled={busy || running || !configured || !cnpj.trim()}>Consultar situação</button></div>
                </form>
                {consulted && (result ? <dl className="receita-situacao-result">{[['CNPJ', result.cnpj], ['Razão social', result.razao_social], ['Situação', `${result.situacao_cadastral_cod} · ${result.situacao_cadastral}`], ['Data da situação', result.situacao_cadastral_data], ['Motivo', result.situacao_motivo], ['Atualização na base', result.ultima_atualizacao]].map(([label, value]) => <div key={label || ''}><dt>{label}</dt><dd>{value || 'Não informado'}</dd></div>)}</dl> : <p role="status">CNPJ não encontrado na base configurada.</p>)}
            </div></section>
            <section className="flow-editor"><header className="flow-editor-heading"><div><h2>Consultar uma lista</h2></div></header><div className="flow-section">
                <p>Selecione XLSX ou CSV com coluna CNPJ. A primeira aba do XLSX será consultada. Todas as linhas permanecem no resultado, incluindo documentos inválidos ou não encontrados.</p>
                <div className="flow-actions"><button disabled={busy || running} onClick={() => void action(() => api.receitaSituacaoFile(), response => { if (response.file) setFile(response.file); })}>Selecionar lista</button></div>
                {file && <p className="flow-explanation">{file.name}</p>}
                <p>O resultado sai em XLSX na mesma pasta, com as colunas originais e os dados da situação acrescentados. O arquivo original é preservado.</p>
                <div className="flow-actions"><button className="flow-primary" disabled={busy || running || !file || !configured} onClick={() => file && void action(() => api.receitaSituacaoStart(file.id), response => setJob(response.job || null))}>Consultar lista</button>{running && <button disabled={busy || job?.status === 'cancelling'} onClick={() => void action(() => api.receitaSituacaoCancel(), () => setJob(previous => previous && { ...previous, status: 'cancelling', message: 'Cancelando consulta…' }))}>Cancelar consulta</button>}</div>
            </div></section>
        </div>
        {job && <section className="flow-job-detail"><h2>{job.name}</h2><p role="status">{job.message}</p><div className="flow-job-counts">{[['Consultados', job.counts.processed], ['Encontrados', job.counts.found], ['Não encontrados', job.counts.notFound], ['Inválidos', job.counts.invalid]].map(([label, value]) => <div key={String(label)}><span>{label}</span><strong>{Number(value).toLocaleString('pt-BR')}</strong></div>)}</div>{job.status === 'completed' && job.output && <div className="flow-actions"><button onClick={() => void action(() => api.receitaSituacaoOpen())}>Abrir resultado</button></div>}</section>}
    </div>;
}
