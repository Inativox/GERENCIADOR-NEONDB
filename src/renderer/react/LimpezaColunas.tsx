import { useCallback, useEffect, useRef, useState } from 'react';
import type { ColumnsProgress, ColumnsResult } from './types';

type Phase = 'idle' | 'running' | 'done' | 'failed';
const MAX_LOG_LINES = 300;
const basename = (path: string) => path.split(/[\\/]/).pop() || path;

function FileIcon() {
    return <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" aria-hidden="true"><path d="M14 3H6a1 1 0 0 0-1 1v16a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1V8Z" /><path d="M14 3v5h5M8 13h8M8 17h5" /></svg>;
}

export function LimpezaColunas() {
    const [files, setFiles] = useState<string[]>([]);
    const [selecting, setSelecting] = useState(false);
    const [phase, setPhase] = useState<Phase>('idle');
    const [error, setError] = useState('');
    const [progress, setProgress] = useState<ColumnsProgress | null>(null);
    const [result, setResult] = useState<ColumnsResult | null>(null);
    const [logs, setLogs] = useState<string[]>([]);
    const pending = useRef<string[]>([]);
    const frame = useRef<number | null>(null);
    const logElement = useRef<HTMLDivElement>(null);
    const running = useRef(false);
    const selectingRef = useRef(false);

    const addLog = useCallback((message: string) => {
        const lines = String(message).slice(0, 32000).split('\n').map(line => line.slice(0, 4000));
        pending.current.push(...lines);
        if (pending.current.length > MAX_LOG_LINES) pending.current.splice(0, pending.current.length - MAX_LOG_LINES);
        if (frame.current === null) frame.current = requestAnimationFrame(() => {
            frame.current = null;
            const batch = pending.current;
            pending.current = [];
            setLogs(previous => [...previous, ...batch].slice(-MAX_LOG_LINES));
        });
    }, []);

    useEffect(() => {
        const unsubscribe = [
            window.electronAPI.onLimpezaColunasLog(addLog),
            window.electronAPI.onLimpezaColunasProgress(setProgress),
            window.electronAPI.onLimpezaColunasFinished(value => {
                running.current = false;
                setResult(value);
                setPhase(value.success ? 'done' : 'failed');
                setError(value.success ? '' : value.message || 'Não foi possível concluir. Confira a atividade abaixo.');
                addLog(`Finalizado: ${value.processados} gerado(s), ${value.pulados} pulado(s).`);
            }),
        ];
        return () => {
            unsubscribe.forEach(remove => remove());
            if (frame.current !== null) cancelAnimationFrame(frame.current);
        };
    }, [addLog]);

    useEffect(() => {
        if (logElement.current) logElement.current.scrollTop = logElement.current.scrollHeight;
    }, [logs]);

    async function selectFiles() {
        if (selectingRef.current || running.current) return;
        selectingRef.current = true;
        setSelecting(true);
        setError('');
        try {
            const selected = await window.electronAPI.selectFile({ title: 'Selecione as planilhas XLSX para limpar', multi: true });
            if (selected?.length) {
                setFiles([...new Set(selected)]);
                setResult(null);
                setProgress(null);
                setPhase('idle');
                addLog(`${selected.length} arquivo(s) selecionado(s).`);
            }
        } catch {
            setError('Não foi possível selecionar os arquivos. Tente novamente.');
        } finally {
            selectingRef.current = false;
            setSelecting(false);
        }
    }

    function start() {
        if (!files.length || running.current || selectingRef.current) return;
        running.current = true;
        setPhase('running');
        setError('');
        setResult(null);
        setProgress({ current: 0, total: files.length, fileName: '' });
        pending.current = [];
        setLogs([]);
        try { window.electronAPI.startLimpezaColunas(files); }
        catch {
            running.current = false;
            setPhase('failed');
            setError('Não foi possível iniciar o processamento. Tente novamente.');
        }
    }

    const busy = phase === 'running' || selecting;
    const status = phase === 'running' ? 'Processando' : phase === 'done' ? (result?.pulados ? 'Concluído com avisos' : 'Concluído') : phase === 'failed' ? 'Erro no processamento' : 'Pronto para iniciar';
    const percent = progress?.total ? Math.round(progress.current / progress.total * 100) : 0;

    return <div className="columns-app">
        <header className="columns-heading">
            <div><h1>Limpeza de Colunas</h1><p>Padronize suas planilhas em NOME, CPF e FONE1.</p></div>
            <span className={`columns-status columns-status--${phase}`} role="status"><span />{status}</span>
        </header>

        <div className="columns-metrics" aria-label="Resumo do processamento">
            <div><span>Arquivos selecionados</span><strong>{files.length.toLocaleString('pt-BR')}</strong></div>
            <div><span>Arquivos gerados</span><strong>{(result?.processados || 0).toLocaleString('pt-BR')}</strong></div>
            <div><span>Arquivos pulados</span><strong>{(result?.pulados || 0).toLocaleString('pt-BR')}</strong></div>
        </div>

        <div className="columns-workspace">
            <section className="columns-panel" aria-labelledby="columns-files-title">
                <div className="columns-panel-heading"><span className="columns-step">01</span><h2 id="columns-files-title">Arquivos de entrada</h2><span className="columns-format">.XLSX</span></div>
                <p className="columns-description">Cada planilha gera um arquivo <code>_LIMPO.xlsx</code> na mesma pasta. O original é preservado.</p>
                <button className="columns-select" id="columns-select" onClick={selectFiles} disabled={busy}><FileIcon />{selecting ? 'Selecionando...' : 'Selecionar planilhas'}</button>
                {files.length ? <ul className="columns-files" aria-label="Planilhas selecionadas">{files.map((file, index) => <li key={file}><span className="columns-file-number">{String(index + 1).padStart(2, '0')}</span><FileIcon /><span className="columns-file-name" title={file}>{basename(file)}</span><button className="columns-remove" disabled={busy} aria-label={`Remover ${basename(file)}`} onClick={() => setFiles(previous => previous.filter(item => item !== file))}>×</button></li>)}</ul> : <div className="columns-empty"><FileIcon /><strong>Nenhuma planilha selecionada</strong><span>Selecione um ou mais arquivos XLSX para começar.</span></div>}
                {error && <p className="columns-error" role="alert">{error}</p>}
                <div className="columns-action"><button className="columns-start" id="columns-start" onClick={start} disabled={!files.length || busy}>{phase === 'running' ? 'Limpeza em andamento...' : 'Iniciar limpeza'}<span aria-hidden="true">→</span></button><span>Saída: NOME · CPF · FONE1</span></div>
                <details className="columns-help"><summary>Cuidados com números em notação científica</summary><p>Use o XLSX original. Se o arquivo já contém texto como <code>5,52199E+12</code> com dígitos perdidos, o campo ficará em branco e será informado na atividade. Alterar a formatação não recupera dígitos que já foram truncados.</p></details>
            </section>

            <section className="columns-panel columns-activity-panel" aria-labelledby="columns-activity-title">
                <div className="columns-panel-heading"><span className="columns-step">02</span><h2 id="columns-activity-title">Atividade</h2><span className="columns-format">{logs.length} linhas</span></div>
                <div className="columns-progress">
                    <div><span>{phase === 'running' ? progress?.fileName || 'Preparando arquivos...' : status}</span><strong>{percent}%</strong></div>
                    <progress value={percent} max="100" aria-label="Progresso da limpeza" />
                    <span>{progress ? `${progress.current} de ${progress.total} arquivo(s) verificado(s)` : 'O progresso será exibido ao iniciar.'}</span>
                </div>
                <div className="columns-activity custom-scrollbar" ref={logElement} id="columns-activity" aria-label="Mensagens do processamento">{logs.length ? logs.map((line, index) => <p key={index}>{line || '\u00a0'}</p>) : <div className="columns-activity-empty"><span aria-hidden="true">›_</span><strong>Aguardando processamento</strong><p>Selecione suas planilhas e inicie a limpeza.</p></div>}</div>
                <div className="columns-log-footer"><span className="columns-live-dot" />{phase === 'running' ? 'Processamento em andamento' : 'Últimas 300 linhas de atividade'}</div>
            </section>
        </div>
    </div>;
}
