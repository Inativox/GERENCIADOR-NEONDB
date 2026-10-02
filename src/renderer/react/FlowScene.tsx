import { useEffect, useId, useRef, useState } from 'react';
import type { CSSProperties, PointerEvent, ReactNode } from 'react';
import type { Flow, FlowJob } from './flowTypes';
import './flowScene.css';

type Stage = 'generation' | 'enrichment' | 'api' | 'cleaning' | 'export';
type StageState = 'ready' | 'waiting' | 'active' | 'complete' | 'paused' | 'failed' | 'skipped' | 'slow';
interface SceneStep { id: Stage; label: string; description: string; state: StageState; metric?: string }
const stateLabels: Record<StageState, string> = { ready: 'Configurado', waiting: 'Na sequência', active: 'Processando', complete: 'Concluído', paused: 'Interrompido', failed: 'Falhou', skipped: 'Desligado', slow: 'Sem avanço' };
const operations = { c6: 'C6', santander: 'Santander', pagbank: 'PagBank', mercadopago: 'Mercado Pago' };
const displayNumber = (value: number) => value.toLocaleString('pt-BR');
const variables = (values: Record<string, number | string>) => values as CSSProperties;
const percentLabel = (value: number) => `${value.toLocaleString('pt-BR', { maximumFractionDigits: 1 })}%`;
const SLOW_AFTER_MS = 120_000;

function progressFor(job?: FlowJob) {
    const stage = job?.stage === 'output' ? 'export' : job?.stage;
    const progress = job?.progress && job.progress.stage === stage ? job.progress : null;
    const counts = job?.counts || {};
    // Contact removals and enriched matches are not counts of processed rows.
    const legacyProcessed = stage === 'generation' ? counts.generated : stage === 'api' ? counts.apiConsulted : stage === 'export' ? counts.exported
        : stage === 'cleaning' ? ['kept', 'removedRoot', 'removedCnae', 'removedBlocklist', 'repeatedDocuments', 'withoutPhones'].reduce((sum, key) => sum + (counts[key] || 0), 0) : null;
    const processed = progress?.processed ?? legacyProcessed ?? null;
    const total = progress?.total ?? null;
    const percent = progress?.complete ? 100 : total != null && total > 0 && processed != null ? Math.min(100, Math.max(0, processed / total * 100)) : null;
    return { processed, total, percent, complete: progress?.complete === true };
}

function useIdleTime(job: FlowJob | undefined, processed: number | null, complete: boolean) {
    const marker = `${job?.id}:${job?.status}:${job?.stage}:${processed}:${complete}`;
    const [activity, setActivity] = useState(() => ({ marker, jobId: job?.id, at: Math.min(Date.now(), Date.parse(job?.updatedAt || '') || Date.now()) }));
    const [now, setNow] = useState(Date.now);
    useEffect(() => {
        setActivity(previous => previous.marker === marker ? previous : { marker, jobId: job?.id, at: previous.jobId === job?.id ? Date.now() : Math.min(Date.now(), Date.parse(job?.updatedAt || '') || Date.now()) });
        setNow(Date.now());
    }, [marker]);
    useEffect(() => {
        if (job?.status !== 'running') return;
        const timer = window.setInterval(() => setNow(Date.now()), 10_000);
        return () => window.clearInterval(timer);
    }, [job?.status]);
    // Log/heartbeat updates cannot hide a lack of progress in processed rows.
    return job?.status === 'running' && !complete && activity.marker === marker ? Math.max(0, now - activity.at) : 0;
}

function stepsFor(flow?: Flow, job?: FlowJob): SceneStep[] {
    const hasApi = flow?.operation === 'c6' && flow.api?.enabled !== false || job?.stage === 'api' || job?.counts.apiConsulted != null;
    const stages: Omit<SceneStep, 'state'>[] = [
        { id: 'generation', label: 'Receita', description: 'As empresas são selecionadas no banco da Receita de acordo com os filtros do fluxo.' },
        { id: 'enrichment', label: 'Enriquecimento', description: flow?.enrichment?.enabled === false ? 'O fluxo segue com os contatos da lista original.' : 'Telefones e dados complementares são cruzados com o banco do Gerenciador.' },
        ...(hasApi ? [{ id: 'api' as Stage, label: 'API C6', description: 'A consulta com chave dupla verifica a disponibilidade dos CNPJs no C6.' }] : []),
        { id: 'cleaning', label: 'Limpeza', description: 'Contatos são ajustados, duplicidades são cruzadas e as regras de limpeza do fluxo são aplicadas.' },
        { id: 'export', label: 'Exportação', description: `A lista é dividida em partes com o nome definido e salva em ${flow?.output?.csv ? 'XLSX e CSV' : 'XLSX'}.` },
    ];
    const stage = job?.stage === 'output' ? 'export' : job?.stage;
    const current = stages.findIndex(item => item.id === stage);
    const finished = job && ['completed', 'empty'].includes(job.status);
    const metricKeys: Record<Stage, [string, string]> = { generation: ['generated', 'gerados'], enrichment: ['enriched', 'enriquecidos'], api: ['apiAvailable', 'disponíveis'], cleaning: ['cleaned', 'após limpeza'], export: ['exported', 'exportados'] };
    return stages.map((item, index) => {
        let state: StageState = job ? 'waiting' : 'ready';
        if (finished || current > index || job?.stage === 'finalizing') state = 'complete';
        else if (index === current) state = job?.status === 'running' ? 'active' : job?.status === 'failed' ? 'failed' : 'paused';
        if (item.id === 'enrichment' && flow?.enrichment?.enabled === false) state = 'skipped';
        const [key, label] = metricKeys[item.id], count = job?.counts[key];
        return { ...item, state, metric: count == null || state === 'skipped' ? undefined : `${displayNumber(count)} ${label}` };
    });
}

function Volume({ className = '', children, style }: { className?: string; children?: ReactNode; style?: CSSProperties }) {
    return <div className={`flow-volume ${className}`} style={style}>
        <span className="flow-volume-front"><i /><i /><i /></span><span className="flow-volume-side" /><span className="flow-volume-top">{children}</span>
    </div>;
}

function Machine({ kind }: { kind: Stage }) {
    if (kind === 'generation') return <div className="flow-machine flow-machine-rack">{[0, 1, 2].map(layer => <Volume key={layer} style={variables({ '--lift': `${layer * 21 + 8}px` })}><span className="flow-machine-vents" /><span className="flow-machine-mark">MB</span></Volume>)}</div>;
    if (kind === 'enrichment') return <div className="flow-machine flow-machine-core"><Volume><span className="flow-machine-core-ring"><i /></span></Volume>{[0, 1, 2, 3].map(pin => <Volume key={pin} className={`flow-machine-pin pin-${pin}`} />)}</div>;
    if (kind === 'api') return <div className="flow-machine flow-machine-api"><Volume className="flow-machine-gate gate-left" /><Volume className="flow-machine-gate gate-right" /><Volume className="flow-machine-bridge"><span className="flow-machine-mark">C6 / IM</span></Volume><span className="flow-machine-gate-light" /></div>;
    if (kind === 'cleaning') return <div className="flow-machine flow-machine-filter">{[0, 1].map(layer => <Volume key={layer} style={variables({ '--lift': `${layer * 35 + 10}px` })}><span className="flow-machine-mesh" /></Volume>)}</div>;
    return <div className="flow-machine flow-machine-output"><Volume className="flow-machine-tray" />{[0, 1, 2, 3].map(layer => <Volume key={layer} className="flow-machine-sheet" style={variables({ '--lift': `${layer * 7 + 16}px`, '--offset': `${layer * 3}px` })}><span className="flow-machine-sheet-lines" /><span className="flow-machine-sheet-symbol">↗</span></Volume>)}</div>;
}

export function FlowScene({ flow, job }: { flow?: Flow; job?: FlowJob }) {
    const panel = useRef<HTMLElement>(null), viewport = useRef<HTMLDivElement>(null);
    const frame = useRef(0), reducedMotion = useRef(false), visible = useRef(false);
    const sceneId = useId();
    const [compact, setCompact] = useState(() => { try { return localStorage.getItem('flows-scene-view') === 'compact'; } catch { return false; } });
    const [selection, setSelection] = useState<Stage | ''>('');
    const progress = progressFor(job);
    const idleTime = useIdleTime(job, progress.processed, progress.complete);
    const slow = idleTime >= SLOW_AFTER_MS;
    const steps = stepsFor(flow, job).map(step => ({ ...step, state: step.state === 'active' && slow ? 'slow' as const : step.state }));
    const active = steps.find(step => ['active', 'failed', 'paused', 'slow'].includes(step.state));
    const selected = steps.find(step => step.id === selection) || active || steps[0];
    const ended = job && ['completed', 'empty'].includes(job.status);
    const status = !job ? 'Prévia do fluxo' : ended ? job.status === 'empty' ? 'Sem registros na saída' : 'Listas prontas' : job.status === 'failed' ? 'Falhou' : ['cancelled', 'interrupted'].includes(job.status) ? 'Execução interrompida' : slow ? `Sem avanço há ${Math.floor(idleTime / 60_000)} min` : active ? `${active.label} em processamento` : job.stage === 'finalizing' ? 'Finalizando arquivos' : 'Preparando a raiz';
    const health: StageState = !job ? 'ready' : ended ? 'complete' : job.status === 'failed' ? 'failed' : job.status !== 'running' ? 'paused' : slow ? 'slow' : 'active';
    const progressTitle = ended ? 'Fluxo concluído' : active ? `${active.label} · progresso da etapa` : job?.stage === 'finalizing' ? 'Finalização' : 'Preparação';
    const shownPercent = ended ? 100 : progress.percent;
    const detail = ended ? `${displayNumber(job?.counts.exported ?? 0)} registros exportados` : progress.processed == null ? 'Aguardando contagem da etapa' : progress.total != null ? `${displayNumber(progress.processed)} de ${displayNumber(progress.total)} registros processados` : `${displayNumber(progress.processed)} registros ${job?.stage === 'generation' ? 'gerados' : 'processados'} · total ainda não informado`;
    const activeLabel = shownPercent == null ? health === 'failed' ? 'Falhou' : health === 'paused' ? 'Interrompido' : job?.stage === 'generation' ? 'Buscando' : 'Em curso' : percentLabel(shownPercent);

    useEffect(() => { setSelection(''); }, [job?.id, job?.stage, flow?.id]);
    useEffect(() => {
        const element = panel.current, surface = viewport.current;
        if (!element || !surface || compact) return;
        const media = matchMedia('(prefers-reduced-motion: reduce)');
        const updateMotion = () => {
            reducedMotion.current = media.matches;
            element.dataset.motion = visible.current && !document.hidden && !media.matches && job?.status === 'running' ? 'on' : 'off';
        };
        const observer = new IntersectionObserver(entries => { visible.current = entries[0]?.isIntersecting === true; updateMotion(); });
        observer.observe(surface);
        const resize = new ResizeObserver(entries => {
            const width = entries[0]?.contentRect.width || 0;
            const height = entries[0]?.contentRect.height || 0;
            if (!width || !height) return;
            const scale = Math.min(1.35, Math.max(.2, (width - 64) / (steps.length * 164 + 40)));
            element.style.setProperty('--scene-scale', String(scale));
            element.style.setProperty('--scene-offset-y', '0px');
            // Fit the projected machines and their upright readouts together.
            // This runs on resize only, not on each processing update.
            const bounds = () => {
                const boxes = [...surface.querySelectorAll('.flow-scene-ground, .flow-volume-top, .flow-scene-station-readout')].map(node => node.getBoundingClientRect());
                return { top: Math.min(...boxes.map(box => box.top)), bottom: Math.max(...boxes.map(box => box.bottom)) };
            };
            let box = bounds();
            if (box.bottom - box.top > height - 56) {
                element.style.setProperty('--scene-scale', String(scale * (height - 56) / (box.bottom - box.top)));
                box = bounds();
            }
            const center = surface.getBoundingClientRect().top + height / 2 + 8;
            element.style.setProperty('--scene-offset-y', `${center - (box.top + box.bottom) / 2}px`);
        });
        resize.observe(surface);
        media.addEventListener('change', updateMotion);
        document.addEventListener('visibilitychange', updateMotion);
        updateMotion();
        return () => { observer.disconnect(); resize.disconnect(); media.removeEventListener('change', updateMotion); document.removeEventListener('visibilitychange', updateMotion); cancelAnimationFrame(frame.current); };
    }, [compact, steps.length, job?.status]);

    function tilt(event: PointerEvent<HTMLDivElement>) {
        if (reducedMotion.current || !visible.current || event.pointerType === 'touch') return;
        const box = event.currentTarget.getBoundingClientRect();
        const x = (event.clientX - box.left) / Math.max(box.width, 1) - .5;
        const y = (event.clientY - box.top) / Math.max(box.height, 1) - .5;
        cancelAnimationFrame(frame.current);
        frame.current = requestAnimationFrame(() => {
            panel.current?.style.setProperty('--scene-tilt-x', `${y * -4}deg`);
            panel.current?.style.setProperty('--scene-tilt-y', `${x * 4}deg`);
        });
    }
    function resetTilt() {
        cancelAnimationFrame(frame.current);
        panel.current?.style.setProperty('--scene-tilt-x', '0deg');
        panel.current?.style.setProperty('--scene-tilt-y', '0deg');
    }
    function toggle() {
        resetTilt(); setCompact(value => !value);
        try { localStorage.setItem('flows-scene-view', compact ? 'expanded' : 'compact'); } catch { /* Optional visual preference. */ }
    }

    return <section ref={panel} className={`flow-scene${compact ? ' is-compact' : ''}`} aria-label="Visão 3D do fluxo" data-motion="off" data-mode={job ? 'execution' : 'preview'} data-health={health}>
        <header className="flow-scene-heading"><div><span className="flow-scene-eyebrow">MB / OPERAÇÕES</span><h2>Da origem à lista pronta<span className="flow-scene-operation">{flow ? operations[flow.operation] : 'Fluxo'}</span></h2></div><button type="button" className="flow-scene-toggle" aria-expanded={!compact} aria-controls={sceneId} onClick={toggle}>{compact ? 'Vista 3D' : 'Recolher 3D'}<span aria-hidden="true">{compact ? '↗' : '−'}</span></button></header>
        <div className="flow-scene-status"><span className={`flow-scene-status-dot${job?.status === 'running' ? ' is-live' : ''}`} /><span>{status}</span><span className="flow-scene-caption">{job ? job.flowName : flow?.name || 'Configure as etapas abaixo'}</span></div>
        {job && <div className="flow-scene-progress-summary">
            <div className="flow-scene-progress-heading"><div><span>{progressTitle}</span><strong>{activeLabel}</strong></div><p>{detail}</p></div>
            <div className={`flow-scene-progress-track${shownPercent == null ? ' is-indeterminate' : ''}`} role="progressbar" aria-label={progressTitle} aria-valuemin={0} aria-valuemax={100} aria-valuenow={shownPercent ?? undefined} aria-valuetext={`${shownPercent == null ? 'Total desconhecido' : percentLabel(shownPercent)}. ${detail}`}><span style={shownPercent == null ? undefined : { transform: `scaleX(${shownPercent / 100})` }} /></div>
            {slow && <p className="flow-scene-warning">Nenhum novo lote confirmado há {Math.floor(idleTime / 60_000)} min. A etapa pode estar aguardando a fonte de dados.</p>}
        </div>}
        <div id={sceneId} hidden={compact}>{!compact && <>
            <div className="flow-scene-viewport" ref={viewport} onPointerMove={tilt} onPointerLeave={resetTilt} aria-hidden="true">
                <span className="flow-scene-coordinate coordinate-left">ENTRADA / 01</span><span className="flow-scene-coordinate coordinate-right">SAÍDA / {String(steps.length).padStart(2, '0')}</span>
                <div className="flow-scene-fit" style={variables({ '--scene-width': `${steps.length * 164 - 12}px` })}><div className="flow-scene-world">
                    <div className="flow-scene-ground" />
                    {steps.map((step, index) => <div key={step.id} className={`flow-scene-station state-${step.state}${selected.id === step.id ? ' is-selected' : ''}`} data-stage={step.id} style={variables({ '--station-x': `${index * 164 + 16}px` })}>
                        {index < steps.length - 1 && <div className={`flow-scene-link${steps[index + 1].state === 'active' ? ' is-feeding' : ''}`}><i /><i /><i /></div>}
                        <Volume className="flow-machine-plinth" /><span className="flow-scene-floor-number">{String(index + 1).padStart(2, '0')}</span>
                        <Machine kind={step.id} /><span className="flow-scene-beacon" />
                        <span className="flow-scene-station-readout"><span>{step.label}</span><strong>{step.state === 'complete' ? '✓ 100%' : ['active', 'slow', 'failed', 'paused'].includes(step.state) ? progress.percent == null ? stateLabels[step.state] : percentLabel(progress.percent) : step.state === 'skipped' ? 'Desligado' : '—'}</strong></span>
                    </div>)}
                </div></div>
            </div>
            <ol className="flow-scene-steps">{steps.map((step, index) => <li key={step.id} className={`state-${step.state}`}><button type="button" data-stage={step.id} aria-current={active?.id === step.id ? 'step' : undefined} aria-pressed={selected.id === step.id} onClick={() => setSelection(step.id)}><span className="flow-scene-step-number">{step.state === 'complete' ? '✓' : step.state === 'failed' ? '!' : String(index + 1).padStart(2, '0')}</span><span><strong>{step.label}</strong><small>{stateLabels[step.state]}{active?.id === step.id && progress.percent != null ? ` · ${percentLabel(progress.percent)}` : ''}</small></span></button></li>)}</ol>
            <div className="flow-scene-legend" aria-label="Legenda dos estados"><span className="state-complete">Processado</span><span className="state-active">Em andamento</span><span className="state-slow" title="Dois minutos sem avanço na contagem de registros">Sem avanço / atenção</span><span className="state-failed">Erro</span></div>
            <div className="flow-scene-insight"><span className="flow-scene-insight-label">{selected.label}</span><p>{selected.description}</p>{selected.metric && <strong>{selected.metric}</strong>}</div>
        </>}</div>
    </section>;
}
