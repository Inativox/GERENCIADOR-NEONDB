import { useEffect, useRef, useState } from 'react';
import type { ReactNode } from 'react';
import './fluxos.css';
import { ReceitaSituacao } from './ReceitaSituacao';
// Mesma lista de SERVICES em src/main/handlers/importarMailing.js; o main confere o ID de novo antes de enviar.
type Service = { id: number; group: string; label: string; name: string };
type Job = { status: 'running' | 'completed' | 'failed'; file: string; rows: number; service: Service; message: string };
type MailingFile = { id: string; name: string; rows: number };
type Response = { success: boolean; message?: string; cancelled?: boolean; services?: Service[]; configured?: boolean; admin?: boolean; maxRows?: number; job?: Job | null; file?: MailingFile };
type API = { mailingImportState(): Promise<Response>; mailingImportFile(): Promise<Response>; mailingImportSend(fileId: string, serviceId: number): Promise<Response> };
type Section = 'mailing' | 'receita';
const count = (value: number) => value.toLocaleString('pt-BR');
const READ_FAILURE = 'Não foi possível ler o arquivo. Tente novamente.';
const SEND_FAILURE = 'Não foi possível confirmar o envio. Confira o painel da Fastway antes de enviar de novo.';

export function ImportarMailing() {
    const api = window.electronAPI as unknown as API;
    const [initial, setInitial] = useState<Response | null>(null), [section, setSection] = useState<Section>('mailing'), [failure, setFailure] = useState('');
    useEffect(() => {
        let mounted = true;
        // Reopening the tab reloads the state, so Fastway access saved meanwhile in the Fluxos tab is picked up.
        const load = async (first: boolean) => {
            try { const response = await api.mailingImportState(); if (!mounted) return; if (response.success) setInitial(response); else if (first) setFailure(response.message || 'Acesso indisponível.'); }
            catch { if (mounted && first) setFailure('Reabra o aplicativo para carregar a importação de mailing.'); }
        };
        const reopened = (event: Event) => { if ((event as CustomEvent<{ tabId?: string }>).detail?.tabId === 'importarMailing') void load(false); };
        void load(true);
        document.addEventListener('tab-changed', reopened);
        return () => { mounted = false; document.removeEventListener('tab-changed', reopened); };
    }, [api]);
    if (failure) return <div className="flows-app"><p role="alert" className="flow-error">{failure}</p></div>;
    if (!initial) return null;
    const switcher = initial.admin ? <div className="flow-view-switch" aria-label="Seções da importação">
        <button aria-pressed={section === 'mailing'} onClick={() => setSection('mailing')}>Importar Mailing</button>
        <button aria-pressed={section === 'receita'} onClick={() => setSection('receita')}>Situação Receita</button>
    </div> : null;
    return <div className="importar-mailing">
        <div hidden={section !== 'mailing'}><MailingUpload api={api} initial={initial} switcher={switcher} /></div>
        {initial.admin && <div hidden={section !== 'receita'}><ReceitaSituacao aside={switcher} /></div>}
    </div>;
}

function MailingUpload({ api, initial, switcher }: { api: API; initial: Response; switcher: ReactNode }) {
    const [file, setFile] = useState<MailingFile | null>(null), [fileError, setFileError] = useState(''), [service, setService] = useState<Service | null>(null);
    const [job, setJob] = useState<Job | null>(initial.job || null), [confirming, setConfirming] = useState(false), [busy, setBusy] = useState(false);
    const locked = useRef(false);
    // Reading a 250k-row CSV takes seconds; only the upload itself locks the service choice.
    const sending = job?.status === 'running', reading = busy && !sending;
    async function guarded(run: () => Promise<void>) {
        if (locked.current) return; locked.current = true; setBusy(true); setConfirming(false);
        try { await run(); } finally { locked.current = false; setBusy(false); }
    }
    const choose = () => guarded(async () => {
        setFileError('');
        try {
            const response = await api.mailingImportFile(); if (response.cancelled) return;
            if (!response.success || !response.file) { setFile(null); setFileError(response.message || READ_FAILURE); return; }
            setFile(response.file); setJob(null);
        } catch { setFile(null); setFileError(READ_FAILURE); }
    });
    const send = () => guarded(async () => {
        if (!file || !service) return;
        setJob({ status: 'running', file: file.name, rows: file.rows, service, message: 'Enviando para a Fastway…' });
        try { const response = await api.mailingImportSend(file.id, service.id); if (!response.success || !response.job) throw new Error(response.message); setJob(response.job); setFile(null); }
        catch (reason) { const text = reason instanceof Error && reason.message ? reason.message : SEND_FAILURE; setJob(previous => previous && { ...previous, status: 'failed', message: text }); }
    });
    const pick = (item: Service) => { setService(item); setConfirming(false); if (job?.status !== 'running') setJob(null); };
    return <div className="flows-app mailing-app" aria-busy={busy}>
        <header className="flows-heading"><div><h1>Importar Mailing</h1><p>Envie um CSV direto para um serviço da Fastway, sem passar pela geração de lista.</p></div>{switcher}</header>
        <div className="mailing-layout">
            <ServicePicker services={initial.services || []} selected={service} disabled={sending} onPick={pick} />
            <aside className="mailing-panel" aria-label="Envio para a Fastway">
                <FileSlot file={file} error={fileError} maxRows={initial.maxRows || 0} reading={reading} disabled={busy || sending} onChoose={() => void choose()} />
                <Destination service={service} />
                <SendArea file={file} service={service} configured={Boolean(initial.configured)} job={job} confirming={confirming} busy={busy}
                    onAsk={() => setConfirming(true)} onConfirm={() => void send()} onCancel={() => setConfirming(false)} onReset={() => setJob(null)} />
            </aside>
        </div>
    </div>;
}

function ServicePicker({ services, selected, disabled, onPick }: { services: Service[]; selected: Service | null; disabled: boolean; onPick: (service: Service) => void }) {
    const groups = [...new Set(services.map(item => item.group))];
    return <section className="mailing-services" aria-labelledby="mailing-services-title">
        <header className="mailing-services-heading"><h2 id="mailing-services-title">Serviço de destino</h2><span>{services.length} serviços</span></header>
        {groups.map(group => <div key={group} className="mailing-group" role="group" aria-label={group}>
            <span className="mailing-group-name" aria-hidden="true">{group}</span>
            <div className="mailing-chips">{services.filter(item => item.group === group).map(item => <label key={item.id} className="mailing-chip">
                <input type="radio" name="mailing-service" disabled={disabled} checked={selected?.id === item.id} onChange={() => onPick(item)} />
                <span className="mailing-chip-id">{item.id}</span>{item.label}
            </label>)}</div>
        </div>)}
    </section>;
}

function FileSlot({ file, error, maxRows, reading, disabled, onChoose }: { file: MailingFile | null; error: string; maxRows: number; reading: boolean; disabled: boolean; onChoose: () => void }) {
    return <section className="mailing-panel-section" aria-busy={reading}>
        <h2>Arquivo</h2>
        {reading
            ? <div className="mailing-file mailing-file--reading" role="status"><SpinnerIcon /><div><strong>Abrindo o CSV…</strong><span>Depois de escolher, o gerenciador confere o cabeçalho e conta as linhas.</span></div></div>
            : file
            ? <div className="mailing-file"><FileIcon /><div><strong title={file.name}>{file.name}</strong><span>{count(file.rows)} linhas · limite {count(maxRows)}</span></div><button disabled={disabled} onClick={onChoose}>Trocar</button></div>
            : <button className="mailing-file-pick" disabled={disabled} onClick={onChoose}><FileIcon />Selecionar CSV</button>}
        {error ? <p role="alert" className="mailing-file-error">{error}</p>
            : !file && !reading && <p className="mailing-hint">Separado por ponto e vírgula, com <b>nome</b>, <b>cpf</b> e <b>fone1</b> no cabeçalho. Outras colunas vão junto. Até {count(maxRows)} linhas.</p>}
    </section>;
}

function Destination({ service }: { service: Service | null }) {
    return <section className="mailing-panel-section">
        <h2>Destino</h2>
        {service
            ? <div className="mailing-destination" key={service.id}><span className="mailing-destination-id">{service.id}</span><div><strong>{service.name}</strong><span>{service.group}</span></div></div>
            : <p className="mailing-hint">Escolha um serviço na lista.</p>}
    </section>;
}

type SendProps = { file: MailingFile | null; service: Service | null; configured: boolean; job: Job | null; confirming: boolean; busy: boolean; onAsk(): void; onConfirm(): void; onCancel(): void; onReset(): void };
function SendArea({ file, service, configured, job, confirming, busy, onAsk, onConfirm, onCancel, onReset }: SendProps) {
    if (job?.status === 'running') return <div className="mailing-send-area mailing-progress" role="status"><SpinnerIcon /><div><strong>Enviando {count(job.rows)} linhas para {job.service.id}</strong><p>A Fastway só responde depois de importar o mailing, o que pode levar alguns minutos. Não feche o aplicativo.</p></div></div>;
    if (job) {
        const done = job.status === 'completed';
        return <div className={`mailing-send-area mailing-result mailing-result--${job.status}`} role="status">{done ? <CheckIcon /> : <AlertIcon />}<div>
            <strong>{done ? 'Envio confirmado' : 'Envio não concluído'}</strong><p>{job.message}</p>
            <button onClick={onReset}>{done ? 'Fazer outro envio' : 'Voltar'}</button>
        </div></div>;
    }
    if (confirming && file && service) return <div className="mailing-send-area mailing-confirm" role="alert">
        <p>Enviar <strong>{count(file.rows)} linhas</strong> de {file.name} para <strong>{service.id} · {service.name}</strong>? Depois de enviado, não dá para desfazer.</p>
        <div className="mailing-confirm-actions"><button className="flow-primary" onClick={onConfirm}>Confirmar envio</button><button onClick={onCancel}>Cancelar</button></div>
    </div>;
    const missing = [!file && 'o arquivo', !service && 'o serviço'].filter(Boolean);
    return <div className="mailing-send-area">
        {!configured && <p className="mailing-access" role="status">Seu usuário ainda não tem login Fastway cadastrado. Peça a um admin para incluir no seu acesso.</p>}
        <button className="flow-primary mailing-send" disabled={busy || !file || !service || !configured} onClick={onAsk}>Enviar para a Fastway</button>
        {configured && missing.length > 0 && <p className="mailing-missing">Falta escolher {missing.join(' e ')}.</p>}
    </div>;
}

const icon = { width: 18, height: 18, viewBox: '0 0 24 24', fill: 'none', stroke: 'currentColor', strokeWidth: 1.8, strokeLinecap: 'round' as const, strokeLinejoin: 'round' as const, 'aria-hidden': true };
const FileIcon = () => <svg {...icon}><path d="M14 3H7a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V8Z" /><path d="M14 3v5h5M9 13h6M9 17h4" /></svg>;
const CheckIcon = () => <svg {...icon}><circle cx="12" cy="12" r="9" /><path d="m8 12.5 2.6 2.5L16 9.5" /></svg>;
const AlertIcon = () => <svg {...icon}><circle cx="12" cy="12" r="9" /><path d="M12 7.5v5.5M12 16.5v.01" /></svg>;
const SpinnerIcon = () => <svg {...icon}><path d="M21 12a9 9 0 1 1-6.2-8.56" /></svg>;
