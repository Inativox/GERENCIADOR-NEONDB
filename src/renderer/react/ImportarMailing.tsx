import { useEffect, useRef, useState } from 'react';
import type { ReactNode } from 'react';
import './fluxos.css';
import { ReceitaSituacao } from './ReceitaSituacao';
import { AlertIcon, CheckIcon, FileIcon, KeyIcon, SpinnerIcon } from './icons';
// Mesma lista de SERVICES em src/main/handlers/importarMailing.js; o main confere o ID de novo antes de enviar.
type Service = { id: number; group: string; label: string; name: string };
type Job = { status: 'running' | 'completed' | 'failed'; file: string; rows: number; service: Service; message: string };
type MailingFile = { id: string; name: string; rows: number };
// A senha só vai da tela para o main; de volta vem apenas se há login salvo e qual é.
type Access = { saved: boolean; login: string | null; canSave: boolean };
type Typed = { login: string; password: string; save: boolean };
type Response = { success: boolean; message?: string; cancelled?: boolean; services?: Service[]; fastway?: Access; admin?: boolean; maxRows?: number; job?: Job | null; file?: MailingFile };
type API = { mailingImportState(): Promise<Response>; mailingImportFile(): Promise<Response>; mailingImportSend(fileId: string, serviceId: number, fastway?: Typed): Promise<Response>; mailingImportForget(): Promise<Response> };
type Section = 'mailing' | 'receita';
const count = (value: number) => value.toLocaleString('pt-BR');
const READ_FAILURE = 'Não foi possível ler o arquivo. Tente novamente.';
const SEND_FAILURE = 'Não foi possível confirmar o envio. Confira o painel da Fastway antes de enviar de novo.';
const NO_ACCESS: Access = { saved: false, login: null, canSave: false };
const EMPTY_LOGIN: Typed = { login: '', password: '', save: false };

export function ImportarMailing() {
    const api = window.electronAPI as unknown as API;
    const [initial, setInitial] = useState<Response | null>(null), [section, setSection] = useState<Section>('mailing'), [failure, setFailure] = useState('');
    useEffect(() => {
        let mounted = true;
        // Reopening the tab reloads the state from the main process.
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
    const [access, setAccess] = useState<Access>(initial.fastway || NO_ACCESS), [typing, setTyping] = useState<Typed>(EMPTY_LOGIN), [changing, setChanging] = useState(false), [accessError, setAccessError] = useState('');
    const locked = useRef(false);
    // Reading a 250k-row CSV takes seconds; only the upload itself locks the service choice.
    const sending = job?.status === 'running', reading = busy && !sending;
    // Sem login salvo, ou depois de "Trocar", o envio usa o que foi digitado.
    const typed = !access.saved || changing, loginReady = !typed || Boolean(typing.login.trim() && typing.password);
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
        if (!file || !service || !loginReady) return;
        setJob({ status: 'running', file: file.name, rows: file.rows, service, message: 'Enviando para a Fastway…' });
        try {
            const response = await api.mailingImportSend(file.id, service.id, typed ? typing : undefined);
            if (!response.success || !response.job) throw new Error(response.message);
            setJob(response.job); setFile(null);
            // Sem salvar, o login fica no campo e só a senha é pedida de novo no próximo envio.
            if (response.fastway) setAccess(response.fastway);
            setTyping(previous => ({ ...previous, password: '', save: false })); setChanging(false);
        }
        catch (reason) { const text = reason instanceof Error && reason.message ? reason.message : SEND_FAILURE; setJob(previous => previous && { ...previous, status: 'failed', message: text }); }
    });
    const forget = () => guarded(async () => {
        setAccessError('');
        try {
            const response = await api.mailingImportForget();
            if (!response.success || !response.fastway) throw new Error(response.message);
            setAccess(response.fastway); setTyping(EMPTY_LOGIN); setChanging(false);
        } catch (reason) { setAccessError(reason instanceof Error && reason.message ? reason.message : 'Não foi possível esquecer o login. Tente novamente.'); }
    });
    const pick = (item: Service) => { setService(item); setConfirming(false); if (job?.status !== 'running') setJob(null); };
    return <div className="flows-app mailing-app" aria-busy={busy}>
        <header className="flows-heading"><div><h1>Importar Mailing</h1><p>Envie um CSV direto para um serviço da Fastway, sem passar pela geração de lista.</p></div>{switcher}</header>
        <div className="mailing-layout">
            <ServicePicker services={initial.services || []} selected={service} disabled={sending} onPick={pick} />
            <aside className="mailing-panel" aria-label="Envio para a Fastway">
                <FileSlot file={file} error={fileError} maxRows={initial.maxRows || 0} reading={reading} disabled={busy || sending} onChoose={() => void choose()} />
                <Destination service={service} />
                <FastwayLogin access={access} typed={typed} typing={typing} error={accessError} disabled={busy || sending}
                    onType={setTyping} onChange={() => { setChanging(true); setAccessError(''); }} onKeep={() => { setChanging(false); setTyping(EMPTY_LOGIN); }} onForget={() => void forget()} />
                <SendArea file={file} service={service} loginReady={loginReady} job={job} confirming={confirming} busy={busy}
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

type LoginProps = { access: Access; typed: boolean; typing: Typed; error: string; disabled: boolean; onType(next: Typed): void; onChange(): void; onKeep(): void; onForget(): void };
function FastwayLogin({ access, typed, typing, error, disabled, onType, onChange, onKeep, onForget }: LoginProps) {
    if (!typed) return <section className="mailing-panel-section">
        <h2>Login Fastway</h2>
        <div className="mailing-login-saved"><KeyIcon /><div><strong title={access.login || ''}>{access.login}</strong><span>Salvo neste computador</span></div>
            <div className="mailing-login-actions"><button disabled={disabled} onClick={onChange}>Trocar</button><button disabled={disabled} onClick={onForget}>Esquecer</button></div>
        </div>
        {error && <p role="alert" className="mailing-file-error">{error}</p>}
    </section>;
    return <section className="mailing-panel-section mailing-login">
        <h2>Login Fastway</h2>
        <label className="flow-field"><span>Login</span><input autoComplete="off" spellCheck={false} value={typing.login} disabled={disabled} onChange={event => onType({ ...typing, login: event.target.value })} /></label>
        <label className="flow-field"><span>Senha</span><input type="password" autoComplete="new-password" value={typing.password} disabled={disabled} onChange={event => onType({ ...typing, password: event.target.value })} /></label>
        <label className="flow-check"><input type="checkbox" checked={typing.save} disabled={disabled || !access.canSave} onChange={event => onType({ ...typing, save: event.target.checked })} /><span>Salvar neste computador</span></label>
        <p className="mailing-hint">{access.canSave ? 'O login só é salvo depois que a Fastway confirmar o envio.' : 'A proteção do Windows não está disponível nesta máquina. O login vale só para este envio.'}</p>
        {access.saved && <button className="mailing-login-keep" disabled={disabled} onClick={onKeep}>Usar o login salvo ({access.login})</button>}
    </section>;
}

type SendProps = { file: MailingFile | null; service: Service | null; loginReady: boolean; job: Job | null; confirming: boolean; busy: boolean; onAsk(): void; onConfirm(): void; onCancel(): void; onReset(): void };
function SendArea({ file, service, loginReady, job, confirming, busy, onAsk, onConfirm, onCancel, onReset }: SendProps) {
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
        <button className="flow-primary mailing-send" disabled={busy || !file || !service || !loginReady} onClick={onAsk}>Enviar para a Fastway</button>
        {missing.length > 0 ? <p className="mailing-missing">Falta escolher {missing.join(' e ')}.</p>
            : !loginReady && <p className="mailing-missing">Digite o login e a senha da Fastway.</p>}
    </div>;
}

