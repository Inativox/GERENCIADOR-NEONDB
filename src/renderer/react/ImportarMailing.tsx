import { useEffect, useRef, useState } from 'react';
import type { ReactNode } from 'react';
import './fluxos.css';
import { ReceitaSituacao } from './ReceitaSituacao';
import { FileIcon, KeyIcon, SpinnerIcon } from './icons';
// Mesma lista de SERVICES em src/main/handlers/importarMailing.js; o main confere o ID de novo antes de enviar.
type Service = { id: number; group: string; label: string; name: string };
type Status = 'waiting' | 'running' | 'completed' | 'failed';
type QueueItem = { id: string; name: string; rows: number; service: Service; status: Status; message: string };
type Queue = { running: boolean; items: QueueItem[] };
type Rejected = { name: string; message: string };
// A senha só vai da tela para o main; de volta vem apenas se há login salvo e qual é.
type Access = { saved: boolean; login: string | null; canSave: boolean };
type Typed = { login: string; password: string; save: boolean };
type Update = { queue: Queue; fastway?: Access };
type Response = { success: boolean; message?: string; cancelled?: boolean; services?: Service[]; fastway?: Access; admin?: boolean; maxRows?: number; maxQueue?: number; queue?: Queue; rejected?: Rejected[] };
type API = {
    mailingImportState(): Promise<Response>; mailingImportAdd(serviceId: number): Promise<Response>; mailingImportRemove(itemId: string): Promise<Response>;
    mailingImportClear(): Promise<Response>; mailingImportStart(fastway?: Typed): Promise<Response>; mailingImportForget(): Promise<Response>;
    onMailingImportUpdate(callback: (update: Update) => void): () => void;
};
type Section = 'mailing' | 'receita';
const count = (value: number) => value.toLocaleString('pt-BR');
const lists = (value: number) => `${value} ${value === 1 ? 'lista' : 'listas'}`;
const reasonText = (reason: unknown, fallback: string) => reason instanceof Error && reason.message ? reason.message : fallback;
const READ_FAILURE = 'Não foi possível ler os arquivos. Tente novamente.';
const SEND_FAILURE = 'Não foi possível iniciar o envio. Tente novamente.';
const STATUS: Record<Status, string> = { waiting: 'Na fila', running: 'Enviando', completed: 'Enviada', failed: 'Falhou' };
const NO_ACCESS: Access = { saved: false, login: null, canSave: false };
const EMPTY_LOGIN: Typed = { login: '', password: '', save: false };
const EMPTY_QUEUE: Queue = { running: false, items: [] };

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
    const [service, setService] = useState<Service | null>(null), [queue, setQueue] = useState<Queue>(initial.queue || EMPTY_QUEUE);
    const [notice, setNotice] = useState<string[]>([]), [startError, setStartError] = useState(''), [confirming, setConfirming] = useState(false);
    const [busy, setBusy] = useState(false), [adding, setAdding] = useState(false);
    const [access, setAccess] = useState<Access>(initial.fastway || NO_ACCESS), [typing, setTyping] = useState<Typed>(EMPTY_LOGIN), [changing, setChanging] = useState(false), [accessError, setAccessError] = useState('');
    const locked = useRef(false);
    // O main avisa cada mudança da fila, inclusive com a aba escondida.
    useEffect(() => api.onMailingImportUpdate(update => { setQueue(update.queue); if (update.fastway) setAccess(update.fastway); }), [api]);
    useEffect(() => { if (initial.queue) setQueue(initial.queue); }, [initial]);
    // Sem login salvo, ou depois de "Trocar", o envio usa o que foi digitado.
    const typed = !access.saved || changing, loginReady = !typed || Boolean(typing.login.trim() && typing.password);
    async function guarded(run: () => Promise<void>) {
        if (locked.current) return; locked.current = true; setBusy(true); setConfirming(false);
        try { await run(); } finally { locked.current = false; setBusy(false); }
    }
    // Toda resposta traz a fila; numa falha a tela fica com a fila que já mostrava.
    const apply = (response: Response) => { if (response.queue) setQueue(response.queue); if (!response.success) throw new Error(response.message); return response; };
    const add = () => guarded(async () => {
        if (!service) return;
        setNotice([]); setAdding(true);
        try {
            const response = apply(await api.mailingImportAdd(service.id));
            setNotice((response.rejected || []).map(item => `${item.name}: ${item.message}`));
        } catch (reason) { setNotice([reasonText(reason, READ_FAILURE)]); }
        finally { setAdding(false); }
    });
    const remove = (item: QueueItem) => guarded(async () => {
        try { apply(await api.mailingImportRemove(item.id)); } catch (reason) { setNotice([reasonText(reason, 'Não foi possível tirar a lista da fila.')]); }
    });
    const clear = () => guarded(async () => {
        try { apply(await api.mailingImportClear()); } catch (reason) { setNotice([reasonText(reason, 'Não foi possível limpar a fila.')]); }
    });
    const start = () => guarded(async () => {
        setStartError('');
        try {
            apply(await api.mailingImportStart(typed ? typing : undefined));
            // Sem salvar, o login fica no campo e só a senha é pedida de novo na próxima fila.
            setTyping(previous => ({ ...previous, password: '', save: false })); setChanging(false);
        } catch (reason) { setStartError(reasonText(reason, SEND_FAILURE)); }
    });
    const forget = () => guarded(async () => {
        setAccessError('');
        try {
            const response = await api.mailingImportForget();
            if (!response.success || !response.fastway) throw new Error(response.message);
            setAccess(response.fastway); setTyping(EMPTY_LOGIN); setChanging(false);
        } catch (reason) { setAccessError(reasonText(reason, 'Não foi possível esquecer o login. Tente novamente.')); }
    });
    return <div className="flows-app mailing-app" aria-busy={busy}>
        <header className="flows-heading"><div><h1>Importar Mailing</h1><p>Monte uma fila de CSVs, cada um para o seu serviço da Fastway. As listas sobem uma por vez, sem passar pela geração de lista.</p></div>{switcher}</header>
        <div className="mailing-layout">
            <ServicePicker services={initial.services || []} selected={service} disabled={busy} onPick={item => { setService(item); setNotice([]); }} />
            <aside className="mailing-panel" aria-label="Fila de envio para a Fastway">
                <AddSlot service={service} maxRows={initial.maxRows || 0} notice={notice} adding={adding} disabled={busy} onAdd={() => void add()} />
                <QueueList queue={queue} maxQueue={initial.maxQueue || 0} disabled={busy} onRemove={item => void remove(item)} onClear={() => void clear()} />
                <FastwayLogin access={access} typed={typed} typing={typing} error={accessError} disabled={busy || queue.running}
                    onType={setTyping} onChange={() => { setChanging(true); setAccessError(''); }} onKeep={() => { setChanging(false); setTyping(EMPTY_LOGIN); }} onForget={() => void forget()} />
                <SendArea queue={queue} loginReady={loginReady} confirming={confirming} busy={busy} error={startError}
                    onAsk={() => setConfirming(true)} onConfirm={() => void start()} onCancel={() => setConfirming(false)} />
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

type AddProps = { service: Service | null; maxRows: number; notice: string[]; adding: boolean; disabled: boolean; onAdd(): void };
function AddSlot({ service, maxRows, notice, adding, disabled, onAdd }: AddProps) {
    return <section className="mailing-panel-section mailing-add" aria-busy={adding}>
        <h2>Adicionar à fila</h2>
        {service
            ? <div className="mailing-destination" key={service.id}><span className="mailing-destination-id">{service.id}</span><div><strong>{service.name}</strong><span>{service.group}</span></div></div>
            : <p className="mailing-hint">Escolha um serviço na lista.</p>}
        {adding
            ? <div className="mailing-file mailing-file--reading" role="status"><SpinnerIcon /><div><strong>Conferindo os CSVs…</strong><span>O gerenciador confere o cabeçalho e conta as linhas de cada arquivo.</span></div></div>
            : <button className="mailing-file-pick" disabled={disabled || !service} onClick={onAdd}><FileIcon />Adicionar CSV à fila</button>}
        {notice.length > 0
            ? <ul role="alert" className="mailing-file-error mailing-notice">{notice.map(text => <li key={text}>{text}</li>)}</ul>
            : !adding && <p className="mailing-hint">Pode escolher vários CSVs de uma vez: todos vão para o serviço marcado. Separados por ponto e vírgula, com <b>nome</b>, <b>cpf</b> e <b>fone1</b> no cabeçalho. Até {count(maxRows)} linhas cada.</p>}
    </section>;
}

type QueueProps = { queue: Queue; maxQueue: number; disabled: boolean; onRemove(item: QueueItem): void; onClear(): void };
function QueueList({ queue, maxQueue, disabled, onRemove, onClear }: QueueProps) {
    const finished = queue.items.some(item => item.status === 'completed' || item.status === 'failed');
    return <section className="mailing-panel-section" aria-labelledby="mailing-queue-title">
        <div className="mailing-queue-heading"><h2 id="mailing-queue-title">Fila</h2><span>{queue.items.length} de {maxQueue}</span></div>
        {queue.items.length
            ? <ol className="mailing-queue">{queue.items.map(item => <li key={item.id} className={`mailing-queue-item mailing-queue-item--${item.status}`}>
                <div>
                    <strong title={item.name}>{item.name}</strong>
                    <span className="mailing-queue-meta">{count(item.rows)} linhas · {item.service.id} · {item.service.name}</span>
                    {item.status === 'failed' && <p className="mailing-queue-message">{item.message}</p>}
                </div>
                <span className="mailing-queue-status">{item.status === 'running' && <SpinnerIcon />}{STATUS[item.status]}</span>
                {item.status !== 'running' && <button type="button" disabled={disabled} aria-label={`Tirar ${item.name} da fila`} title="Tirar da fila" onClick={() => onRemove(item)}>×</button>}
            </li>)}</ol>
            : <p className="mailing-hint">Nenhuma lista na fila.</p>}
        {finished && <button type="button" className="mailing-queue-clear" disabled={disabled} onClick={onClear}>Limpar concluídas</button>}
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
        <p className="mailing-hint">{access.canSave ? 'O login só é salvo depois que a Fastway confirmar um envio.' : 'A proteção do Windows não está disponível nesta máquina. O login vale só para esta fila.'}</p>
        {access.saved && <button className="mailing-login-keep" disabled={disabled} onClick={onKeep}>Usar o login salvo ({access.login})</button>}
    </section>;
}

type SendProps = { queue: Queue; loginReady: boolean; confirming: boolean; busy: boolean; error: string; onAsk(): void; onConfirm(): void; onCancel(): void };
function SendArea({ queue, loginReady, confirming, busy, error, onAsk, onConfirm, onCancel }: SendProps) {
    const waiting = queue.items.filter(item => item.status === 'waiting');
    if (queue.running) return <SendProgress current={queue.items.find(item => item.status === 'running')} waiting={waiting.length} />;
    if (confirming && waiting.length) return <div className="mailing-send-area mailing-confirm" role="alert">
        <p>Enviar <strong>{lists(waiting.length)}</strong> ({count(waiting.reduce((sum, item) => sum + item.rows, 0))} linhas no total) para a Fastway, uma por vez? Cada lista vai para o serviço marcado nela. Depois de enviado, não dá para desfazer.</p>
        <div className="mailing-confirm-actions"><button className="flow-primary" onClick={onConfirm}>Confirmar envio</button><button onClick={onCancel}>Cancelar</button></div>
    </div>;
    const done = queue.items.filter(item => item.status === 'completed').length, failed = queue.items.filter(item => item.status === 'failed').length;
    return <div className="mailing-send-area">
        {done + failed > 0 && <p className="mailing-summary" role="status">{done} {done === 1 ? 'enviada' : 'enviadas'} · {failed} com falha</p>}
        <button className="flow-primary mailing-send" disabled={busy || !waiting.length || !loginReady} onClick={onAsk}>Enviar fila{waiting.length ? ` (${waiting.length})` : ''}</button>
        {error ? <p role="alert" className="mailing-file-error">{error}</p>
            : !waiting.length ? <p className="mailing-missing">Adicione listas à fila para enviar.</p>
            : !loginReady && <p className="mailing-missing">Digite o login e a senha da Fastway.</p>}
    </div>;
}

function SendProgress({ current, waiting }: { current?: QueueItem; waiting: number }) {
    return <div className="mailing-send-area mailing-progress" role="status"><SpinnerIcon /><div>
        <strong>{current ? `Enviando ${current.name} para ${current.service.id}` : 'Enviando a fila…'}</strong>
        <p>{waiting ? `${lists(waiting)} esperando. ` : ''}Pode adicionar mais listas: elas sobem na sequência. A Fastway só responde depois de importar cada mailing. Não feche o aplicativo.</p>
    </div></div>;
}
