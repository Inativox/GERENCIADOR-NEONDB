import { useEffect, useRef, useState } from 'react';
import type { ReactNode } from 'react';
import './fluxos.css';
import './alertaMailing.css';
import { KeyIcon, SpinnerIcon } from './icons';

// Mesmo formato de src/main/alertaMailing/monitor.js (linhas) e estado.js (eventos).
type Linha = { servicoId: number; servico: string; mailingId: number | null; arquivo: string; registros: number; restantes: number; percentual: number;
    situacao: 'OK' | 'QUASE' | 'ESGOTADO' | 'SEM_MAILING'; acao: 'NADA' | 'PODE_RECICLAR' | 'TROCAR' | 'PROXIMA_LISTA' | 'VERIFICAR_SERVICO';
    reciclagensHoje: number; limiteReciclagens: number; minutosRestantes: number | null; ritmoPorMinuto: number | null; transbordo: boolean; parado: boolean };
type Evento = { id: string; hora: string; tipo: string; servico: string; texto: string; servicoId: number | null; mailingId: number | null; status: 'aberta' | 'resolvida' | 'dispensada'; motivo: string; horaFim: string; oculto: boolean };
type Painel = { configurado: boolean; login: { saved: boolean; login: string | null; canSave: boolean }; reciclador: boolean; foco: number | null;
    linhas: Linha[]; eventos: Evento[]; verificadoEm: string | null; conectado: boolean; erro: string | null; verificando?: boolean; intervaloMinutos?: number };
type Response = { success: boolean; message?: string; painel?: Painel };
type API = {
    alertaMailingState(): Promise<Response>; alertaMailingVerificar(): Promise<Response>; alertaMailingDispensar(eventoId: string | null): Promise<Response>;
    alertaMailingLimparHistorico(): Promise<Response>; alertaMailingReciclador(ligar: boolean): Promise<Response>; alertaMailingServico(servicoId: number, parar: boolean): Promise<Response>;
    alertaMailingLogin(login: string, password: string): Promise<Response>; onAlertaMailingUpdate(callback: (painel: Painel) => void): () => void;
    onAlertaMailingFoco(callback: (foco: { servicoId: number | null }) => void): () => void; showConfirmDialog(options: { title: string; message: string }): Promise<boolean>;
};
type Visual = 'TROCAR' | 'ESGOTADO' | 'QUASE' | 'SEM_MAILING' | 'SERVICO_PARADO' | 'OK';
type Tone = 'ok' | 'warn' | 'stop' | 'none';
type Folha = { tipo: 'servico'; servicoId: number } | { tipo: 'historico' } | null;

const INFO: Record<Visual, { rotulo: string; verbo: string; tom: Tone }> = {
    TROCAR: { rotulo: 'Trocar lista', verbo: 'Trocar a lista', tom: 'stop' }, ESGOTADO: { rotulo: 'Esgotado', verbo: 'Reciclar agora', tom: 'stop' },
    QUASE: { rotulo: 'Acabando', verbo: 'Reciclar em breve', tom: 'warn' }, SEM_MAILING: { rotulo: 'Sem mailing', verbo: 'Verificar o serviço', tom: 'stop' },
    SERVICO_PARADO: { rotulo: 'Parado', verbo: 'Serviço parado', tom: 'none' }, OK: { rotulo: 'OK', verbo: 'Nada a fazer', tom: 'ok' },
};
// Serviço de transbordo (LTDA, PT 2): nunca recicla, o alerta pede para seguir para a próxima lista.
const PROXIMA: Partial<Record<Linha['situacao'], string>> = { QUASE: 'Próxima lista em breve', ESGOTADO: 'Iniciar a próxima lista' };
const TITULO_EVENTO: Record<string, string> = { QUASE: 'Mailing em 99%', ESGOTADO: 'Mailing esgotado', SEM_MAILING: 'Sem mailing ativo', TROCAR: 'Trocar a lista', FALHA: 'Fastway sem resposta',
    RECICLADA: 'Reciclada automaticamente', NADA_RECICLAR: 'Nada para reciclar', FALHA_RECICLAGEM: 'Falha ao reciclar', PARADO: 'Serviço parado automaticamente', FALHA_PARADA: 'Falha ao parar o serviço' };
const TOM_EVENTO: Record<string, Tone> = { QUASE: 'warn', ESGOTADO: 'stop', SEM_MAILING: 'stop', TROCAR: 'stop', FALHA: 'stop', FALHA_RECICLAGEM: 'stop', FALHA_PARADA: 'stop', RECICLADA: 'ok', PARADO: 'none', NADA_RECICLAR: 'none' };
// Ordem do mural: o que pede ação primeiro, parado por último.
const RANK: Record<Visual, number> = { TROCAR: 0, ESGOTADO: 1, SEM_MAILING: 2, QUASE: 3, OK: 5, SERVICO_PARADO: 6 };
const SEM_RECICLAGEM = 'LTDA e PT2 (transbordo) nunca são reciclados nem parados automaticamente: seguem para a próxima lista e só param pelo botão do painel.';
const HINT_AUTO = `Só um PC deve ficar com a chave ligada por vez. ${SEM_RECICLAGEM}`;

const numero = new Intl.NumberFormat('pt-BR'), decimal = new Intl.NumberFormat('pt-BR', { minimumFractionDigits: 1, maximumFractionDigits: 1 });
const porcentoNum = (p: number) => decimal.format(Math.floor(p * 10) / 10); // 99,97 mostra 99,9 e não 100
const porcento = (p: number) => `${porcentoNum(p)}%`;
const visual = (l: Linha): Visual => (l.parado ? (l.situacao === 'ESGOTADO' ? 'TROCAR' : 'SERVICO_PARADO') : l.acao === 'TROCAR' ? 'TROCAR' : l.situacao);
const emAlerta = (l: Linha) => l.acao !== 'NADA' && visual(l) !== 'SERVICO_PARADO';
const proxima = (l: Linha) => (l.acao === 'PROXIMA_LISTA' && visual(l) !== 'SERVICO_PARADO' && PROXIMA[l.situacao]) || '';
const verbo = (l: Linha) => proxima(l) || INFO[visual(l)].verbo;
const pendenciaDe = (painel: Painel, l: Linha) => painel.eventos.find(e => e.servicoId === l.servicoId && e.status === 'aberta');
const horaDe = (iso: string | null) => (iso ? new Date(iso).toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit' }) : '');
// Os nomes do C6 começam todos iguais; no mural o que distingue é o final.
const nomeCurto = (nome: string) => nome.replace(/^\[C6 BANK\] - NOVO TRANSBORDO 98/, 'C6 Transbordo 98');
const ordenar = (linhas: Linha[]) => [...linhas].sort((a, b) => RANK[visual(a)] - RANK[visual(b)] || (a.minutosRestantes ?? Infinity) - (b.minutosRestantes ?? Infinity) || b.percentual - a.percentual);
const visiveis = (painel: Painel) => painel.eventos.filter(e => !e.oculto).sort((a, b) => b.hora.localeCompare(a.hora));

function duracao(min: number) {
    const total = Math.max(1, Math.round(min));
    if (total < 60) return `~${total} min`;
    if (total >= 24 * 60) return 'mais de 1 dia';
    const arredondado = Math.round(total / 10) * 10; // acima de 1 h, minuto exato é precisão falsa
    const h = Math.floor(arredondado / 60), m = arredondado % 60;
    return m ? `~${h} h ${String(m).padStart(2, '0')}` : `~${h} h`;
}
const acabaEm = (l: Linha) => (l.minutosRestantes == null ? '' : duracao(l.minutosRestantes));

function previsao(l: Linha) {
    if (!l.mailingId || l.situacao === 'ESGOTADO') return '';
    if (l.minutosRestantes != null) return `acaba em ${acabaEm(l)}`;
    return l.ritmoPorMinuto === 0 ? 'sem consumo agora' : 'calculando previsão…';
}

function explicacao(l: Linha, reciclador: boolean) {
    const usadas = `${l.reciclagensHoje} de ${l.limiteReciclagens}`, restam = `Restam ${numero.format(l.restantes)} registros${acabaEm(l) ? `, acaba em ${acabaEm(l)}` : ''}.`;
    if (l.parado && l.situacao === 'ESGOTADO') return l.transbordo ? 'A lista acabou e o serviço está parado. Este serviço é de transbordo: inicie a próxima lista do mailing e depois o serviço.'
        : 'A lista acabou e o serviço está parado. Suba uma lista nova na Fastway e depois inicie o serviço.';
    if (l.parado) return `O serviço está parado na Fastway: a discagem fica parada até alguém iniciar de novo.${l.mailingId ? ` Restam ${numero.format(l.restantes)} registros na lista.` : ''}`;
    if (l.acao === 'TROCAR') return `Esta lista já foi reciclada ${l.reciclagensHoje} vezes hoje, o limite do dia. Suba uma lista nova na Fastway.${reciclador && l.situacao === 'QUASE' ? ' Este PC para o serviço quando a lista acabar.' : ''}`;
    if (l.acao === 'PROXIMA_LISTA' && l.situacao === 'ESGOTADO') return 'A lista acabou. Este serviço é de transbordo e não é reciclado: inicie a próxima lista do mailing na Fastway.';
    if (l.acao === 'PROXIMA_LISTA') return `${restam} Este serviço é de transbordo e não é reciclado: quando acabar, inicie a próxima lista do mailing.`;
    if (reciclador && (l.situacao === 'ESGOTADO' || l.situacao === 'QUASE')) return `Este PC recicla automaticamente as listas em 99% ou esgotadas (${usadas} reciclagens usadas hoje). Se não reciclar em alguns minutos, confira o histórico de hoje.`;
    if (l.situacao === 'ESGOTADO') return `A lista acabou. Ainda dá para reciclar hoje: ${usadas} reciclagens usadas.`;
    if (l.situacao === 'QUASE') return `${restam} Prepare a reciclagem: ${usadas} usadas hoje.`;
    if (l.situacao === 'SEM_MAILING') return l.mailingId ? 'O serviço está em execução, mas sem mailing ativo. Confira a lista do serviço na Fastway.'
        : 'O serviço está em execução e nenhuma lista foi encontrada para ele. Confira na Fastway.';
    return 'Consumo abaixo de 99%. Você recebe uma notificação quando chegar lá.';
}

// Relógio da próxima verificação: só os componentes que mostram a contagem renderizam a cada segundo.
function useAgora() {
    const [agora, setAgora] = useState(() => Date.now());
    useEffect(() => { const id = window.setInterval(() => setAgora(Date.now()), 1000); return () => window.clearInterval(id); }, []);
    return agora;
}
function segundosAteProxima(painel: Painel, agora: number) {
    if (!painel.verificadoEm) return null;
    const intervalo = (painel.intervaloMinutos ?? 3) * 60;
    return { restam: Math.max(0, Math.round((new Date(painel.verificadoEm).getTime() + intervalo * 1000 - agora) / 1000)), intervalo };
}
const minSeg = (s: number) => `${Math.floor(s / 60)}:${String(s % 60).padStart(2, '0')}`;

export function AlertaMailing() {
    const api = window.electronAPI as unknown as API;
    const [painel, setPainel] = useState<Painel | null>(null), [folha, setFolha] = useState<Folha>(null);
    const [mensagem, setMensagem] = useState(''), [busy, setBusy] = useState(false);
    const locked = useRef(false);
    const receber = (dados: Painel) => { setPainel(dados); if (dados.foco != null) setFolha({ tipo: 'servico', servicoId: dados.foco }); };
    useEffect(() => {
        let mounted = true;
        const carregar = () => api.alertaMailingState().then(r => { if (mounted && r.success && r.painel) receber(r.painel); else if (mounted && !r.success) setMensagem(r.message || 'Alerta indisponível.'); }).catch(() => {});
        void carregar();
        const offUpdate = api.onAlertaMailingUpdate(dados => { if (mounted) receber(dados); });
        // Clique na notificação: abre esta aba já no serviço que avisou.
        const offFoco = api.onAlertaMailingFoco(({ servicoId }) => {
            if (!mounted) return;
            (document.querySelector('.tab-button[data-tab-name="alertaMailing"]') as HTMLElement | null)?.click();
            if (servicoId != null) setFolha({ tipo: 'servico', servicoId });
        });
        const aoAbrirAba = (event: Event) => { if ((event as CustomEvent<{ tabId?: string }>).detail?.tabId === 'alertaMailing') void carregar(); };
        document.addEventListener('tab-changed', aoAbrirAba);
        return () => { mounted = false; offUpdate(); offFoco(); document.removeEventListener('tab-changed', aoAbrirAba); };
    }, [api]);

    async function acao(run: () => Promise<Response>) {
        if (locked.current) return;
        locked.current = true; setBusy(true); setMensagem('');
        try { const r = await run(); if (!r.success) throw new Error(r.message); if (r.painel) receber(r.painel); }
        catch (reason) { setMensagem(reason instanceof Error && reason.message ? reason.message : 'Não foi possível concluir. Tente novamente.'); }
        finally { locked.current = false; setBusy(false); }
    }

    async function mudarServico(l: Linha) {
        const parar = !l.parado;
        const confirmado = await api.showConfirmDialog({ title: parar ? 'Parar serviço' : 'Iniciar serviço',
            message: parar ? `Parar ${l.servico} (#${l.servicoId}) na Fastway? A discagem dele para até alguém iniciar de novo.` : `Iniciar ${l.servico} (#${l.servicoId}) na Fastway?` });
        if (confirmado) await acao(() => api.alertaMailingServico(l.servicoId, parar));
    }

    if (!painel) return mensagem ? <div className="flows-app"><p role="alert" className="flow-error">{mensagem}</p></div> : null;
    if (!painel.configurado) return <div className="flows-app alerta-app" aria-busy={busy}>
        <header className="flows-heading"><div><h1>Alerta de Mailing</h1><p>Confere o mailing ativo de cada serviço da Fastway e avisa quando a lista está acabando, esgotou ou o serviço ficou sem lista.</p></div></header>
        {mensagem && <p role="alert" className="flow-notice flow-notice--error">{mensagem}</p>}
        <LoginFastway painel={painel} busy={busy} onSalvar={(login, senha) => void acao(() => api.alertaMailingLogin(login, senha))} />
    </div>;

    const linhas = ordenar(painel.linhas), historico = visiveis(painel);
    const atual = folha?.tipo === 'servico' ? painel.linhas.find(l => l.servicoId === folha.servicoId) : null;
    return <div className="flows-app alerta-app" aria-busy={busy}>
        <div className="alerta-mural">
            <header className="alerta-topo">
                <div>
                    <h1 className="alerta-titulo">Alerta de Mailing</h1>
                    <p className="alerta-sub"><span className="alerta-dot" data-tone={!painel.conectado ? 'stop' : 'ok'} aria-hidden="true" />
                        {painel.conectado ? `${linhas.length} ${linhas.length === 1 ? 'serviço' : 'serviços'} · o nível de cada tanque é o que falta da lista` : 'Sem conexão com a Fastway'}</p>
                </div>
                <Relogio painel={painel} />
                <div className="alerta-topo-acoes">
                    <Contagens linhas={linhas} />
                    <button className="flow-primary alerta-verificar" disabled={busy || painel.verificando} onClick={() => void acao(() => api.alertaMailingVerificar())}>
                        {painel.verificando && <SpinnerIcon />}{painel.verificando ? 'Verificando…' : 'Verificar'}</button>
                </div>
            </header>
            <Varredura painel={painel} />
            <div className="alerta-corpo">
                {mensagem && <p role="alert" className="flow-notice flow-notice--error">{mensagem}</p>}
                {!painel.conectado && painel.erro && <p role="alert" className="flow-notice flow-notice--error">{painel.erro}</p>}
                {linhas.length ? <div className={`alerta-grade${painel.verificando ? ' is-varrendo' : ''}${painel.conectado ? '' : ' is-antigo'}`}>
                    {linhas.map(l => <Tanque key={l.servicoId} linha={l} onAbrir={() => setFolha({ tipo: 'servico', servicoId: l.servicoId })} />)}
                </div> : <p className="alerta-vazio">Os serviços aparecem aqui assim que estiverem em execução na Fastway. O monitoramento continua automaticamente.</p>}
            </div>
            <footer className="alerta-rodape">
                <ol className="alerta-feed" aria-label="Últimos avisos">{historico.slice(0, 3).map(e => <li key={e.id} data-tone={TOM_EVENTO[e.tipo] || 'none'}>
                    <span className="alerta-dot" aria-hidden="true" /><time>{e.hora}</time><b>{TITULO_EVENTO[e.tipo] || e.tipo}</b>{e.servico && <span>{nomeCurto(e.servico)}</span>}</li>)}</ol>
                <button className="alerta-dia" onClick={() => setFolha({ tipo: 'historico' })}>Dia completo{historico.length ? ` (${historico.length})` : ''}</button>
                <Switch label="Auto-reciclar neste PC" hint={HINT_AUTO} checked={painel.reciclador} disabled={busy} onChange={ligar => void acao(() => api.alertaMailingReciclador(ligar))} />
            </footer>
        </div>
        {atual && <FolhaLateral titulo="Detalhes do serviço" onFechar={() => setFolha(null)}>
            <Detalhe linha={atual} painel={painel} busy={busy} onServico={() => void mudarServico(atual)} onDispensar={id => void acao(() => api.alertaMailingDispensar(id))} />
        </FolhaLateral>}
        {folha?.tipo === 'historico' && <FolhaLateral titulo="Alertas de hoje" onFechar={() => setFolha(null)}>
            <Historico eventos={historico} busy={busy} onDispensarTodas={() => void acao(() => api.alertaMailingDispensar(null))} onLimpar={() => void acao(() => api.alertaMailingLimparHistorico())} />
        </FolhaLateral>}
    </div>;
}

function Relogio({ painel }: { painel: Painel }) {
    const agora = useAgora(), proxima = segundosAteProxima(painel, agora);
    const depois = painel.verificando ? 'verificando a Fastway…' : !painel.conectado ? 'tentando de novo' : !proxima ? 'aguardando a primeira verificação'
        : proxima.restam ? <>próxima em <b>{minSeg(proxima.restam)}</b></> : 'próxima em instantes';
    return <div className="alerta-relogio" role="status"><span>verificado</span><b>{horaDe(painel.verificadoEm) || '--:--'}</b><span>{depois}</span></div>;
}

// Linha fina sob o cabeçalho: enche até a próxima verificação.
function Varredura({ painel }: { painel: Painel }) {
    const agora = useAgora(), proxima = segundosAteProxima(painel, agora);
    const cheio = !proxima || !painel.conectado ? 0 : ((proxima.intervalo - proxima.restam) / proxima.intervalo) * 100;
    return <div className="alerta-varredura" aria-hidden="true"><span style={{ width: `${cheio}%` }} /></div>;
}

function Contagens({ linhas }: { linhas: Linha[] }) {
    const agir = linhas.filter(l => emAlerta(l) && INFO[visual(l)].tom === 'stop').length, acabando = linhas.filter(l => emAlerta(l) && INFO[visual(l)].tom === 'warn').length;
    const rodando = linhas.filter(l => visual(l) === 'OK').length, parados = linhas.filter(l => visual(l) === 'SERVICO_PARADO').length;
    const item = (n: number, rotulo: string, tom: Tone) => <span className="alerta-contagem" data-tone={tom} data-zero={n ? undefined : ''}><b>{n}</b>{rotulo}</span>;
    return <div className="alerta-contagens">{item(agir, 'agir', 'stop')}{item(acabando, 'acabando', 'warn')}{item(rodando, 'rodando', 'ok')}{item(parados, parados === 1 ? 'parado' : 'parados', 'none')}</div>;
}

function Tanque({ linha: l, onAbrir }: { linha: Linha; onAbrir(): void }) {
    const v = visual(l), tom = INFO[v].tom, alerta = emAlerta(l), resto = l.mailingId ? Math.max(0, 100 - l.percentual) : 0, prev = previsao(l);
    const meta = l.mailingId ? `${numero.format(l.restantes)} restantes${prev ? ` · ${prev}` : ''}` : 'nenhuma lista no serviço';
    return <button className="alerta-tank" data-tone={tom} data-alerta={alerta ? '' : undefined} data-big={alerta && tom === 'stop' ? '' : undefined} title={l.servico} onClick={onAbrir}>
        <span className="alerta-tank-nivel" style={{ height: `${resto}%` }} aria-hidden="true" />
        <span className="alerta-tank-topo"><span className="alerta-tank-estado"><Icone nome={proxima(l) ? 'proxima' : v} />{INFO[v].rotulo}</span><span className="alerta-tank-id">#{l.servicoId}</span></span>
        <strong className="alerta-tank-nome">{nomeCurto(l.servico)}</strong>
        <span className="alerta-tank-pct">{l.mailingId ? <>{porcentoNum(l.percentual)}<small>%</small></> : '—'}</span>
        {alerta && <span className="alerta-tank-verbo">{verbo(l)}</span>}
        <span className="alerta-tank-meta">{meta}</span>
    </button>;
}

function FolhaLateral({ titulo, onFechar, children }: { titulo: string; onFechar(): void; children: ReactNode }) {
    const fechar = useRef<HTMLButtonElement>(null);
    useEffect(() => {
        fechar.current?.focus();
        const aoTeclar = (event: KeyboardEvent) => { if (event.key === 'Escape') onFechar(); };
        document.addEventListener('keydown', aoTeclar);
        return () => document.removeEventListener('keydown', aoTeclar);
    }, [onFechar]);
    return <>
        <div className="alerta-scrim" onClick={onFechar} aria-hidden="true" />
        <section className="alerta-sheet" role="dialog" aria-modal="true" aria-labelledby="alerta-sheet-titulo">
            <header className="alerta-sheet-head"><h2 id="alerta-sheet-titulo">{titulo}</h2>
                <button ref={fechar} className="alerta-fechar" aria-label="Fechar" onClick={onFechar}><Icone nome="fechar" /></button></header>
            {children}
        </section>
    </>;
}

function LoginFastway({ painel, busy, onSalvar }: { painel: Painel; busy: boolean; onSalvar(login: string, senha: string): void }) {
    const [login, setLogin] = useState(''), [senha, setSenha] = useState('');
    return <section className="alerta-login" aria-labelledby="alerta-login-titulo">
        <KeyIcon />
        <div>
            <h2 id="alerta-login-titulo">Login Fastway</h2>
            <p className="alerta-hint">O alerta usa o seu login Fastway, o mesmo do Importar Mailing. Ele é conferido na Fastway e salvo neste computador com a proteção do Windows; não vai para nenhum arquivo .env.</p>
            <form className="alerta-login-form" onSubmit={event => { event.preventDefault(); onSalvar(login, senha); }}>
                <label className="flow-field"><span>Login</span><input autoComplete="off" spellCheck={false} value={login} disabled={busy} onChange={event => setLogin(event.target.value)} /></label>
                <label className="flow-field"><span>Senha</span><input type="password" autoComplete="new-password" value={senha} disabled={busy} onChange={event => setSenha(event.target.value)} /></label>
                <button className="flow-primary" disabled={busy || !painel.login.canSave || !login.trim() || !senha}>{busy ? 'Conferindo…' : 'Conferir e salvar'}</button>
            </form>
            {!painel.login.canSave && <p className="alerta-hint">A proteção do Windows não está disponível nesta máquina, então o login não pode ser salvo e o alerta não roda aqui.</p>}
        </div>
    </section>;
}

function Barra({ percentual, tom }: { percentual: number; tom: Tone }) {
    return <span className="alerta-barra" data-tone={tom} role="img" aria-label={`${porcento(percentual)} consumido`}>
        <span style={{ width: `${Math.min(100, Math.max(0, percentual))}%` }} /><i title="Limite de alerta: 99%" />
    </span>;
}

function Detalhe({ linha: l, painel, busy, onServico, onDispensar }: { linha: Linha; painel: Painel; busy: boolean; onServico(): void; onDispensar(id: string): void }) {
    const v = visual(l), pendencia = pendenciaDe(painel, l), eventos = visiveis(painel).filter(e => e.servicoId === l.servicoId);
    return <div className="alerta-detalhe" key={l.servicoId}>
        <section className="alerta-secao alerta-d-head">
            <h2>{l.servico} <small>#{l.servicoId}</small></h2>
            <p className="alerta-hint">{l.mailingId ? `${l.arquivo} · mailing #${l.mailingId}` : 'Nenhuma lista associada'}</p>
            {(l.transbordo || l.parado) && <p className="alerta-tags">{l.transbordo && <span>Transbordo · sem reciclagem automática</span>}{l.parado && <span>Serviço parado na Fastway</span>}</p>}
        </section>
        <section className="alerta-secao alerta-acao alerta-d-acao" data-tone={INFO[v].tom}>
            <strong>{verbo(l)}</strong>
            <p>{explicacao(l, painel.reciclador)}</p>
        </section>
        {l.mailingId != null && <section className="alerta-secao alerta-d-consumo">
            <h3>Consumo do mailing</h3>
            <p className="alerta-medidor"><b>{porcento(l.percentual)}</b><span>consumido · {numero.format(l.restantes)} restantes de {numero.format(l.registros)}</span></p>
            <Barra percentual={l.percentual} tom={INFO[v].tom} />
        </section>}
        <dl className="alerta-secao alerta-fatos alerta-d-fatos">
            <div><dt>{l.transbordo ? 'Reciclagem automática' : 'Reciclagens hoje'}</dt><dd>{l.transbordo ? 'não disponível' : <><Vagas usadas={l.reciclagensHoje} limite={l.limiteReciclagens} />{l.reciclagensHoje} de {l.limiteReciclagens}</>}</dd></div>
            {l.mailingId != null && l.situacao !== 'ESGOTADO' && !l.parado && <div><dt>Previsão</dt><dd>{previsao(l)}</dd>
                {l.ritmoPorMinuto ? <small>~{numero.format(Math.round(l.ritmoPorMinuto))} registros/min</small> : l.ritmoPorMinuto === 0 ? <small>a lista não foi consumida desde a última leitura</small> : <small>precisa de mais uma leitura</small>}</div>}
            {pendencia && <div><dt>Alerta desde</dt><dd>{pendencia.hora}</dd></div>}
        </dl>
        <section className="alerta-secao alerta-botoes alerta-d-botoes">
            <button disabled={busy} onClick={onServico}>{l.parado ? 'Iniciar serviço' : 'Parar serviço'}</button>
            {pendencia && <button disabled={busy} onClick={() => onDispensar(pendencia.id)}>Dispensar alerta</button>}
            {pendencia && <p className="alerta-hint">Some daqui e da central de notificações. Volta se a situação piorar.</p>}
        </section>
        {eventos.length > 0 && <section className="alerta-secao alerta-d-hoje"><h3>Hoje neste serviço</h3><LinhaDoTempo eventos={eventos} comServico={false} /></section>}
    </div>;
}

function Vagas({ usadas, limite }: { usadas: number; limite: number }) {
    return <span className="alerta-vagas" aria-hidden="true">{Array.from({ length: limite }, (_, i) => <i key={i} className={i < usadas ? 'usada' : undefined} />)}</span>;
}

function LinhaDoTempo({ eventos, comServico }: { eventos: Evento[]; comServico: boolean }) {
    return <ol className="alerta-tempo">{eventos.map(e => <li key={e.id} data-status={e.status} data-tone={TOM_EVENTO[e.tipo] || 'none'}>
        <span className="alerta-tempo-hora">{e.hora}</span>
        <div><strong>{TITULO_EVENTO[e.tipo] || e.tipo}</strong>{comServico && e.servico && <span className="alerta-tempo-servico">{nomeCurto(e.servico)}</span>}<p>{e.texto}</p>
            {e.status !== 'aberta' && <small>{e.status === 'dispensada' ? 'Dispensado' : `Resolvido: ${e.motivo}`}{e.horaFim ? ` às ${e.horaFim}` : ''}</small>}</div>
    </li>)}</ol>;
}

function Historico({ eventos, busy, onDispensarTodas, onLimpar }: { eventos: Evento[]; busy: boolean; onDispensarTodas(): void; onLimpar(): void }) {
    const abertas = eventos.filter(e => e.status === 'aberta').length, fechadas = eventos.length - abertas;
    return <section className="alerta-historico" aria-label="Alertas de hoje">
        {(abertas > 0 || fechadas > 0) && <div className="alerta-botoes">{abertas > 0 && <button disabled={busy} onClick={onDispensarTodas}>Dispensar todas ({abertas})</button>}
            {fechadas > 0 && <button disabled={busy} onClick={onLimpar}>Limpar histórico</button>}</div>}
        {eventos.length ? <LinhaDoTempo eventos={eventos} comServico /> : <p className="alerta-vazio">Nenhum alerta hoje.</p>}
        {fechadas > 0 && <p className="alerta-hint">Limpar tira daqui os alertas já resolvidos ou dispensados; os que ainda precisam de ação continuam.</p>}
    </section>;
}

// Ícones de traço do mural, um por situação.
const TRACOS: Record<Visual | 'proxima' | 'fechar', ReactNode> = {
    TROCAR: <><path d="M7 4 3 8l4 4" /><path d="M3 8h14" /><path d="m17 20 4-4-4-4" /><path d="M21 16H7" /></>,
    ESGOTADO: <><path d="M21 12a9 9 0 0 1-15.4 6.4" /><path d="M3 12A9 9 0 0 1 18.4 5.6" /><path d="M18.5 2v3.8h-3.8" /><path d="M5.5 22v-3.8h3.8" /></>,
    QUASE: <><circle cx="12" cy="12" r="9" /><path d="M12 7v5l3 2" /></>,
    SEM_MAILING: <><path d="M9 17H7A5 5 0 0 1 7 7h2" /><path d="M15 7h2a5 5 0 0 1 4 8" /><path d="M8 12h4" /><path d="m3 3 18 18" /></>,
    SERVICO_PARADO: <><rect x="6.5" y="5" width="3.5" height="14" rx="1" /><rect x="14" y="5" width="3.5" height="14" rx="1" /></>,
    OK: <><circle cx="12" cy="12" r="9" /><path d="m8.5 12.5 2.5 2.5 5-6" /></>,
    proxima: <><path d="M5 5.5v13a1 1 0 0 0 1.6.8l8.6-6.5a1 1 0 0 0 0-1.6L6.6 4.7A1 1 0 0 0 5 5.5z" /><path d="M19 5v14" /></>,
    fechar: <path d="M18 6 6 18M6 6l12 12" />,
};
function Icone({ nome }: { nome: keyof typeof TRACOS }) {
    return <svg className="alerta-icone" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">{TRACOS[nome]}</svg>;
}

// Mesma chave da Limpeza Local (estilos globais .toggle-switch em src/styles/index.css).
function Switch({ label, hint, checked, disabled, onChange }: { label: string; hint: string; checked: boolean; disabled: boolean; onChange: (value: boolean) => void }) {
    return <label className="toggle-switch alerta-switch" title={hint}>
        <span className="switch"><input type="checkbox" checked={checked} disabled={disabled} aria-describedby="alerta-switch-hint" onChange={event => onChange(event.target.checked)} /><span className="slider" /></span>
        <span className="toggle-label">{label}<span id="alerta-switch-hint" className="alerta-sr">{hint}</span></span>
    </label>;
}
