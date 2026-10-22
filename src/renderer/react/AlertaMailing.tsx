import { useEffect, useRef, useState } from 'react';
import type { ReactNode } from 'react';
import './fluxos.css';
import './alertaMailing.css';
import { CheckIcon, KeyIcon, SpinnerIcon } from './icons';

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

const INFO: Record<Visual, { rotulo: string; verbo: string; tom: Tone }> = {
    TROCAR: { rotulo: 'Trocar lista', verbo: 'Trocar a lista', tom: 'stop' }, ESGOTADO: { rotulo: 'Esgotado', verbo: 'Reciclar agora', tom: 'stop' },
    QUASE: { rotulo: 'Acabando', verbo: 'Reciclar em breve', tom: 'warn' }, SEM_MAILING: { rotulo: 'Sem mailing', verbo: 'Verificar o serviço', tom: 'stop' },
    SERVICO_PARADO: { rotulo: 'Parado', verbo: 'Serviço parado', tom: 'none' }, OK: { rotulo: 'OK', verbo: 'Nada a fazer', tom: 'ok' },
};
// Serviço de transbordo (LTDA, PT 2): nunca recicla, o alerta pede para seguir para a próxima lista.
const PROXIMA: Partial<Record<Linha['situacao'], string>> = { QUASE: 'Próxima lista em breve', ESGOTADO: 'Iniciar a próxima lista' };
const TITULO_EVENTO: Record<string, string> = { QUASE: 'Mailing em 99%', ESGOTADO: 'Mailing esgotado', SEM_MAILING: 'Sem mailing ativo', TROCAR: 'Trocar a lista', FALHA: 'Fastway sem resposta',
    RECICLADA: 'Reciclada automaticamente', NADA_RECICLAR: 'Nada para reciclar', FALHA_RECICLAGEM: 'Falha ao reciclar', PARADO: 'Serviço parado automaticamente', FALHA_PARADA: 'Falha ao parar o serviço' };
const SEM_RECICLAGEM = 'LTDA e PT2 (transbordo) nunca são reciclados nem parados automaticamente: seguem para a próxima lista e só param pelo botão do painel.';

const numero = new Intl.NumberFormat('pt-BR'), decimal = new Intl.NumberFormat('pt-BR', { minimumFractionDigits: 1, maximumFractionDigits: 1 });
const porcento = (p: number) => `${decimal.format(Math.floor(p * 10) / 10)}%`; // 99,97 mostra 99,9% e não 100%
const visual = (l: Linha): Visual => (l.parado ? (l.situacao === 'ESGOTADO' ? 'TROCAR' : 'SERVICO_PARADO') : l.acao === 'TROCAR' ? 'TROCAR' : l.situacao);
const emAlerta = (l: Linha) => l.acao !== 'NADA' && visual(l) !== 'SERVICO_PARADO';
const verbo = (l: Linha) => (l.acao === 'PROXIMA_LISTA' && visual(l) !== 'SERVICO_PARADO' && PROXIMA[l.situacao]) || INFO[visual(l)].verbo;
const pendenciaDe = (painel: Painel, l: Linha) => painel.eventos.find(e => e.servicoId === l.servicoId && e.status === 'aberta');
const horaDe = (iso: string | null) => (iso ? new Date(iso).toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit' }) : '');

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

export function AlertaMailing() {
    const api = window.electronAPI as unknown as API;
    const [painel, setPainel] = useState<Painel | null>(null), [selecionado, setSelecionado] = useState<number | null>(null), [aba, setAba] = useState<'servicos' | 'historico'>('servicos');
    const [mensagem, setMensagem] = useState(''), [busy, setBusy] = useState(false);
    const locked = useRef(false);
    const receber = (dados: Painel) => { setPainel(dados); if (dados.foco != null) { setSelecionado(dados.foco); setAba('servicos'); } };
    useEffect(() => {
        let mounted = true;
        const carregar = () => api.alertaMailingState().then(r => { if (mounted && r.success && r.painel) receber(r.painel); else if (mounted && !r.success) setMensagem(r.message || 'Alerta indisponível.'); }).catch(() => {});
        void carregar();
        const offUpdate = api.onAlertaMailingUpdate(dados => { if (mounted) receber(dados); });
        // Clique na notificação: abre esta aba já no serviço que avisou.
        const offFoco = api.onAlertaMailingFoco(({ servicoId }) => {
            if (!mounted) return;
            (document.querySelector('.tab-button[data-tab-name="alertaMailing"]') as HTMLElement | null)?.click();
            if (servicoId != null) setSelecionado(servicoId);
            setAba('servicos');
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
    const alertas = painel.linhas.filter(emAlerta);
    const atual = painel.linhas.find(l => l.servicoId === selecionado) || alertas[0] || null;
    const historico = painel.eventos.filter(e => !e.oculto);
    return <div className="flows-app alerta-app" aria-busy={busy}>
        <header className="flows-heading"><div><h1>Alerta de Mailing</h1><p>Confere o mailing ativo de cada serviço da Fastway e avisa quando a lista está acabando, esgotou ou o serviço ficou sem lista.</p></div>
            {painel.configurado && <Status painel={painel} busy={busy} onVerificar={() => void acao(() => api.alertaMailingVerificar())} />}</header>
        {mensagem && <p role="alert" className="flow-notice flow-notice--error">{mensagem}</p>}
        {!painel.configurado ? <LoginFastway painel={painel} busy={busy} onSalvar={(login, senha) => void acao(() => api.alertaMailingLogin(login, senha))} /> : <>
            {!painel.conectado && painel.erro && <p role="alert" className="flow-notice flow-notice--error">{painel.erro}</p>}
            <div className="alerta-toolbar">
                <Switch label="Reciclar e parar automaticamente neste PC" hint={`Só um PC deve ficar com a chave ligada por vez. ${SEM_RECICLAGEM}`} checked={painel.reciclador} disabled={busy}
                    onChange={ligar => void acao(() => api.alertaMailingReciclador(ligar))} />
                <div className="flow-view-switch" aria-label="Visão do alerta">
                    <button aria-pressed={aba === 'servicos'} onClick={() => setAba('servicos')}>Serviços</button>
                    <button aria-pressed={aba === 'historico'} onClick={() => setAba('historico')}>Histórico{historico.length ? ` (${historico.length})` : ''}</button>
                </div>
            </div>
            {aba === 'servicos' ? <div className="alerta-layout">
                <ListaServicos linhas={painel.linhas} alertas={alertas.length} selecionado={atual?.servicoId ?? null} onSelecionar={setSelecionado} />
                <aside className="alerta-panel" aria-label="Detalhes do serviço">
                    {atual ? <Detalhe linha={atual} painel={painel} busy={busy} onServico={() => void mudarServico(atual)} onDispensar={id => void acao(() => api.alertaMailingDispensar(id))} />
                        : <TudoCerto total={painel.linhas.length} />}
                </aside>
            </div> : <Historico eventos={historico} busy={busy} onDispensarTodas={() => void acao(() => api.alertaMailingDispensar(null))} onLimpar={() => void acao(() => api.alertaMailingLimparHistorico())} />}
        </>}
    </div>;
}

function Status({ painel, busy, onVerificar }: { painel: Painel; busy: boolean; onVerificar(): void }) {
    const texto = painel.verificando ? 'Verificando a Fastway…' : !painel.conectado ? 'Sem conexão com a Fastway'
        : painel.verificadoEm ? `Verificado às ${horaDe(painel.verificadoEm)} · a cada ${painel.intervaloMinutos ?? 3} min` : 'Aguardando a primeira verificação';
    return <div className="alerta-status" data-tone={!painel.conectado ? 'stop' : 'ok'}>
        <span role="status">{painel.verificando ? <SpinnerIcon /> : <span className="alerta-dot" aria-hidden="true" />}{texto}</span>
        <button disabled={busy || painel.verificando} onClick={onVerificar}>Verificar agora</button>
    </div>;
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

function ListaServicos({ linhas, alertas, selecionado, onSelecionar }: { linhas: Linha[]; alertas: number; selecionado: number | null; onSelecionar(id: number): void }) {
    return <section className="alerta-lista" aria-labelledby="alerta-lista-titulo">
        <header className="alerta-lista-heading"><h2 id="alerta-lista-titulo">Serviços</h2><span>{alertas ? `${alertas} em alerta · ` : ''}{linhas.length} serviços</span></header>
        {!linhas.length && <p className="alerta-vazio">Os serviços aparecem aqui assim que estiverem em execução na Fastway.</p>}
        <ul>{linhas.map(l => <li key={l.servicoId}>
            <button className="alerta-item" data-tone={INFO[visual(l)].tom} aria-pressed={l.servicoId === selecionado} onClick={() => onSelecionar(l.servicoId)}>
                <span className="alerta-item-nome"><span className="alerta-dot" aria-hidden="true" /><strong>{l.servico}</strong><small>{INFO[visual(l)].rotulo}</small></span>
                <span className="alerta-item-lista">{l.mailingId ? l.arquivo : 'Nenhuma lista associada'}{previsao(l) ? ` · ${previsao(l)}` : ''}</span>
                {l.mailingId != null && <span className="alerta-item-consumo"><Barra percentual={l.percentual} /><b>{porcento(l.percentual)}</b></span>}
            </button>
        </li>)}</ul>
    </section>;
}

function Barra({ percentual }: { percentual: number }) {
    return <span className="alerta-barra" role="img" aria-label={`${porcento(percentual)} consumido`}>
        <span style={{ width: `${Math.min(100, Math.max(0, percentual))}%` }} /><i title="Limite de alerta: 99%" />
    </span>;
}

function Detalhe({ linha: l, painel, busy, onServico, onDispensar }: { linha: Linha; painel: Painel; busy: boolean; onServico(): void; onDispensar(id: string): void }) {
    const v = visual(l), pendencia = pendenciaDe(painel, l), eventos = painel.eventos.filter(e => e.servicoId === l.servicoId && !e.oculto);
    return <div className="alerta-detalhe" key={l.servicoId}>
        <section className="alerta-secao">
            <h2>{l.servico} <small>#{l.servicoId}</small></h2>
            <p className="alerta-hint">{l.mailingId ? `${l.arquivo} · mailing #${l.mailingId}` : 'Nenhuma lista associada'}</p>
            {(l.transbordo || l.parado) && <p className="alerta-tags">{l.transbordo && <span>Transbordo · sem reciclagem automática</span>}{l.parado && <span>Serviço parado na Fastway</span>}</p>}
        </section>
        <section className="alerta-secao alerta-acao" data-tone={INFO[v].tom}>
            <strong>{verbo(l)}</strong>
            <p>{explicacao(l, painel.reciclador)}</p>
        </section>
        {l.mailingId != null && <section className="alerta-secao">
            <h3>Consumo do mailing</h3>
            <p className="alerta-medidor"><b>{porcento(l.percentual)}</b><span>consumido · {numero.format(l.restantes)} restantes de {numero.format(l.registros)}</span></p>
            <Barra percentual={l.percentual} />
        </section>}
        <dl className="alerta-secao alerta-fatos">
            <div><dt>{l.transbordo ? 'Reciclagem automática' : 'Reciclagens hoje'}</dt><dd>{l.transbordo ? 'não disponível' : <><Vagas usadas={l.reciclagensHoje} limite={l.limiteReciclagens} />{l.reciclagensHoje} de {l.limiteReciclagens}</>}</dd></div>
            {l.mailingId != null && l.situacao !== 'ESGOTADO' && !l.parado && <div><dt>Previsão</dt><dd>{previsao(l)}</dd>
                {l.ritmoPorMinuto ? <small>~{numero.format(Math.round(l.ritmoPorMinuto))} registros/min</small> : l.ritmoPorMinuto === 0 ? <small>a lista não foi consumida desde a última leitura</small> : <small>precisa de mais uma leitura</small>}</div>}
            {pendencia && <div><dt>Alerta desde</dt><dd>{pendencia.hora}</dd></div>}
        </dl>
        <section className="alerta-secao alerta-botoes">
            <button disabled={busy} onClick={onServico}>{l.parado ? 'Iniciar serviço' : 'Parar serviço'}</button>
            {pendencia && <button disabled={busy} onClick={() => onDispensar(pendencia.id)}>Dispensar alerta</button>}
            {pendencia && <p className="alerta-hint">Some daqui e da central de notificações. Volta se a situação piorar.</p>}
        </section>
        {eventos.length > 0 && <section className="alerta-secao"><h3>Hoje neste serviço</h3><LinhaDoTempo eventos={eventos} comServico={false} /></section>}
    </div>;
}

function Vagas({ usadas, limite }: { usadas: number; limite: number }) {
    return <span className="alerta-vagas" aria-hidden="true">{Array.from({ length: limite }, (_, i) => <i key={i} className={i < usadas ? 'usada' : undefined} />)}</span>;
}

function LinhaDoTempo({ eventos, comServico }: { eventos: Evento[]; comServico: boolean }) {
    return <ol className="alerta-tempo">{eventos.map(e => <li key={e.id} data-status={e.status}>
        <span className="alerta-tempo-hora">{e.hora}</span>
        <div><strong>{TITULO_EVENTO[e.tipo] || e.tipo}{comServico && e.servico ? ` · ${e.servico}` : ''}</strong><p>{e.texto}</p>
            {e.status !== 'aberta' && <small>{e.status === 'dispensada' ? 'Dispensado' : `Resolvido: ${e.motivo}`}{e.horaFim ? ` às ${e.horaFim}` : ''}</small>}</div>
    </li>)}</ol>;
}

function Historico({ eventos, busy, onDispensarTodas, onLimpar }: { eventos: Evento[]; busy: boolean; onDispensarTodas(): void; onLimpar(): void }) {
    const abertas = eventos.filter(e => e.status === 'aberta').length, fechadas = eventos.length - abertas;
    return <section className="alerta-historico" aria-labelledby="alerta-historico-titulo">
        <header className="alerta-lista-heading"><h2 id="alerta-historico-titulo">Alertas de hoje</h2>
            <div className="alerta-botoes">{abertas > 0 && <button disabled={busy} onClick={onDispensarTodas}>Dispensar todas ({abertas})</button>}
                {fechadas > 0 && <button disabled={busy} onClick={onLimpar}>Limpar histórico</button>}</div></header>
        {eventos.length ? <LinhaDoTempo eventos={eventos} comServico /> : <p className="alerta-vazio">Nenhum alerta hoje.</p>}
        {fechadas > 0 && <p className="alerta-hint">Limpar tira daqui os alertas já resolvidos ou dispensados; os que ainda precisam de ação continuam.</p>}
    </section>;
}

function TudoCerto({ total }: { total: number }) {
    return <div className="alerta-tudo-certo" data-tone="ok"><CheckIcon />
        <h2>{total ? 'Tudo certo por aqui.' : 'Nenhum serviço em execução'}</h2>
        <p className="alerta-hint">{total ? `Os ${total} serviços estão sem pendências. Quando uma lista precisar de atenção, o alerta aparece aqui. Selecione um serviço para acompanhar o consumo e as reciclagens.`
            : 'Os serviços aparecem aqui assim que estiverem em execução na Fastway. O monitoramento continua automaticamente.'}</p>
    </div>;
}

// Mesma chave da Limpeza Local (estilos globais .toggle-switch em src/styles/index.css).
function Switch({ label, hint, checked, disabled, onChange }: { label: string; hint: ReactNode; checked: boolean; disabled: boolean; onChange: (value: boolean) => void }) {
    return <label className="toggle-switch alerta-switch">
        <span className="switch"><input type="checkbox" checked={checked} disabled={disabled} onChange={event => onChange(event.target.checked)} /><span className="slider" /></span>
        <span className="toggle-label">{label}<small>{hint}</small></span>
    </label>;
}
