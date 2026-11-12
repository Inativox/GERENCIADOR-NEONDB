import { useEffect, useRef, useState } from 'react';
import type { ReactNode } from 'react';

/*
 * Alerta de Mailing dentro do Ritmo do Discador: o estado vem dos mesmos handlers alerta-mailing-* do main,
 * e cada peça aqui é desenhada no vocabulário rd- da mesa (cabeçalho, avisos, canal, gaveta e painel de baixo).
 */

// Mesmo formato de src/main/alertaMailing/monitor.js (linhas) e estado.js (eventos).
export type Linha = { servicoId: number; servico: string; mailingId: number | null; arquivo: string; registros: number; restantes: number; percentual: number;
    situacao: 'OK' | 'QUASE' | 'ESGOTADO' | 'SEM_MAILING'; acao: 'NADA' | 'PODE_RECICLAR' | 'TROCAR' | 'PROXIMA_LISTA' | 'VERIFICAR_SERVICO';
    reciclagensHoje: number; limiteReciclagens: number; minutosRestantes: number | null; ritmoPorMinuto: number | null; transbordo: boolean; parado: boolean };
type EventoMailing = { id: string; hora: string; tipo: string; servico: string; texto: string; servicoId: number | null; mailingId: number | null; status: 'aberta' | 'resolvida' | 'dispensada'; motivo: string; horaFim: string; oculto: boolean };
export type PainelMailing = { configurado: boolean; reciclador: boolean; foco: number | null; linhas: Linha[]; eventos: EventoMailing[];
    verificadoEm: string | null; conectado: boolean; erro: string | null; verificando?: boolean; intervaloMinutos?: number };
type RespMailing = { success: boolean; message?: string; painel?: PainelMailing };
type ApiMailing = {
    alertaMailingState(): Promise<RespMailing>; alertaMailingVerificar(): Promise<RespMailing>; alertaMailingDispensar(eventoId: string | null): Promise<RespMailing>;
    alertaMailingLimparHistorico(): Promise<RespMailing>; alertaMailingReciclador(ligar: boolean): Promise<RespMailing>; alertaMailingServico(servicoId: number, parar: boolean): Promise<RespMailing>;
    onAlertaMailingUpdate(callback: (painel: PainelMailing) => void): () => void; onAlertaMailingFoco(callback: (foco: { servicoId: number | null }) => void): () => void;
    showConfirmDialog(options: { title: string; message: string }): Promise<boolean>;
};
type Tom = 'ok' | 'warn' | 'stop' | 'none' | 'info';
type Visual = 'TROCAR' | 'ESGOTADO' | 'QUASE' | 'SEM_MAILING' | 'SERVICO_PARADO' | 'OK';
type NomeIcone = keyof typeof TRACOS;

const ABA = 'ritmoDiscador';
const TOM: Record<Visual, Tom> = { TROCAR: 'stop', ESGOTADO: 'stop', QUASE: 'warn', SEM_MAILING: 'stop', SERVICO_PARADO: 'none', OK: 'ok' };
const TITULO: Record<Visual, string> = { TROCAR: 'Trocar a lista', ESGOTADO: 'Mailing esgotado', QUASE: 'Mailing acabando', SEM_MAILING: 'Sem mailing ativo', SERVICO_PARADO: 'Serviço parado', OK: 'Mailing em dia' };
// Ordem dos avisos: o que pede ação primeiro.
const RANK: Record<Visual, number> = { TROCAR: 0, ESGOTADO: 1, SEM_MAILING: 2, QUASE: 3, OK: 5, SERVICO_PARADO: 6 };
const TITULO_EVENTO: Record<string, string> = { QUASE: 'Mailing em 99%', ESGOTADO: 'Mailing esgotado', SEM_MAILING: 'Sem mailing ativo', TROCAR: 'Trocar a lista', FALHA: 'Fastway sem resposta',
    RECICLADA: 'Reciclada automaticamente', NADA_RECICLAR: 'Nada para reciclar', FALHA_RECICLAGEM: 'Falha ao reciclar', PARADO: 'Serviço parado automaticamente', FALHA_PARADA: 'Falha ao parar o serviço' };
const TOM_EVENTO: Record<string, Tom> = { QUASE: 'warn', ESGOTADO: 'stop', SEM_MAILING: 'stop', TROCAR: 'stop', FALHA: 'stop', FALHA_RECICLAGEM: 'stop', FALHA_PARADA: 'stop', RECICLADA: 'ok', PARADO: 'none', NADA_RECICLAR: 'none' };
const ICONE_EVENTO: Record<string, NomeIcone> = { QUASE: 'relogio', ESGOTADO: 'alerta', SEM_MAILING: 'semLista', TROCAR: 'trocar', FALHA: 'alerta', FALHA_RECICLAGEM: 'alerta', FALHA_PARADA: 'alerta',
    RECICLADA: 'reciclar', PARADO: 'parar', NADA_RECICLAR: 'reciclar' };
const HINT_AUTO = 'Recicla sozinho as listas em 99% ou esgotadas, até o limite do dia. Só um PC deve ficar com a chave ligada. LTDA e PT2 (transbordo) nunca são reciclados nem parados automaticamente.';

const numero = new Intl.NumberFormat('pt-BR'), decimal = new Intl.NumberFormat('pt-BR', { minimumFractionDigits: 1, maximumFractionDigits: 1 });
const porcento = (p: number) => `${decimal.format(Math.floor(p * 10) / 10)}%`; // 99,97 mostra 99,9 e não 100
const visual = (l: Linha): Visual => (l.parado ? (l.situacao === 'ESGOTADO' ? 'TROCAR' : 'SERVICO_PARADO') : l.acao === 'TROCAR' ? 'TROCAR' : l.situacao);
export const tomDaLinha = (l: Linha) => TOM[visual(l)];
const emAlerta = (l: Linha) => RANK[visual(l)] < RANK.OK;
const visiveis = (m: PainelMailing) => m.eventos.filter(e => !e.oculto).sort((a, b) => b.hora.localeCompare(a.hora));
const pendenciaDe = (m: PainelMailing, servicoId: number) => m.eventos.find(e => e.servicoId === servicoId && e.status === 'aberta');

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
    if (l.parado) return 'serviço parado';
    if (l.situacao === 'ESGOTADO') return 'acabou';
    if (!l.mailingId) return '';
    if (l.minutosRestantes != null) return `acaba em ${acabaEm(l)}`;
    return l.ritmoPorMinuto === 0 ? 'sem consumo agora' : 'calculando previsão…';
}

/** Estado curto da lista no canal: o que vai acontecer com ela. */
function estadoCurto(l: Linha, reciclador: boolean) {
    const v = visual(l);
    if (v === 'OK' || v === 'SERVICO_PARADO') return previsao(l);
    if (v === 'SEM_MAILING') return 'Sem mailing ativo';
    if (v === 'TROCAR') return 'Trocar a lista';
    if (l.transbordo) return v === 'QUASE' ? `Próxima lista em breve · ${acabaEm(l)}` : 'Iniciar a próxima lista';
    const quem = reciclador ? 'Recicla sozinho' : 'Reciclar na Fastway';
    return v === 'QUASE' && acabaEm(l) ? `${quem} · ${acabaEm(l)}` : quem;
}

/** Uma linha de aviso: o que fazer, sem repetir o que o canal já mostra. */
function resumo(l: Linha, reciclador: boolean) {
    const v = visual(l), usadas = `${l.reciclagensHoje} de ${l.limiteReciclagens} reciclagens hoje`, prazo = acabaEm(l) ? `acaba em ${acabaEm(l)}` : '';
    const juntar = (...partes: string[]) => partes.filter(Boolean).join(' · ');
    if (v === 'SEM_MAILING') return 'O serviço está em execução sem lista ativa. Confira na Fastway.';
    if (v === 'TROCAR') return juntar(l.situacao === 'QUASE' ? prazo : '', `${usadas}: suba uma lista nova na Fastway`);
    if (l.transbordo) return v === 'QUASE' ? juntar(prazo, 'transbordo: inicie a próxima lista quando acabar') : 'Transbordo: inicie a próxima lista do mailing na Fastway.';
    return juntar(v === 'QUASE' ? prazo : '', reciclador ? `este PC recicla sozinho (${usadas})` : `Auto-reciclar desligado: recicle na Fastway (${usadas})`);
}

function explicacao(l: Linha, reciclador: boolean) {
    const usadas = `${l.reciclagensHoje} de ${l.limiteReciclagens}`, restam = `Restam ${numero.format(l.restantes)} registros${acabaEm(l) ? `, acaba em ${acabaEm(l)}` : ''}.`;
    if (l.parado && l.situacao === 'ESGOTADO') return l.transbordo ? 'A lista acabou e o serviço está parado. Este serviço é de transbordo: inicie a próxima lista do mailing e depois o serviço.'
        : 'A lista acabou e o serviço está parado. Suba uma lista nova na Fastway e depois inicie o serviço.';
    if (l.parado) return `O serviço está parado na Fastway.${l.mailingId ? ` Restam ${numero.format(l.restantes)} registros na lista.` : ''}`;
    if (l.acao === 'TROCAR') return `Esta lista já foi reciclada ${l.reciclagensHoje} vezes hoje, o limite do dia. Suba uma lista nova na Fastway.${reciclador && l.situacao === 'QUASE' ? ' Este PC para o serviço quando a lista acabar.' : ''}`;
    if (l.acao === 'PROXIMA_LISTA' && l.situacao === 'ESGOTADO') return 'A lista acabou. Este serviço é de transbordo e não é reciclado: inicie a próxima lista do mailing na Fastway.';
    if (l.acao === 'PROXIMA_LISTA') return `${restam} Este serviço é de transbordo e não é reciclado: quando acabar, inicie a próxima lista do mailing.`;
    if (reciclador && (l.situacao === 'ESGOTADO' || l.situacao === 'QUASE')) return `Auto-reciclar ligado: este PC recicla sozinho (${usadas} reciclagens usadas hoje). Se não reciclar em alguns minutos, confira o mailing de hoje.`;
    if (l.situacao === 'ESGOTADO') return `A lista acabou. Auto-reciclar desligado: recicle na Fastway (${usadas} reciclagens usadas hoje).`;
    if (l.situacao === 'QUASE') return `${restam} Auto-reciclar desligado: recicle na Fastway (${usadas} usadas hoje).`;
    if (l.situacao === 'SEM_MAILING') return l.mailingId ? 'O serviço está em execução, mas sem mailing ativo. Confira a lista do serviço na Fastway.'
        : 'O serviço está em execução e nenhuma lista foi encontrada para ele. Confira na Fastway.';
    return 'Consumo abaixo de 99%. Você recebe uma notificação quando chegar lá.';
}

/* ---------- Estado ---------- */

/**
 * Carrega o painel do mailing, recebe cada verificação e trata o clique na notificação (abre a aba do Ritmo no serviço).
 * As ações têm a própria trava: o mailing e o ritmo respondem com painéis diferentes.
 */
export function useMailing(onFoco: (servicoId: number) => void) {
    const api = window.electronAPI as unknown as ApiMailing;
    const [painel, setPainel] = useState<PainelMailing | null>(null), [busy, setBusy] = useState(false), [mensagem, setMensagem] = useState('');
    const locked = useRef(false), onFocoRef = useRef(onFoco);
    onFocoRef.current = onFoco;
    useEffect(() => {
        let mounted = true;
        if (typeof api.alertaMailingState !== 'function') return;
        const receber = (dados: PainelMailing) => { setPainel(dados); if (dados.foco != null) onFocoRef.current(dados.foco); };
        async function carregar() {
            try { const r = await api.alertaMailingState(); if (mounted && r.success && r.painel) receber(r.painel); }
            catch { /* a próxima verificação traz o painel */ }
        }
        void carregar();
        const offUpdate = api.onAlertaMailingUpdate(dados => { if (mounted) receber(dados); });
        const offFoco = api.onAlertaMailingFoco(({ servicoId }) => {
            if (!mounted) return;
            (document.querySelector(`.tab-button[data-tab-name="${ABA}"]`) as HTMLElement | null)?.click();
            if (servicoId != null) onFocoRef.current(servicoId);
        });
        const aoAbrirAba = (event: Event) => { if ((event as CustomEvent<{ tabId?: string }>).detail?.tabId === ABA) void carregar(); };
        document.addEventListener('tab-changed', aoAbrirAba);
        return () => { mounted = false; offUpdate(); offFoco(); document.removeEventListener('tab-changed', aoAbrirAba); };
    }, [api]);

    async function acao(run: () => Promise<RespMailing>) {
        if (locked.current) return;
        locked.current = true; setBusy(true); setMensagem('');
        try { const r = await run(); if (!r.success) throw new Error(r.message); if (r.painel) setPainel(r.painel); }
        catch (reason) { setMensagem(reason instanceof Error && reason.message ? reason.message : 'Não foi possível concluir. Tente novamente.'); }
        finally { locked.current = false; setBusy(false); }
    }

    async function parar(l: Linha) {
        const ok = await api.showConfirmDialog({ title: 'Parar serviço', message: `Parar ${l.servico} (#${l.servicoId}) na Fastway? A discagem dele para até alguém iniciar de novo.` });
        if (ok) await acao(() => api.alertaMailingServico(l.servicoId, true));
    }

    const linhaDe = (servicoId: number) => painel?.linhas.find(l => l.servicoId === servicoId);
    return {
        painel, busy, mensagem, linhaDe, parar,
        verificar: () => acao(() => api.alertaMailingVerificar()),
        reciclador: (ligar: boolean) => acao(() => api.alertaMailingReciclador(ligar)),
        dispensar: (eventoId: string | null) => acao(() => api.alertaMailingDispensar(eventoId)),
        limpar: () => acao(() => api.alertaMailingLimparHistorico()),
    };
}
export type Mailing = ReturnType<typeof useMailing>;

/* ---------- Cabeçalho ---------- */

function useAgora() {
    const [agora, setAgora] = useState(() => Date.now());
    useEffect(() => { const id = window.setInterval(() => setAgora(Date.now()), 1000); return () => window.clearInterval(id); }, []);
    return agora;
}
const minSeg = (s: number) => `${Math.floor(s / 60)}:${String(s % 60).padStart(2, '0')}`;
const horaCurta = (iso: string) => new Date(iso).toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit' });

function RelogioMailing({ m }: { m: PainelMailing }) {
    const agora = useAgora(), raio = 7, volta = 2 * Math.PI * raio, intervalo = (m.intervaloMinutos ?? 3) * 60;
    const restam = m.verificadoEm ? Math.min(intervalo, Math.max(0, Math.round((Date.parse(m.verificadoEm) + intervalo * 1000 - agora) / 1000))) : intervalo;
    const depois = m.verificando ? 'verificando a Fastway…' : !m.verificadoEm ? 'aguardando a primeira verificação' : restam ? <>próxima em <b>{minSeg(restam)}</b></> : 'próxima em instantes';
    return <span className="rd-leitura" role="status">
        <span className="rd-anel" aria-hidden="true"><svg viewBox="0 0 18 18"><circle className="rd-anel-fundo" cx="9" cy="9" r={raio} />
            <circle className="rd-anel-frente rd-anel-frente--mailing" cx="9" cy="9" r={raio} strokeDasharray={volta} strokeDashoffset={volta * (restam / intervalo)} /></svg></span>
        {m.verificadoEm && <>verificado às <b>{horaCurta(m.verificadoEm)}</b> · </>}{depois}
    </span>;
}

/** Grupo do mailing no cabeçalho: relógio da verificação, "Verificar" e a chave do Auto-reciclar. */
export function MailingCabecalho({ mailing, spinner }: { mailing: Mailing; spinner: ReactNode }) {
    const m = mailing.painel;
    if (!m?.configurado) return null;
    return <div className="rd-ctl-grupo rd-ctl-grupo--mailing" role="group" aria-label="Mailing">
        <span className="rd-ctl-rotulo"><IconeMailing nome="lista" />Mailing</span>
        <RelogioMailing m={m} />
        <button className="rd-btn rd-btn--sm" disabled={mailing.busy || m.verificando} onClick={() => void mailing.verificar()}>
            {m.verificando ? spinner : <IconeMailing nome="atualizar" />}{m.verificando ? 'Verificando…' : 'Verificar'}</button>
        <label className="toggle-switch rd-switch" title={HINT_AUTO}>
            <span className="switch"><input type="checkbox" checked={m.reciclador} disabled={mailing.busy} onChange={event => void mailing.reciclador(event.target.checked)} /><span className="slider" /></span>
            <span className="toggle-label">Auto-reciclar neste PC</span>
        </label>
    </div>;
}

/* ---------- Avisos ---------- */

/** Os alertas de mailing em aberto num bloco só, uma linha por serviço; mais a falha de conexão e o erro da última ação. */
export function MailingFaixas({ mailing }: { mailing: Mailing }) {
    const m = mailing.painel;
    if (!m?.configurado) return null;
    const abertas = m.linhas.filter(l => emAlerta(l) && pendenciaDe(m, l.servicoId)).sort((a, b) => RANK[visual(a)] - RANK[visual(b)]);
    const pior = abertas.some(l => tomDaLinha(l) === 'stop') ? 'stop' : 'warn';
    return <>
        {mailing.mensagem && <div className="rd-faixa" data-tone="stop" role="alert"><IconeMailing nome="alerta" /><p><b>{mailing.mensagem}</b></p></div>}
        {!m.conectado && m.erro && <div className="rd-faixa" data-tone="stop" role="alert"><IconeMailing nome="alerta" /><p><b>Mailing sem resposta da Fastway.</b><span> {m.erro}</span></p></div>}
        {abertas.length > 0 && <section className="rd-faixa rd-avisos" data-tone={pior} aria-label="Avisos de mailing">
            <ul>{abertas.map(l => {
                const v = visual(l), pendencia = pendenciaDe(m, l.servicoId);
                return <li key={l.servicoId} data-tone={TOM[v]} title={explicacao(l, m.reciclador)}>
                    <IconeMailing nome={ICONE_EVENTO[v] ?? 'alerta'} />
                    <p><b>{l.servico}</b><span className="rd-aviso-tit">{TITULO[v]}</span><span className="rd-aviso-txt">{resumo(l, m.reciclador)}</span></p>
                    {pendencia && <button className="rd-btn rd-btn--sm rd-btn--ghost" disabled={mailing.busy} onClick={() => void mailing.dispensar(pendencia.id)}>Dispensar</button>}
                </li>;
            })}</ul>
        </section>}
    </>;
}

/* ---------- Canal ---------- */

function Vagas({ usadas, limite }: { usadas: number; limite: number }) {
    return <span className="rd-vagas" title={`${usadas} de ${limite} reciclagens hoje`} aria-label={`${usadas} de ${limite} reciclagens hoje`}>
        {Array.from({ length: limite }, (_, i) => <i key={i} className={i < usadas ? 'u' : undefined} />)}</span>;
}

/** Lista, consumo, reciclagens e previsão no pé do canal, abaixo da próxima ação do ritmo. */
export function MailingCanal({ linha: l, reciclador }: { linha: Linha | undefined; reciclador: boolean }) {
    if (!l) return null;
    const tom = tomDaLinha(l);
    if (!l.mailingId) return <div className="rd-mail" data-tone={tom} title={explicacao(l, reciclador)}>
        <p className="rd-mail-estado"><IconeMailing nome="semLista" />Sem mailing ativo</p></div>;
    return <div className="rd-mail" data-tone={tom} title={explicacao(l, reciclador)}>
        <div className="rd-mail-cab"><span className="rd-mail-arq">{l.arquivo || `mailing #${l.mailingId}`}</span><b className="rd-mono">{porcento(l.percentual)}</b></div>
        <div className="rd-mail-barra" role="img" aria-label={`${porcento(l.percentual)} da lista consumido`}><i style={{ width: `${Math.min(100, l.percentual)}%` }} /></div>
        <div className="rd-mail-pe"><p className="rd-mail-estado">{estadoCurto(l, reciclador)}</p>{!l.transbordo && (l.reciclagensHoje > 0 || tom !== 'ok') && <Vagas usadas={l.reciclagensHoje} limite={l.limiteReciclagens} />}</div>
    </div>;
}

/** Canal de um serviço parado que só o mailing conhece (a mesa do ritmo mostra os em execução e os com regra). */
export function CanalSoMailing({ linha: l, reciclador }: { linha: Linha; reciclador: boolean }) {
    return <article className="rd-canal" data-estado="parado" aria-label={l.servico}>
        <div className="rd-canal-cab"><h2 className="rd-canal-nome">{l.servico}</h2><p className="rd-canal-meta">#{l.servicoId} · parado</p></div>
        <div className="rd-canal-nums"><div><b>—</b><span>ocupação</span></div><div><b>—</b><span>agressividade</span></div></div>
        <div className="rd-medidores rd-medidores--so-mailing"><div className="rd-tubo"><div className="rd-tubo-vazio">serviço parado na Fastway</div></div></div>
        <div className="rd-canal-pe">
            <p className="rd-acao" data-tone="none"><IconeMailing nome="parar" /><span>Serviço parado na Fastway. Sem regra, o ritmo não mexe.</span></p>
            <MailingCanal linha={l} reciclador={reciclador} />
        </div>
    </article>;
}

/* ---------- Gaveta ---------- */

function LinhaDoTempo({ eventos, comServico }: { eventos: EventoMailing[]; comServico: boolean }) {
    return <ol className="rd-hist rd-hist--mailing">{eventos.map(e => <li key={e.id} data-tone={TOM_EVENTO[e.tipo] || 'none'} data-status={e.status}>
        <time>{e.hora}</time><IconeMailing nome={ICONE_EVENTO[e.tipo] ?? 'alerta'} />
        <div><b>{TITULO_EVENTO[e.tipo] || e.tipo}{comServico && e.servico && <span className="rd-hist-servico">{e.servico}</span>}</b><p>{e.texto}</p>
            {e.status !== 'aberta' && <p className="rd-hist-fim">{e.status === 'dispensada' ? 'Dispensado' : `Resolvido: ${e.motivo}`}{e.horaFim ? ` às ${e.horaFim}` : ''}</p>}</div>
    </li>)}</ol>;
}

/** Seção "Mailing" da gaveta, logo depois de "Agora": consumo, previsão, reciclagens e as ações do serviço. */
export function MailingGaveta({ mailing, servicoId }: { mailing: Mailing; servicoId: number }) {
    const m = mailing.painel, l = mailing.linhaDe(servicoId);
    if (!m?.configurado || !l) return null;
    const tom = tomDaLinha(l), pendencia = pendenciaDe(m, servicoId);
    return <section className="rd-gsec"><h3 className="rd-sec">Mailing</h3>
        {l.mailingId ? <>
            <div className="rd-hbar" data-tone={tom} role="img" aria-label={`${porcento(l.percentual)} da lista consumido`}>
                <div className="rd-hbar-fill rd-hbar-fill--mail" style={{ width: `max(0px, calc(${Math.min(100, l.percentual)}% - 6px))` }} /></div>
            <p className="rd-hint rd-agora-txt"><b className="rd-mono">{porcento(l.percentual)}</b> consumido · {numero.format(l.restantes)} de {numero.format(l.registros)} restantes · <span className="rd-mono">{l.arquivo || `mailing #${l.mailingId}`}</span></p>
        </> : <div className="rd-hbar"><div className="rd-hbar-vazio" /></div>}
        <dl className="rd-kv">
            {l.mailingId != null && <><dt>Previsão</dt><dd>{previsao(l)}{l.ritmoPorMinuto && !l.parado && l.situacao !== 'ESGOTADO' ? <small> · ~{numero.format(Math.round(l.ritmoPorMinuto))} registros/min</small> : null}</dd></>}
            <dt>Reciclagens hoje</dt><dd>{l.transbordo ? 'transbordo, não recicla' : <><Vagas usadas={l.reciclagensHoje} limite={l.limiteReciclagens} />{l.reciclagensHoje} de {l.limiteReciclagens}</>}</dd>
            {pendencia && <><dt>Alerta desde</dt><dd>{pendencia.hora}</dd></>}
        </dl>
        <div className="rd-agora-acao">
            <p className="rd-acao" data-tone={tom}><IconeMailing nome={tom === 'ok' ? 'ok' : ICONE_EVENTO[visual(l)] ?? 'alerta'} /><span>{explicacao(l, m.reciclador)}</span></p>
            {(!l.parado || pendencia) && <div className="rd-btns">
                {!l.parado && <button className="rd-btn rd-btn--sm" disabled={mailing.busy} onClick={() => void mailing.parar(l)}><IconeMailing nome="parar" />Parar serviço</button>}
                {pendencia && <button className="rd-btn rd-btn--sm rd-btn--ghost" disabled={mailing.busy} onClick={() => void mailing.dispensar(pendencia.id)}>Dispensar alerta</button>}
            </div>}
        </div>
    </section>;
}

/** "Mailing de hoje" no fim da gaveta, junto de "Ajustes de hoje". */
export function MailingGavetaHoje({ mailing, servicoId }: { mailing: Mailing; servicoId: number }) {
    const m = mailing.painel;
    if (!m?.configurado || !mailing.linhaDe(servicoId)) return null;
    const eventos = visiveis(m).filter(e => e.servicoId === servicoId);
    return <section className="rd-gsec"><h3 className="rd-sec">Mailing de hoje</h3>
        {eventos.length ? <LinhaDoTempo eventos={eventos} comServico={false} /> : <p className="rd-vazio">Nenhum aviso de mailing hoje.</p>}</section>;
}

/* ---------- Painel de baixo ---------- */

/** "Mailing de hoje": os avisos do dia, com Dispensar todas e Limpar. Fica abaixo de "Ajustes de hoje". */
export function MailingHoje({ mailing }: { mailing: Mailing }) {
    const m = mailing.painel;
    if (!m?.configurado) return null;
    const eventos = visiveis(m), abertas = eventos.filter(e => e.status === 'aberta').length, fechadas = eventos.length - abertas;
    const resumoDia = eventos.length ? `${eventos.length} ${eventos.length === 1 ? 'aviso' : 'avisos'}${abertas ? ` · ${abertas} ${abertas === 1 ? 'pendente' : 'pendentes'}` : ''}` : 'nenhum aviso';
    return <section className="rd-painel"><div className="rd-painel-cab"><h3>Mailing de hoje</h3>
        <div className="rd-painel-acoes"><small>{resumoDia}</small>
            {abertas > 0 && <button className="rd-btn rd-btn--sm rd-btn--ghost" disabled={mailing.busy} onClick={() => void mailing.dispensar(null)}>Dispensar todas</button>}
            {fechadas > 0 && <button className="rd-btn rd-btn--sm rd-btn--ghost" disabled={mailing.busy} title="Tira daqui os avisos já resolvidos ou dispensados; os pendentes continuam." onClick={() => void mailing.limpar()}>Limpar</button>}</div></div>
        <div className="rd-painel-corpo rd-hist-rolagem">
            {eventos.length ? <LinhaDoTempo eventos={eventos} comServico /> : <p className="rd-vazio">Nenhum aviso de mailing hoje.</p>}
        </div>
    </section>;
}

/* ---------- Ícones (mesmo traço do Icone do Ritmo) ---------- */

const TRACOS = {
    alerta: <><path d="M10.3 3.9 1.8 18a2 2 0 0 0 1.7 3h17a2 2 0 0 0 1.7-3L13.7 3.9a2 2 0 0 0-3.4 0z" /><path d="M12 9v4M12 17h.01" /></>,
    lista: <><path d="M4 6h16M4 12h16M4 18h10" /></>,
    atualizar: <><path d="M20 11a8 8 0 0 0-14.6-4.5L4 8" /><path d="M4 3v5h5" /><path d="M4 13a8 8 0 0 0 14.6 4.5L20 16" /><path d="M20 21v-5h-5" /></>,
    relogio: <><circle cx="12" cy="12" r="9" /><path d="M12 7v5l3 2" /></>,
    semLista: <><circle cx="12" cy="12" r="9" /><path d="m5.6 5.6 12.8 12.8" /></>,
    trocar: <><path d="M7 4 3 8l4 4" /><path d="M3 8h14" /><path d="m17 20 4-4-4-4" /><path d="M21 16H7" /></>,
    reciclar: <><path d="M21 12a9 9 0 0 1-15.4 6.4" /><path d="M3 12A9 9 0 0 1 18.4 5.6" /><path d="M18.5 2v3.8h-3.8" /><path d="M5.5 22v-3.8h3.8" /></>,
    parar: <rect x="6" y="6" width="12" height="12" rx="1.5" />,
    ok: <><circle cx="12" cy="12" r="9" /><path d="m8.5 12.5 2.5 2.5 5-6" /></>,
};
function IconeMailing({ nome }: { nome: NomeIcone }) {
    return <svg className="rd-ic" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.9" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">{TRACOS[nome]}</svg>;
}
