import { useEffect, useMemo, useRef, useState } from 'react';
import type { MouseEvent as ReactMouseEvent, ReactNode } from 'react';
import './fluxos.css';
import './ritmoDiscador.css';
import { KeyIcon, SpinnerIcon } from './icons';
import { useMailing, MailingCabecalho, MailingFaixas, MailingCanal, CanalSoMailing, MailingGaveta, MailingGavetaHoje, MailingHoje, tomDaLinha } from './MailingDoRitmo';
import type { Mailing } from './MailingDoRitmo';

// Mesmo formato do painel montado em src/main/ritmoDiscador (ocupação = falando ÷ (falando + livres), como na Fastway).
type Resp = { success: boolean; message?: string; painel?: Painel };
type Regra = { desce: number; sobe: number; passo: number; minimo: number; maximo: number };
type Contagem = { falando: number; livres: number; pausa: number; outros: number; logados: number };
type Operador = { id: number; nome: string; estado: 'falando' | 'livre' | 'pausa' | 'outro'; detalhe: string; segundos: number };
type Decisao = { tipo: 'semregra' | 'parado' | 'pausado' | 'semgente' | 'mantem' | 'limite' | 'muitaPausa' | 'teto' | 'pendente' | 'ajustou' | 'confirmando' | 'falhou' | 'acimaDoMaximo'; dir?: -1 | 1; alvo?: number; de?: number; mensagem?: string; motivo?: 'maximo'; teto?: 'ura' | 'callcenter' };
type Pausado = { de: number; para: number; em: string };
type Ponto = { t: number; ocupacao: number | null; agressividade: number };
type Servico = { id: number; nome: string; modo: string; emExecucao: boolean; agressividade: number; limite: number; regra: Regra | null; pausado: Pausado | null;
    contagem: Contagem; ocupacao: number | null; decisao: Decisao; operadores: Operador[]; parceiros: number[]; serie: Ponto[] };
type Corte = 'minimo' | 'maximo' | 'tetoUra' | 'tetoCallcenter';
type Evento = { id: string; em: string; tipo: 'ajuste' | 'manual' | 'falha' | 'sistema'; servicoId: number | null; servico: string; modo?: string; de?: number; para?: number; dir?: -1 | 1;
    ocupacao?: number | null; contagem?: Contagem; faixa?: { sobe: number; desce: number; passo: number }; corte?: Corte | null; motivo?: 'maximo' | null; texto: string };
type Painel = { configurado: boolean; login: { saved: boolean; login: string | null; canSave: boolean }; admin: boolean; ativa: boolean;
    lidoEm: string | null; lendo: boolean; conectado: boolean; erro: string | null; intervaloSegundos: number; limitePausa: number; teto: number; tetoCallcenter: number; soma: number | null; servicos: Servico[]; eventos: Evento[] };
type API = {
    ritmoDiscadorState(): Promise<Resp>; ritmoDiscadorAssistir(ativo: boolean): Promise<Resp>; ritmoDiscadorLer(): Promise<Resp>;
    ritmoDiscadorLogin(login: string, password: string): Promise<Resp>; ritmoDiscadorAtiva(ligar: boolean): Promise<Resp>;
    ritmoDiscadorRegra(servicoId: number, regra: Regra | null): Promise<Resp>; ritmoDiscadorRetomar(servicoId: number): Promise<Resp>; ritmoDiscadorTeto(valor: number): Promise<Resp>;
    onRitmoDiscadorUpdate(callback: (painel: Painel) => void): () => void; showConfirmDialog(options: { title: string; message: string }): Promise<boolean>;
};
type Tom = 'ok' | 'warn' | 'stop' | 'none' | 'info';
type Campo = keyof Regra;
type Gaveta = { id: number; criar: boolean } | null;
type Validacao = { erro?: string; aviso?: string; campo?: Campo };

const ABA = 'ritmoDiscador';
const CAMPOS: Campo[] = ['desce', 'sobe', 'passo', 'minimo', 'maximo'];
// Mesmos critérios de src/main/ritmoDiscador/regras.js: URA divide o teto geral; call center ativo (POWER) tem teto por serviço.
const ehCallcenter = (modo: string) => modo.trim().toUpperCase() === 'POWER';
const SUBTITULO = 'Ajusta a agressividade de cada serviço pela ocupação dos operadores (em atendimento ÷ (em atendimento + livres), como no painel da Fastway) e acompanha o mailing de cada um.';
const disponiveis = (c: Contagem) => c.falando + c.livres;
// "7 de 9 falando (7 falando + 2 livres) · 2 em pausa e 3 em outro estado fora da conta", como no painel da Fastway.
const foraDaConta = (c: Contagem) => [c.pausa && `${c.pausa} em pausa`, c.outros && `${c.outros} em outro estado`].filter(Boolean).join(' e ');
const textoOcupacao = (c: Contagem) => `${c.falando} de ${disponiveis(c)} falando (${c.livres} ${c.livres === 1 ? 'livre' : 'livres'})${foraDaConta(c) ? ` · ${foraDaConta(c)} fora da conta` : ''}`;
const semOcupacao = (c: Contagem) => (c.logados ? 'ninguém disponível' : 'ninguém logado');
const PAUSA_LONGA = 600; // passou de 10 min em pausa: fica vermelho na lista
const numero = new Intl.NumberFormat('pt-BR');
const fmt = (v: number | undefined) => numero.format(v ?? 0);
const clamp = (v: number, a: number, b: number) => Math.min(b, Math.max(a, v));
const pct = (o: number | null | undefined) => (o == null ? '—' : `${Math.round(o * 100)}%`);
const doisDigitos = (n: number) => String(n).padStart(2, '0');
const hora = (ms: number, comSegundos = false) => { const d = new Date(ms); return `${doisDigitos(d.getHours())}:${doisDigitos(d.getMinutes())}${comSegundos ? `:${doisDigitos(d.getSeconds())}` : ''}`; };
const horaIso = (iso: string | null | undefined, comSegundos = false) => (iso ? hora(Date.parse(iso), comSegundos) : '');
function duracao(segundos: number) {
    const s = Math.max(0, Math.floor(segundos)), h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60);
    return h ? `${h}:${doisDigitos(m)}:${doisDigitos(s % 60)}` : `${m}:${doisDigitos(s % 60)}`;
}
const nomesDe = (painel: Painel, ids: number[]) => ids.map(id => painel.servicos.find(s => s.id === id)?.nome).filter(Boolean).join(', ');

function useAgora() {
    const [agora, setAgora] = useState(() => Date.now());
    useEffect(() => { const id = window.setInterval(() => setAgora(Date.now()), 1000); return () => window.clearInterval(id); }, []);
    return agora;
}

export function RitmoDiscador() {
    const api = window.electronAPI as unknown as API;
    const [painel, setPainel] = useState<Painel | null>(null), [recebidoEm, setRecebidoEm] = useState(() => Date.now());
    const [mensagem, setMensagem] = useState(''), [busy, setBusy] = useState(false);
    const [selecionado, setSelecionado] = useState<number | null>(null), [gaveta, setGaveta] = useState<Gaveta>(null);
    const [confirmarAtivacao, setConfirmarAtivacao] = useState(false);
    const locked = useRef(false);
    const receber = (dados: Painel) => { setPainel(dados); setRecebidoEm(Date.now()); };
    useSessao(api, receber, setMensagem);
    // Clique na notificação do mailing: abre a gaveta do serviço que avisou.
    const mailing = useMailing(id => { setSelecionado(id); setGaveta({ id, criar: false }); });

    async function acao(run: () => Promise<Resp>) {
        if (locked.current) return false;
        locked.current = true; setBusy(true); setMensagem('');
        try { const r = await run(); if (!r.success) throw new Error(r.message); if (r.painel) receber(r.painel); return true; }
        catch (reason) { setMensagem(reason instanceof Error && reason.message ? reason.message : 'Não foi possível concluir. Tente novamente.'); return false; }
        finally { locked.current = false; setBusy(false); }
    }

    async function salvarRegra(s: Servico, regra: Regra | null) {
        if (!regra && !await api.showConfirmDialog({ title: 'Remover regra', message: `Remover a regra de ${s.nome} (#${s.id})? A automação deixa de mexer neste serviço e a agressividade fica como está.` })) return false;
        return acao(() => api.ritmoDiscadorRegra(s.id, regra));
    }

    function fecharGaveta() {
        const id = gaveta?.id;
        setGaveta(null);
        window.requestAnimationFrame(() => (document.querySelector(`.rd-canal[data-id="${id}"]`) as HTMLElement | null)?.focus());
    }

    if (!painel) return mensagem ? <div className="flows-app rd-app"><p role="alert" className="flow-notice rd-erro">{mensagem}</p></div> : null;
    if (!painel.configurado) return <div className="flows-app rd-app" aria-busy={busy}>
        <header className="rd-topo"><div><h1 className="rd-titulo">Ritmo do Discador</h1><p className="rd-sub">{SUBTITULO}</p></div></header>
        {mensagem && <p role="alert" className="flow-notice rd-erro">{mensagem}</p>}
        <LoginFastway painel={painel} busy={busy} onSalvar={(login, senha) => void acao(() => api.ritmoDiscadorLogin(login, senha))} />
    </div>;

    const atual = painel.servicos.find(s => s.id === selecionado) ?? painel.servicos[0];
    const aberto = gaveta ? painel.servicos.find(s => s.id === gaveta.id) : undefined;
    const retomar = (s: Servico) => void acao(() => api.ritmoDiscadorRetomar(s.id));
    // Serviços parados que só o mailing conhece entram na mesa como canal parado.
    const soMailing = (mailing.painel?.configurado ? mailing.painel.linhas : []).filter(l => !painel.servicos.some(s => s.id === l.servicoId));
    return <div className="flows-app rd-app" aria-busy={busy}>
        <div className="rd-mesa-area">
            <Cabecalho painel={painel} busy={busy} onLer={() => void acao(() => api.ritmoDiscadorLer())}
                onAtiva={ligar => { if (ligar) setConfirmarAtivacao(true); else void acao(() => api.ritmoDiscadorAtiva(false)); }}
                onTeto={valor => acao(() => api.ritmoDiscadorTeto(valor))} mailing={mailing} />
            <Faixas painel={painel} busy={busy} mensagem={mensagem} onRetomar={retomar} />
            <MailingFaixas mailing={mailing} />
            {painel.servicos.length || soMailing.length ? <>
                <section className={`rd-mesa${painel.conectado ? '' : ' is-antigo'}`} aria-label="Serviços">
                    {painel.servicos.map(s => <Canal key={s.id} servico={s} painel={painel} atual={s.id === atual?.id} busy={busy}
                        onSelecionar={() => setSelecionado(s.id)} onRetomar={() => retomar(s)}
                        onAbrir={() => { setSelecionado(s.id); setGaveta({ id: s.id, criar: !s.regra && painel.admin }); }} mailing={mailing} />)}
                    {soMailing.map(l => <CanalSoMailing key={l.servicoId} linha={l} reciclador={!!mailing.painel?.reciclador} />)}
                </section>
                <div className="rd-baixo">
                    {atual && <section className="rd-painel"><div className="rd-painel-cab"><h3>{atual.nome} hoje</h3><small>clique num canal para trocar · passe o mouse no gráfico</small></div>
                        <div className="rd-painel-corpo"><Grafico servico={atual} painel={painel} /></div></section>}
                    <div className="rd-coluna">
                        <section className="rd-painel"><div className="rd-painel-cab"><h3>Ajustes de hoje</h3><small>{resumoEventos(painel.eventos)}</small></div>
                            <div className="rd-painel-corpo rd-hist-rolagem"><HistoricoPorCampanha painel={painel} /></div></section>
                        <MailingHoje mailing={mailing} />
                    </div>
                </div>
            </> : <p className="rd-vazio rd-vazio--mesa">Os serviços aparecem aqui assim que estiverem em execução na Fastway ou tiverem regra.</p>}
        </div>
        {aberto && <GavetaServico servico={aberto} painel={painel} criar={gaveta?.criar ?? false} busy={busy} recebidoEm={recebidoEm} onFechar={fecharGaveta}
            onRetomar={() => retomar(aberto)} onRegra={regra => salvarRegra(aberto, regra)} mailing={mailing} />}
        {confirmarAtivacao && <ConfirmarAtivacao painel={painel} busy={busy} onCancelar={() => setConfirmarAtivacao(false)}
            onConfirmar={() => { setConfirmarAtivacao(false); void acao(() => api.ritmoDiscadorAtiva(true)); }} />}
    </div>;
}

// Carrega o painel, recebe cada leitura e avisa o main se a aba está visível (ele só lê a cada 30 s enquanto alguém olha).
function useSessao(api: API, receber: (painel: Painel) => void, setMensagem: (texto: string) => void) {
    const receberRef = useRef(receber);
    receberRef.current = receber;
    useEffect(() => {
        let mounted = true, visivel = false;
        if (typeof api.ritmoDiscadorState !== 'function') { setMensagem('O Ritmo do Discador não está disponível nesta versão do app.'); return; }
        const aplicar = (r: Resp | undefined) => { if (mounted && r?.painel) receberRef.current(r.painel); };
        async function carregar() {
            try { const r = await api.ritmoDiscadorState(); if (!mounted) return; if (r.success) aplicar(r); else setMensagem(r.message || 'Ritmo do Discador indisponível.'); }
            catch { if (mounted) setMensagem('Ritmo do Discador indisponível.'); }
        }
        async function assistir(ativo: boolean) {
            if (ativo === visivel) return;
            visivel = ativo;
            try { aplicar(await api.ritmoDiscadorAssistir(ativo)); } catch { /* a próxima troca de aba tenta de novo */ }
        }
        void carregar();
        void assistir(!!document.getElementById(ABA)?.classList.contains('active'));
        const offUpdate = api.onRitmoDiscadorUpdate(dados => { if (mounted) receberRef.current(dados); });
        const aoTrocarAba = (event: Event) => { void assistir((event as CustomEvent<{ tabId?: string }>).detail?.tabId === ABA); };
        document.addEventListener('tab-changed', aoTrocarAba);
        return () => {
            mounted = false; offUpdate(); document.removeEventListener('tab-changed', aoTrocarAba);
            api.ritmoDiscadorAssistir(false).catch(() => {});
        };
    }, [api, setMensagem]);
}

function resumoEventos(eventos: Evento[]) {
    const ajustes = eventos.filter(e => e.tipo === 'ajuste'), campanhas = new Set(ajustes.map(e => e.servicoId)).size;
    return `${ajustes.length} ${ajustes.length === 1 ? 'ajuste' : 'ajustes'} na Fastway${campanhas ? ` em ${campanhas} ${campanhas === 1 ? 'campanha' : 'campanhas'}` : ''}`;
}

/* ---------- Cabeçalho, relógio e faixas de aviso ---------- */

function Cabecalho({ painel, busy, onLer, onAtiva, onTeto, mailing }: { painel: Painel; busy: boolean; onLer(): void; onAtiva(ligar: boolean): void; onTeto(valor: number): Promise<boolean>; mailing: Mailing }) {
    const admin = painel.admin;
    return <header className="rd-topo">
        <div><h1 className="rd-titulo">Ritmo do Discador</h1>
            <p className="rd-sub">{SUBTITULO}</p></div>
        <div className="rd-ctl">
            <div className="rd-ctl-grupo" role="group" aria-label="Ritmo">
                <Relogio painel={painel} />
                <Teto painel={painel} busy={busy} onSalvar={onTeto} />
                <button className="rd-btn rd-btn--sm" disabled={busy || painel.lendo} onClick={onLer}>
                    {painel.lendo ? <SpinnerIcon /> : <Icone nome="refresh" />}{painel.lendo ? 'Lendo…' : 'Ler agora'}</button>
                <Chave rotulo="Ativa neste PC" dica={admin ? 'Só um PC deve ficar ativo' : 'Só admin liga ou desliga'} ligado={painel.ativa} desabilitado={!admin || busy} onMudar={onAtiva} />
            </div>
            <MailingCabecalho mailing={mailing} spinner={<SpinnerIcon />} />
        </div>
    </header>;
}

// Soma da agressividade de todos os serviços em execução contra o teto geral: a automação nunca sobe acima dele.
function Teto({ painel, busy, onSalvar }: { painel: Painel; busy: boolean; onSalvar(valor: number): Promise<boolean> }) {
    const [editando, setEditando] = useState(false), [valor, setValor] = useState(String(painel.teto));
    const valido = /^d+$/.test(valor) && Number(valor) >= 1;
    if (editando) return <form className="rd-teto" onSubmit={async event => { event.preventDefault(); if (valido && await onSalvar(Number(valor))) setEditando(false); }}>
        <label className="rd-teto-campo"><span>Teto das URAs</span><input type="number" inputMode="numeric" min={1} step={1} value={valor} autoFocus disabled={busy}
            aria-label="Teto da agressividade somada das URAs" onChange={event => setValor(event.target.value)}
            onKeyDown={event => { if (event.key === 'Escape') { event.stopPropagation(); setEditando(false); } }} /></label>
        <button className="rd-btn rd-btn--sm rd-btn--primary" disabled={busy || !valido}>Salvar</button>
        <button type="button" className="rd-btn rd-btn--sm rd-btn--ghost" onClick={() => setEditando(false)}>Cancelar</button>
    </form>;
    return <span className="rd-teto" data-acima={painel.soma != null && painel.soma > painel.teto ? '' : undefined}
        title={`Soma da agressividade das URAs em execução, com ou sem regra: a automação nunca sobe uma URA acima do teto. Call center ativo (POWER) vai no máximo até ${fmt(painel.tetoCallcenter)} por serviço.`}>
        URAs <b>{painel.soma == null ? '—' : fmt(painel.soma)}</b> de <b>{fmt(painel.teto)}</b>
        {painel.admin && <button className="rd-btn rd-btn--sm rd-btn--ghost" disabled={busy} onClick={() => { setValor(String(painel.teto)); setEditando(true); }}>Mudar teto</button>}
    </span>;
}

function Relogio({ painel }: { painel: Painel }) {
    const agora = useAgora(), raio = 7, volta = 2 * Math.PI * raio, intervalo = Math.max(1, painel.intervaloSegundos || 30);
    const restam = painel.lidoEm ? clamp(Math.round((Date.parse(painel.lidoEm) + intervalo * 1000 - agora) / 1000), 0, intervalo) : intervalo;
    const depois = painel.lendo ? 'lendo a Fastway…' : !painel.lidoEm ? 'aguardando a primeira leitura' : restam ? <>próxima em <b>{duracao(restam)}</b></> : 'próxima em instantes';
    return <span className="rd-leitura" role="status">
        <span className="rd-anel" aria-hidden="true"><svg viewBox="0 0 18 18"><circle className="rd-anel-fundo" cx="9" cy="9" r={raio} />
            <circle className="rd-anel-frente" cx="9" cy="9" r={raio} strokeDasharray={volta} strokeDashoffset={volta * (restam / intervalo)} /></svg></span>
        {painel.lidoEm && <>lida às <b>{horaIso(painel.lidoEm, true)}</b> · </>}{depois}
    </span>;
}

function Faixas({ painel, busy, mensagem, onRetomar }: { painel: Painel; busy: boolean; mensagem: string; onRetomar(s: Servico): void }) {
    const pausados = painel.servicos.filter(s => s.pausado);
    return <>
        {mensagem && <Faixa tom="stop" icone="alert" titulo={mensagem} papel="alert" />}
        {!painel.conectado && painel.erro && <Faixa tom="stop" icone="alert" titulo="Sem conexão com a Fastway." texto={painel.erro} papel="alert" />}
        {!painel.ativa && <Faixa tom="none" icone="power" titulo="Automação desligada neste PC." texto="As regras ficam salvas. A agressividade fica como está." />}
        {painel.soma != null && painel.soma > painel.teto && <Faixa tom="warn" icone="alert" titulo={`A soma das URAs (${fmt(painel.soma)}) passou do teto de ${fmt(painel.teto)}.`}
            texto="Alguém subiu direto na Fastway. A automação não sobe nenhuma URA até a soma voltar para baixo do teto; descer continua normal. Call center ativo segue o teto de 99 por serviço." />}
        {pausados.map(s => <Faixa key={s.id} tom="warn" icone="alert" titulo={`${s.nome} mudou na Fastway`}
            texto={`de ${fmt(s.pausado?.de)} para ${fmt(s.pausado?.para)} às ${horaIso(s.pausado?.em)}, sem ter sido a automação. Pausei esse serviço até alguém retomar.`}>
            {painel.admin && <button className="rd-btn rd-btn--sm" disabled={busy} onClick={() => onRetomar(s)}><Icone nome="play" />Retomar</button>}
        </Faixa>)}
    </>;
}

function Faixa({ tom, icone, titulo, texto, papel, children }: { tom: Tom; icone: NomeIcone; titulo: string; texto?: string; papel?: 'alert'; children?: ReactNode }) {
    return <div className="rd-faixa" data-tone={tom} role={papel}><Icone nome={icone} /><p><b>{titulo}</b>{texto && <span> {texto}</span>}</p>{children}</div>;
}

/* ---------- Canal (uma faixa da mesa por serviço) ---------- */

function Canal({ servico: s, painel, atual, busy, onSelecionar, onAbrir, onRetomar, mailing }: { servico: Servico; painel: Painel; atual: boolean; busy: boolean; onSelecionar(): void; onAbrir(): void; onRetomar(): void; mailing: Mailing }) {
    const linha = mailing.linhaDe(s.id), tomMailing = linha ? tomDaLinha(linha) : null;
    const estado = s.pausado ? 'pausado' : !s.emExecucao ? 'parado' : s.regra ? '' : 'semregra', c = s.contagem;
    const botao = s.regra ? 'Regra e operadores' : painel.admin ? 'Criar regra' : 'Ver operadores';
    return <article className="rd-canal" data-id={s.id} data-estado={estado || undefined} data-mail={tomMailing === 'warn' || tomMailing === 'stop' ? tomMailing : undefined} aria-current={atual} aria-label={s.nome} tabIndex={0}
        onClick={onSelecionar} onKeyDown={event => { if (event.target === event.currentTarget && (event.key === 'Enter' || event.key === ' ')) { event.preventDefault(); onSelecionar(); } }}>
        <div className="rd-canal-cab"><h2 className="rd-canal-nome">{s.nome}</h2><p className="rd-canal-meta">#{s.id} · {s.modo}{s.emExecucao ? '' : ' · parado'}</p></div>
        <div className="rd-canal-nums">
            <div><b>{s.ocupacao == null ? '—' : Math.round(s.ocupacao * 100)}{s.ocupacao != null && <small>%</small>}</b><span>ocupação</span></div>
            <div><b key={s.decisao.tipo === 'ajustou' ? painel.lidoEm ?? '' : 'fixo'} className={s.decisao.tipo === 'ajustou' ? 'rd-flash' : undefined}>{fmt(s.agressividade)}</b><span>agressividade</span></div>
        </div>
        <div className="rd-medidores">
            <Tubo servico={s} />
            <Fader servico={s} />
        </div>
        <div className="rd-canal-pe">
            <Contagens c={c} />
            {s.pausado ? <div className="rd-canal-pausa"><span><b>Pausado.</b> Mudou na Fastway de {fmt(s.pausado.de)} para {fmt(s.pausado.para)} às {horaIso(s.pausado.em)}.</span>
                {painel.admin && <button className="rd-btn rd-btn--sm" disabled={busy} onClick={event => { event.stopPropagation(); onRetomar(); }}><Icone nome="play" />Retomar</button>}</div>
                : <Proxima servico={s} painel={painel} />}
            <MailingCanal linha={linha} reciclador={!!mailing.painel?.reciclador} />
            <button className="rd-btn rd-btn--sm rd-btn--ghost rd-canal-abrir" onClick={event => { event.stopPropagation(); onAbrir(); }}>{botao}<Icone nome="chevron" /></button>
        </div>
    </article>;
}

function Tubo({ servico: s }: { servico: Servico }) {
    const c = s.contagem, r = s.regra;
    if (!disponiveis(c) || s.ocupacao == null) return <div className="rd-tubo" title={semOcupacao(c)}><div className="rd-tubo-vazio">{semOcupacao(c)}</div></div>;
    return <div className="rd-tubo" title={textoOcupacao(c)}>
        {r && <><div className="rd-tubo-z rd-tubo-z--desce" style={{ height: `${100 - r.desce}%` }} /><div className="rd-tubo-z rd-tubo-z--sobe" style={{ height: `${r.sobe}%` }} /></>}
        <div className="rd-tubo-fill" style={{ height: `max(0px, calc(${s.ocupacao * 100}% - 10px))` }} />
        {r && <><div className="rd-tubo-linha rd-tubo-linha--desce" style={{ top: `${100 - r.desce}%` }}><span><Icone nome="down" />{r.desce}</span></div>
            <div className="rd-tubo-linha rd-tubo-linha--sobe" style={{ top: `${100 - r.sobe}%` }}><span><Icone nome="up" />{r.sobe}</span></div></>}
    </div>;
}

function Fader({ servico: s }: { servico: Servico }) {
    const r = s.regra, lim = r ? { min: r.minimo, max: r.maximo } : { min: Math.max(0, s.agressividade * 0.5), max: s.agressividade * 1.5 || 1 };
    const pos = (v: number) => `calc(22px + (100% - 44px) * ${(1 - clamp((v - lim.min) / ((lim.max - lim.min) || 1), 0, 1)).toFixed(4)})`;
    const alvo = s.decisao.tipo === 'pendente' && s.decisao.alvo != null ? s.decisao.alvo : null;
    return <div className="rd-fader" title={r ? `Faixa da regra: ${fmt(r.minimo)} a ${fmt(r.maximo)}` : 'Sem regra'}>
        <span className="rd-fader-lim rd-fader-lim--max">{r ? fmt(r.maximo) : '—'}</span>
        <div className="rd-fader-trilho" /><div className="rd-fader-marcas" />
        {alvo != null && <div className="rd-fader-alvo" data-alvo={alvo} style={{ top: pos(alvo) }} />}
        <div className="rd-fader-knob" style={{ top: pos(s.agressividade) }} />
        <span className="rd-fader-lim rd-fader-lim--min">{r ? fmt(r.minimo) : '—'}</span>
    </div>;
}

function Contagens({ c }: { c: Contagem }) {
    return <div className="rd-cont">
        <span data-tone="ok"><i className="rd-dot" /><b>{c.falando}</b> falando</span>
        <span><i className="rd-dot rd-dot--livre" /><b>{c.livres}</b> livres</span>
        <span data-tone="warn"><i className="rd-dot" /><b>{c.pausa}</b> em pausa</span>
        {c.outros > 0 && <span data-tone="none"><i className="rd-dot" /><b>{c.outros}</b> outros</span>}
    </div>;
}

/* ---------- Próxima ação: o que a regra faz com a leitura de agora ---------- */

function Proxima({ servico, painel }: { servico: Servico; painel: Painel }) {
    const a = proxima(servico, painel);
    return <p className="rd-acao" data-tone={a.tom}><Icone nome={a.icone} /><span>{a.texto}</span></p>;
}

const Num = ({ v }: { v: number | undefined }) => <b className="rd-mono">{fmt(v)}</b>;
const Conf = () => <span className="rd-conf" title="1 de 2 leituras" aria-label="1 de 2 leituras"><i className="on" /><i /></span>;

function proxima(s: Servico, painel: Painel): { tom: Tom; icone: NomeIcone; texto: ReactNode } {
    const d = s.decisao, r = s.regra, desce = d.dir === -1, seta: NomeIcone = desce ? 'down' : 'up';
    switch (d.tipo) {
        case 'pendente': return pendente(s, painel);
        case 'ajustou': return { tom: 'info', icone: seta, texto: <>{desce ? 'Desceu' : 'Subiu'}{d.de != null && <> de <Num v={d.de} /></>} para <Num v={d.alvo} /> agora{d.motivo === 'maximo' ? ': é o novo máximo da regra' : ''}.</> };
        case 'acimaDoMaximo': return { tom: 'warn', icone: 'down', texto: <>Acima do máximo da regra (<Num v={d.alvo} />). Desce direto para ele quando a automação estiver ligada neste PC.</> };
        case 'confirmando': return { tom: 'info', icone: 'refresh', texto: <>Ajustado para <Num v={d.alvo} />; esperando a Fastway mostrar o novo valor (ainda aparece <Num v={d.de} />).</> };
        case 'mantem': return { tom: 'ok', icone: 'equal', texto: r ? `Dentro da faixa (${r.sobe}% a ${r.desce}%). Mantém.` : 'Dentro da faixa. Mantém.' };
        case 'limite': return { tom: 'none', icone: seta, texto: desce ? <>Lotado, mas já está no mínimo (<Num v={r?.minimo} />).</> : <>Ocioso, mas já está no máximo (<Num v={r?.maximo} />).</> };
        case 'muitaPausa': return { tom: 'warn', icone: 'users', texto: `Ocioso, mas ${textoPausa(painel.limitePausa)} dos logados está em pausa. Não sobe.` };
        case 'teto': return { tom: 'warn', icone: 'up', texto: d.teto === 'callcenter' ? <>Ocioso, mas call center ativo vai no máximo até <Num v={painel.tetoCallcenter} />. Não sobe.</>
            : <>Ocioso, mas a soma das URAs já chegou no teto (<Num v={painel.teto} />). Não sobe.</> };
        case 'semgente': return { tom: 'none', icone: 'userOff', texto: s.contagem.logados ? 'Ninguém falando nem livre (todos em pausa ou outro estado). Não mexe.' : 'Ninguém logado. Não mexe.' };
        case 'semregra': return { tom: 'none', icone: 'slash', texto: 'Sem regra. A automação não mexe neste serviço.' };
        case 'parado': return { tom: 'none', icone: 'stop', texto: 'Serviço parado na Fastway. Não mexe.' };
        case 'pausado': return { tom: 'warn', icone: 'pause', texto: s.pausado ? <><b>Pausado</b>: mudou na Fastway de <Num v={s.pausado.de} /> para <Num v={s.pausado.para} /> às {horaIso(s.pausado.em)}.</>
            : <><b>Pausado</b>: mudou na Fastway fora da automação.</> };
        case 'falhou': return { tom: 'stop', icone: 'alert', texto: <>A Fastway recusou o ajuste{d.alvo != null && <> para <Num v={d.alvo} /></>}{d.mensagem ? `: ${d.mensagem}` : '.'}</> };
        default: return { tom: 'none', icone: 'equal', texto: 'Sem decisão nesta leitura.' };
    }
}

function pendente(s: Servico, painel: Painel): { tom: Tom; icone: NomeIcone; texto: ReactNode } {
    const d = s.decisao, r = s.regra, desce = d.dir === -1, icone: NomeIcone = desce ? 'down' : 'up';
    const faixa = desce ? <><b>Lotado</b>{r && ` (acima de ${r.desce}%)`}</> : <><b>Ocioso</b>{r && ` (abaixo de ${r.sobe}%)`}</>;
    if (!painel.ativa) return { tom: 'none', icone, texto: <>{faixa}. {desce ? 'Desceria' : 'Subiria'} para <Num v={d.alvo} />, mas a automação está desligada neste PC.</> };
    return { tom: 'info', icone, texto: <>{faixa}. {desce ? 'Desce' : 'Sobe'} para <Num v={d.alvo} /> se a próxima leitura confirmar <Conf /></> };
}

const textoPausa = (limite: number) => (Math.round((limite || 0.5) * 100) === 50 ? 'mais da metade' : `mais de ${Math.round((limite || 0.5) * 100)}%`);

/* ---------- Confirmação para ligar a automação ---------- */

// Ligar vale direto na Fastway: o modal mostra o que muda (serviços com regra, tetos e travas) antes de confirmar.
function ConfirmarAtivacao({ painel, busy, onConfirmar, onCancelar }: { painel: Painel; busy: boolean; onConfirmar(): void; onCancelar(): void }) {
    const caixa = useRef<HTMLElement>(null), cancelar = useRef<HTMLButtonElement>(null), fechar = useRef(onCancelar);
    fechar.current = onCancelar;
    useEffect(() => {
        const antes = document.activeElement as HTMLElement | null;
        cancelar.current?.focus();
        const aoTeclar = (event: KeyboardEvent) => {
            if (event.key === 'Escape') { event.preventDefault(); fechar.current(); return; }
            if (event.key !== 'Tab' || !caixa.current) return; // o foco fica preso no modal
            const focaveis = [...caixa.current.querySelectorAll<HTMLElement>('button:not(:disabled)')];
            const primeiro = focaveis[0], ultimo = focaveis[focaveis.length - 1];
            if (event.shiftKey && document.activeElement === primeiro) { event.preventDefault(); ultimo?.focus(); }
            else if (!event.shiftKey && document.activeElement === ultimo) { event.preventDefault(); primeiro?.focus(); }
        };
        document.addEventListener('keydown', aoTeclar);
        return () => { document.removeEventListener('keydown', aoTeclar); antes?.focus(); };
    }, []);
    const comRegra = painel.servicos.filter(s => s.regra), acimaDoTeto = painel.soma != null && painel.soma > painel.teto;
    const titulo = comRegra.length ? `${comRegra.length} ${comRegra.length === 1 ? 'serviço com regra' : 'serviços com regra'}` : 'Nenhum serviço com regra';
    return <>
        <div className="rd-scrim rd-scrim--modal" onClick={onCancelar} aria-hidden="true" />
        <section ref={caixa} className="rd-modal" role="dialog" aria-modal="true" aria-labelledby="rd-modal-titulo" aria-describedby="rd-modal-desc">
            <header className="rd-modal-cab">
                <span className="rd-modal-icone" aria-hidden="true"><Icone nome="power" /></span>
                <div><h2 id="rd-modal-titulo">Ligar a automação neste PC?</h2>
                    <p id="rd-modal-desc">A partir da próxima leitura, este PC muda a agressividade <b>direto na Fastway</b> dos serviços com regra.</p></div>
            </header>
            <div className="rd-modal-corpo">
                <h3>{titulo}</h3>
                {comRegra.length ? <ul className="rd-modal-lista">{comRegra.map(s => {
                    const r = s.regra as Regra;
                    return <li key={s.id} data-pausado={s.pausado ? '' : undefined}>
                        <span className="rd-modal-nome">{s.nome}<small>{s.modo}{s.emExecucao ? '' : ' · parado'}</small></span>
                        <span className="rd-modal-valor"><b className="rd-mono">{fmt(s.agressividade)}</b><small>agora</small></span>
                        <span className="rd-modal-faixa">{s.pausado ? <><Icone nome="pause" />pausado: não mexe até retomar</>
                            : <>entre <b className="rd-mono">{fmt(r.minimo)}</b> e <b className="rd-mono">{fmt(r.maximo)}</b>, de <b className="rd-mono">{fmt(r.passo)}</b> em <b className="rd-mono">{fmt(r.passo)}</b></>}</span>
                    </li>;
                })}</ul> : <p className="rd-hint">Ligar não muda nada até alguém criar uma regra em algum serviço.</p>}
                <ul className="rd-modal-travas">
                    <li><Icone nome="refresh" /><span>Lê a cada {painel.intervaloSegundos} s e só muda depois de <b>2 leituras seguidas</b> na mesma faixa.</span></li>
                    <li data-alerta={acimaDoTeto ? '' : undefined}><Icone nome="up" /><span>As URAs somadas nunca sobem acima de <b className="rd-mono">{fmt(painel.teto)}</b>
                        {painel.soma != null && <> (agora <b className="rd-mono">{fmt(painel.soma)}</b>{acimaDoTeto ? ': nenhuma URA sobe até baixar' : ''})</>}.
                        Call center ativo vai no máximo até <b className="rd-mono">{fmt(painel.tetoCallcenter)}</b> por serviço.</span></li>
                    <li><Icone nome="users" /><span>Não sobe com {textoPausa(painel.limitePausa)} da equipe em pausa.</span></li>
                    <li><Icone nome="pause" /><span>Se alguém mudar direto na Fastway, aquele serviço pausa até você retomar.</span></li>
                    <li><Icone nome="alert" /><span>Deixe ligado em <b>um PC só</b>: dois PCs ligados brigam pelo mesmo valor.</span></li>
                </ul>
            </div>
            <footer className="rd-modal-pe">
                <button ref={cancelar} className="rd-btn rd-btn--ghost" onClick={onCancelar}>Cancelar</button>
                <button className="rd-btn rd-btn--primary" disabled={busy} onClick={onConfirmar}><Icone nome="power" />Ligar automação</button>
            </footer>
        </section>
    </>;
}

/* ---------- Gaveta com o detalhe do serviço ---------- */

function GavetaServico({ servico: s, painel, criar, busy, recebidoEm, onFechar, onRetomar, onRegra, mailing }: { servico: Servico; painel: Painel; criar: boolean; busy: boolean; recebidoEm: number;
    onFechar(): void; onRetomar(): void; onRegra(regra: Regra | null): Promise<boolean>; mailing: Mailing }) {
    const fechar = useRef<HTMLButtonElement>(null), onFecharRef = useRef(onFechar);
    onFecharRef.current = onFechar;
    useEffect(() => {
        fechar.current?.focus();
        const aoTeclar = (event: KeyboardEvent) => { if (event.key === 'Escape') onFecharRef.current(); };
        document.addEventListener('keydown', aoTeclar);
        return () => document.removeEventListener('keydown', aoTeclar);
    }, [s.id]);
    const parceiros = nomesDe(painel, s.parceiros), c = s.contagem;
    return <>
        <div className="rd-scrim" onClick={onFechar} aria-hidden="true" />
        <section className="rd-gaveta" role="dialog" aria-modal="true" aria-labelledby="rd-gaveta-titulo">
            <header className="rd-gaveta-cab"><div><h2 id="rd-gaveta-titulo">{s.nome}</h2><p>#{s.id} · {s.modo} · limite de chamadas {fmt(s.limite)}</p></div>
                <button ref={fechar} className="rd-ib" aria-label="Fechar" onClick={onFechar}><Icone nome="x" /></button></header>
            <div className="rd-gaveta-corpo">
                <section className="rd-gsec"><h3 className="rd-sec">Agora</h3>
                    <BarraOcupacao servico={s} />
                    <p className="rd-hint rd-agora-txt">{disponiveis(c) ? <><b className="rd-mono">{pct(s.ocupacao)}</b> · {textoOcupacao(c)}</> : (c.logados ? 'Ninguém disponível' : 'Ninguém logado')}</p>
                    <Contagens c={c} />
                    <div className="rd-agora-acao"><Proxima servico={s} painel={painel} />
                        {s.pausado && painel.admin && <button className="rd-btn rd-btn--sm" disabled={busy} onClick={onRetomar}><Icone nome="play" />Retomar</button>}</div>
                    {parceiros && <p className="rd-hint">Mesmos operadores de {parceiros}. Cada serviço segue a própria regra.</p>}
                </section>
                <MailingGaveta mailing={mailing} servicoId={s.id} />
                <section className="rd-gsec"><h3 className="rd-sec">Regra</h3>
                    <Editor key={s.id} servico={s} admin={painel.admin} criar={criar} busy={busy} parceiros={parceiros} tetoCallcenter={painel.tetoCallcenter ?? 99} onRegra={onRegra} /></section>
                <section className="rd-gsec"><h3 className="rd-sec">Operadores ({c.logados})</h3><Operadores operadores={s.operadores} recebidoEm={recebidoEm} /></section>
                <section className="rd-gsec"><h3 className="rd-sec">Ajustes de hoje</h3><Historico eventos={painel.eventos} filtro={s.id} max={12} /></section>
                <MailingGavetaHoje mailing={mailing} servicoId={s.id} />
            </div>
        </section>
    </>;
}

function BarraOcupacao({ servico: s }: { servico: Servico }) {
    const r = s.regra;
    if (!disponiveis(s.contagem) || s.ocupacao == null) return <div className="rd-hbar"><div className="rd-hbar-vazio" /></div>;
    return <div className="rd-hbar" role="img" aria-label={`Ocupação ${pct(s.ocupacao)}`}>
        {r && <><div className="rd-hbar-z rd-hbar-z--sobe" style={{ width: `${r.sobe}%` }} /><div className="rd-hbar-z rd-hbar-z--desce" style={{ width: `${100 - r.desce}%` }} /></>}
        <div className="rd-hbar-fill" style={{ width: `max(0px, calc(${s.ocupacao * 100}% - 6px))` }} />
    </div>;
}

/* ---------- Editor de regra ---------- */

type Valores = Record<Campo, string>;
const ROTULOS: Record<Campo, { rotulo: string; sufixo?: string }> = {
    desce: { rotulo: 'Desce quando ocupação passa de', sufixo: '%' }, sobe: { rotulo: 'Sobe quando ocupação fica abaixo de', sufixo: '%' },
    passo: { rotulo: 'Passo por ajuste' }, minimo: { rotulo: 'Mínimo' }, maximo: { rotulo: 'Máximo' },
};
function regraPadrao(s: Servico, tetoCallcenter: number): Regra {
    const a = Math.max(1, s.agressividade), teto = ehCallcenter(s.modo) ? Math.min(s.limite, tetoCallcenter) : s.limite;
    const maximo = Math.max(1, Math.min(teto, Math.round(a * 1.5)));
    return { desce: 90, sobe: 60, passo: Math.max(1, Math.round(a * 0.1)), minimo: Math.min(maximo, Math.max(1, Math.round(a * 0.5))), maximo };
}
const paraValores = (r: Regra): Valores => ({ desce: String(r.desce), sobe: String(r.sobe), passo: String(r.passo), minimo: String(r.minimo), maximo: String(r.maximo) });
const paraRegra = (v: Valores): Regra => ({ desce: Number(v.desce || NaN), sobe: Number(v.sobe || NaN), passo: Number(v.passo || NaN), minimo: Number(v.minimo || NaN), maximo: Number(v.maximo || NaN) });
const mesmaRegra = (a: Regra, b: Regra | null) => !!b && CAMPOS.every(k => a[k] === b[k]);

// Mesmas mensagens de src/main/ritmoDiscador/regras.js; o main confere de novo ao salvar.
function validar(r: Regra, limite: number, tetoCallcenter: number | null): Validacao {
    if (!CAMPOS.every(k => Number.isInteger(r[k]))) return { erro: 'Preencha todos os campos com números inteiros.', campo: CAMPOS.find(k => !Number.isInteger(r[k])) };
    if (r.desce < 1 || r.desce > 100) return { erro: '“Desce acima de” vai de 1% a 100%.', campo: 'desce' };
    if (r.sobe < 0 || r.sobe >= r.desce) return { erro: '“Sobe abaixo de” precisa ser menor que “desce acima de”, senão as faixas se cruzam.', campo: 'sobe' };
    if (r.passo < 1) return { erro: 'O passo precisa ser pelo menos 1.', campo: 'passo' };
    if (r.minimo < 1 || r.minimo > r.maximo) return { erro: 'O mínimo precisa ser pelo menos 1 e não pode passar do máximo.', campo: 'minimo' };
    if (tetoCallcenter != null && r.maximo > tetoCallcenter) return { erro: `Serviço de call center ativo vai no máximo até ${tetoCallcenter}.`, campo: 'maximo' };
    if (r.maximo > limite) return { aviso: `O máximo passa do limite de chamadas do serviço (${fmt(limite)}).`, campo: 'maximo' };
    return {};
}

function Editor({ servico: s, admin, criar, busy, parceiros, tetoCallcenter, onRegra }: { servico: Servico; admin: boolean; criar: boolean; busy: boolean; parceiros: string; tetoCallcenter: number; onRegra(regra: Regra | null): Promise<boolean> }) {
    // O rascunho vale para a regra em que foi começado; se a regra mudar (salvou aqui ou em outro PC), ele some.
    const base = JSON.stringify(s.regra);
    const [rascunho, setRascunho] = useState<{ base: string; valores: Valores } | null>(() => (criar && admin && !s.regra ? { base, valores: paraValores(regraPadrao(s, tetoCallcenter)) } : null));
    const ativo = rascunho?.base === base ? rascunho : null;
    if (!s.regra && !ativo) return <div className="rd-regra-nada">
        <p className="rd-hint">Este serviço não tem regra: a automação não mexe nele{parceiros ? `, mesmo vendo os mesmos operadores de ${parceiros}` : ''}.</p>
        {admin ? <button className="rd-btn rd-btn--sm" disabled={busy} onClick={() => setRascunho({ base, valores: paraValores(regraPadrao(s, tetoCallcenter)) })}><Icone nome="plus" />Criar regra</button>
            : <p className="rd-so-admin"><Icone nome="lock" />Só admin cria regras.</p>}
    </div>;
    const valores = ativo?.valores ?? paraValores(s.regra as Regra), regra = paraRegra(valores), v = validar(regra, s.limite, ehCallcenter(s.modo) ? tetoCallcenter : null);
    const mudou = !!ativo && !mesmaRegra(regra, s.regra);
    const editar = (campo: Campo, valor: string) => setRascunho({ base, valores: { ...valores, [campo]: valor } });
    async function salvar() { if (!v.erro && await onRegra(regra)) setRascunho(null); }
    return <div className="rd-regra">
        <div className="rd-regra-campos">{CAMPOS.map(k => <label key={k} className="rd-campo"><span>{ROTULOS[k].rotulo}</span>
            <span className="rd-inp" data-erro={v.campo === k && v.erro ? '' : undefined} data-aviso={v.campo === k && v.aviso ? '' : undefined}>
                <input type="number" inputMode="numeric" name={k} min={k === 'sobe' ? 0 : 1} max={k === 'desce' || k === 'sobe' ? 100 : undefined} step={1} value={valores[k]} disabled={!admin || busy}
                    aria-invalid={v.campo === k && !!v.erro} onChange={event => editar(k, event.target.value)} />{ROTULOS[k].sufixo && <i>{ROTULOS[k].sufixo}</i>}</span></label>)}</div>
        <FaixaRegra regra={regra} ocupacao={s.ocupacao} invalida={!!v.erro} />
        <p className="rd-regra-erro" data-aviso={v.aviso ? '' : undefined} aria-live="polite">{v.erro || v.aviso || ''}</p>
        {admin ? <div className="rd-regra-botoes">
            <button className="rd-btn rd-btn--primary rd-btn--sm" disabled={busy || !!v.erro || !mudou} onClick={() => void salvar()}><Icone nome="check" />Salvar regra</button>
            {mudou && <button className="rd-btn rd-btn--ghost rd-btn--sm" disabled={busy} onClick={() => setRascunho(null)}>Descartar</button>}
            {s.regra && <button className="rd-btn rd-btn--ghost rd-btn--sm rd-regra-remover" disabled={busy} onClick={() => void onRegra(null)}>Remover regra</button>}
        </div> : <p className="rd-so-admin"><Icone nome="lock" />Só admin altera regras.</p>}
    </div>;
}

// Pré-visualização das três zonas da regra, com a ocupação de agora marcada.
function FaixaRegra({ regra: r, ocupacao, invalida }: { regra: Regra; ocupacao: number | null; invalida: boolean }) {
    const desce = Number.isFinite(r.desce) ? clamp(r.desce, 0, 100) : 100, sobe = Number.isFinite(r.sobe) ? clamp(r.sobe, 0, desce) : 0;
    const passo = Number.isFinite(r.passo) ? fmt(r.passo) : '?';
    return <div className="rd-faixa-regra" data-invalida={invalida ? '' : undefined}>
        <div className="rd-faixa-barra">
            <div className="rd-fx-sobe" style={{ flexBasis: `${sobe}%` }}><Icone nome="up" />+{passo}</div>
            <div className="rd-fx-mantem" style={{ flexBasis: `${desce - sobe}%` }}>mantém</div>
            <div className="rd-fx-desce" style={{ flexBasis: `${100 - desce}%` }}><Icone nome="down" />−{passo}</div>
            {ocupacao != null && <i className="rd-faixa-agora" style={{ left: `${ocupacao * 100}%` }}><span>agora {pct(ocupacao)}</span></i>}
        </div>
        <div className="rd-faixa-esc"><span>0%</span><span>{sobe}%</span><span>{desce}%</span><span>100%</span></div>
    </div>;
}

/* ---------- Operadores ---------- */

const ORDEM_ESTADO: Record<Operador['estado'], number> = { falando: 0, livre: 1, pausa: 2, outro: 3 };
const GRUPO_ESTADO: Record<Operador['estado'], string> = { falando: 'Falando', livre: 'Livres', pausa: 'Em pausa', outro: 'Outros' };
const TOM_ESTADO: Record<Operador['estado'], Tom | undefined> = { falando: 'ok', livre: undefined, pausa: 'warn', outro: 'none' };
function detalheOperador(o: Operador) {
    const d = (o.detalhe || '').toLowerCase();
    if (o.estado === 'livre') return 'esperando ligação';
    if (o.estado === 'pausa') return d.startsWith('pausa') ? d : `pausa${d ? ` ${d}` : ''}`;
    return d;
}

function Operadores({ operadores, recebidoEm }: { operadores: Operador[]; recebidoEm: number }) {
    const agora = useAgora(), passou = Math.max(0, Math.floor((agora - recebidoEm) / 1000));
    if (!operadores.length) return <p className="rd-vazio">Ninguém logado neste serviço agora.</p>;
    const lista = [...operadores].sort((a, b) => ORDEM_ESTADO[a.estado] - ORDEM_ESTADO[b.estado] || b.segundos - a.segundos);
    return <div className="rd-ops">{lista.map((o, i) => {
        const segundos = o.segundos + passou, longa = o.estado === 'pausa' && segundos > PAUSA_LONGA;
        return <div key={o.id} className="rd-op-bloco">
            {(i === 0 || lista[i - 1].estado !== o.estado) && <p className="rd-ops-grupo">{GRUPO_ESTADO[o.estado]}</p>}
            <div className="rd-op" data-longa={longa ? '' : undefined} title={longa ? 'Passou dos 10 min de pausa' : undefined}>
                <i className={`rd-dot${o.estado === 'livre' ? ' rd-dot--livre' : ''}`} data-tone={TOM_ESTADO[o.estado]} />
                <span className="rd-op-nome">{o.nome}</span><span className="rd-op-est">{detalheOperador(o)}</span><span className="rd-op-t">{duracao(segundos)}</span>
            </div>
        </div>;
    })}</div>;
}

/* ---------- Histórico ---------- */

function Historico({ eventos, filtro, max = 40 }: { eventos: Evento[]; filtro: number | null; max?: number }) {
    const lista = eventos.filter(e => filtro == null || e.servicoId === filtro).sort((a, b) => Date.parse(b.em) - Date.parse(a.em)).slice(0, max);
    if (!lista.length) return <p className="rd-vazio">Nenhum ajuste hoje.</p>;
    return <ol className="rd-hist">{lista.map(e => <ItemHistorico key={e.id} evento={e} filtrado={filtro != null} />)}</ol>;
}

const variacao = (de: number, para: number) => `${para >= de ? '+' : '−'}${fmt(Math.abs(para - de))}`;
const TEXTO_CORTE: Record<Corte, string> = {
    minimo: 'parou no mínimo da regra', maximo: 'parou no máximo da regra',
    tetoUra: 'subiu só o que cabia no teto das URAs', tetoCallcenter: 'subiu só até o teto do call center',
};

// "ocupação 39% · 4 falando, 6 livres, 3 em pausa · abaixo de 60% · subiu só o que cabia no teto das URAs"
function detalheAjuste(e: Evento, desce: boolean) {
    const c = e.contagem, f = e.faixa;
    const pessoas = c ? [`${c.falando} falando`, `${c.livres} ${c.livres === 1 ? 'livre' : 'livres'}`, c.pausa && `${c.pausa} em pausa`, c.outros && `${c.outros} em outro estado`].filter(Boolean).join(', ') : '';
    const motivo = e.motivo === 'maximo' ? `o máximo da regra baixou para ${fmt(e.para)}` : f ? (desce ? `acima de ${f.desce}%` : `abaixo de ${f.sobe}%`) : '';
    return [e.ocupacao !== undefined ? `ocupação ${pct(e.ocupacao)}` : '', pessoas, motivo, e.corte ? TEXTO_CORTE[e.corte] : ''].filter(Boolean).join(' · ');
}

function ItemHistorico({ evento: e, filtrado }: { evento: Evento; filtrado: boolean }) {
    const quando = <time dateTime={e.em}>{horaIso(e.em)}</time>;
    const dePara = e.de != null && e.para != null ? <span className="rd-de-para">{fmt(e.de)} → {fmt(e.para)}<em>{variacao(e.de, e.para)}</em></span> : null;
    if (e.tipo === 'sistema') return <li data-tone="none">{quando}<Icone nome="power" /><div><p>{e.texto}</p></div></li>;
    if (e.tipo === 'falha') return <li data-tone="stop">{quando}<Icone nome="alert" /><div><b>{filtrado || !e.servico ? 'Falha no ajuste' : e.servico}</b><p>{e.texto}</p></div></li>;
    if (e.tipo === 'manual') return <li data-tone="warn">{quando}<Icone nome="alert" /><div><b>{filtrado ? 'Mudou direto na Fastway' : e.servico} {dePara}</b>
        <p>{filtrado ? '' : 'mudou direto na Fastway · '}automação pausada neste serviço até retomar</p></div></li>;
    const desce = e.dir === -1 || (e.dir == null && (e.para ?? 0) < (e.de ?? 0));
    return <li data-tone="info" title={e.texto}>{quando}<Icone nome={desce ? 'down' : 'up'} />
        <div><b>{filtrado ? (desce ? 'Desceu' : 'Subiu') : e.servico} {dePara}</b><p>{detalheAjuste(e, desce)}</p></div></li>;
}

/* ---------- Ajustes de hoje agrupados por campanha ---------- */

type Grupo = { id: number; nome: string; modo: string; eventos: Evento[]; subidas: number; descidas: number; manuais: number; falhas: number; inicio: number | null; agora: number | null };

function agrupar(eventos: Evento[], servicos: Servico[]) {
    const grupos = new Map<number, Grupo>(), sistema: Evento[] = [];
    for (const e of [...eventos].sort((a, b) => Date.parse(a.em) - Date.parse(b.em))) {
        if (e.servicoId == null) { sistema.push(e); continue; }
        const g = grupos.get(e.servicoId) ?? { id: e.servicoId, nome: e.servico, modo: e.modo ?? '', eventos: [], subidas: 0, descidas: 0, manuais: 0, falhas: 0, inicio: null, agora: null };
        grupos.set(e.servicoId, g);
        g.eventos.push(e);
        if (e.tipo === 'ajuste') { if ((e.dir ?? Math.sign((e.para ?? 0) - (e.de ?? 0))) < 0) g.descidas++; else g.subidas++; }
        if (e.tipo === 'manual') g.manuais++;
        if (e.tipo === 'falha') g.falhas++;
        if ((e.tipo === 'ajuste' || e.tipo === 'manual') && e.de != null && e.para != null) { g.inicio ??= e.de; g.agora = e.para; }
    }
    for (const g of grupos.values()) {
        const s = servicos.find(x => x.id === g.id);
        if (s) { g.nome = s.nome; g.modo = s.modo; if (g.agora != null) g.agora = s.agressividade; }
        g.eventos.reverse();
    }
    // Campanhas com mudança de agressividade primeiro, da mais recente para a mais antiga.
    const lista = [...grupos.values()].sort((a, b) => Number(b.inicio != null) - Number(a.inicio != null) || Date.parse(b.eventos[0].em) - Date.parse(a.eventos[0].em));
    return { grupos: lista, sistema: sistema.reverse() };
}

const VISIVEIS = 4;

function HistoricoPorCampanha({ painel }: { painel: Painel }) {
    const { grupos, sistema } = useMemo(() => agrupar(painel.eventos, painel.servicos), [painel.eventos, painel.servicos]);
    const [abertos, setAbertos] = useState<Set<number | 'sistema'>>(() => new Set());
    if (!grupos.length && !sistema.length) return <p className="rd-vazio">Nenhum ajuste hoje.</p>;
    const alternar = (id: number | 'sistema') => setAbertos(atual => { const novo = new Set(atual); if (novo.has(id)) novo.delete(id); else novo.add(id); return novo; });
    const itens = (id: number | 'sistema', eventos: Evento[]) => {
        const todos = abertos.has(id), mostrar = todos ? eventos : eventos.slice(0, VISIVEIS);
        return <>
            <ol className="rd-hist">{mostrar.map(e => <ItemHistorico key={e.id} evento={e} filtrado />)}</ol>
            {eventos.length > VISIVEIS && <button type="button" className="rd-camp-mais" aria-expanded={todos} onClick={() => alternar(id)}>
                {todos ? 'Mostrar só os últimos' : `Mostrar todos os ${eventos.length} registros`}</button>}
        </>;
    };
    return <div className="rd-camps">
        {grupos.map(g => {
            const resumo = [g.subidas && `${g.subidas} ${g.subidas === 1 ? 'subida' : 'subidas'}`, g.descidas && `${g.descidas} ${g.descidas === 1 ? 'descida' : 'descidas'}`,
                g.manuais && `${g.manuais} ${g.manuais === 1 ? "mudança" : "mudanças"} direto na Fastway`, g.falhas && `${g.falhas} com falha`].filter(Boolean).join(' · ');
            return <section key={g.id} className="rd-camp" aria-label={g.nome}>
                <header className="rd-camp-cab">
                    <div className="rd-camp-nome"><b>{g.nome}</b>{g.modo && <small>{g.modo}</small>}</div>
                    {g.inicio != null && g.agora != null && <span className="rd-camp-dia" title="Primeiro valor alterado hoje → valor de agora">
                        <span className="rd-mono">{fmt(g.inicio)} → {fmt(g.agora)}</span>
                        <em data-tone={g.agora > g.inicio ? 'info' : g.agora < g.inicio ? 'warn' : 'none'}>{g.agora === g.inicio ? '0' : variacao(g.inicio, g.agora)}</em></span>}
                    {resumo && <p className="rd-camp-resumo">{resumo} · último às {horaIso(g.eventos[0].em)}</p>}
                </header>
                {itens(g.id, g.eventos)}
            </section>;
        })}
        {!!sistema.length && <section className="rd-camp rd-camp--sistema" aria-label="Automação">
            <header className="rd-camp-cab"><div className="rd-camp-nome"><b>Automação</b><small>liga, desliga e teto</small></div></header>
            {itens('sistema', sistema)}
        </section>}
    </div>;
}

/* ---------- Gráfico do dia: ocupação em cima, agressividade embaixo, das 08h às 18h ---------- */

const G = { padL: 44, padR: 14, padT: 8, gap: 26, h1: 112, h2: 76 };
const HORAS = [8, 10, 12, 14, 16, 18];
type Geo = { ini: number; fim: number; w: number; base: number; x(t: number): number; yo(v: number): number; yb(v: number): number; pontos: Ponto[] };

function geometria(s: Servico, w: number, agora: number): Geo {
    const dia = new Date(agora); dia.setHours(8, 0, 0, 0);
    const ini = dia.getTime(), fim = ini + 10 * 3600e3, pontos = s.serie.filter(p => p.t >= ini && p.t <= fim).sort((a, b) => a.t - b.t);
    const x = (t: number) => G.padL + ((t - ini) / (fim - ini)) * (w - G.padL - G.padR), yo = (v: number) => G.padT + (1 - v) * G.h1;
    const vals = pontos.map(p => p.agressividade).concat(s.regra ? [s.regra.minimo, s.regra.maximo] : [s.agressividade]);
    let lo = Math.min(...vals), hi = Math.max(...vals);
    const folga = Math.max(1, (hi - lo) * 0.12); lo = Math.max(0, lo - folga); hi += folga;
    const topo = G.padT + G.h1 + G.gap, base = topo + G.h2, yb = (v: number) => topo + (1 - (v - lo) / (hi - lo || 1)) * G.h2;
    return { ini, fim, w, base, x, yo, yb, pontos };
}

function caminhoOcupacao(g: Geo) {
    let d = '', caneta = false;
    for (const p of g.pontos) {
        if (p.ocupacao == null) { caneta = false; continue; }
        d += `${caneta ? 'L' : 'M'}${g.x(p.t).toFixed(1)},${g.yo(p.ocupacao).toFixed(1)}`; caneta = true;
    }
    return d;
}

function caminhoAgressividade(g: Geo, agora: number) {
    let d = '', ultimo: number | null = null;
    for (const p of g.pontos) {
        const px = g.x(p.t).toFixed(1), py = g.yb(p.agressividade).toFixed(1);
        if (ultimo == null) d = `M${px},${py}`;
        else if (p.agressividade !== ultimo) d += `H${px}V${py}`;
        ultimo = p.agressividade;
    }
    return d ? `${d}H${g.x(clamp(agora, g.ini, g.fim)).toFixed(1)}` : '';
}

const eventosDoGrafico = (eventos: Evento[], s: Servico) => eventos.filter(e => e.servicoId === s.id && (e.tipo === 'ajuste' || e.tipo === 'manual') && e.para != null);

function Grafico({ servico: s, painel }: { servico: Servico; painel: Painel }) {
    const caixa = useRef<HTMLDivElement>(null), [largura, setLargura] = useState(0), [hover, setHover] = useState<Ponto | null>(null);
    useEffect(() => {
        const el = caixa.current; if (!el) return;
        const observer = new ResizeObserver(([entrada]) => setLargura(Math.floor(entrada.contentRect.width)));
        observer.observe(el);
        return () => observer.disconnect();
    }, []);
    useEffect(() => setHover(null), [s.id]);
    const agora = painel.lidoEm ? Date.parse(painel.lidoEm) : Date.now();
    const g = useMemo(() => (largura ? geometria(s, largura, agora) : null), [s, largura, agora]);
    const eventos = useMemo(() => eventosDoGrafico(painel.eventos, s), [painel.eventos, s]);
    function mover(event: ReactMouseEvent<SVGRectElement>) {
        if (!g || !g.pontos.length) return;
        const t = g.ini + ((event.clientX - event.currentTarget.getBoundingClientRect().left) / (g.w - G.padL - G.padR)) * (g.fim - g.ini);
        if (t < g.pontos[0].t - 60e3 || t > g.pontos[g.pontos.length - 1].t + 60e3) { setHover(null); return; }
        setHover(g.pontos.reduce((a, b) => (Math.abs(b.t - t) < Math.abs(a.t - t) ? b : a)));
    }
    return <>
        <Legenda comRegra={!!s.regra} />
        <div className="rd-grafico" ref={caixa} style={{ height: G.padT + G.h1 + G.gap + G.h2 + 24 }}>
            {g && <svg width={g.w} height={g.base + 24} role="img" aria-label={`Ocupação e agressividade de ${s.nome} hoje`}>
                <FundoGrafico g={g} s={s} agora={agora} />
                {caminhoOcupacao(g) ? <path className="rd-l-occ" d={caminhoOcupacao(g)} /> : <text x={(G.padL + g.w) / 2} y={g.yo(0.5)} textAnchor="middle">sem leituras hoje</text>}
                <path className="rd-l-ag" d={caminhoAgressividade(g, agora)} />
                {eventos.map(e => <circle key={e.id} className={`rd-ev${e.tipo === 'manual' ? ' rd-ev--manual' : ''}`}
                    cx={g.x(clamp(Date.parse(e.em), g.ini, g.fim))} cy={g.yb(e.para ?? 0)} r={4} />)}
                {hover && <Cruz g={g} p={hover} />}
                <rect x={G.padL} y={0} width={Math.max(0, g.w - G.padL - G.padR)} height={g.base} fill="transparent" onMouseMove={mover} onMouseLeave={() => setHover(null)} />
            </svg>}
            {g && hover && <DicaGrafico g={g} p={hover} eventos={eventos} />}
        </div>
    </>;
}

function Legenda({ comRegra }: { comRegra: boolean }) {
    return <div className="rd-legenda">
        <span className="rd-leg-occ"><i />Ocupação</span><span className="rd-leg-ag"><i />Agressividade</span>
        {comRegra && <><span className="rd-leg-desce"><i className="z" />Zona que desce</span><span className="rd-leg-sobe"><i className="z" />Zona que sobe</span></>}
        <span className="rd-leg-manual"><i className="o" />Mudou na Fastway</span>
    </div>;
}

function FundoGrafico({ g, s, agora }: { g: Geo; s: Servico; agora: number }) {
    const r = s.regra, largura = g.w - G.padL - G.padR, direita = g.w - G.padR;
    const marcaAg = (v: number, k: string) => <g key={k}><line className="rd-lim" x1={G.padL} x2={direita} y1={g.yb(v)} y2={g.yb(v)} /><text x={G.padL - 8} y={g.yb(v) + 3} textAnchor="end">{fmt(v)}</text></g>;
    return <>
        {r && <><rect className="rd-z-desce" x={G.padL} width={largura} y={g.yo(1)} height={g.yo(r.desce / 100) - g.yo(1)} />
            <rect className="rd-z-sobe" x={G.padL} width={largura} y={g.yo(r.sobe / 100)} height={g.yo(0) - g.yo(r.sobe / 100)} /></>}
        {HORAS.map(h => { const t = g.ini + (h - 8) * 3600e3; return <g key={h}><line className="rd-grade" x1={g.x(t)} x2={g.x(t)} y1={G.padT} y2={g.base} />
            <text x={g.x(t)} y={g.base + 16} textAnchor="middle">{doisDigitos(h)}h</text></g>; })}
        {[0, 0.5, 1].map(v => <text key={v} x={G.padL - 8} y={g.yo(v) + 3} textAnchor="end">{v * 100}%</text>)}
        {r ? <>{marcaAg(r.maximo, 'max')}{marcaAg(r.minimo, 'min')}</> : <text x={G.padL - 8} y={g.yb(s.agressividade) + 3} textAnchor="end">{fmt(s.agressividade)}</text>}
        <line className="rd-grade" x1={G.padL} x2={direita} y1={g.yo(0)} y2={g.yo(0)} />
        {agora >= g.ini && agora <= g.fim && <><line className="rd-agora" x1={g.x(agora)} x2={g.x(agora)} y1={G.padT} y2={g.base} /><text x={g.x(agora) + 4} y={G.padT + 9}>agora</text></>}
    </>;
}

function Cruz({ g, p }: { g: Geo; p: Ponto }) {
    return <g className="rd-cruz">
        <line x1={g.x(p.t)} x2={g.x(p.t)} y1={G.padT} y2={g.base} />
        {p.ocupacao != null && <circle className="rd-cruz-occ" cx={g.x(p.t)} cy={g.yo(p.ocupacao)} r={4} />}
        <circle className="rd-cruz-ag" cx={g.x(p.t)} cy={g.yb(p.agressividade)} r={4} />
    </g>;
}

function textoEventoGrafico(e: Evento) {
    const desce = e.dir === -1 || (e.dir == null && (e.para ?? 0) < (e.de ?? 0)), dePara = `${fmt(e.de)} → ${fmt(e.para)}`;
    if (e.tipo === 'manual') return `Mudou na Fastway ${dePara}`;
    return `${desce ? 'Desceu' : 'Subiu'} ${dePara}`;
}

function DicaGrafico({ g, p, eventos }: { g: Geo; p: Ponto; eventos: Evento[] }) {
    const perto = eventos.filter(e => Math.abs(Date.parse(e.em) - p.t) <= 150e3);
    return <div className="rd-dica" style={{ left: clamp(g.x(p.t), 110, Math.max(110, g.w - 110)) }} role="status">
        <b className="rd-mono">{hora(p.t)}</b><br />Ocupação <b className="rd-mono">{pct(p.ocupacao)}</b> · agressividade <b className="rd-mono">{fmt(p.agressividade)}</b>
        {perto.map(e => <span key={e.id}><br />{textoEventoGrafico(e)}</span>)}
    </div>;
}

/* ---------- Login e peças pequenas ---------- */

function LoginFastway({ painel, busy, onSalvar }: { painel: Painel; busy: boolean; onSalvar(login: string, senha: string): void }) {
    const [login, setLogin] = useState(''), [senha, setSenha] = useState('');
    return <section className="rd-login" aria-labelledby="rd-login-titulo">
        <KeyIcon />
        <div>
            <h2 id="rd-login-titulo">Login Fastway</h2>
            <p className="rd-hint">O Ritmo do Discador usa o seu login Fastway, o mesmo do Importar Mailing e do Alerta de Mailing. Ele é conferido na Fastway e salvo neste computador com a proteção do Windows; não vai para nenhum arquivo .env.</p>
            <form className="rd-login-form" onSubmit={event => { event.preventDefault(); onSalvar(login, senha); }}>
                <label className="flow-field"><span>Login</span><input autoComplete="off" spellCheck={false} value={login} disabled={busy} onChange={event => setLogin(event.target.value)} /></label>
                <label className="flow-field"><span>Senha</span><input type="password" autoComplete="new-password" value={senha} disabled={busy} onChange={event => setSenha(event.target.value)} /></label>
                <button className="flow-primary" disabled={busy || !painel.login.canSave || !login.trim() || !senha}>{busy ? 'Conferindo…' : 'Conferir e salvar'}</button>
            </form>
            {!painel.login.canSave && <p className="rd-hint">A proteção do Windows não está disponível nesta máquina, então o login não pode ser salvo e o Ritmo do Discador não roda aqui.</p>}
        </div>
    </section>;
}

// Mesma chave da Limpeza Local (estilos globais .toggle-switch), com a cor de cada modo.
function Chave({ rotulo, dica, ligado, desabilitado, sim, onMudar }: { rotulo: string; dica: string; ligado: boolean; desabilitado: boolean; sim?: boolean; onMudar(valor: boolean): void }) {
    return <label className={`toggle-switch rd-switch${sim ? ' rd-switch--sim' : ''}`} title={dica}>
        <span className="switch"><input type="checkbox" checked={ligado} disabled={desabilitado} onChange={event => onMudar(event.target.checked)} /><span className="slider" /></span>
        <span className="toggle-label">{rotulo}</span>
    </label>;
}

const TRACOS = {
    up: <><path d="M12 19V5" /><path d="m5 12 7-7 7 7" /></>,
    down: <><path d="M12 5v14" /><path d="m19 12-7 7-7-7" /></>,
    equal: <path d="M5 9h14M5 15h14" />,
    pause: <><rect x="6.5" y="5" width="3.5" height="14" rx="1" /><rect x="14" y="5" width="3.5" height="14" rx="1" /></>,
    stop: <rect x="6" y="6" width="12" height="12" rx="2" />,
    play: <path d="M7 5.5v13a1 1 0 0 0 1.5.9l10.4-6.5a1 1 0 0 0 0-1.8L8.5 4.6A1 1 0 0 0 7 5.5z" />,
    x: <path d="M18 6 6 18M6 6l12 12" />,
    alert: <><path d="M10.3 3.9 1.8 18a2 2 0 0 0 1.7 3h17a2 2 0 0 0 1.7-3L13.7 3.9a2 2 0 0 0-3.4 0z" /><path d="M12 9v4M12 17h.01" /></>,
    flask: <><path d="M9 3h6" /><path d="M10 3v6.5L4.6 18.4A1.7 1.7 0 0 0 6 21h12a1.7 1.7 0 0 0 1.4-2.6L14 9.5V3" /><path d="M7.5 15h9" /></>,
    users: <><path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2" /><circle cx="9" cy="7" r="4" /><path d="M22 21v-2a4 4 0 0 0-3-3.9M16 3.1a4 4 0 0 1 0 7.8" /></>,
    userOff: <><circle cx="9" cy="7" r="4" /><path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2" /><path d="m17 8 5 5M22 8l-5 5" /></>,
    lock: <><rect x="4" y="11" width="16" height="10" rx="2" /><path d="M8 11V7a4 4 0 0 1 8 0v4" /></>,
    plus: <path d="M12 5v14M5 12h14" />,
    chevron: <path d="m9 6 6 6-6 6" />,
    check: <path d="m5 12.5 4.5 4.5L19 7" />,
    slash: <><circle cx="12" cy="12" r="9" /><path d="m5.6 5.6 12.8 12.8" /></>,
    power: <><path d="M12 3v9" /><path d="M6.3 7.5a8 8 0 1 0 11.4 0" /></>,
    refresh: <><path d="M20 11a8 8 0 0 0-14.6-4.5L4 8" /><path d="M4 3v5h5" /><path d="M4 13a8 8 0 0 0 14.6 4.5L20 16" /><path d="M20 21v-5h-5" /></>,
};
type NomeIcone = keyof typeof TRACOS;
function Icone({ nome }: { nome: NomeIcone }) {
    return <svg className="rd-ic" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.9" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">{TRACOS[nome]}</svg>;
}
