'use strict';
/**
 * Regras do Ritmo do Discador: estado dos operadores, ocupação e a decisão de subir ou descer a agressividade.
 * Ocupação = falando ÷ (logados − em pausa): quem está em pausa não está disponível e fica fora da conta.
 */
const LIMITE_PAUSA = 0.5; // com mais da metade em pausa a regra nunca sobe: falta gente, não ligação
const CAMPOS = ['desce', 'sobe', 'passo', 'minimo', 'maximo'];
const TETO_PADRAO = 1500;     // URAs: a soma de todas as URAs em execução não passa disso
const TETO_CALLCENTER = 99;  // call center ativo (modo POWER): cada serviço vai no máximo até aqui
const ehUra = modo => String(modo || '').trim().toUpperCase() === 'URA';
const ehCallcenter = modo => String(modo || '').trim().toUpperCase() === 'POWER';

function estadoDoOperador(status) {
    const s = String(status || '').trim().toUpperCase();
    if (s === 'EM ATENDIMENTO') return 'falando';
    if (s === 'CONECTADO') return 'livre';
    if (s === 'EM PAUSA') return 'pausa';
    return 'outro'; // MODO MANUAL, CALLBACK e o que mais a Fastway inventar
}

/** "2026-11-06 10:03:33.64" (hora local da Fastway) até agora, em segundos. */
function segundosDesde(hora, agora) {
    const m = /^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})/.exec(hora || '');
    if (!m) return 0;
    const inicio = new Date(+m[1], +m[2] - 1, +m[3], +m[4], +m[5], +m[6]);
    return Math.max(0, Math.round((agora - inicio) / 1000));
}

function operadoresDe(lista, agora = new Date()) {
    return lista.map(o => {
        const estado = estadoDoOperador(o.status), segundos = o.segundos == null || o.segundos === '' ? NaN : Number(o.segundos);
        return { id: o.id, nome: o.nome, estado, detalhe: estado === 'outro' ? o.status : o.complemento,
            segundos: Number.isFinite(segundos) && segundos >= 0 ? Math.round(segundos) : segundosDesde(o.horaStatus, agora) };
    });
}

function contar(operadores) {
    const c = { falando: 0, livres: 0, pausa: 0, outros: 0, logados: operadores.length };
    for (const o of operadores) c[{ falando: 'falando', livre: 'livres', pausa: 'pausa' }[o.estado] || 'outros']++;
    return c;
}

/** Disponíveis = logados fora de pausa (falando, livres e outros). Sem ninguém disponível não há ocupação. */
const disponiveis = c => c.logados - c.pausa;
const ocupacao = c => (disponiveis(c) > 0 ? c.falando / disponiveis(c) : null);

/** Confere a regra digitada. Devolve { regra } normalizada ou { erro }; passar do limite de chamadas é só aviso. */
function validarRegra(entrada, limite = null, modo = '') {
    if (!entrada || typeof entrada !== 'object') return { erro: 'Regra inválida.' };
    const regra = {};
    for (const campo of CAMPOS) {
        const valor = Number(entrada[campo]);
        if (!Number.isInteger(valor)) return { erro: 'Preencha todos os campos com números inteiros.' };
        regra[campo] = valor;
    }
    if (regra.desce < 1 || regra.desce > 100) return { erro: '“Desce quando” vai de 1% a 100%.' };
    if (regra.sobe < 0 || regra.sobe >= regra.desce) return { erro: '“Sobe quando” precisa ser menor que “desce quando”, senão as faixas se cruzam.' };
    if (regra.passo < 1) return { erro: 'O passo precisa ser pelo menos 1.' };
    if (regra.minimo < 1 || regra.minimo > regra.maximo) return { erro: 'O mínimo precisa ser pelo menos 1 e não pode passar do máximo.' };
    if (ehCallcenter(modo) && regra.maximo > TETO_CALLCENTER) return { erro: `Serviço de call center ativo vai no máximo até ${TETO_CALLCENTER}.` };
    const aviso = limite != null && regra.maximo > limite ? `O máximo passa do limite de chamadas do serviço (${limite}).` : null;
    return { regra, aviso };
}

/**
 * O que fazer com um serviço nesta leitura. `pendente` é a direção vista na leitura anterior:
 * o passo só é aplicado ("ajustar") quando a mesma faixa aparece em duas leituras seguidas.
 * `folga` é quanto ainda cabe até o teto (soma das URAs ou o teto do call center): subida nunca passa dele.
 */
function decidir({ regra, agressividade, contagem, emExecucao, pausado, pendente = null, folga = Infinity }) {
    if (!regra) return { tipo: 'semregra' };
    if (!emExecucao) return { tipo: 'parado' };
    if (pausado) return { tipo: 'pausado' };
    if (agressividade == null) return { tipo: 'semgente' };
    const occ = ocupacao(contagem);
    if (occ == null) return { tipo: 'semgente' };
    const pct = occ * 100;
    const dir = pct >= regra.desce ? -1 : pct <= regra.sobe ? 1 : 0;
    if (!dir) return { tipo: 'mantem' };
    if (dir > 0 && contagem.pausa / contagem.logados > LIMITE_PAUSA) return { tipo: 'muitaPausa', dir };
    if (dir > 0 ? agressividade >= regra.maximo : agressividade <= regra.minimo) return { tipo: 'limite', dir };
    let alvo = dir > 0 ? Math.min(regra.maximo, agressividade + regra.passo) : Math.max(regra.minimo, agressividade - regra.passo);
    if (dir > 0) {
        alvo = Math.min(alvo, agressividade + Math.max(0, folga)); // sobe só o que cabe no teto
        if (alvo <= agressividade) return { tipo: 'teto', dir };
    }
    return { tipo: pendente === dir ? 'ajustar' : 'pendente', dir, alvo };
}

/** Soma da agressividade das URAs em execução: é ela que o teto geral limita. */
const somaUra = servicos => servicos.reduce((total, s) => total + (s.emExecucao && ehUra(s.modo) ? s.agressividade || 0 : 0), 0);

/** Quanto ainda cabe subir: URA divide o teto geral com as outras URAs; call center tem teto próprio; o resto não tem teto. */
function folgaDe(modo, agressividade, teto, soma) {
    if (ehUra(modo)) return teto - soma;
    if (ehCallcenter(modo)) return TETO_CALLCENTER - agressividade;
    return Infinity;
}

module.exports = { disponiveis, LIMITE_PAUSA, TETO_PADRAO, TETO_CALLCENTER, ehUra, ehCallcenter, somaUra, folgaDe, estadoDoOperador, operadoresDe, contar, ocupacao, validarRegra, decidir };
