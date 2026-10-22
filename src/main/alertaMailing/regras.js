'use strict';
/**
 * Regras do Alerta de Mailing (sem rede e sem disco). Portadas de alerta-mailing/regras.py.
 */
const Situacao = Object.freeze({ OK: 'OK', QUASE: 'QUASE', ESGOTADO: 'ESGOTADO', SEM_MAILING: 'SEM_MAILING' });
const Acao = Object.freeze({
    NADA: 'NADA',
    PODE_RECICLAR: 'PODE_RECICLAR',
    TROCAR: 'TROCAR',
    PROXIMA_LISTA: 'PROXIMA_LISTA', // serviço de transbordo: não recicla, segue para a próxima lista
    VERIFICAR_SERVICO: 'VERIFICAR_SERVICO',
});

/** Arredonda como o round() do Python do alerta original: empate exato vai para o par (7,25 → 7,2). */
function arredondar(valor, casas) {
    const fator = 10 ** casas, escalado = valor * fator, inteiro = Math.round(escalado);
    const empate = Math.abs(escalado % 1) === 0.5;
    return (empate && inteiro % 2 !== 0 ? inteiro - 1 : inteiro) / fator;
}

function percentualConsumido(registros, disponiveis, aguardando) {
    if (registros <= 0) return 100;
    const restantes = Math.max(0, disponiveis) + Math.max(0, aguardando);
    return Math.min(100, Math.max(0, 100 * (1 - restantes / registros)));
}

/** Minutos até a lista acabar no ritmo atual; null quando não dá para estimar. */
function minutosParaAcabar(restantes, ritmoPorMinuto) {
    if (!ritmoPorMinuto || ritmoPorMinuto <= 0) return null;
    return arredondar(Math.max(0, restantes) / ritmoPorMinuto, 1);
}

/** QUASE em 99% ou, para listas curtas, quando vão acabar antes das próximas verificações. */
function classificar(registros, disponiveis, aguardando, limiar = 99, minutosRestantes = null, minutosAlerta = null) {
    if (Math.max(0, disponiveis) + Math.max(0, aguardando) === 0) return Situacao.ESGOTADO;
    if (percentualConsumido(registros, disponiveis, aguardando) >= limiar) return Situacao.QUASE;
    if (minutosRestantes != null && minutosAlerta && minutosRestantes <= minutosAlerta) return Situacao.QUASE;
    return Situacao.OK;
}

/** O que a Fastway registrou desde a 1ª leitura do dia ou o que este PC reciclou, o que for maior. */
function reciclagensHoje(quantidadeAtual, linhaBase, feitasAqui = 0) {
    return Math.max(0, quantidadeAtual - linhaBase, feitasAqui);
}

function decidir(situacao, reciclagens, limite, transbordo = false) {
    if (situacao === Situacao.SEM_MAILING) return Acao.VERIFICAR_SERVICO;
    if (situacao === Situacao.OK) return Acao.NADA;
    if (transbordo) return Acao.PROXIMA_LISTA;
    return reciclagens >= limite ? Acao.TROCAR : Acao.PODE_RECICLAR;
}

module.exports = { Situacao, Acao, arredondar, percentualConsumido, minutosParaAcabar, classificar, reciclagensHoje, decidir };
