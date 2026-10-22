'use strict';
/**
 * Configuração do Alerta de Mailing. O login vem do login Fastway salvo no Gerenciador; o resto tem os mesmos
 * padrões do alerta-mailing original e aceita os mesmos nomes de variável de ambiente para ajuste fino.
 */
const DOMINIO_PADRAO = 'mbfinance2.fastssl.com.br';
const TRANSBORDO_PADRAO = Object.freeze([120, 62]); // [C6 BANK] - NOVO TRANSBORDO 98 LTDA e PT2
const IGNORAR_PADRAO = Object.freeze(['TESTE']);

function inteiro(valor, padrao, nome) {
    if (valor == null || String(valor).trim() === '') return padrao;
    const numero = Number(String(valor).replace(',', '.'));
    if (!Number.isFinite(numero)) throw new Error(`Valor numérico inválido em ${nome}: ${valor}`);
    return numero;
}

const lista = valor => String(valor || '').split(',').map(item => item.trim()).filter(Boolean);

function criarConfig({ logon, senha, ...opcoes } = {}) {
    const config = {
        dominio: opcoes.dominio || DOMINIO_PADRAO,
        logon,
        senha,
        intervaloMinutos: opcoes.intervaloMinutos ?? 3,
        limiteReciclagensDia: opcoes.limiteReciclagensDia ?? 2,
        limiarPercentual: opcoes.limiarPercentual ?? 99,
        ignorarServicos: opcoes.ignorarServicos ?? IGNORAR_PADRAO,
        servicosTransbordo: opcoes.servicosTransbordo?.length ? opcoes.servicosTransbordo : TRANSBORDO_PADRAO,
        alertaMinutosAntes: opcoes.alertaMinutosAntes ?? 0,
    };
    if (config.intervaloMinutos < 1) throw new Error('O intervalo do alerta precisa ser de 1 minuto ou mais.');
    return config;
}

function configDoAmbiente(credenciais, ambiente = process.env) {
    return criarConfig({
        logon: credenciais.login,
        senha: credenciais.password,
        dominio: (ambiente.FASTWAY_DOMINIO || '').trim() || DOMINIO_PADRAO,
        intervaloMinutos: inteiro(ambiente.INTERVALO_MINUTOS, 3, 'INTERVALO_MINUTOS'),
        limiteReciclagensDia: inteiro(ambiente.LIMITE_RECICLAGENS_DIA, 2, 'LIMITE_RECICLAGENS_DIA'),
        limiarPercentual: inteiro(ambiente.LIMIAR_PERCENTUAL, 99, 'LIMIAR_PERCENTUAL'),
        alertaMinutosAntes: inteiro(ambiente.ALERTA_MINUTOS_ANTES, 0, 'ALERTA_MINUTOS_ANTES'),
        ignorarServicos: ambiente.IGNORAR_SERVICOS == null ? IGNORAR_PADRAO : lista(ambiente.IGNORAR_SERVICOS),
        servicosTransbordo: lista(ambiente.SERVICOS_TRANSBORDO).map(Number).filter(Number.isInteger),
    });
}

/** Alerta quando a lista vai acabar antes das próximas duas verificações (listas curtas pulam os 99%). */
const minutosAlerta = config => config.alertaMinutosAntes || 2 * (config.intervaloMinutos + 1);
const baseUrl = config => `https://${config.dominio}/api/fmi/`;
const ehTransbordo = (config, servicoId) => config.servicosTransbordo.includes(servicoId);

/** Cada item de ignorarServicos é um id exato ou um trecho do nome (sem diferenciar maiúsculas). */
function ignora(config, servicoId, nome) {
    return config.ignorarServicos.some(item => /^\d+$/.test(item)
        ? Number(item) === servicoId
        : String(nome).toLowerCase().includes(item.toLowerCase()));
}

module.exports = { DOMINIO_PADRAO, TRANSBORDO_PADRAO, criarConfig, configDoAmbiente, minutosAlerta, baseUrl, ehTransbordo, ignora };
