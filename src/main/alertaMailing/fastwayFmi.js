'use strict';
/**
 * Cliente da API FMI da Fastway: leituras, reciclagem de listas e parar/iniciar serviços.
 * Portado de alerta-mailing/fastway_client.py. Chamadas que alteram a Fastway nunca são repetidas sozinhas.
 */
const CODIGOS_OK = new Set(['10', 'SUCESSO']);
// igual à tela de reciclagem da Fastway usada pela operação: só a tabulação "NÃO TABULADOS" marcada
const NAO_TABULADOS = -99;
const FILTRO_RECICLAGEM = [{ tabulacao: [NAO_TABULADOS] }];

class FastwayErro extends Error {
    constructor(message) { super(message); this.name = 'FastwayErro'; }
}

class FastwayAuthErro extends FastwayErro {
    constructor(message) { super(message); this.name = 'FastwayAuthErro'; }
}

const mascarar = texto => String(texto).replace(/(api_senha=)[^&\s'"]*/g, '$1***');
const inteiro = valor => Number.parseInt(valor, 10) || 0;
const restantesDe = mailing => Math.max(0, mailing.disponiveis) + Math.max(0, mailing.aguardando);
const respostaOk = dados => ['Status', 'STATUS', 'Id', 'ID'].some(chave => CODIGOS_OK.has(String(dados[chave] ?? '').trim().toUpperCase()));

function mensagemDe(dados) {
    for (const chave of ['Mensagem', 'MENSAGEM', 'mensagem']) if (dados[chave]) return String(dados[chave]);
    return JSON.stringify(dados).slice(0, 200);
}

/** "YYYY-MM-DD" (data local) para o formato do período da Fastway, "dd/mm/yyyy". */
const dataFastway = dia => dia.split('-').reverse().join('/');
const periodo = (inicio, fim) => `${dataFastway(inicio)} - ${dataFastway(fim)}`;

function mailingDe(m) {
    return {
        id: inteiro(m.id),
        arquivo: String(m.arquivo || `mailing ${m.id}`),
        status: String(m.status || ''),
        registros: inteiro(m.registros),
        disponiveis: inteiro(m.total_registros_disponiveis),
        aguardando: inteiro(m.total_registros_aguardando_tentativa),
        quantidadeReciclagem: inteiro(m.quantidade_reciclagem),
        campanhaId: inteiro(m.campanha_id),
        dataImportacao: String(m.data_importacao || ''), // "2026-09-29 09:11:40.99" como vem da Fastway
    };
}

function createFastwayFmi({ baseUrl, logon, senha, http = require('axios'), intervaloMinMs = 5000,
    esperasRetryMs = [15000, 30000, 45000], timeoutMs = 60000,
    sleep = ms => new Promise(resolve => setTimeout(resolve, ms)), relogio = () => performance.now() }) {
    const base = baseUrl.replace(/\/+$/, '') + '/';
    let ultimaChamada = null;

    async function respeitarIntervalo() {
        if (ultimaChamada != null) {
            const falta = intervaloMinMs - (relogio() - ultimaChamada);
            if (falta > 0) await sleep(falta);
        }
        ultimaChamada = relogio();
    }

    async function tentar(url, params) {
        let resposta;
        try {
            resposta = await http.get(url, { params, timeout: timeoutMs, validateStatus: () => true, responseType: 'text', transformResponse: [dados => dados] });
        } catch (erro) {
            return { erro: mascarar(`${erro.code || erro.name || 'Erro'}: ${erro.message}`) };
        }
        if (resposta.status === 401) throw new FastwayAuthErro('Login da Fastway recusado: confira o login Fastway salvo no Gerenciador.');
        if (resposta.status >= 500) return { erro: `HTTP ${resposta.status}` };
        if (resposta.status >= 400) throw new FastwayErro(`HTTP ${resposta.status} em ${url.slice(base.length)}`);
        let dados;
        try { dados = JSON.parse(resposta.data); } catch { return { erro: 'resposta não é JSON' }; }
        if (!dados || typeof dados !== 'object' || Array.isArray(dados)) throw new FastwayErro('resposta inesperada');
        if (!respostaOk(dados)) throw new FastwayErro(mensagemDe(dados));
        return { dados };
    }

    async function get(caminho, params, repetir = true) {
        const url = base + caminho;
        const query = { api_logon: logon, api_senha: senha, ...params };
        let ultimoErro = '';
        for (const espera of [0, ...(repetir ? esperasRetryMs : [])]) {
            if (espera) {
                console.warn(`Fastway falhou (${ultimoErro}); nova tentativa em ${Math.round(espera / 1000)}s`);
                await sleep(espera);
            }
            await respeitarIntervalo();
            const { dados, erro } = await tentar(url, query);
            if (dados) return dados;
            ultimoErro = erro;
        }
        throw new FastwayErro(`Fastway sem resposta (${ultimoErro})`);
    }

    return {
        async listarServicosAtivos() {
            const dados = await get('servico/index.php', { api_comando: 'ListarAtivos' });
            return (dados.SERVICOS || []).map(s => ({
                id: inteiro(s.id), nome: String(s.nome || `Serviço ${s.id}`).trim(), mailingAtivoId: inteiro(s.mailing_ativo_id),
            }));
        },
        async obterMailing(mailingId) {
            const dados = await get('mailing/index.php', { api_comando: 'ListarMailing', mailing_id: mailingId });
            const itens = dados.Mailing || [];
            if (!itens.length) throw new FastwayErro(`Mailing ${mailingId} não retornado pela Fastway`);
            return mailingDe(itens[0]);
        },
        /** Quantidade por tabulação reciclável no período, como na tela da Fastway (ex.: {-99: 188089, 42: 14}). */
        async tabulacoesDisponiveis(mailingId, inicio, fim) {
            const dados = await get('mailingReciclar/index.php', { api_comando: 'tabulacoesDisponiveis', id: mailingId, periodo: periodo(inicio, fim) });
            return Object.fromEntries((dados.Resultado || [])
                .filter(r => /^-?\d+$/.test(String(r.id ?? '')))
                .map(r => [inteiro(r.id), inteiro(r.quantidade)]));
        },
        /** Recicla só NÃO TABULADOS. Altera a Fastway: nunca é repetida automaticamente. */
        async reciclar(mailingId, inicio, fim) {
            console.warn(`reciclando mailing ${mailingId} (período ${periodo(inicio, fim)})`);
            return get('mailingReciclar/index.php', { api_comando: 'reciclarMailing', id: mailingId, periodo: periodo(inicio, fim), reciclarCheckId: JSON.stringify(FILTRO_RECICLAGEM) }, false);
        },
        /** Para o serviço (a discagem dele). Altera a Fastway: nunca é repetida automaticamente. */
        pararServico: servicoId => get('servico/index.php', { api_comando: 'PararServico', servico_id: servicoId }, false),
        /** Inicia o serviço. Altera a Fastway: nunca é repetida automaticamente. */
        iniciarServico: servicoId => get('servico/index.php', { api_comando: 'IniciarServico', servico_id: servicoId }, false),
        async listarMailings() {
            const dados = await get('mailing/index.php', { api_comando: 'Listar' });
            return (dados.Mailings || []).map(m => ({
                id: inteiro(m.id), campanhaId: inteiro(m.campanha_id), status: String(m.status || ''),
                mailingParentId: m.mailing_parent_id ? inteiro(m.mailing_parent_id) : null, ultimoDiscado: String(m.life_discados_data_ultimo || ''),
            }));
        },
    };
}

module.exports = { createFastwayFmi, FastwayErro, FastwayAuthErro, NAO_TABULADOS, FILTRO_RECICLAGEM, mascarar, restantesDe, periodo };
