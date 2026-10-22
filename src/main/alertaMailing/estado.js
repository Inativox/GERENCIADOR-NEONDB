'use strict';
/**
 * Estado local do dia, salvo em JSON: linhas de base, último mailing por serviço, avisos e pendências.
 * Portado de alerta-mailing/estado.py. Datas internas em ISO (UTC); "hora" dos eventos em HH:MM local.
 */
const fs = require('node:fs');
const path = require('node:path');
const { randomUUID } = require('node:crypto');

const MAX_EVENTOS = 200;
const ABERTA = 'aberta', RESOLVIDA = 'resolvida', DISPENSADA = 'dispensada';
// o último mailing de cada serviço e a última foto do painel valem entre dias; o resto zera
const MANTIDOS_NA_VIRADA = ['ultimoMailingServico', 'ultimaVerificacao', 'leituras'];

const doisDigitos = numero => String(numero).padStart(2, '0');
const diaLocal = data => `${data.getFullYear()}-${doisDigitos(data.getMonth() + 1)}-${doisDigitos(data.getDate())}`;
const horaLocal = data => `${doisDigitos(data.getHours())}:${doisDigitos(data.getMinutes())}`;
const dataOuNulo = valor => (valor ? new Date(valor) : null);

class Estado {
    constructor(caminho, agora = () => new Date(), gerarId = () => randomUUID().replace(/-/g, '').slice(0, 10)) {
        this._caminho = caminho;
        this._agora = agora;
        this._gerarId = gerarId;
        this._dados = this._ler();
        this._garantirDia();
    }

    // --- persistência -------------------------------------------------
    _vazio() {
        return { dia: diaLocal(this._agora()), linhasBase: {}, raiz: {}, reciclagens: {}, tentativasReciclagem: {},
            ultimoMailingServico: {}, parados: {}, avisos: {}, eventos: [] };
    }

    _ler() {
        if (!fs.existsSync(this._caminho)) return this._vazio();
        try {
            const dados = JSON.parse(fs.readFileSync(this._caminho, 'utf8'));
            if (!dados || typeof dados !== 'object' || Array.isArray(dados) || !Array.isArray(dados.eventos ?? [])) throw new Error('formato inválido');
            const completo = { ...this._vazio(), ...dados };
            completo.eventos = completo.eventos.map(evento => this._normalizarEvento(evento));
            return completo;
        } catch (erro) {
            console.warn(`estado do alerta corrompido (${erro.message}); recomeçando`);
            try { fs.renameSync(this._caminho, `${this._caminho}.bak`); } catch { /* sem backup, segue do zero */ }
            return this._vazio();
        }
    }

    _normalizarEvento(evento) {
        return { hora: '', tipo: '', servico: '', texto: '', servicoId: null, id: this._gerarId(), mailingId: null, reciclagens: 0,
            status: ABERTA, motivo: '', horaFim: '', oculto: false, ...evento };
    }

    _garantirDia() {
        if (this._dados.dia === diaLocal(this._agora())) return;
        const mantidos = Object.fromEntries(MANTIDOS_NA_VIRADA.filter(chave => chave in this._dados).map(chave => [chave, this._dados[chave]]));
        this._dados = { ...this._vazio(), ...mantidos };
    }

    salvar() {
        fs.mkdirSync(path.dirname(this._caminho), { recursive: true });
        const temporario = `${this._caminho}.tmp`;
        fs.writeFileSync(temporario, JSON.stringify(this._dados, null, 1), 'utf8');
        fs.renameSync(temporario, this._caminho);
    }

    _secao(nome) {
        this._garantirDia();
        return this._dados[nome];
    }

    // --- linha de base de reciclagens ---------------------------------
    conhece(mailingId) {
        return String(mailingId) in this._secao('linhasBase');
    }

    linhaBase(mailingId, quantidadeAtual, parentId = null) {
        const bases = this._secao('linhasBase'), chave = String(mailingId);
        if (!(chave in bases)) {
            const herdada = parentId != null ? bases[String(parentId)] : undefined;
            bases[chave] = herdada ?? quantidadeAtual;
            if (herdada != null) this._dados.raiz[chave] = this._raiz(parentId);
        }
        return bases[chave];
    }

    _raiz(mailingId) {
        return this._dados.raiz[String(mailingId)] ?? String(mailingId);
    }

    // --- reciclagens feitas por este PC -----------------------------------
    registrarReciclagem(mailingId) {
        const reciclagens = this._secao('reciclagens'), raiz = this._raiz(mailingId);
        (reciclagens[raiz] ??= []).push(this._agora().toISOString());
    }

    reciclagensFeitas(mailingId) {
        return (this._secao('reciclagens')[this._raiz(mailingId)] || []).length;
    }

    registrarTentativaReciclagem(mailingId) {
        this._secao('tentativasReciclagem')[String(mailingId)] = this._agora().toISOString();
    }

    ultimaTentativaReciclagem(mailingId) {
        return dataOuNulo(this._secao('tentativasReciclagem')[String(mailingId)]);
    }

    // --- último mailing por serviço -----------------------------------
    ultimoMailing(servicoId) {
        return this._dados.ultimoMailingServico[String(servicoId)] ?? null;
    }

    registrarMailingServico(servicoId, mailingId) {
        this._dados.ultimoMailingServico[String(servicoId)] = mailingId;
    }

    // --- serviços parados pelo app ou pelo painel (continuam no painel) -------
    registrarParado(servicoId) {
        this._secao('parados')[String(servicoId)] = this._agora().toISOString();
    }

    removerParado(servicoId) {
        delete this._secao('parados')[String(servicoId)];
    }

    parados() {
        return new Set(Object.keys(this._secao('parados')).map(Number));
    }

    // --- última verificação (mostrada logo que o app abre) ---------------
    guardarVerificacao(dados) {
        this._dados.ultimaVerificacao = dados;
    }

    ultimaVerificacao() {
        return this._dados.ultimaVerificacao ?? null;
    }

    // --- leituras para a previsão de término ----------------------------
    leitura(mailingId) {
        const dado = this._dados.leituras?.[String(mailingId)];
        return dado ? { quando: new Date(dado[0]), restantes: Number(dado[1]), ritmo: dado[2] ?? null } : null;
    }

    registrarLeitura(mailingId, quando, restantes, ritmo) {
        (this._dados.leituras ??= {})[String(mailingId)] = [quando.toISOString(), restantes, ritmo];
    }

    // --- avisos ---------------------------------------------------------
    ultimoAviso(chave) {
        return dataOuNulo(this._secao('avisos')[chave]);
    }

    registrarAviso(chave) {
        this._secao('avisos')[chave] = this._agora().toISOString();
    }

    // --- pendências -----------------------------------------------------
    /** Cria uma pendência; a anterior aberta do mesmo serviço é substituída. Com `fechado` (o motivo),
     * o evento já nasce resolvido: é só registro na linha do tempo. */
    adicionarEvento(tipo, servico, texto, servicoId = null, mailingId = null, reciclagens = 0, fechado = null) {
        const eventos = this._secao('eventos');
        this._fechar(e => e.servicoId === servicoId, RESOLVIDA, 'atualizada por um alerta mais novo');
        const hora = horaLocal(this._agora());
        const evento = this._normalizarEvento({ hora, tipo, servico, texto, servicoId, id: this._gerarId(), mailingId, reciclagens,
            ...(fechado ? { status: RESOLVIDA, motivo: fechado, horaFim: hora } : {}) });
        eventos.unshift(evento);
        eventos.splice(MAX_EVENTOS);
        return { ...evento };
    }

    pendenciasDoServico(servicoId) {
        return this._secao('eventos').filter(e => e.servicoId === servicoId && e.status === ABERTA).map(e => ({ ...e }));
    }

    resolver(servicoId, motivo) {
        return this._fechar(e => e.servicoId === servicoId, RESOLVIDA, motivo);
    }

    dispensar(eventoId) {
        return this._fechar(e => e.id === eventoId, DISPENSADA, 'dispensada');
    }

    dispensarTodas() {
        return this._fechar(() => true, DISPENSADA, 'dispensada');
    }

    /** Tira do histórico os alertas já fechados; os abertos continuam. Os registros ficam guardados: o app não
     * volta a avisar o que já avisou e o que foi dispensado continua dispensado. */
    limparHistorico() {
        const fechados = this._secao('eventos').filter(e => e.status !== ABERTA && !e.oculto);
        for (const evento of fechados) evento.oculto = true;
        return fechados.length;
    }

    foiDispensado(servicoId, mailingId, tipo) {
        return this._secao('eventos').some(e => e.status === DISPENSADA && e.servicoId === servicoId && e.mailingId === mailingId && e.tipo === tipo);
    }

    _fechar(filtro, status, motivo) {
        const fechados = [];
        for (const evento of this._secao('eventos')) {
            if (evento.status === ABERTA && filtro(evento)) {
                Object.assign(evento, { status, motivo, horaFim: horaLocal(this._agora()) });
                fechados.push({ ...evento });
            }
        }
        return fechados;
    }

    get eventos() {
        return this._secao('eventos').map(e => ({ ...e }));
    }
}

module.exports = { Estado, ABERTA, RESOLVIDA, DISPENSADA, MAX_EVENTOS, diaLocal, horaLocal };
