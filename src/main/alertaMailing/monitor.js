'use strict';
/**
 * Ciclo de verificação do Alerta de Mailing: lê a Fastway, classifica cada serviço, notifica, recicla e para
 * (só no PC com a chave ligada) e publica o snapshot. Portado de alerta-mailing/monitor.py.
 */
const { ABERTA, diaLocal } = require('./estado');
const { FastwayErro, FastwayAuthErro, NAO_TABULADOS, restantesDe } = require('./fastwayFmi');
const { Situacao, Acao, arredondar, classificar, decidir, minutosParaAcabar, percentualConsumido, reciclagensHoje } = require('./regras');
const { minutosAlerta, ehTransbordo, ignora } = require('./config');

const MINUTO = 60 * 1000;
const REPETIR_TROCA = 15 * MINUTO, REPETIR_FALHA = 15 * MINUTO, CARENCIA_RECICLAGEM = 10 * MINUTO;
const LEITURA_VALIDA_MIN = 30; // leituras mais distantes que isso não entram no cálculo do ritmo
const STATUS_EM_USO = new Set(['ATIVO', 'AGUARDANDO TENTATIVA']);

const numero = n => String(n).replace(/\B(?=(\d{3})+(?!\d))/g, '.');
const ehErroFastway = erro => erro instanceof FastwayErro && !(erro instanceof FastwayAuthErro);
const validacao = message => Object.assign(new Error(message), { code: 'ALERTA_VALIDACAO' });

/** Serviço parado com a lista ainda no meio foi parado de propósito: não é alerta. */
const emAlerta = linha => linha.acao !== Acao.NADA && !(linha.parado && linha.situacao !== Situacao.ESGOTADO);

function gravidade(linha) {
    if (linha.acao === Acao.TROCAR) return 0;
    return { [Situacao.SEM_MAILING]: 1, [Situacao.ESGOTADO]: 2, [Situacao.QUASE]: 3 }[linha.situacao] ?? 4;
}

const porGravidade = (a, b) => gravidade(a) - gravidade(b) || (a.servico < b.servico ? -1 : a.servico > b.servico ? 1 : 0);

/** [título, texto, nível de cor] da notificação de uma linha em alerta. */
function mensagem(linha) {
    let rec = `reciclagens hoje ${linha.reciclagensHoje}/${linha.limiteReciclagens}`;
    const eta = linha.minutosRestantes != null ? `~${Math.max(1, Math.round(linha.minutosRestantes))} min` : null;
    if (linha.acao === Acao.TROCAR) {
        return [`${linha.servico}: trocar a lista`,
            `${linha.arquivo} já foi reciclada ${linha.reciclagensHoje}x hoje (limite ${linha.limiteReciclagens}). Suba uma lista nova.`, 'vermelho'];
    }
    if (linha.situacao === Situacao.SEM_MAILING) {
        const texto = linha.mailingId == null ? 'Nenhuma lista ativa encontrada para o serviço.'
            : `Última lista: ${linha.arquivo} (${Math.trunc(linha.percentual)}% consumida).`;
        return [`${linha.servico}: sem mailing ativo`, texto, 'vermelho'];
    }
    if (linha.acao === Acao.PROXIMA_LISTA && linha.situacao === Situacao.ESGOTADO) {
        return [`${linha.servico}: iniciar a próxima lista`,
            `${linha.arquivo} acabou. Serviço de transbordo não recicla: inicie a próxima lista do mailing.`, 'vermelho'];
    }
    if (linha.acao === Acao.PROXIMA_LISTA) rec = 'transbordo: não recicla, a próxima lista segue';
    if (linha.situacao === Situacao.ESGOTADO) return [`${linha.servico}: mailing esgotado`, `${linha.arquivo} · ${rec}, pode reciclar`, 'vermelho'];
    const titulo = eta && linha.percentual < 99 ? `${linha.servico}: mailing acabando (${eta})` : `${linha.servico}: mailing em ${Math.trunc(linha.percentual)}%`;
    const detalhe = eta ? ` · acaba em ${eta}` : '';
    return [titulo, `${linha.arquivo} · ${numero(linha.restantes)} restantes de ${numero(linha.registros)}${detalhe} · ${rec}`, 'ambar'];
}

function snapshotParaDict(snapshot) {
    return { linhas: snapshot.linhas.map(linha => ({ ...linha })), eventos: snapshot.eventos.map(evento => ({ ...evento })),
        verificadoEm: snapshot.verificadoEm ? snapshot.verificadoEm.toISOString() : null, conectado: snapshot.conectado, erro: snapshot.erro };
}

const SITUACOES = new Set(Object.values(Situacao)), ACOES = new Set(Object.values(Acao));
const linhaValida = linha => linha && typeof linha === 'object' && Number.isInteger(linha.servicoId) && SITUACOES.has(linha.situacao) && ACOES.has(linha.acao);

class Monitor {
    constructor({ cliente, estado, config, notificador, agora = () => new Date(), podeReciclar = () => false }) {
        Object.assign(this, { _cliente: cliente, _estado: estado, _config: config, _notificador: notificador, _agora: agora, _podeReciclar: podeReciclar });
        this.reciclador = false; // chave "Reciclar e parar automaticamente neste PC", lida a cada ciclo
        this.verificando = false;
        this._fila = Promise.resolve();
        this.snapshot = this._snapshotSalvo();
    }

    /** Última verificação salva em disco, para o painel não abrir vazio enquanto o 1º ciclo roda. */
    _snapshotSalvo() {
        const salvo = this._estado.ultimaVerificacao() || {};
        const linhas = Array.isArray(salvo.linhas) && salvo.linhas.every(linhaValida) ? salvo.linhas.map(linha => ({ ...linha })) : [];
        const verificadoEm = linhas.length && salvo.verificadoEm ? new Date(salvo.verificadoEm) : null;
        return { linhas, eventos: this._estado.eventos, verificadoEm, conectado: true, erro: null };
    }

    /** Uma operação por vez na Fastway: o ciclo e o parar/iniciar do painel nunca se cruzam. */
    _exclusivo(tarefa) {
        const resultado = this._fila.then(tarefa);
        this._fila = resultado.catch(() => {});
        return resultado;
    }

    _salvar() {
        try { this._estado.salvar(); } catch (erro) { console.error('não foi possível salvar o estado do alerta', erro); }
    }

    _comEventos() {
        this.snapshot = { ...this.snapshot, eventos: this._estado.eventos };
    }

    /** Dispensa uma pendência (ou todas, sem id) e tira as notificações dela da central. */
    dispensar(eventoId = null) {
        const fechados = eventoId ? this._estado.dispensar(eventoId) : this._estado.dispensarTodas();
        for (const servicoId of new Set(fechados.map(e => e.servicoId))) this._notificador.remover(servicoId);
        this._salvar();
        this._comEventos();
        return fechados.length;
    }

    /** Tira do histórico os alertas já resolvidos ou dispensados (os que precisam de ação continuam). */
    limparHistorico() {
        const quantos = this._estado.limparHistorico();
        this._salvar();
        this._comEventos();
        return quantos;
    }

    executarCiclo() {
        return this._exclusivo(async () => {
            this.verificando = true;
            let snapshot;
            try {
                snapshot = await this._ciclo();
            } catch (erro) {
                if (erro instanceof FastwayAuthErro) snapshot = this._falha(erro.message, 'falha:auth', 'Login da Fastway recusado');
                else if (erro instanceof FastwayErro) snapshot = this._falha(`Sem conexão com a Fastway: ${erro.message}`, 'falha:conexao', 'Fastway sem resposta');
                else throw erro;
            } finally {
                this.verificando = false;
            }
            this._salvar();
            this.snapshot = snapshot;
            return snapshot;
        });
    }

    // --- ciclo ------------------------------------------------------------
    async _ciclo() {
        this.reciclador = Boolean(this._podeReciclar());
        const todos = await this._cliente.listarServicosAtivos();
        const servicos = todos.filter(s => !ignora(this._config, s.id, s.nome));
        this._resolver(null, 'conexão restabelecida');
        const ids = new Set(servicos.map(s => s.id));
        const ignorados = new Set(todos.map(s => s.id).filter(id => !ids.has(id)));
        const parados = this._paradosForaDeExecucao(ids);
        this._resolverQuemSaiu(ids, ignorados, new Set(parados.map(l => l.servicoId)));
        let cache = null;
        const resumos = async () => (cache ??= await this._cliente.listarMailings());

        const linhas = [...parados];
        for (const servico of servicos) {
            try {
                linhas.push(await this._avaliar(servico, resumos));
            } catch (erro) {
                if (!ehErroFastway(erro)) throw erro;
                console.warn(`serviço ${servico.nome} (${servico.id}) pulado: ${erro.message}`);
            }
        }
        linhas.sort(porGravidade);
        const snapshot = { linhas, eventos: this._estado.eventos, verificadoEm: this._agora(), conectado: true, erro: null };
        const dados = snapshotParaDict(snapshot);
        this._estado.guardarVerificacao({ linhas: dados.linhas, verificadoEm: dados.verificadoEm });
        return snapshot;
    }

    _resolverQuemSaiu(ids, ignorados, idsParados) {
        const abertos = new Set(this._estado.eventos.filter(e => e.status === ABERTA && e.servicoId != null).map(e => e.servicoId));
        for (const servicoId of abertos) {
            if (!ids.has(servicoId) && !idsParados.has(servicoId)) {
                this._resolver(servicoId, ignorados.has(servicoId) ? 'serviço ignorado' : 'serviço não está mais em execução');
            }
        }
    }

    /** Serviços parados pelo app ou pelo painel: o ListarAtivos não traz mais, então ficam com a última linha. */
    _paradosForaDeExecucao(ids) {
        const anteriores = new Map(this.snapshot.linhas.map(linha => [linha.servicoId, linha]));
        const linhas = [];
        for (const servicoId of [...this._estado.parados()].sort((a, b) => a - b)) {
            if (ids.has(servicoId)) this._estado.removerParado(servicoId); // voltou a rodar
            else if (anteriores.has(servicoId) && !ignora(this._config, servicoId, anteriores.get(servicoId).servico)) {
                linhas.push({ ...anteriores.get(servicoId), parado: true });
            }
        }
        return linhas;
    }

    async _avaliar(servico, resumos) {
        const anterior = this._estado.ultimoMailing(servico.id);
        const mailingId = servico.mailingAtivoId || anterior || Monitor.mailingRecente(servico.id, await resumos());
        if (!mailingId) return this._registrar(this._linhaSemMailing(servico));

        const mailing = await this._cliente.obterMailing(mailingId);
        let parentId = null;
        if (!this._estado.conhece(mailing.id) && anterior && anterior !== mailing.id) {
            parentId = (await resumos()).find(r => r.id === mailing.id)?.mailingParentId ?? null;
        }
        this._estado.registrarMailingServico(servico.id, mailing.id);
        const base = this._estado.linhaBase(mailing.id, mailing.quantidadeReciclagem, parentId);

        const linha = this._classificar(servico, mailing, base);
        if (this.reciclador) {
            if (linha.acao === Acao.PODE_RECICLAR) {
                const reciclada = await this._reciclar(servico, mailing, linha, base);
                if (reciclada) return reciclada;
            }
            if (this._esgotouSemReciclagem(linha)) return this._parar(servico, mailing, linha, base);
        }
        return this._registrar(linha);
    }

    _classificar(servico, mailing, base) {
        const ritmo = this._ritmo(mailing);
        const minutos = minutosParaAcabar(restantesDe(mailing), ritmo);
        let situacao = classificar(mailing.registros, mailing.disponiveis, mailing.aguardando, this._config.limiarPercentual, minutos, minutosAlerta(this._config));
        if (servico.mailingAtivoId === 0 && situacao === Situacao.OK) situacao = Situacao.SEM_MAILING;
        const rec = reciclagensHoje(mailing.quantidadeReciclagem, base, this._estado.reciclagensFeitas(mailing.id));
        return this._linha(servico, mailing, situacao, rec, minutos, ritmo == null ? null : arredondar(ritmo, 1));
    }

    /** Registros consumidos por minuto entre leituras (média móvel). Recomeça quando a lista é reciclada ou
     * quando a leitura anterior é velha demais (ex.: de ontem). */
    _ritmo(mailing) {
        const agora = this._agora(), restantes = restantesDe(mailing);
        const anterior = this._estado.leitura(mailing.id);
        if (!anterior) {
            this._estado.registrarLeitura(mailing.id, agora, restantes, null);
            return null;
        }
        let { ritmo } = anterior;
        const minutos = (agora - anterior.quando) / MINUTO;
        if (restantes > anterior.restantes || minutos > LEITURA_VALIDA_MIN) ritmo = null; // reciclada ou leitura antiga
        else if (minutos < 0.5) return ritmo; // releitura logo em seguida (ex.: antes de reciclar): mantém a medição
        else {
            const medido = (anterior.restantes - restantes) / minutos;
            ritmo = ritmo == null ? medido : 0.6 * medido + 0.4 * ritmo;
        }
        this._estado.registrarLeitura(mailing.id, agora, restantes, ritmo);
        return ritmo;
    }

    // --- reciclagem automática (só no PC com a chave ligada) ----------------
    _inicioReciclagem(mailing) {
        const hoje = diaLocal(this._agora());
        const importacao = /^\d{4}-\d{2}-\d{2}/.test(mailing.dataImportacao) ? mailing.dataImportacao.slice(0, 10) : null;
        if (importacao && !Number.isNaN(new Date(`${importacao}T00:00:00`).getTime())) return importacao < hoje ? importacao : hoje;
        return diaLocal(new Date(this._agora().getTime() - 30 * 24 * 60 * MINUTO));
    }

    /** Recicla a lista se for seguro. null = não reciclou agora; segue o alerta normal. */
    async _reciclar(servico, mailing, linha, base) {
        const ultima = this._estado.ultimaTentativaReciclagem(mailing.id);
        if (ultima && this._agora() - ultima < CARENCIA_RECICLAGEM) return null;

        const atual = await this._cliente.obterMailing(mailing.id); // relê: alguém pode ter reciclado agora há pouco
        if (atual.quantidadeReciclagem !== mailing.quantidadeReciclagem || restantesDe(atual) > restantesDe(mailing)) {
            return this._registrar(this._classificar(servico, atual, base));
        }
        this._estado.registrarTentativaReciclagem(mailing.id);
        const hoje = diaLocal(this._agora()), inicio = this._inicioReciclagem(mailing);

        const tabulacoes = await this._cliente.tabulacoesDisponiveis(mailing.id, inicio, hoje);
        if ((tabulacoes[NAO_TABULADOS] || 0) <= 0) return this._nadaParaReciclar(servico, mailing, linha);

        try {
            await this._cliente.reciclar(mailing.id, inicio, hoje);
        } catch (erro) {
            if (!ehErroFastway(erro)) throw erro;
            const texto = `Não foi possível reciclar ${mailing.arquivo} (${erro.message}). Recicle pela Fastway.`;
            this._estado.adicionarEvento('FALHA_RECICLAGEM', servico.nome, texto, servico.id, mailing.id, linha.reciclagensHoje);
            this._notificador.notificar(`${servico.nome}: falha ao reciclar`, texto, servico.id, 'vermelho');
            return linha;
        }
        return this._depoisDeReciclar(servico, mailing, base);
    }

    _nadaParaReciclar(servico, mailing, linha) {
        const chave = `nada-reciclar:${mailing.id}:${linha.reciclagensHoje}`;
        if (linha.situacao === Situacao.ESGOTADO) {
            this._estado.registrarAviso(chave);
            return null; // acabou e não há o que reciclar: segue para a parada automática
        }
        if (!this._estado.ultimoAviso(chave)) {
            this._estado.registrarAviso(chave);
            const texto = `${mailing.arquivo} não tem registros não tabulados para reciclar. Suba uma lista nova.`;
            this._estado.adicionarEvento('NADA_RECICLAR', servico.nome, texto, servico.id, mailing.id, linha.reciclagensHoje);
            this._notificador.notificar(`${servico.nome}: nada para reciclar`, texto, servico.id, 'vermelho');
        }
        return linha;
    }

    async _depoisDeReciclar(servico, mailing, base) {
        this._estado.registrarReciclagem(mailing.id);
        let depois = mailing;
        try { depois = await this._cliente.obterMailing(mailing.id); } catch (erro) { if (!(erro instanceof FastwayErro)) throw erro; }
        const nova = this._classificar(servico, depois, base), limite = this._config.limiteReciclagensDia;
        const voltaram = Math.max(0, restantesDe(depois) - restantesDe(mailing));
        const texto = `${mailing.arquivo} · ${numero(voltaram)} registros voltaram para a discagem · reciclagens hoje ${nova.reciclagensHoje}/${limite}`;
        this._estado.resolver(servico.id, 'lista reciclada automaticamente');
        this._estado.adicionarEvento('RECICLADA', servico.nome, texto, servico.id, mailing.id, nova.reciclagensHoje, 'reciclagem automática');
        this._notificador.notificar(`${servico.nome}: lista reciclada automaticamente (${nova.reciclagensHoje}/${limite})`, texto, servico.id, 'verde');
        console.warn(`mailing ${mailing.id} reciclado automaticamente: ${texto}`);
        return nova.acao !== Acao.NADA ? this._registrar(nova) : nova;
    }

    // --- parada automática (só no PC com a chave ligada; nunca LTDA/PT 2) ---
    /** Lista que acabou e não pode mais ser reciclada hoje: limite do dia ou nenhum NÃO TABULADO. */
    _esgotouSemReciclagem(linha) {
        if (linha.situacao !== Situacao.ESGOTADO || linha.transbordo) return false;
        if (linha.acao === Acao.TROCAR) return true;
        return linha.acao === Acao.PODE_RECICLAR && this._estado.ultimoAviso(`nada-reciclar:${linha.mailingId}:${linha.reciclagensHoje}`) != null;
    }

    /** Para o serviço uma vez por lista no dia: se alguém iniciar de novo com a mesma lista, o app respeita. */
    async _parar(servico, mailing, linha, base) {
        if (this._estado.ultimoAviso(`parado:${servico.id}:${mailing.id}`)) return this._registrar(linha);
        const ultima = this._estado.ultimoAviso(`tentativa-parar:${servico.id}`);
        if (ultima && this._agora() - ultima < CARENCIA_RECICLAGEM) return this._registrar(linha);

        const atual = await this._cliente.obterMailing(mailing.id); // relê: alguém pode ter reciclado ou recarregado agora
        if (restantesDe(atual) > 0 || atual.quantidadeReciclagem !== mailing.quantidadeReciclagem) {
            return this._registrar(this._classificar(servico, atual, base));
        }
        this._estado.registrarAviso(`tentativa-parar:${servico.id}`);
        try {
            await this._cliente.pararServico(servico.id);
        } catch (erro) {
            if (!ehErroFastway(erro)) throw erro;
            const texto = `Não foi possível parar o serviço (${erro.message}). Pare pela Fastway.`;
            this._estado.adicionarEvento('FALHA_PARADA', servico.nome, texto, servico.id, mailing.id, linha.reciclagensHoje);
            this._notificador.notificar(`${servico.nome}: falha ao parar o serviço`, texto, servico.id, 'vermelho');
            return linha;
        }
        this._estado.registrarAviso(`parado:${servico.id}:${mailing.id}`);
        this._estado.registrarParado(servico.id);
        const motivo = linha.acao === Acao.TROCAR ? `já foi reciclada ${linha.reciclagensHoje}x hoje (limite ${linha.limiteReciclagens})` : 'não tem não tabulados para reciclar';
        const texto = `${mailing.arquivo} acabou e ${motivo}. O serviço foi parado: suba uma lista nova e inicie o serviço.`;
        this._estado.adicionarEvento('PARADO', servico.nome, texto, servico.id, mailing.id, linha.reciclagensHoje);
        this._notificador.notificar(`${servico.nome}: serviço parado automaticamente`, texto, servico.id, 'vermelho');
        console.warn(`serviço ${servico.id} parado automaticamente: ${texto}`);
        return { ...linha, parado: true };
    }

    // --- parar/iniciar pelo painel (qualquer PC e qualquer serviço) ---------
    pararServico(servicoId) { return this._mudarServico(servicoId, true); }

    iniciarServico(servicoId) { return this._mudarServico(servicoId, false); }

    async _mudarServico(servicoId, parar) {
        if (!this.snapshot.linhas.some(linha => linha.servicoId === servicoId)) throw validacao(`O serviço ${servicoId} não está no painel.`);
        return this._exclusivo(async () => {
            await (parar ? this._cliente.pararServico(servicoId) : this._cliente.iniciarServico(servicoId));
            console.warn(`serviço ${servicoId} ${parar ? 'parado' : 'iniciado'} pelo painel`);
            if (parar) { // ao iniciar, sai da lista de parados quando o ciclo o vir rodando
                this._estado.registrarParado(servicoId);
                this._salvar();
            }
            this.snapshot = { ...this.snapshot, linhas: this.snapshot.linhas.map(linha => (linha.servicoId === servicoId ? { ...linha, parado: parar } : linha)) };
        });
    }

    static mailingRecente(servicoId, resumos) {
        const candidatos = resumos.filter(r => r.campanhaId === servicoId && STATUS_EM_USO.has(r.status));
        return candidatos.length ? candidatos.reduce((maior, r) => (r.ultimoDiscado > maior.ultimoDiscado ? r : maior)).id : null;
    }

    _linha(servico, mailing, situacao, rec, minutos = null, ritmo = null) {
        const limite = this._config.limiteReciclagensDia, transbordo = ehTransbordo(this._config, servico.id);
        return { servicoId: servico.id, servico: servico.nome, mailingId: mailing.id, arquivo: mailing.arquivo, registros: mailing.registros,
            restantes: restantesDe(mailing), percentual: arredondar(percentualConsumido(mailing.registros, mailing.disponiveis, mailing.aguardando), 2),
            situacao, acao: decidir(situacao, rec, limite, transbordo), reciclagensHoje: rec, limiteReciclagens: limite,
            minutosRestantes: minutos, ritmoPorMinuto: ritmo, transbordo, parado: false };
    }

    _linhaSemMailing(servico) {
        return { servicoId: servico.id, servico: servico.nome, mailingId: null, arquivo: '', registros: 0, restantes: 0, percentual: 0,
            situacao: Situacao.SEM_MAILING, acao: Acao.VERIFICAR_SERVICO, reciclagensHoje: 0, limiteReciclagens: this._config.limiteReciclagensDia,
            minutosRestantes: null, ritmoPorMinuto: null, transbordo: ehTransbordo(this._config, servico.id), parado: false };
    }

    // --- notificação ------------------------------------------------------
    _resolver(servicoId, motivo) {
        if (this._estado.resolver(servicoId, motivo).length) this._notificador.remover(servicoId);
    }

    static motivoResolucao(pendencia, linha) {
        if (pendencia.mailingId !== linha.mailingId) return 'lista trocada';
        if (linha.reciclagensHoje > pendencia.reciclagens) return 'lista reciclada';
        return 'voltou ao normal';
    }

    _registrar(linha) {
        if (linha.acao === Acao.NADA) {
            const pendencias = this._estado.pendenciasDoServico(linha.servicoId);
            if (pendencias.length) this._resolver(linha.servicoId, Monitor.motivoResolucao(pendencias[0], linha));
            return linha;
        }
        let chave, primeiraVez = true;
        if (linha.acao === Acao.TROCAR) {
            if (this._estado.foiDispensado(linha.servicoId, linha.mailingId, 'TROCAR')) return linha;
            chave = `troca:${linha.mailingId}`;
            const ultimo = this._estado.ultimoAviso(chave);
            if (ultimo && this._agora() - ultimo < REPETIR_TROCA) return linha;
            primeiraVez = ultimo == null;
        } else {
            chave = `${linha.situacao}:${linha.servicoId}:${linha.mailingId}:${linha.reciclagensHoje}`;
            if (this._estado.ultimoAviso(chave)) return linha;
        }
        const [titulo, texto, nivel] = mensagem(linha);
        this._estado.registrarAviso(chave);
        if (primeiraVez) {
            const tipo = linha.acao === Acao.TROCAR ? 'TROCAR' : linha.situacao;
            this._estado.adicionarEvento(tipo, linha.servico, texto, linha.servicoId, linha.mailingId, linha.reciclagensHoje);
        }
        this._notificador.notificar(titulo, texto, linha.servicoId, nivel);
        return linha;
    }

    _falha(erro, chave, titulo) {
        console.error(erro);
        const ultimo = this._estado.ultimoAviso(chave);
        if (!ultimo || this._agora() - ultimo >= REPETIR_FALHA) {
            this._estado.registrarAviso(chave);
            this._estado.adicionarEvento('FALHA', 'Fastway', erro);
            const texto = chave === 'falha:auth' ? erro : `Tentando de novo em ${this._config.intervaloMinutos} min.`;
            this._notificador.notificar(titulo, texto, null, 'cinza');
        }
        return { linhas: this.snapshot.linhas, eventos: this._estado.eventos, verificadoEm: this.snapshot.verificadoEm, conectado: false, erro };
    }
}

module.exports = { Monitor, mensagem, snapshotParaDict, emAlerta, gravidade };
