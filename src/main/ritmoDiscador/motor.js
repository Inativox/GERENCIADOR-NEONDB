'use strict';
/**
 * Ciclo do Ritmo do Discador: lê serviços e operadores na Fastway, decide pela regra de cada serviço e,
 * com a automação ativa neste PC, ajusta a agressividade. Ajustar altera a Fastway: uma falha nunca é
 * repetida sozinha, a próxima leitura decide de novo.
 */
const { operadoresDe, contar, ocupacao, decidir, somaUra, folgaDe, ehUra, TETO_CALLCENTER } = require('./regras');

const EM_EXECUCAO = 'EM EXECUÇÃO';
const numero = new Intl.NumberFormat('pt-BR');
const pct = valor => (valor == null ? '—' : `${Math.round(valor * 100)}%`);

/** Serviços com exatamente os mesmos operadores (ex.: MP1 e MP3) se marcam como parceiros. */
function marcarParceiros(servicos) {
    const chave = s => s.operadores.map(o => o.id).sort((a, b) => a - b).join(',');
    for (const s of servicos) {
        const minha = chave(s);
        s.parceiros = minha ? servicos.filter(x => x !== s && chave(x) === minha).map(x => x.id) : [];
    }
}

class MotorRitmo {
    /**
     * armazem: configuração deste PC ({ regras(), pausados(), ativa(), teto(), pausar(id, info), retomar(id) }).
     * diario: Diario do dia (eventos e série).
     */
    constructor({ cliente, armazem, diario, relogio = () => new Date() }) {
        Object.assign(this, { cliente, armazem, diario, relogio });
        this.servicos = [];
        this.pendentes = new Map(); // serviço → direção vista na leitura anterior
        this.ultimos = new Map();   // serviço → última agressividade vista ou ajustada (para notar mudança feita na Fastway)
        this.esperados = new Map(); // serviço → { de, para } do ajuste feito na leitura anterior
        this.soma = null; // agressividade somada das URAs em execução, depois dos ajustes desta leitura
        this.lidoEm = null;
        this.lendo = false;
        this.conectado = true;
        this.erro = null;
    }

    async ler() {
        if (this.lendo) return;
        this.lendo = true;
        try {
            const todos = await this.cliente.listarServicos();
            const regras = this.armazem.regras();
            // O teto geral vale para a soma de todas as URAs em execução, com ou sem regra.
            this.soma = somaUra(todos.map(x => ({ ...x, emExecucao: x.status === EM_EXECUCAO })));
            const lista = [];
            for (const s of todos.filter(x => x.status === EM_EXECUCAO || regras[x.id])) {
                const emExecucao = s.status === EM_EXECUCAO;
                const operadores = emExecucao ? operadoresDe(await this.cliente.listarLogados(s.id), this.relogio()) : [];
                lista.push(await this.avaliar(s, emExecucao, operadores, regras[s.id] || null));
            }
            marcarParceiros(lista);
            this.soma = somaUra(lista);
            this.servicos = lista;
            this.conectado = true;
            this.erro = null;
        } catch (erro) {
            this.conectado = false;
            this.erro = erro.message || 'Fastway sem resposta';
            console.warn(`ritmo do discador: leitura falhou (${this.erro})`);
        } finally {
            this.lendo = false;
            this.lidoEm = this.relogio().toISOString();
            this.diario.salvar();
        }
    }

    async avaliar(s, emExecucao, operadores, regra) {
        const ativa = this.armazem.ativa(), contagem = contar(operadores), occ = ocupacao(contagem);
        let agressividade = s.agressividade, pausado = this.armazem.pausados()[s.id] || null;
        if (ativa && regra && emExecucao && !pausado && agressividade != null) pausado = this.conferirMudanca(s, agressividade);
        if (ativa) this.ultimos.set(s.id, agressividade);
        let decisao = decidir({ regra, agressividade, contagem, emExecucao, pausado, pendente: ativa ? this.pendentes.get(s.id) ?? null : null, folga: folgaDe(s.modo, agressividade, this.armazem.teto(), this.soma) });
        if (decisao.tipo === 'teto') decisao = { ...decisao, teto: ehUra(s.modo) ? 'ura' : 'callcenter' };
        if (decisao.tipo === 'pendente' && ativa) this.pendentes.set(s.id, decisao.dir);
        else this.pendentes.delete(s.id);
        if (decisao.tipo === 'ajustar') {
            decisao = await this.ajustar(s, decisao, occ);
            if (decisao.tipo === 'ajustou') { if (ehUra(s.modo)) this.soma += decisao.alvo - agressividade; agressividade = decisao.alvo; }
        }
        this.diario.ponto(s.id, { t: this.relogio().getTime(), ocupacao: occ, agressividade: agressividade ?? 0 });
        return { id: s.id, nome: s.nome, modo: s.modo, emExecucao, agressividade: agressividade ?? 0, limite: s.limite, regra, pausado,
            contagem, ocupacao: occ, decisao, operadores, parceiros: [] };
    }

    /** Agressividade diferente da última vista sem ter sido a automação: pausa o serviço até alguém retomar. */
    conferirMudanca(s, atual) {
        const esperado = this.esperados.get(s.id);
        this.esperados.delete(s.id);
        let de = this.ultimos.get(s.id);
        if (esperado) {
            if (atual === esperado.para) return null;
            if (atual === esperado.de) {
                this.registrar({ tipo: 'falha', servicoId: s.id, servico: s.nome, de: esperado.de, para: esperado.para,
                    texto: `A Fastway aceitou o ajuste para ${numero.format(esperado.para)}, mas a agressividade continua ${numero.format(atual)}.` });
                return null;
            }
            de = esperado.para;
        } else if (de == null || de === atual) return null;
        const pausado = { de, para: atual, em: this.relogio().toISOString() };
        this.armazem.pausar(s.id, pausado);
        this.pendentes.delete(s.id);
        this.registrar({ tipo: 'manual', servicoId: s.id, servico: s.nome, de, para: atual,
            texto: `A agressividade mudou na Fastway de ${numero.format(de)} para ${numero.format(atual)} sem ser pela automação. Ajustes pausados neste serviço até alguém retomar.` });
        return pausado;
    }

    async ajustar(s, decisao, occ) {
        const base = { dir: decisao.dir, de: s.agressividade, alvo: decisao.alvo };
        const dePara = `de ${numero.format(s.agressividade)} para ${numero.format(decisao.alvo)} (ocupação ${pct(occ)})`;
        const evento = { servicoId: s.id, servico: s.nome, de: s.agressividade, para: decisao.alvo, dir: decisao.dir, ocupacao: occ };
        try {
            await this.cliente.ajustarAgressividade(s.id, decisao.alvo);
        } catch (erro) {
            this.registrar({ ...evento, tipo: 'falha', texto: `A Fastway recusou o ajuste ${dePara}: ${erro.message}` });
            return { tipo: 'falhou', ...base, mensagem: erro.message };
        }
        console.warn(`ritmo do discador: ${s.nome} (#${s.id}) ${decisao.dir < 0 ? 'desceu' : 'subiu'} ${dePara}`);
        this.esperados.set(s.id, { de: s.agressividade, para: decisao.alvo });
        this.ultimos.set(s.id, decisao.alvo);
        this.registrar({ ...evento, tipo: 'ajuste', texto: `${decisao.dir < 0 ? 'Desceu' : 'Subiu'} ${dePara}.` });
        return { tipo: 'ajustou', ...base };
    }

    registrar(evento) { return this.diario.registrar(evento); }

    /** Retoma um serviço pausado a partir do valor que está na Fastway agora. */
    retomar(servicoId) {
        this.armazem.retomar(servicoId);
        const s = this.servicos.find(x => x.id === servicoId);
        if (s) { s.pausado = null; this.ultimos.set(servicoId, s.agressividade); }
        this.pendentes.delete(servicoId);
        this.esperados.delete(servicoId);
        return s;
    }

    /** Ligar ou desligar a automação esquece o que foi visto: mudanças feitas enquanto estava desligada não pausam nada. */
    esquecer() {
        this.ultimos.clear();
        this.pendentes.clear();
        this.esperados.clear();
    }

    painel() {
        const regras = this.armazem.regras(), pausados = this.armazem.pausados();
        return {
            lidoEm: this.lidoEm, lendo: this.lendo, conectado: this.conectado, erro: this.erro, soma: this.soma, teto: this.armazem.teto(), tetoCallcenter: TETO_CALLCENTER,
            servicos: this.servicos.map(s => ({ ...s, regra: regras[s.id] || null, pausado: pausados[s.id] || null, serie: this.diario.serie(s.id) })),
            eventos: [...this.diario.eventos()].reverse(),
        };
    }
}

module.exports = { MotorRitmo, EM_EXECUCAO, marcarParceiros };
