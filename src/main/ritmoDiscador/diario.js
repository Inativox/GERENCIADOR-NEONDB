'use strict';
/**
 * Diário do dia do Ritmo do Discador: ajustes, avisos e a série de ocupação e agressividade de cada serviço.
 * Fica num arquivo que recomeça a cada dia; o gráfico da aba mostra só o dia de hoje.
 */
const fs = require('node:fs');
const path = require('node:path');

const MAX_EVENTOS = 500;
const PASSO_SERIE_MS = 55 * 1000; // um ponto por minuto basta para o gráfico; mudança de agressividade sempre entra

const diaDe = data => `${data.getFullYear()}-${String(data.getMonth() + 1).padStart(2, '0')}-${String(data.getDate()).padStart(2, '0')}`;
let sequencia = 0;

class Diario {
    constructor(arquivo, relogio = () => new Date()) {
        this.arquivo = arquivo;
        this.relogio = relogio;
        this.dados = this.carregar();
    }

    vazio() { return { dia: diaDe(this.relogio()), eventos: [], series: {} }; }

    carregar() {
        try {
            const dados = JSON.parse(fs.readFileSync(this.arquivo, 'utf8'));
            if (dados?.dia === diaDe(this.relogio()) && Array.isArray(dados.eventos) && dados.series && typeof dados.series === 'object') return dados;
        } catch { /* sem arquivo ou corrompido: começa o dia do zero */ }
        return this.vazio();
    }

    virarDia() {
        if (this.dados.dia !== diaDe(this.relogio())) this.dados = this.vazio();
    }

    registrar(evento) {
        this.virarDia();
        const agora = this.relogio();
        const registro = { id: `${agora.getTime().toString(36)}-${(sequencia++).toString(36)}`, em: agora.toISOString(), servicoId: null, servico: '', ...evento };
        this.dados.eventos.push(registro);
        if (this.dados.eventos.length > MAX_EVENTOS) this.dados.eventos.splice(0, this.dados.eventos.length - MAX_EVENTOS);
        return registro;
    }

    ponto(servicoId, ponto) {
        this.virarDia();
        const serie = (this.dados.series[servicoId] ||= []);
        const ultimo = serie[serie.length - 1];
        if (ultimo && ponto.t - ultimo.t < PASSO_SERIE_MS && ultimo.agressividade === ponto.agressividade) return;
        serie.push(ponto);
    }

    serie(servicoId) { this.virarDia(); return this.dados.series[servicoId] || []; }

    eventos() { this.virarDia(); return this.dados.eventos; }

    salvar() {
        try {
            fs.mkdirSync(path.dirname(this.arquivo), { recursive: true });
            const temporario = `${this.arquivo}.tmp`;
            fs.writeFileSync(temporario, JSON.stringify(this.dados));
            fs.renameSync(temporario, this.arquivo);
        } catch (erro) {
            console.error(`não foi possível salvar o diário do ritmo do discador: ${erro.message}`);
        }
    }
}

module.exports = { Diario, diaDe };
