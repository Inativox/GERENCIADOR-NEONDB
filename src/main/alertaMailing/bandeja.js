'use strict';
/**
 * Bandeja do Alerta de Mailing: cor do pior caso, texto do ícone e o desenho do ícone (um disco de status).
 * Portado de alerta-mailing/bandeja.py. Sem Electron aqui: o handler transforma o bitmap em nativeImage.
 */
const { emAlerta } = require('./monitor');
const { Acao, Situacao } = require('./regras');

// A bandeja e as notificações não leem CSS: estas são as cores --accent-* do tema escuro do Gerenciador.
const CORES = Object.freeze({ verde: [0x22, 0xc5, 0x5e], ambar: [0xea, 0xb3, 0x08], vermelho: [0xf4, 0x3f, 0x5e], cinza: [0x80, 0x80, 0x8f] });

function corDoSnapshot(snapshot) {
    if (!snapshot.conectado) return 'cinza';
    const alerta = snapshot.linhas.filter(emAlerta);
    if (alerta.some(l => l.acao === Acao.TROCAR || l.acao === Acao.VERIFICAR_SERVICO || l.situacao === Situacao.ESGOTADO)) return 'vermelho';
    if (alerta.some(l => l.situacao === Situacao.QUASE)) return 'ambar';
    return 'verde';
}

function resumo(snapshot) {
    if (!snapshot.conectado) return 'Alerta de Mailing: sem conexão com a Fastway';
    const alerta = snapshot.linhas.filter(emAlerta);
    if (!alerta.length) return `Alerta de Mailing: tudo ok (${snapshot.linhas.length} serviços)`;
    return `Alerta de Mailing: ${alerta.length} serviço(s) em alerta`.slice(0, 127);
}

/** Disco colorido com borda mais escura, em BGRA pré-multiplicado (o formato de nativeImage.createFromBitmap). */
function desenharDisco(tamanho, cor) {
    const [r, g, b] = CORES[cor] || CORES.cinza;
    const buffer = Buffer.alloc(tamanho * tamanho * 4);
    const centro = (tamanho - 1) / 2, raio = tamanho * 0.42, borda = Math.max(1, tamanho / 16);
    for (let y = 0; y < tamanho; y++) {
        for (let x = 0; x < tamanho; x++) {
            const distancia = Math.hypot(x - centro, y - centro);
            const alfa = Math.min(1, Math.max(0, raio + 0.5 - distancia)); // borda suavizada
            const escuro = distancia > raio - borda ? 0.72 : 1;
            const i = (y * tamanho + x) * 4;
            buffer[i] = Math.round(b * escuro * alfa);
            buffer[i + 1] = Math.round(g * escuro * alfa);
            buffer[i + 2] = Math.round(r * escuro * alfa);
            buffer[i + 3] = Math.round(255 * alfa);
        }
    }
    return buffer;
}

module.exports = { CORES, corDoSnapshot, resumo, desenharDisco };
