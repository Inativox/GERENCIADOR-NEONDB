const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const { Situacao, Acao, classificar, decidir, minutosParaAcabar, percentualConsumido, reciclagensHoje } = require('../src/main/alertaMailing/regras');
const { Estado } = require('../src/main/alertaMailing/estado');

class Relogio {
    constructor(inicio = new Date(2026, 9, 7, 9, 0)) { this.agora = inicio; }
    get funcao() { return () => this.agora; }
    avancar({ minutes = 0, hours = 0, days = 0 }) { this.agora = new Date(this.agora.getTime() + ((days * 24 + hours) * 60 + minutes) * 60000); }
}

function pasta(t) {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'alerta-estado-'));
    t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
    return dir;
}

// --- regras -------------------------------------------------------------------------------
test('percentual conta disponíveis e aguardando e respeita os limites', () => {
    assert.ok(Math.abs(percentualConsumido(43474, 1, 10) - 99.9747) < 1e-3);
    assert.equal(percentualConsumido(0, 0, 0), 100);
    assert.equal(percentualConsumido(100, 150, 0), 0);
});

test('classificar: 99%, esgotado e aguardando tentativa', () => {
    for (const [registros, disp, aguard, esperado] of [[43474, 1, 10, Situacao.QUASE], [217428, 162919, 0, Situacao.OK], [1000, 10, 0, Situacao.QUASE],
        [1000, 11, 0, Situacao.OK], [1000, 0, 0, Situacao.ESGOTADO], [1000, 0, 3, Situacao.QUASE]]) {
        assert.equal(classificar(registros, disp, aguard, 99), esperado, `${registros}/${disp}/${aguard}`);
    }
});

test('reciclagens de hoje nunca ficam negativas', () => {
    assert.equal(reciclagensHoje(3, 1), 2);
    assert.equal(reciclagensHoje(1, 3), 0);
});

test('decidir a ação, inclusive no transbordo que nunca recicla', () => {
    for (const [situacao, rec, esperado] of [[Situacao.OK, 5, Acao.NADA], [Situacao.QUASE, 0, Acao.PODE_RECICLAR], [Situacao.ESGOTADO, 1, Acao.PODE_RECICLAR],
        [Situacao.ESGOTADO, 2, Acao.TROCAR], [Situacao.QUASE, 3, Acao.TROCAR], [Situacao.SEM_MAILING, 0, Acao.VERIFICAR_SERVICO]]) {
        assert.equal(decidir(situacao, rec, 2), esperado);
    }
    for (const [situacao, rec, esperado] of [[Situacao.OK, 0, Acao.NADA], [Situacao.QUASE, 0, Acao.PROXIMA_LISTA], [Situacao.ESGOTADO, 0, Acao.PROXIMA_LISTA],
        [Situacao.ESGOTADO, 2, Acao.PROXIMA_LISTA], [Situacao.SEM_MAILING, 0, Acao.VERIFICAR_SERVICO]]) {
        assert.equal(decidir(situacao, rec, 2, true), esperado);
    }
});

test('listas curtas alertam por tempo até acabar; sem ritmo vale só os 99%', () => {
    assert.equal(minutosParaAcabar(370, 37), 10);
    assert.equal(minutosParaAcabar(370, 0), null);
    assert.equal(minutosParaAcabar(370, null), null);
    assert.equal(classificar(6062, 250, 0, 99, 6.8, 8), Situacao.QUASE);
    assert.equal(classificar(6062, 600, 0, 99, 16.2, 8), Situacao.OK);
    assert.equal(classificar(6062, 250, 0, 99, null, 8), Situacao.OK);
});

// --- estado -------------------------------------------------------------------------------
test('linha de base fica na primeira leitura e o filho herda a da mãe', t => {
    const e = new Estado(path.join(pasta(t), 'estado.json'), new Relogio().funcao);
    assert.equal(e.linhaBase(100, 3), 3);
    assert.equal(e.linhaBase(100, 5), 3);
    assert.ok(e.conhece(100));
    assert.equal(e.linhaBase(200, 2, 100), 3);
    assert.equal(e.linhaBase(300, 4, 999), 4); // mãe desconhecida
});

test('persiste e recarrega no mesmo dia', t => {
    const relogio = new Relogio(), arquivo = path.join(pasta(t), 'estado.json');
    const e = new Estado(arquivo, relogio.funcao);
    e.linhaBase(100, 1);
    e.registrarMailingServico(18, 100);
    e.registrarAviso('QUASE:100:0');
    e.adicionarEvento('QUASE', 'MP PRINCIPAL', 'texto', 18);
    e.salvar();
    const e2 = new Estado(arquivo, relogio.funcao);
    assert.equal(e2.linhaBase(100, 9), 1);
    assert.equal(e2.ultimoMailing(18), 100);
    assert.equal(e2.ultimoAviso('QUASE:100:0').getTime(), new Date(2026, 9, 7, 9, 0).getTime());
    assert.equal(e2.eventos[0].servico, 'MP PRINCIPAL');
});

test('a virada do dia zera tudo menos o último mailing de cada serviço', t => {
    const relogio = new Relogio();
    const e = new Estado(path.join(pasta(t), 'estado.json'), relogio.funcao);
    e.linhaBase(100, 1);
    e.registrarMailingServico(18, 100);
    e.registrarAviso('x');
    e.adicionarEvento('QUASE', 's', 't');
    relogio.avancar({ days: 1 });
    assert.equal(e.conhece(100), false);
    assert.equal(e.linhaBase(100, 3), 3);
    assert.equal(e.ultimoAviso('x'), null);
    assert.deepEqual(e.eventos, []);
    assert.equal(e.ultimoMailing(18), 100);
});

test('arquivo corrompido vira .bak e o estado recomeça', t => {
    const dir = pasta(t), arquivo = path.join(dir, 'estado.json');
    fs.writeFileSync(arquivo, '{ isso não é json');
    const e = new Estado(arquivo, new Relogio().funcao);
    assert.deepEqual(e.eventos, []);
    assert.ok(fs.existsSync(`${arquivo}.bak`));
    e.salvar();
    assert.ok(fs.existsSync(arquivo));
});

test('eventos mais recentes primeiro, com a hora local', t => {
    const relogio = new Relogio();
    const e = new Estado(path.join(pasta(t), 'estado.json'), relogio.funcao);
    e.adicionarEvento('QUASE', 'A', '1');
    relogio.avancar({ minutes: 3 });
    e.adicionarEvento('ESGOTADO', 'B', '2');
    assert.deepEqual(e.eventos.map(x => [x.hora, x.servico]), [['09:03', 'B'], ['09:00', 'A']]);
});

test('evento salvo sem os campos de pendência continua funcionando', t => {
    const arquivo = path.join(pasta(t), 'estado.json');
    fs.writeFileSync(arquivo, JSON.stringify({ dia: '2026-10-07', eventos: [{ hora: '12:26', tipo: 'SEM_MAILING', servico: 'T', texto: 'x', servicoId: 146 }] }));
    const e = new Estado(arquivo, new Relogio().funcao);
    assert.equal(e.eventos[0].status, 'aberta');
    assert.ok(e.eventos[0].id);
    assert.equal(e.resolver(146, 'ok')[0].motivo, 'ok');
});

test('dispensar por id afeta só aquela pendência', t => {
    const e = new Estado(path.join(pasta(t), 'estado.json'), new Relogio().funcao);
    const a = e.adicionarEvento('QUASE', 'A', 't', 1, 10, 0);
    e.adicionarEvento('QUASE', 'B', 't', 2, 20, 0);
    assert.deepEqual(e.dispensar(a.id).map(x => x.servico), ['A']);
    assert.ok(e.foiDispensado(1, 10, 'QUASE'));
    assert.equal(e.foiDispensado(2, 20, 'QUASE'), false);
    assert.deepEqual(e.pendenciasDoServico(2).map(x => x.servico), ['B']);
});

test('limpar histórico esconde só os fechados, continua limpo após reiniciar e não desfaz dispensas', t => {
    const arquivo = path.join(pasta(t), 'estado.json'), relogio = new Relogio();
    const e = new Estado(arquivo, relogio.funcao);
    const resolvido = e.adicionarEvento('QUASE', 'PAG', '99%', 1);
    e.resolver(1, 'lista reciclada');
    const dispensado = e.adicionarEvento('TROCAR', 'MP', 'trocar a lista', 2, 20);
    e.dispensar(dispensado.id);
    const aberto = e.adicionarEvento('TROCAR', 'BMG', 'trocar a lista', 3);
    assert.equal(e.limparHistorico(), 2);
    assert.deepEqual(Object.fromEntries(e.eventos.map(ev => [ev.id, ev.oculto])), { [resolvido.id]: true, [dispensado.id]: true, [aberto.id]: false });
    assert.equal(e.limparHistorico(), 0);
    assert.ok(e.foiDispensado(2, 20, 'TROCAR')); // o lembrete de 15 min continua desligado
    e.salvar();
    assert.deepEqual(new Estado(arquivo, relogio.funcao).eventos.map(ev => ev.oculto), [false, true, true]);
    const novo = e.adicionarEvento('ESGOTADO', 'PAG', 'acabou', 1);
    assert.deepEqual([e.eventos[0].id, e.eventos[0].oculto], [novo.id, false]);
});
