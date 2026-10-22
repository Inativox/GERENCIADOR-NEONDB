// Cenários portados de alerta-mailing/tests/test_monitor.py: são a especificação do alerta, inclusive da
// reciclagem e da parada automáticas, que mexem na Fastway de verdade.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const { Monitor, snapshotParaDict } = require('../src/main/alertaMailing/monitor');
const { Estado } = require('../src/main/alertaMailing/estado');
const { criarConfig } = require('../src/main/alertaMailing/config');
const { FastwayErro, FastwayAuthErro } = require('../src/main/alertaMailing/fastwayFmi');
const { Situacao, Acao } = require('../src/main/alertaMailing/regras');

class Relogio {
    constructor() { this.agora = new Date(2026, 9, 7, 9, 0); }
    get funcao() { return () => this.agora; }
    avancar({ minutes = 0, hours = 0 }) { this.agora = new Date(this.agora.getTime() + (hours * 60 + minutes) * 60000); }
}

class ClienteFalso {
    constructor() {
        Object.assign(this, { servicos: [], mailings: new Map(), resumos: [], erro: null, erroMailing: new Map(), chamadas: [], releituras: new Map(),
            tabulacoes: { '-99': 188089, 42: 14, 11: 165 }, recicladas: [], erroReciclar: null, voltam: 600, incrementa: 1, paradas: [], iniciados: [], erroParar: null, fora: new Map() });
    }
    async listarServicosAtivos() { this.chamadas.push('ListarAtivos'); if (this.erro) throw this.erro; return [...this.servicos]; }
    async obterMailing(id) {
        this.chamadas.push(`ListarMailing:${id}`);
        if (this.erroMailing.has(id)) throw this.erroMailing.get(id);
        const fila = this.releituras.get(id);
        if (fila?.length) this.mailings.set(id, fila.shift());
        return this.mailings.get(id);
    }
    async tabulacoesDisponiveis(id, inicio, fim) { this.chamadas.push(`tabulacoesDisponiveis:${id}:${inicio}:${fim}`); return { ...this.tabulacoes }; }
    async reciclar(id, inicio, fim) {
        this.chamadas.push(`reciclarMailing:${id}`);
        this.recicladas.push([id, inicio, fim]);
        if (this.erroReciclar) throw this.erroReciclar;
        const m = this.mailings.get(id);
        this.mailings.set(id, { ...m, disponiveis: m.disponiveis + this.voltam, quantidadeReciclagem: m.quantidadeReciclagem + this.incrementa });
        return { Id: 10, Status: 'Sucesso' };
    }
    async listarMailings() { this.chamadas.push('Listar'); return [...this.resumos]; }
    async pararServico(id) {
        this.chamadas.push(`PararServico:${id}`);
        if (this.erroParar) throw this.erroParar;
        this.paradas.push(id);
        for (const s of this.servicos) if (s.id === id) this.fora.set(id, s);
        this.servicos = this.servicos.filter(s => s.id !== id);
        return { Status: '10' };
    }
    async iniciarServico(id) {
        this.chamadas.push(`IniciarServico:${id}`);
        this.iniciados.push(id);
        this.servicos.push(this.fora.get(id));
        this.fora.delete(id);
        return { Status: '10' };
    }
}

class NotificadorFalso {
    constructor() { this.enviadas = []; this.removidas = []; }
    notificar(titulo, texto, servicoId = null, nivel = 'ambar') { this.enviadas.push([titulo, texto, servicoId, nivel]); }
    remover(servicoId) { this.removidas.push(servicoId); }
    titulos() { return this.enviadas.map(([titulo]) => titulo); }
}

const servico = (id, nome, mailingAtivoId) => ({ id, nome, mailingAtivoId });
const resumo = (id, campanhaId, status, mailingParentId, ultimoDiscado) => ({ id, campanhaId, status, mailingParentId, ultimoDiscado });
const mailing = (id, { registros = 1000, disp = 500, aguard = 0, rec = 0, campanha = 18, arquivo = 'lista.csv', importado = '2026-10-01 09:00:00' } = {}) =>
    ({ id, arquivo, status: 'ATIVO', registros, disponiveis: disp, aguardando: aguard, quantidadeReciclagem: rec, campanhaId: campanha, dataImportacao: importado });

function arquivo(t) {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'alerta-monitor-'));
    t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
    return path.join(dir, 'estado.json');
}

const config = (extra = {}) => criarConfig({ logon: 'u', senha: 's', intervaloMinutos: 3, limiteReciclagensDia: 2, limiarPercentual: 99, ignorarServicos: [], ...extra });

function ctx(t, { podeReciclar = () => false, extra } = {}) {
    const relogio = new Relogio(), cliente = new ClienteFalso(), notif = new NotificadorFalso(), caminho = arquivo(t);
    const estado = new Estado(caminho, relogio.funcao);
    const monitor = new Monitor({ cliente, estado, config: config(extra), notificador: notif, agora: relogio.funcao, podeReciclar });
    return { monitor, cliente, notif, relogio, caminho };
}

function rec(t) {
    const c = ctx(t, { podeReciclar: () => true });
    c.cliente.servicos = [servico(92, 'PAG CADENCIA', 100)];
    return c;
}

const abertas = monitor => monitor.snapshot.eventos.filter(e => e.status === 'aberta');
const ehMesmaHora = (data, esperada) => assert.equal(new Date(data).getTime(), esperada.getTime());

test('serviço OK não notifica', async t => {
    const { monitor, cliente, notif } = ctx(t);
    cliente.servicos = [servico(87, 'PAG PRINCIPAL', 100)];
    cliente.mailings.set(100, mailing(100, { disp: 500 }));
    const snap = await monitor.executarCiclo();
    assert.deepEqual([snap.linhas[0].situacao, snap.linhas[0].acao], [Situacao.OK, Acao.NADA]);
    assert.deepEqual(notif.enviadas, []);
    assert.ok(snap.conectado);
});

test('quase notifica uma vez', async t => {
    const { monitor, cliente, notif, relogio } = ctx(t);
    cliente.servicos = [servico(87, 'PAG PRINCIPAL', 100)];
    cliente.mailings.set(100, mailing(100, { registros: 43474, disp: 1, aguard: 10, arquivo: 'MP-28-09.csv' }));
    await monitor.executarCiclo();
    relogio.avancar({ minutes: 3 });
    await monitor.executarCiclo();
    assert.deepEqual(notif.enviadas, [['PAG PRINCIPAL: mailing em 99%', 'MP-28-09.csv · 11 restantes de 43.474 · reciclagens hoje 0/2', 87, 'ambar']]);
    assert.equal(monitor.snapshot.eventos[0].tipo, 'QUASE');
});

test('quase depois esgotado notifica de novo', async t => {
    const { monitor, cliente, notif } = ctx(t);
    cliente.servicos = [servico(87, 'PAG', 100)];
    cliente.mailings.set(100, mailing(100, { disp: 5 }));
    await monitor.executarCiclo();
    cliente.mailings.set(100, mailing(100, { disp: 0 }));
    await monitor.executarCiclo();
    assert.deepEqual(notif.titulos(), ['PAG: mailing em 99%', 'PAG: mailing esgotado']);
    assert.equal(notif.enviadas[1][3], 'vermelho');
});

test('histórico antigo de reciclagem não conta para hoje', async t => {
    const { monitor, cliente } = ctx(t);
    cliente.servicos = [servico(87, 'PAG', 100)];
    cliente.mailings.set(100, mailing(100, { disp: 0, rec: 8 }));
    const snap = await monitor.executarCiclo();
    assert.deepEqual([snap.linhas[0].acao, snap.linhas[0].reciclagensHoje], [Acao.PODE_RECICLAR, 0]);
});

test('duas reciclagens hoje pedem troca e repetem a cada 15 min', async t => {
    const { monitor, cliente, notif, relogio } = ctx(t);
    cliente.servicos = [servico(87, 'PAG', 100)];
    cliente.mailings.set(100, mailing(100, { disp: 400, rec: 1 }));
    await monitor.executarCiclo();
    cliente.mailings.set(100, mailing(100, { disp: 0, rec: 3 }));
    const snap = await monitor.executarCiclo();
    assert.equal(snap.linhas[0].acao, Acao.TROCAR);
    assert.deepEqual(notif.titulos(), ['PAG: trocar a lista']);
    relogio.avancar({ minutes: 10 });
    await monitor.executarCiclo();
    assert.equal(notif.enviadas.length, 1);
    relogio.avancar({ minutes: 6 });
    await monitor.executarCiclo();
    assert.deepEqual(notif.titulos(), ['PAG: trocar a lista', 'PAG: trocar a lista']);
    assert.deepEqual(monitor.snapshot.eventos.map(e => e.tipo), ['TROCAR']);
});

test('volta a esgotar depois de reciclar e notifica de novo', async t => {
    const { monitor, cliente, notif } = ctx(t);
    cliente.servicos = [servico(87, 'PAG', 100)];
    cliente.mailings.set(100, mailing(100, { disp: 0, rec: 0 }));
    await monitor.executarCiclo();
    cliente.mailings.set(100, mailing(100, { disp: 600, rec: 1 }));
    await monitor.executarCiclo();
    cliente.mailings.set(100, mailing(100, { disp: 0, rec: 1 }));
    await monitor.executarCiclo();
    assert.deepEqual(notif.titulos(), ['PAG: mailing esgotado', 'PAG: mailing esgotado']);
    assert.ok(notif.enviadas[1][1].includes('reciclagens hoje 1/2'));
});

test('serviço sem mailing usa o último conhecido', async t => {
    const { monitor, cliente } = ctx(t);
    cliente.servicos = [servico(18, 'MP PRINCIPAL', 32586)];
    cliente.mailings.set(32586, mailing(32586, { registros: 43474, disp: 300 }));
    await monitor.executarCiclo();
    cliente.servicos = [servico(18, 'MP PRINCIPAL', 0)];
    cliente.mailings.set(32586, mailing(32586, { registros: 43474, disp: 1, aguard: 10 }));
    const snap = await monitor.executarCiclo();
    assert.deepEqual([snap.linhas[0].mailingId, snap.linhas[0].situacao], [32586, Situacao.QUASE]);
    assert.ok(!cliente.chamadas.includes('Listar'));
});

test('serviço sem mailing com a lista ok vira sem mailing', async t => {
    const { monitor, cliente, notif } = ctx(t);
    cliente.servicos = [servico(18, 'MP PRINCIPAL', 0)];
    cliente.resumos = [resumo(500, 18, 'PAUSA', null, '2026-10-07 10:00'), resumo(501, 18, 'ATIVO', null, '2026-10-06 10:00'),
        resumo(502, 18, 'AGUARDANDO TENTATIVA', null, '2026-10-07 08:00'), resumo(503, 99, 'ATIVO', null, '2026-10-07 11:00')];
    cliente.mailings.set(502, mailing(502, { disp: 600, arquivo: 'lista-502.csv' }));
    const linha = (await monitor.executarCiclo()).linhas[0];
    assert.deepEqual([linha.mailingId, linha.situacao, linha.acao], [502, Situacao.SEM_MAILING, Acao.VERIFICAR_SERVICO]);
    assert.deepEqual(notif.enviadas[0].slice(0, 2), ['MP PRINCIPAL: sem mailing ativo', 'Última lista: lista-502.csv (40% consumida).']);
});

test('serviço sem nenhuma lista', async t => {
    const { monitor, cliente, notif } = ctx(t);
    cliente.servicos = [servico(146, 'TESTE', 0)];
    const snap = await monitor.executarCiclo();
    assert.deepEqual([snap.linhas[0].mailingId, snap.linhas[0].situacao], [null, Situacao.SEM_MAILING]);
    assert.deepEqual(notif.titulos(), ['TESTE: sem mailing ativo']);
});

test('mailing filho herda a contagem da mãe', async t => {
    const { monitor, cliente } = ctx(t);
    cliente.servicos = [servico(18, 'MP', 100)];
    cliente.mailings.set(100, mailing(100, { disp: 300, rec: 1 }));
    await monitor.executarCiclo();
    cliente.servicos = [servico(18, 'MP', 200)];
    cliente.mailings.set(200, mailing(200, { disp: 0, rec: 2, arquivo: 'lista - R01' }));
    cliente.resumos = [resumo(200, 18, 'ATIVO', 100, '')];
    assert.equal((await monitor.executarCiclo()).linhas[0].reciclagensHoje, 1);
    cliente.servicos = [servico(18, 'MP', 300)];
    cliente.mailings.set(300, mailing(300, { disp: 0, rec: 3, arquivo: 'lista - R02' }));
    cliente.resumos = [resumo(300, 18, 'ATIVO', 200, '')];
    assert.equal((await monitor.executarCiclo()).linhas[0].acao, Acao.TROCAR);
});

test('lista nova trocada começa do zero', async t => {
    const { monitor, cliente } = ctx(t);
    cliente.servicos = [servico(18, 'MP', 100)];
    cliente.mailings.set(100, mailing(100, { disp: 0, rec: 1 }));
    await monitor.executarCiclo();
    cliente.servicos = [servico(18, 'MP', 400)];
    cliente.mailings.set(400, mailing(400, { disp: 0, rec: 0 }));
    cliente.resumos = [resumo(400, 18, 'ATIVO', null, '')];
    assert.equal((await monitor.executarCiclo()).linhas[0].reciclagensHoje, 0);
});

test('falha de conexão mantém os dados e avisa a cada 15 min', async t => {
    const { monitor, cliente, notif, relogio } = ctx(t);
    cliente.servicos = [servico(87, 'PAG', 100)];
    cliente.mailings.set(100, mailing(100));
    await monitor.executarCiclo();
    cliente.erro = new FastwayErro('Fastway sem resposta (HTTP 500)');
    const snap = await monitor.executarCiclo();
    assert.ok(!snap.conectado && snap.linhas[0].servico === 'PAG' && snap.erro.includes('HTTP 500'));
    relogio.avancar({ minutes: 3 });
    await monitor.executarCiclo();
    assert.deepEqual(notif.titulos(), ['Fastway sem resposta']);
    relogio.avancar({ minutes: 15 });
    await monitor.executarCiclo();
    assert.deepEqual(notif.titulos(), ['Fastway sem resposta', 'Fastway sem resposta']);
    assert.equal(notif.enviadas[0][3], 'cinza');
});

test('credencial recusada', async t => {
    const { monitor, cliente, notif } = ctx(t);
    cliente.erro = new FastwayAuthErro('Login da Fastway recusado: confira o login Fastway salvo no Gerenciador.');
    const snap = await monitor.executarCiclo();
    assert.ok(!snap.conectado && snap.erro.includes('login Fastway salvo'));
    assert.deepEqual(notif.titulos(), ['Login da Fastway recusado']);
});

test('erro em um serviço não derruba os outros', async t => {
    const { monitor, cliente } = ctx(t);
    cliente.servicos = [servico(1, 'A', 10), servico(2, 'B', 20)];
    cliente.erroMailing.set(10, new FastwayErro('Mailing informado nao existe'));
    cliente.mailings.set(20, mailing(20, { disp: 0 }));
    const snap = await monitor.executarCiclo();
    assert.deepEqual(snap.linhas.map(l => l.servico), ['B']);
    assert.ok(snap.conectado);
});

test('serviços ignorados não são consultados nem notificados', async t => {
    const { monitor, cliente, notif } = ctx(t, { extra: { ignorarServicos: ['TESTE'] } });
    cliente.servicos = [servico(146, 'TESTE - ONBOARDING', 0), servico(87, 'PAG', 100)];
    cliente.mailings.set(100, mailing(100));
    const snap = await monitor.executarCiclo();
    assert.deepEqual(snap.linhas.map(l => l.servico), ['PAG']);
    assert.deepEqual(notif.enviadas, []);
    assert.ok(!cliente.chamadas.includes('Listar'));
});

test('ordena por gravidade', async t => {
    const { monitor, cliente } = ctx(t);
    cliente.servicos = [servico(1, 'OK', 10), servico(2, 'QUASE', 20), servico(3, 'ESGOTADO', 30), servico(4, 'SEM', 0)];
    for (const [id, m] of [[10, mailing(10)], [20, mailing(20, { disp: 5 })], [30, mailing(30, { disp: 0 })]]) cliente.mailings.set(id, m);
    assert.deepEqual((await monitor.executarCiclo()).linhas.map(l => l.servico), ['SEM', 'ESGOTADO', 'QUASE', 'OK']);
});

test('estado salvo a cada ciclo', async t => {
    const { monitor, cliente, caminho } = ctx(t);
    cliente.servicos = [servico(87, 'PAG', 100)];
    cliente.mailings.set(100, mailing(100, { rec: 4 }));
    await monitor.executarCiclo();
    assert.deepEqual(JSON.parse(fs.readFileSync(caminho, 'utf8')).linhasBase, { 100: 4 });
});

test('snapshot vira JSON', async t => {
    const { monitor, cliente } = ctx(t);
    cliente.servicos = [servico(87, 'PAG', 100)];
    cliente.mailings.set(100, mailing(100, { disp: 0 }));
    const d = snapshotParaDict(await monitor.executarCiclo());
    JSON.stringify(d);
    assert.equal(d.linhas[0].situacao, 'ESGOTADO');
    ehMesmaHora(d.verificadoEm, new Date(2026, 9, 7, 9, 0));
});

// --- pendências -------------------------------------------------------------------------------------
test('reciclar resolve a pendência e tira a notificação', async t => {
    const { monitor, cliente, notif } = ctx(t);
    cliente.servicos = [servico(87, 'PAG', 100)];
    cliente.mailings.set(100, mailing(100, { disp: 0, rec: 0 }));
    await monitor.executarCiclo();
    assert.equal(abertas(monitor).length, 1);
    cliente.mailings.set(100, mailing(100, { disp: 600, rec: 1 }));
    await monitor.executarCiclo();
    assert.deepEqual(abertas(monitor), []);
    const e = monitor.snapshot.eventos[0];
    assert.deepEqual([e.status, e.motivo, e.horaFim], ['resolvida', 'lista reciclada', '09:00']);
    assert.deepEqual(notif.removidas, [87]);
});

test('trocar a lista resolve com o motivo', async t => {
    const { monitor, cliente } = ctx(t);
    cliente.servicos = [servico(87, 'PAG', 100)];
    cliente.mailings.set(100, mailing(100, { disp: 0 }));
    await monitor.executarCiclo();
    cliente.servicos = [servico(87, 'PAG', 101)];
    cliente.mailings.set(101, mailing(101, { disp: 900 }));
    await monitor.executarCiclo();
    assert.equal(monitor.snapshot.eventos[0].motivo, 'lista trocada');
});

test('alerta mais grave substitui o anterior', async t => {
    const { monitor, cliente } = ctx(t);
    cliente.servicos = [servico(87, 'PAG', 100)];
    cliente.mailings.set(100, mailing(100, { disp: 5 }));
    await monitor.executarCiclo();
    cliente.mailings.set(100, mailing(100, { disp: 0 }));
    await monitor.executarCiclo();
    assert.deepEqual(monitor.snapshot.eventos.map(e => [e.tipo, e.status]), [['ESGOTADO', 'aberta'], ['QUASE', 'resolvida']]);
});

test('dispensar tira da lista e da central', async t => {
    const { monitor, cliente, notif } = ctx(t);
    cliente.servicos = [servico(1, 'A', 10), servico(2, 'B', 20)];
    cliente.mailings.set(10, mailing(10, { disp: 0 }));
    cliente.mailings.set(20, mailing(20, { disp: 5 }));
    await monitor.executarCiclo();
    const primeiro = abertas(monitor)[0];
    assert.equal(monitor.dispensar(primeiro.id), 1);
    assert.equal(abertas(monitor).length, 1);
    assert.deepEqual(notif.removidas, [primeiro.servicoId]);
    assert.equal(monitor.dispensar(), 1);
    assert.deepEqual(abertas(monitor), []);
    await monitor.executarCiclo();
    assert.equal(notif.enviadas.length, 2);
    assert.deepEqual(abertas(monitor), []);
});

test('dispensar trocar desliga o lembrete de 15 min', async t => {
    const { monitor, cliente, notif, relogio } = ctx(t);
    cliente.servicos = [servico(87, 'PAG', 100)];
    cliente.mailings.set(100, mailing(100, { disp: 400, rec: 0 }));
    await monitor.executarCiclo();
    cliente.mailings.set(100, mailing(100, { disp: 0, rec: 2 }));
    await monitor.executarCiclo();
    monitor.dispensar();
    relogio.avancar({ minutes: 30 });
    await monitor.executarCiclo();
    assert.deepEqual(notif.titulos(), ['PAG: trocar a lista']);
});

test('serviço que parou resolve a pendência', async t => {
    const { monitor, cliente } = ctx(t);
    cliente.servicos = [servico(87, 'PAG', 100)];
    cliente.mailings.set(100, mailing(100, { disp: 0 }));
    await monitor.executarCiclo();
    cliente.servicos = [];
    await monitor.executarCiclo();
    assert.equal(monitor.snapshot.eventos[0].motivo, 'serviço não está mais em execução');
});

test('conexão de volta resolve a falha', async t => {
    const { monitor, cliente, notif } = ctx(t);
    cliente.erro = new FastwayErro('caiu');
    await monitor.executarCiclo();
    assert.equal(abertas(monitor)[0].tipo, 'FALHA');
    cliente.erro = null;
    await monitor.executarCiclo();
    assert.deepEqual(abertas(monitor), []);
    assert.equal(monitor.snapshot.eventos[0].motivo, 'conexão restabelecida');
    assert.deepEqual(notif.removidas, [null]);
});

test('serviço que passou a ser ignorado resolve a pendência', async t => {
    const relogio = new Relogio(), cliente = new ClienteFalso(), notif = new NotificadorFalso(), caminho = arquivo(t);
    const estado = new Estado(caminho, relogio.funcao);
    cliente.servicos = [servico(146, 'TESTE - ONBOARDING', 0)];
    await new Monitor({ cliente, estado, config: config(), notificador: notif, agora: relogio.funcao }).executarCiclo();
    const segundo = new Monitor({ cliente, estado, config: config({ ignorarServicos: ['TESTE'] }), notificador: notif, agora: relogio.funcao });
    await segundo.executarCiclo();
    assert.equal(segundo.snapshot.eventos[0].motivo, 'serviço ignorado');
});

test('ao reiniciar mostra a última verificação antes do primeiro ciclo', async t => {
    const relogio = new Relogio(), cliente = new ClienteFalso(), notif = new NotificadorFalso(), caminho = arquivo(t);
    cliente.servicos = [servico(87, 'PAG', 100), servico(1, 'A', 10)];
    cliente.mailings.set(100, mailing(100, { disp: 0 }));
    cliente.mailings.set(10, mailing(10));
    await new Monitor({ cliente, estado: new Estado(caminho, relogio.funcao), config: config(), notificador: notif, agora: relogio.funcao }).executarCiclo();
    const reiniciado = new Monitor({ cliente: new ClienteFalso(), estado: new Estado(caminho, relogio.funcao), config: config(), notificador: notif, agora: relogio.funcao });
    assert.deepEqual(reiniciado.snapshot.linhas.map(l => [l.servico, l.situacao]), [['PAG', Situacao.ESGOTADO], ['A', Situacao.OK]]);
    ehMesmaHora(reiniciado.snapshot.verificadoEm, new Date(2026, 9, 7, 9, 0));
});

// --- reciclagem automática -------------------------------------------------------------------------
test('com a chave ligada recicla em 99% e avisa', async t => {
    const { monitor, cliente, notif } = rec(t);
    cliente.mailings.set(100, mailing(100, { disp: 5, arquivo: 'pag.csv' }));
    const snap = await monitor.executarCiclo();
    assert.deepEqual(cliente.recicladas, [[100, '2026-10-01', '2026-10-07']]);
    assert.ok(cliente.chamadas.includes('tabulacoesDisponiveis:100:2026-10-01:2026-10-07'));
    assert.deepEqual(notif.titulos(), ['PAG CADENCIA: lista reciclada automaticamente (1/2)']);
    assert.ok(notif.enviadas[0][1].includes('600 registros voltaram'));
    assert.equal(notif.enviadas[0][3], 'verde');
    assert.deepEqual([snap.linhas[0].situacao, snap.linhas[0].reciclagensHoje], [Situacao.OK, 1]);
    assert.deepEqual([snap.eventos[0].tipo, snap.eventos[0].status, snap.eventos[0].motivo], ['RECICLADA', 'resolvida', 'reciclagem automática']);
    assert.ok(monitor.reciclador);
});

test('sem a chave só alerta', async t => {
    const { monitor, cliente, notif } = ctx(t);
    cliente.servicos = [servico(92, 'PAG', 100)];
    cliente.mailings.set(100, mailing(100, { disp: 0 }));
    await monitor.executarCiclo();
    assert.deepEqual(cliente.recicladas, []);
    assert.deepEqual(notif.titulos(), ['PAG: mailing esgotado']);
    assert.equal(monitor.reciclador, false);
});

test('no máximo duas reciclagens por dia e depois para e pede lista nova', async t => {
    const { monitor, cliente, notif, relogio } = rec(t);
    cliente.mailings.set(100, mailing(100, { disp: 0 }));
    for (let i = 0; i < 2; i++) {
        await monitor.executarCiclo();
        cliente.mailings.set(100, { ...cliente.mailings.get(100), disponiveis: 0, aguardando: 0 });
        relogio.avancar({ minutes: 11 });
    }
    const snap = await monitor.executarCiclo();
    assert.equal(cliente.recicladas.length, 2);
    assert.equal(snap.linhas[0].acao, Acao.TROCAR);
    assert.equal(notif.titulos().at(-1), 'PAG CADENCIA: serviço parado automaticamente');
});

test('o limite vale mesmo se a Fastway não contar a reciclagem', async t => {
    const { monitor, cliente, relogio } = rec(t);
    cliente.incrementa = 0;
    cliente.mailings.set(100, mailing(100, { disp: 0 }));
    for (let i = 0; i < 4; i++) {
        await monitor.executarCiclo();
        cliente.mailings.set(100, { ...cliente.mailings.get(100), disponiveis: 0 });
        relogio.avancar({ minutes: 11 });
    }
    assert.equal(cliente.recicladas.length, 2);
});

test('falha ao reciclar avisa e espera 10 min para tentar de novo', async t => {
    const { monitor, cliente, notif, relogio } = rec(t);
    cliente.erroReciclar = new FastwayErro('Fastway sem resposta (HTTP 500)');
    cliente.mailings.set(100, mailing(100, { disp: 0 }));
    const snap = await monitor.executarCiclo();
    assert.deepEqual(notif.titulos(), ['PAG CADENCIA: falha ao reciclar']);
    assert.deepEqual([snap.eventos[0].tipo, snap.eventos[0].status], ['FALHA_RECICLAGEM', 'aberta']);
    assert.equal(snap.linhas[0].reciclagensHoje, 0);
    relogio.avancar({ minutes: 5 });
    await monitor.executarCiclo();
    assert.equal(cliente.recicladas.length, 1);
    cliente.erroReciclar = null;
    relogio.avancar({ minutes: 6 });
    await monitor.executarCiclo();
    assert.equal(cliente.recicladas.length, 2);
    assert.ok(notif.titulos().at(-1).endsWith('reciclada automaticamente (1/2)'));
});

test('não recicla se alguém acabou de reciclar', async t => {
    const { monitor, cliente, notif } = rec(t);
    cliente.mailings.set(100, mailing(100, { disp: 0, rec: 0 }));
    cliente.releituras.set(100, [mailing(100, { disp: 0, rec: 0 }), mailing(100, { disp: 700, rec: 1 })]);
    await monitor.executarCiclo();
    assert.deepEqual(cliente.recicladas, []);
    assert.ok(!notif.titulos().join(' ').includes('reciclada automaticamente'));
});

test('nada para reciclar avisa e não recicla', async t => {
    const { monitor, cliente, notif } = rec(t);
    cliente.tabulacoes = { 42: 14, 11: 165 };
    cliente.mailings.set(100, mailing(100, { disp: 4 }));
    const snap = await monitor.executarCiclo();
    assert.deepEqual(cliente.recicladas, []);
    assert.deepEqual(notif.titulos(), ['PAG CADENCIA: nada para reciclar']);
    assert.equal(snap.eventos[0].tipo, 'NADA_RECICLAR');
});

test('recicla a lista do serviço que ficou sem mailing ativo', async t => {
    const { monitor, cliente } = rec(t);
    cliente.mailings.set(100, mailing(100, { disp: 300 }));
    await monitor.executarCiclo();
    cliente.servicos = [servico(92, 'PAG CADENCIA', 0)];
    cliente.mailings.set(100, mailing(100, { registros: 43474, disp: 1, aguard: 10 }));
    await monitor.executarCiclo();
    assert.deepEqual(cliente.recicladas.map(r => r[0]), [100]);
});

test('a chave é lida a cada ciclo', async t => {
    const chave = { ligada: false };
    const { monitor, cliente } = ctx(t, { podeReciclar: () => chave.ligada });
    cliente.servicos = [servico(92, 'PAG', 100)];
    cliente.mailings.set(100, mailing(100, { disp: 0 }));
    await monitor.executarCiclo();
    assert.deepEqual(cliente.recicladas, []);
    chave.ligada = true;
    await monitor.executarCiclo();
    assert.deepEqual(cliente.recicladas.map(r => r[0]), [100]);
    assert.ok(monitor.reciclador);
});

// --- transbordo (LTDA 120, PT2 62): nunca recicla, segue para a próxima lista ----------------------
test('transbordo não recicla e pede a próxima lista', async t => {
    const { monitor, cliente, notif, relogio } = rec(t);
    cliente.servicos = [servico(120, 'LTDA', 100)];
    cliente.mailings.set(100, mailing(100, { registros: 1000, disp: 5, arquivo: 'ltda.csv' }));
    let snap = await monitor.executarCiclo();
    assert.equal(snap.linhas[0].acao, Acao.PROXIMA_LISTA);
    assert.ok(snap.linhas[0].transbordo);
    assert.deepEqual(notif.enviadas, [['LTDA: mailing em 99%', 'ltda.csv · 5 restantes de 1.000 · transbordo: não recicla, a próxima lista segue', 120, 'ambar']]);

    cliente.servicos = [servico(120, 'LTDA', 0)];
    cliente.mailings.set(100, mailing(100, { registros: 1000, disp: 0, arquivo: 'ltda.csv' }));
    relogio.avancar({ minutes: 11 });
    snap = await monitor.executarCiclo();
    assert.deepEqual(cliente.recicladas, []);
    assert.ok(!cliente.chamadas.some(c => c.includes('tabulacoes')));
    assert.deepEqual([snap.linhas[0].situacao, snap.linhas[0].acao], [Situacao.ESGOTADO, Acao.PROXIMA_LISTA]);
    assert.deepEqual(notif.enviadas.at(-1), ['LTDA: iniciar a próxima lista', 'ltda.csv acabou. Serviço de transbordo não recicla: inicie a próxima lista do mailing.', 120, 'vermelho']);

    cliente.servicos = [servico(120, 'LTDA', 101)];
    cliente.mailings.set(101, mailing(101, { disp: 900 }));
    snap = await monitor.executarCiclo();
    assert.equal(snap.linhas[0].acao, Acao.NADA);
    assert.deepEqual([snap.eventos[0].tipo, snap.eventos[0].status, snap.eventos[0].motivo], ['ESGOTADO', 'resolvida', 'lista trocada']);
    assert.ok(snap.eventos.every(e => e.status === 'resolvida'));
});

test('transbordo não afeta os outros serviços', async t => {
    const { monitor, cliente } = rec(t);
    cliente.servicos = [servico(62, 'PT 2', 100), servico(92, 'PAG CADENCIA', 200)];
    cliente.mailings.set(100, mailing(100, { disp: 0 }));
    cliente.mailings.set(200, mailing(200, { disp: 0 }));
    const snap = await monitor.executarCiclo();
    assert.deepEqual(cliente.recicladas.map(r => r[0]), [200]);
    assert.deepEqual(Object.fromEntries(snap.linhas.map(l => [l.servicoId, l.transbordo])), { 62: true, 92: false });
});

// --- listas curtas --------------------------------------------------------------------------------
test('lista curta alerta antes de esgotar', async t => {
    const { monitor, cliente, notif, relogio } = ctx(t);
    cliente.servicos = [servico(107, 'MP PRINCIPAL 2', 100)];
    cliente.mailings.set(100, mailing(100, { registros: 6062, disp: 450 }));
    await monitor.executarCiclo();
    assert.deepEqual(notif.enviadas, []);
    relogio.avancar({ minutes: 4 });
    cliente.mailings.set(100, mailing(100, { registros: 6062, disp: 290 }));
    const linha = (await monitor.executarCiclo()).linhas[0];
    assert.equal(linha.situacao, Situacao.QUASE);
    assert.ok(linha.percentual < 99);
    assert.equal(linha.minutosRestantes, 7.2);
    assert.equal(notif.enviadas[0][0], 'MP PRINCIPAL 2: mailing acabando (~7 min)');
    assert.ok(notif.enviadas[0][1].includes('acaba em ~7 min'));
});

test('lista longa no mesmo ritmo não alerta', async t => {
    const { monitor, cliente, notif, relogio } = ctx(t);
    cliente.servicos = [servico(87, 'PAG PRINCIPAL', 100)];
    cliente.mailings.set(100, mailing(100, { registros: 217428, disp: 150000 }));
    await monitor.executarCiclo();
    relogio.avancar({ minutes: 4 });
    cliente.mailings.set(100, mailing(100, { registros: 217428, disp: 149852 }));
    const snap = await monitor.executarCiclo();
    assert.equal(snap.linhas[0].situacao, Situacao.OK);
    assert.deepEqual(notif.enviadas, []);
});

test('a reciclagem zera o ritmo', async t => {
    const { monitor, cliente, relogio } = ctx(t);
    cliente.servicos = [servico(107, 'MP', 100)];
    cliente.mailings.set(100, mailing(100, { registros: 6062, disp: 450 }));
    await monitor.executarCiclo();
    relogio.avancar({ minutes: 4 });
    cliente.mailings.set(100, mailing(100, { registros: 6062, disp: 3000, rec: 1 }));
    const linha = (await monitor.executarCiclo()).linhas[0];
    assert.deepEqual([linha.minutosRestantes, linha.situacao], [null, Situacao.OK]);
});

test('com a chave ligada recicla a lista curta antes de esgotar', async t => {
    const { monitor, cliente, relogio } = rec(t);
    cliente.mailings.set(100, mailing(100, { registros: 6319, disp: 450 }));
    await monitor.executarCiclo();
    relogio.avancar({ minutes: 4 });
    cliente.mailings.set(100, mailing(100, { registros: 6319, disp: 290 }));
    await monitor.executarCiclo();
    assert.deepEqual(cliente.recicladas.map(r => r[0]), [100]);
});

// --- previsão de término em todos os serviços ----------------------------------------------------
test('previsão também em lista longa', async t => {
    const { monitor, cliente, relogio } = ctx(t);
    cliente.servicos = [servico(87, 'PAG PRINCIPAL', 100)];
    cliente.mailings.set(100, mailing(100, { registros: 217428, disp: 150000 }));
    const primeira = (await monitor.executarCiclo()).linhas[0];
    assert.deepEqual([primeira.ritmoPorMinuto, primeira.minutosRestantes], [null, null]);
    relogio.avancar({ minutes: 4 });
    cliente.mailings.set(100, mailing(100, { registros: 217428, disp: 149852 }));
    const linha = (await monitor.executarCiclo()).linhas[0];
    assert.deepEqual([linha.ritmoPorMinuto, linha.minutosRestantes, linha.situacao], [37, 4050.1, Situacao.OK]);
});

test('lista parada tem ritmo zero e sem previsão', async t => {
    const { monitor, cliente, relogio } = ctx(t);
    cliente.servicos = [servico(87, 'PAG', 100)];
    cliente.mailings.set(100, mailing(100, { disp: 500 }));
    await monitor.executarCiclo();
    relogio.avancar({ minutes: 4 });
    const linha = (await monitor.executarCiclo()).linhas[0];
    assert.deepEqual([linha.ritmoPorMinuto, linha.minutosRestantes], [0, null]);
});

test('a previsão continua depois de reiniciar', async t => {
    const relogio = new Relogio(), notif = new NotificadorFalso(), cliente = new ClienteFalso(), caminho = arquivo(t);
    cliente.servicos = [servico(87, 'PAG', 100)];
    cliente.mailings.set(100, mailing(100, { registros: 10000, disp: 5000 }));
    await new Monitor({ cliente, estado: new Estado(caminho, relogio.funcao), config: config(), notificador: notif, agora: relogio.funcao }).executarCiclo();
    relogio.avancar({ minutes: 4 });
    cliente.mailings.set(100, mailing(100, { registros: 10000, disp: 4800 }));
    const reiniciado = new Monitor({ cliente, estado: new Estado(caminho, relogio.funcao), config: config(), notificador: notif, agora: relogio.funcao });
    assert.equal((await reiniciado.executarCiclo()).linhas[0].ritmoPorMinuto, 50);
});

test('leitura antiga não entra no ritmo', async t => {
    const { monitor, cliente, relogio } = ctx(t);
    cliente.servicos = [servico(87, 'PAG', 100)];
    cliente.mailings.set(100, mailing(100, { registros: 10000, disp: 5000 }));
    await monitor.executarCiclo();
    relogio.avancar({ hours: 15 });
    cliente.mailings.set(100, mailing(100, { registros: 10000, disp: 4990 }));
    assert.equal((await monitor.executarCiclo()).linhas[0].ritmoPorMinuto, null);
});

// --- parar serviço: automático (fora LTDA 120 e PT2 62) e manual --------------------------------------
const PEDE_LISTA = 'O serviço foi parado: suba uma lista nova e inicie o serviço.';

/** 1ª leitura com a lista no meio; depois ela aparece reciclada 2x hoje (por outra pessoa) e esgotada. */
async function esgotouNoLimite(monitor, cliente, mailingId = 100, arquivoLista = 'pag.csv') {
    cliente.mailings.set(mailingId, mailing(mailingId, { disp: 500, rec: 0, arquivo: arquivoLista }));
    await monitor.executarCiclo();
    cliente.mailings.set(mailingId, mailing(mailingId, { disp: 0, rec: 2, arquivo: arquivoLista }));
    return monitor.executarCiclo();
}

test('para o serviço quando a lista esgota no limite de reciclagens', async t => {
    const { monitor, cliente, notif, relogio } = rec(t);
    let snap = await esgotouNoLimite(monitor, cliente);
    assert.deepEqual(cliente.paradas, [92]);
    assert.deepEqual(cliente.recicladas, []);
    const linha = snap.linhas[0];
    assert.ok(linha.parado);
    assert.deepEqual([linha.situacao, linha.acao], [Situacao.ESGOTADO, Acao.TROCAR]);
    assert.deepEqual(notif.enviadas, [['PAG CADENCIA: serviço parado automaticamente', `pag.csv acabou e já foi reciclada 2x hoje (limite 2). ${PEDE_LISTA}`, 92, 'vermelho']]);
    assert.deepEqual([snap.eventos[0].tipo, snap.eventos[0].status], ['PARADO', 'aberta']);

    relogio.avancar({ minutes: 16 });
    snap = await monitor.executarCiclo();
    assert.deepEqual(snap.linhas.map(l => [l.servicoId, l.parado]), [[92, true]]);
    assert.equal(notif.enviadas.length, 1);
    assert.deepEqual([snap.eventos[0].tipo, snap.eventos[0].status], ['PARADO', 'aberta']);

    cliente.servicos = [servico(92, 'PAG CADENCIA', 101)];
    cliente.mailings.set(101, mailing(101, { disp: 900 }));
    snap = await monitor.executarCiclo();
    assert.equal(snap.linhas[0].parado, false);
    assert.deepEqual([snap.eventos[0].tipo, snap.eventos[0].status, snap.eventos[0].motivo], ['PARADO', 'resolvida', 'lista trocada']);

    cliente.servicos = [];
    assert.deepEqual((await monitor.executarCiclo()).linhas, []);
});

test('para quando não há nada para reciclar', async t => {
    const { monitor, cliente, notif } = rec(t);
    cliente.tabulacoes = { 42: 14 };
    cliente.mailings.set(100, mailing(100, { disp: 0, arquivo: 'pag.csv' }));
    const snap = await monitor.executarCiclo();
    assert.deepEqual(cliente.recicladas, []);
    assert.deepEqual(cliente.paradas, [92]);
    assert.deepEqual(notif.enviadas, [['PAG CADENCIA: serviço parado automaticamente', `pag.csv acabou e não tem não tabulados para reciclar. ${PEDE_LISTA}`, 92, 'vermelho']]);
    assert.equal(snap.eventos[0].tipo, 'PARADO');
});

test('nada para reciclar em 99% para assim que acabar', async t => {
    const { monitor, cliente, notif, relogio } = rec(t);
    cliente.tabulacoes = { 42: 14 };
    cliente.mailings.set(100, mailing(100, { disp: 4 }));
    await monitor.executarCiclo();
    assert.deepEqual(cliente.paradas, []);
    relogio.avancar({ minutes: 3 });
    cliente.mailings.set(100, mailing(100, { disp: 0 }));
    await monitor.executarCiclo();
    assert.deepEqual(cliente.paradas, [92]);
    assert.deepEqual(notif.titulos(), ['PAG CADENCIA: nada para reciclar', 'PAG CADENCIA: serviço parado automaticamente']);
});

test('transbordo nunca para sozinho', async t => {
    const { monitor, cliente } = rec(t);
    cliente.servicos = [servico(120, 'LTDA', 100), servico(62, 'PT 2', 200)];
    cliente.mailings.set(100, mailing(100, { disp: 0 }));
    cliente.mailings.set(200, mailing(200, { disp: 0, rec: 5 }));
    await monitor.executarCiclo();
    assert.deepEqual(cliente.paradas, []);
    assert.ok(!cliente.chamadas.some(c => c.startsWith('PararServico')));
});

test('sem a chave não para', async t => {
    const { monitor, cliente, notif } = ctx(t);
    cliente.servicos = [servico(92, 'PAG CADENCIA', 100)];
    await esgotouNoLimite(monitor, cliente);
    assert.deepEqual(cliente.paradas, []);
    assert.deepEqual(notif.titulos(), ['PAG CADENCIA: trocar a lista']);
});

test('para uma vez só por lista', async t => {
    const { monitor, cliente, relogio } = rec(t);
    await esgotouNoLimite(monitor, cliente);
    await cliente.iniciarServico(92);
    for (let i = 0; i < 3; i++) {
        relogio.avancar({ minutes: 11 });
        await monitor.executarCiclo();
    }
    assert.deepEqual(cliente.paradas, [92]);
});

test('não para se a lista voltou a ter registros', async t => {
    const { monitor, cliente } = rec(t);
    cliente.mailings.set(100, mailing(100, { disp: 500, rec: 0 }));
    await monitor.executarCiclo();
    cliente.mailings.set(100, mailing(100, { disp: 0, rec: 2 }));
    cliente.releituras.set(100, [mailing(100, { disp: 0, rec: 2 }), mailing(100, { disp: 800, rec: 2 })]);
    await monitor.executarCiclo();
    assert.deepEqual(cliente.paradas, []);
});

test('falha ao parar avisa e tenta de novo depois de 10 min', async t => {
    const { monitor, cliente, notif, relogio } = rec(t);
    cliente.erroParar = new FastwayErro('Fastway sem resposta (HTTP 500)');
    const snap = await esgotouNoLimite(monitor, cliente);
    assert.deepEqual(notif.enviadas.at(-1), ['PAG CADENCIA: falha ao parar o serviço', 'Não foi possível parar o serviço (Fastway sem resposta (HTTP 500)). Pare pela Fastway.', 92, 'vermelho']);
    assert.deepEqual([snap.eventos[0].tipo, snap.eventos[0].status], ['FALHA_PARADA', 'aberta']);
    assert.equal(snap.linhas[0].parado, false);
    relogio.avancar({ minutes: 5 });
    await monitor.executarCiclo();
    assert.equal(cliente.chamadas.filter(c => c === 'PararServico:92').length, 1);
    cliente.erroParar = null;
    relogio.avancar({ minutes: 6 });
    await monitor.executarCiclo();
    assert.deepEqual(cliente.paradas, [92]);
    assert.equal(notif.titulos().at(-1), 'PAG CADENCIA: serviço parado automaticamente');
});

test('serviço parado continua no painel depois de reiniciar', async t => {
    const relogio = new Relogio(), cliente = new ClienteFalso(), caminho = arquivo(t);
    const monitor = new Monitor({ cliente, estado: new Estado(caminho, relogio.funcao), config: config(), notificador: new NotificadorFalso(), agora: relogio.funcao, podeReciclar: () => true });
    cliente.servicos = [servico(92, 'PAG CADENCIA', 100)];
    await esgotouNoLimite(monitor, cliente);
    const reiniciado = new Monitor({ cliente, estado: new Estado(caminho, relogio.funcao), config: config(), notificador: new NotificadorFalso(), agora: relogio.funcao });
    assert.deepEqual(reiniciado.snapshot.linhas.map(l => [l.servicoId, l.parado]), [[92, true]]);
    relogio.avancar({ minutes: 3 });
    const snap = await reiniciado.executarCiclo();
    assert.deepEqual(snap.linhas.map(l => [l.servicoId, l.parado]), [[92, true]]);
});

test('parar e iniciar pelo painel em qualquer serviço', async t => {
    const { monitor, cliente, notif } = ctx(t);
    cliente.servicos = [servico(120, 'LTDA', 100)];
    cliente.mailings.set(100, mailing(100, { disp: 500 }));
    await monitor.executarCiclo();
    await monitor.pararServico(120);
    assert.deepEqual(cliente.paradas, [120]);
    assert.ok(monitor.snapshot.linhas[0].parado);
    let snap = await monitor.executarCiclo();
    assert.deepEqual(snap.linhas.map(l => [l.servicoId, l.parado]), [[120, true]]);
    assert.deepEqual(notif.enviadas, []);
    await monitor.iniciarServico(120);
    assert.deepEqual(cliente.iniciados, [120]);
    assert.equal(monitor.snapshot.linhas[0].parado, false);
    snap = await monitor.executarCiclo();
    assert.deepEqual(snap.linhas.map(l => [l.servicoId, l.parado]), [[120, false]]);
});

test('o painel só para serviço que está nele', async t => {
    const { monitor, cliente } = ctx(t);
    cliente.servicos = [servico(92, 'PAG', 100)];
    cliente.mailings.set(100, mailing(100, { disp: 500 }));
    await monitor.executarCiclo();
    await assert.rejects(monitor.pararServico(999), error => error.code === 'ALERTA_VALIDACAO');
    assert.deepEqual(cliente.paradas, []);
});

test('falha ao parar pelo painel chega ao painel', async t => {
    const { monitor, cliente } = ctx(t);
    cliente.servicos = [servico(92, 'PAG', 100)];
    cliente.mailings.set(100, mailing(100, { disp: 500 }));
    await monitor.executarCiclo();
    cliente.erroParar = new FastwayErro('Servico nao encontrado');
    await assert.rejects(monitor.pararServico(92), /nao encontrado/);
    assert.equal(monitor.snapshot.linhas[0].parado, false);
});

test('parar pelo painel espera o ciclo em andamento', async t => {
    const { monitor, cliente } = ctx(t);
    cliente.servicos = [servico(92, 'PAG', 100)];
    cliente.mailings.set(100, mailing(100, { disp: 500 }));
    await monitor.executarCiclo();
    let liberar;
    const lento = cliente.listarServicosAtivos.bind(cliente);
    cliente.listarServicosAtivos = () => new Promise(resolve => { liberar = () => resolve(lento()); });
    const ciclo = monitor.executarCiclo();
    const parada = monitor.pararServico(92);
    await new Promise(setImmediate);
    assert.ok(!cliente.chamadas.includes('PararServico:92'), 'não fala com a Fastway no meio do ciclo');
    liberar();
    await ciclo;
    await parada;
    assert.deepEqual(cliente.paradas, [92]);
});

// --- limpar histórico ---------------------------------------------------------------------------------
test('limpar histórico atualiza o painel e salva', async t => {
    const { monitor, cliente, caminho } = ctx(t);
    cliente.servicos = [servico(87, 'PAG', 100)];
    cliente.mailings.set(100, mailing(100, { disp: 5 }));
    await monitor.executarCiclo();
    cliente.mailings.set(100, mailing(100, { disp: 900 }));
    await monitor.executarCiclo();
    assert.equal(monitor.limparHistorico(), 1);
    assert.deepEqual(monitor.snapshot.eventos.map(e => e.oculto), [true]);
    assert.deepEqual(JSON.parse(fs.readFileSync(caminho, 'utf8')).eventos.map(e => e.oculto), [true]);
});
