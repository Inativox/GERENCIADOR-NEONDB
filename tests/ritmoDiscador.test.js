// Ritmo do Discador: as regras de ocupação e o ciclo que ajusta a agressividade na Fastway de verdade.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const { operadoresDe, contar, ocupacao, validarRegra, decidir } = require('../src/main/ritmoDiscador/regras');
const { MotorRitmo } = require('../src/main/ritmoDiscador/motor');
const { Diario } = require('../src/main/ritmoDiscador/diario');
const { createFastwayFmi, FastwayErro } = require('../src/main/alertaMailing/fastwayFmi');

const REGRA = { desce: 80, sobe: 40, passo: 10, minimo: 20, maximo: 100 };
const ops = (falando, livres = 0, pausa = 0, outros = 0) => [
    ...Array.from({ length: falando }, () => ({ estado: 'falando' })), ...Array.from({ length: livres }, () => ({ estado: 'livre' })),
    ...Array.from({ length: pausa }, () => ({ estado: 'pausa' })), ...Array.from({ length: outros }, () => ({ estado: 'outro' }))];

test('ocupação é em atendimento ÷ (em atendimento + livres), como no painel da Fastway', () => {
    const c = contar(ops(15, 3, 8, 1));
    assert.deepEqual(c, { falando: 15, livres: 3, pausa: 8, outros: 1, logados: 27 });
    assert.equal(ocupacao(c), 15 / 18);
    assert.equal(Math.round(ocupacao(contar(ops(7, 2, 2, 3))) * 100), 78, 'painel do C6: 7 em atendimento, 2 em pausa, 14 logados = 78%');
    assert.equal(ocupacao(contar(ops(6, 0, 3))), 1, '6 falando e 3 em pausa: 100%, não 67%');
    assert.equal(ocupacao(contar(ops(0, 0, 4))), null, 'todos em pausa: sem ocupação');
    assert.equal(ocupacao(contar([])), null);
});

test('estado e tempo de cada operador vêm só dos campos de status', () => {
    const agora = new Date(2026, 10, 6, 10, 5, 0);
    const lista = operadoresDe([
        { id: 1, nome: 'Ana', status: 'EM ATENDIMENTO', complemento: 'RECEPTIVO', segundos: '95', horaStatus: '' },
        { id: 2, nome: 'Bruno', status: 'CONECTADO', complemento: '', segundos: null, horaStatus: '2026-11-06 10:03:30.12' },
        { id: 3, nome: 'Carla', status: 'EM PAUSA', complemento: 'PAUSA ADM', segundos: 700, horaStatus: '' },
        { id: 4, nome: 'Diego', status: 'MODO MANUAL', complemento: '', segundos: 12, horaStatus: '' },
    ], agora);
    assert.deepEqual(lista, [
        { id: 1, nome: 'Ana', estado: 'falando', detalhe: 'RECEPTIVO', segundos: 95 },
        { id: 2, nome: 'Bruno', estado: 'livre', detalhe: '', segundos: 90 },
        { id: 3, nome: 'Carla', estado: 'pausa', detalhe: 'PAUSA ADM', segundos: 700 },
        { id: 4, nome: 'Diego', estado: 'outro', detalhe: 'MODO MANUAL', segundos: 12 },
    ]);
});

test('o cliente FMI descarta hash de senha, token e CPF dos operadores', async () => {
    const http = { get: async () => ({ status: 200, data: JSON.stringify({ Status: '10', Total: 1, Operadores: [{ id: 190, logon: 'ana', nome: 'Ana Coelho', status: 'EM ATENDIMENTO',
        status_complemento: 'ATIVO', tempo_status_segundos: 33, hora_status: '2026-11-06 10:03:33', senha_ha1: 'x', senha_ha2: 'y', api_token: 'z', cpf: '123' }] }) }) };
    const cliente = createFastwayFmi({ baseUrl: 'https://fastway.test/api/fmi/', logon: 'l', senha: 's', http, intervaloMinMs: 0 });
    const [o] = await cliente.listarLogados(18);
    assert.deepEqual(Object.keys(o).sort(), ['complemento', 'horaStatus', 'id', 'nome', 'segundos', 'status']);
    assert.equal(JSON.stringify(o).includes('senha'), false);
});

test('validação da regra recusa faixas cruzadas e avisa quando o máximo passa do limite', () => {
    assert.deepEqual(validarRegra(REGRA, 600), { regra: REGRA, aviso: null });
    assert.match(validarRegra({ ...REGRA, sobe: 80 }).erro, /menor que/);
    assert.match(validarRegra({ ...REGRA, desce: 101 }).erro, /1% a 100%/);
    assert.match(validarRegra({ ...REGRA, passo: 0 }).erro, /passo/);
    assert.match(validarRegra({ ...REGRA, minimo: 200 }).erro, /mínimo/);
    assert.match(validarRegra({ ...REGRA, passo: 'dez' }).erro, /inteiros/);
    assert.match(validarRegra(REGRA, 50).aviso, /limite de chamadas do serviço \(50\)/);
});

test('decisão: lotado desce, ocioso sobe, só ajusta na segunda leitura e respeita mínimo e máximo', () => {
    const base = { regra: REGRA, agressividade: 50, emExecucao: true, pausado: null };
    assert.deepEqual(decidir({ ...base, contagem: contar(ops(9, 1)) }), { tipo: 'pendente', dir: -1, alvo: 40 });
    assert.deepEqual(decidir({ ...base, contagem: contar(ops(9, 1)), pendente: -1 }), { tipo: 'ajustar', dir: -1, alvo: 40 });
    assert.deepEqual(decidir({ ...base, contagem: contar(ops(9, 1)), pendente: 1 }), { tipo: 'pendente', dir: -1, alvo: 40 }, 'direção trocada recomeça a contagem');
    const faixa = { ...base, regra: { ...REGRA, sobe: 60, desce: 90 } };
    assert.equal(decidir({ ...faixa, contagem: contar(ops(6, 4)) }).tipo, 'mantem', '60% exatos mantém');
    assert.equal(decidir({ ...faixa, contagem: contar(ops(9, 1)) }).tipo, 'mantem', '90% exatos mantém');
    assert.equal(decidir({ ...faixa, contagem: contar(ops(5, 4)) }).tipo, 'pendente', '56% sobe');
    assert.equal(decidir({ ...faixa, contagem: contar(ops(10, 1)) }).tipo, 'pendente', '91% desce');
    assert.deepEqual(decidir({ ...base, contagem: contar(ops(2, 8)) }), { tipo: 'pendente', dir: 1, alvo: 60 });
    assert.deepEqual(decidir({ ...base, contagem: contar(ops(6, 4)) }), { tipo: 'mantem' });
    assert.deepEqual(decidir({ ...base, agressividade: 20, contagem: contar(ops(10)) }), { tipo: 'limite', dir: -1 });
    assert.deepEqual(decidir({ ...base, agressividade: 95, contagem: contar(ops(1, 9)) }), { tipo: 'pendente', dir: 1, alvo: 100 }, 'o passo para no máximo');
    assert.deepEqual(decidir({ ...base, agressividade: 300, contagem: contar(ops(1, 9)) }), { tipo: 'acimaDoMaximo', dir: -1, alvo: 100 }, 'acima do máximo nunca sobe: desce para ele');
});

test('decisão: com mais da metade em pausa não sobe; sem regra, parado, pausado e sem gente não mexem', () => {
    const base = { regra: REGRA, agressividade: 50, emExecucao: true, pausado: null };
    assert.deepEqual(decidir({ ...base, contagem: contar(ops(1, 3, 7)) }), { tipo: 'muitaPausa', dir: 1 }, '25% dos disponíveis, mas 7 de 11 em pausa');
    assert.equal(decidir({ ...base, contagem: contar(ops(0, 0, 5)) }).tipo, 'semgente', 'todos em pausa não mexe');
    assert.deepEqual(decidir({ ...base, contagem: contar(ops(9, 0, 1)) }).tipo, 'pendente', 'pausa não impede descer');
    assert.equal(decidir({ ...base, regra: null, contagem: contar(ops(9, 1)) }).tipo, 'semregra');
    assert.equal(decidir({ ...base, emExecucao: false, contagem: contar([]) }).tipo, 'parado');
    assert.equal(decidir({ ...base, pausado: { de: 50, para: 80 }, contagem: contar(ops(9, 1)) }).tipo, 'pausado');
    assert.equal(decidir({ ...base, contagem: contar([]) }).tipo, 'semgente');
});

test('teto geral: a subida para no que cabe e, sem espaço, não sobe; descer continua livre', () => {
    const base = { regra: REGRA, agressividade: 50, emExecucao: true, pausado: null, contagem: contar(ops(2, 8)) };
    assert.deepEqual(decidir({ ...base, folga: 4 }), { tipo: 'pendente', dir: 1, alvo: 54 });
    assert.deepEqual(decidir({ ...base, folga: 0 }), { tipo: 'teto', dir: 1 });
    assert.deepEqual(decidir({ ...base, folga: -169 }), { tipo: 'teto', dir: 1 }, 'acima do teto por mudança manual: não sobe');
    assert.deepEqual(decidir({ ...base, folga: -169, contagem: contar(ops(9, 1)) }), { tipo: 'pendente', dir: -1, alvo: 40 }, 'descer não depende do teto');
});

// --- motor ----------------------------------------------------------------------------------------------
class FastwayFalsa {
    constructor() {
        this.servicos = [{ id: 52, nome: 'MERCADO PAGO 3', status: 'EM EXECUÇÃO', modo: 'POWER', agressividade: 50, limite: 100 },
            { id: 18, nome: 'MERCADO PAGO 1', status: 'EM EXECUÇÃO', modo: 'URA', agressividade: 20, limite: 800 },
            { id: 99, nome: 'PARADO SEM REGRA', status: 'PARADA', modo: 'URA', agressividade: 5, limite: 10 }];
        this.logados = { 52: estados(9, 1), 18: estados(9, 1) };
        this.ajustes = [];
        this.erroAjuste = null;
        this.aplica = true;
    }
    async listarServicos() { return this.servicos.map(s => ({ ...s })); }
    async listarLogados(id) { return this.logados[id] || []; }
    async ajustarAgressividade(id, valor) {
        this.ajustes.push([id, valor]);
        if (this.erroAjuste) throw this.erroAjuste;
        if (this.aplica) this.servicos.find(s => s.id === id).agressividade = valor;
        return { Status: '10' };
    }
}
function estados(falando, livres = 0, pausa = 0) {
    let id = 0;
    const um = (status, complemento = '') => ({ id: ++id, nome: `Op ${id}`, status, complemento, segundos: 10, horaStatus: '' });
    return [...Array.from({ length: falando }, () => um('EM ATENDIMENTO', 'ATIVO')), ...Array.from({ length: livres }, () => um('CONECTADO')),
        ...Array.from({ length: pausa }, () => um('EM PAUSA', 'PAUSA ADM'))];
}
function montar({ ativa = true, regras = { 52: REGRA }, teto = 100000 } = {}) {
    const pasta = fs.mkdtempSync(path.join(os.tmpdir(), 'ritmo-'));
    let agora = new Date(2026, 10, 6, 10, 0, 0);
    const relogio = () => agora;
    const cfg = { ativa, regras, teto, pausados: {} };
    const armazem = { regras: () => cfg.regras, pausados: () => cfg.pausados, ativa: () => cfg.ativa, teto: () => cfg.teto,
        pausar: (id, info) => { cfg.pausados[id] = info; }, retomar: id => { delete cfg.pausados[id]; } };
    const fastway = new FastwayFalsa();
    const diario = new Diario(path.join(pasta, 'dia.json'), relogio);
    const motor = new MotorRitmo({ cliente: fastway, armazem, diario, relogio });
    const ler = async () => { await motor.ler(); agora = new Date(agora.getTime() + 30000); return motor; };
    const servico = id => motor.servicos.find(s => s.id === id);
    return { motor, fastway, cfg, ler, servico, diario, pasta };
}

test('motor: mostra serviços em execução e os com regra; ajusta só na segunda leitura lotada', async () => {
    const { fastway, ler, servico, motor } = montar();
    await ler();
    assert.deepEqual(motor.servicos.map(s => s.id), [52, 18], 'parado sem regra fica de fora');
    assert.deepEqual(servico(52).decisao, { tipo: 'pendente', dir: -1, alvo: 40 });
    assert.equal(servico(18).decisao.tipo, 'semregra');
    assert.deepEqual(servico(52).parceiros, [18], 'mesmos operadores');
    assert.deepEqual(fastway.ajustes, []);
    await ler();
    assert.deepEqual(fastway.ajustes, [[52, 40]]);
    assert.deepEqual(servico(52).decisao, { tipo: 'ajustou', dir: -1, de: 50, alvo: 40 });
    assert.equal(servico(52).agressividade, 40);
    await ler();
    assert.equal(servico(52).pausado, null, 'o próprio ajuste não conta como mudança manual');
    assert.deepEqual(servico(52).decisao, { tipo: 'pendente', dir: -1, alvo: 30 });
    const ajustes = motor.painel().eventos.filter(e => e.tipo === 'ajuste');
    assert.equal(ajustes.length, 1);
    assert.match(ajustes[0].texto, /^Desceu de 50 para 40 \(ocupação 90%\)\.$/);
    assert.deepEqual({ modo: ajustes[0].modo, contagem: ajustes[0].contagem, faixa: ajustes[0].faixa, corte: ajustes[0].corte },
        { modo: 'POWER', contagem: { falando: 9, livres: 1, pausa: 0, outros: 0, logados: 10 }, faixa: { sobe: 40, desce: 80, passo: 10 }, corte: null }, 'o histórico guarda o detalhe do ajuste');
});

test('decidir: acima do máximo da regra desce direto para ele, sem esperar leitura nem olhar a ocupação', () => {
    const base = { regra: { ...REGRA, maximo: 30 }, agressividade: 50, emExecucao: true, pausado: null };
    assert.deepEqual(decidir({ ...base, contagem: contar(ops(6, 4)) }), { tipo: 'acimaDoMaximo', dir: -1, alvo: 30 }, 'mesmo dentro da faixa');
    assert.deepEqual(decidir({ ...base, contagem: contar(ops(1, 9)) }), { tipo: 'acimaDoMaximo', dir: -1, alvo: 30 }, 'mesmo ocioso');
    assert.equal(decidir({ ...base, agressividade: 30, contagem: contar(ops(1, 9)) }).tipo, 'limite', 'no máximo novo, segue a regra normal');
    assert.equal(decidir({ ...base, pausado: { de: 30, para: 50 }, contagem: contar(ops(6, 4)) }).tipo, 'pausado', 'pausado não mexe');
    assert.equal(decidir({ ...base, emExecucao: false, contagem: contar([]) }).tipo, 'parado');
});

test('motor: baixar o máximo da regra desce o serviço para o novo máximo já na próxima leitura', async () => {
    const { fastway, ler, servico, motor, cfg } = montar();
    fastway.logados[52] = estados(6, 4); // 60%: dentro da faixa, a ocupação sozinha não mexeria
    cfg.regras[52] = { ...REGRA, maximo: 30 };
    await ler();
    assert.deepEqual(fastway.ajustes, [[52, 30]], 'uma leitura só basta');
    assert.deepEqual(servico(52).decisao, { tipo: 'ajustou', dir: -1, de: 50, alvo: 30, motivo: 'maximo' });
    const evento = motor.painel().eventos.find(e => e.tipo === 'ajuste');
    assert.equal(evento.texto, 'Desceu de 50 para 30 (novo máximo da regra).');
    assert.equal(evento.motivo, 'maximo');
    await ler();
    assert.equal(servico(52).pausado, null, 'o próprio ajuste não conta como mudança manual');
    assert.deepEqual(fastway.ajustes, [[52, 30]]);
});

test('motor: acima do máximo com a automação desligada só mostra, não ajusta', async () => {
    const { fastway, ler, servico, cfg } = montar({ ativa: false });
    cfg.regras[52] = { ...REGRA, maximo: 30 };
    await ler(); await ler();
    assert.deepEqual(fastway.ajustes, []);
    assert.equal(servico(52).decisao.tipo, 'acimaDoMaximo');
});

test('motor: com a automação desligada só mostra a decisão, sem acumular leituras nem ajustar', async () => {
    const { fastway, ler, servico } = montar({ ativa: false });
    await ler(); await ler(); await ler();
    assert.deepEqual(fastway.ajustes, []);
    assert.equal(servico(52).decisao.tipo, 'pendente');
});

test('motor: mudança feita direto na Fastway pausa o serviço até retomar', async () => {
    const { fastway, ler, servico, motor, cfg } = montar();
    fastway.logados[52] = estados(6, 4); // dentro da faixa
    await ler();
    fastway.servicos[0].agressividade = 80;
    await ler();
    assert.deepEqual(cfg.pausados[52], { de: 50, para: 80, em: cfg.pausados[52].em });
    assert.equal(servico(52).decisao.tipo, 'pausado');
    assert.equal(motor.painel().eventos[0].tipo, 'manual');
    fastway.logados[52] = estados(9, 1);
    await ler(); await ler();
    assert.deepEqual(fastway.ajustes, [], 'pausado não ajusta');
    motor.retomar(52);
    await ler(); await ler();
    assert.deepEqual(fastway.ajustes, [[52, 70]], 'retoma a partir do valor da Fastway');
    assert.equal(cfg.pausados[52], undefined);
});

test('motor: falha ao ajustar não é repetida e a decisão recomeça', async () => {
    const { fastway, ler, servico, motor } = montar();
    fastway.erroAjuste = new FastwayErro('sem permissão');
    await ler(); await ler();
    assert.deepEqual(fastway.ajustes, [[52, 40]]);
    assert.equal(servico(52).decisao.tipo, 'falhou');
    assert.equal(servico(52).decisao.mensagem, 'sem permissão');
    assert.match(motor.painel().eventos[0].texto, /recusou o ajuste de 50 para 40/);
    await ler();
    assert.deepEqual(fastway.ajustes, [[52, 40]], 'a leitura seguinte só marca pendência de novo');
    assert.equal(servico(52).decisao.tipo, 'pendente');
});

test('motor: ajuste que a Fastway demora a mostrar não vira falha nem mudança manual', async () => {
    const { fastway, ler, servico, motor, cfg } = montar();
    fastway.aplica = false;
    await ler(); await ler(); // ajusta 50 → 40, mas a Fastway ainda mostra 50
    await ler();
    assert.deepEqual(servico(52).decisao, { tipo: 'confirmando', de: 50, alvo: 40 });
    assert.equal(servico(52).agressividade, 40, 'mostra o valor ajustado');
    await ler();
    assert.deepEqual(fastway.ajustes, [[52, 40]], 'esperando não ajusta de novo');
    fastway.servicos[0].agressividade = 40; // agora a Fastway reflete
    await ler();
    assert.equal(cfg.pausados[52], undefined, 'o atraso não conta como mudança manual');
    assert.equal(servico(52).decisao.tipo, 'pendente');
    assert.equal(motor.painel().eventos.filter(e => e.tipo !== 'ajuste').length, 0, 'nem falha nem manual');
});

test('motor: ajuste aceito que não aparece na Fastway depois do prazo vira aviso, não pausa', async () => {
    const { fastway, ler, servico, motor } = montar();
    fastway.aplica = false;
    await ler(); await ler(); // ajusta; cada leitura do teste anda 30 s
    for (let i = 0; i < 3; i++) await ler(); // 30, 60 e 90 s depois do ajuste
    assert.ok(!motor.painel().eventos.some(e => e.tipo === 'falha'), 'dentro de 2 min só espera');
    await ler();
    assert.equal(servico(52).pausado, null);
    assert.ok(motor.painel().eventos.some(e => e.tipo === 'falha' && /depois de 2 min a agressividade continua 50/.test(e.texto)));
    assert.deepEqual(fastway.ajustes, [[52, 40]]);
});

test('motor: URA esperando a Fastway conta no teto pelo valor ajustado', async () => {
    const { fastway, ler, motor } = montar({ teto: 100000 });
    fastway.servicos[0].modo = 'URA';
    fastway.aplica = false;
    await ler(); await ler();
    await ler();
    assert.equal(motor.painel().soma, 60, '40 ajustado (Fastway ainda mostra 50) + 20 do MP1');
});

test('motor: Fastway fora do ar marca desconectado e mantém a última leitura', async () => {
    const { fastway, ler, motor } = montar();
    await ler();
    fastway.listarServicos = async () => { throw new FastwayErro('Fastway sem resposta (HTTP 502)'); };
    await ler();
    assert.equal(motor.conectado, false);
    assert.match(motor.erro, /502/);
    assert.equal(motor.servicos.length, 2);
});

test('diário guarda a série do dia em arquivo e recomeça no dia seguinte', async () => {
    const { ler, diario, pasta } = montar();
    await ler(); await ler(); await ler();
    const salvo = JSON.parse(fs.readFileSync(path.join(pasta, 'dia.json'), 'utf8'));
    assert.equal(salvo.dia, '2026-11-06');
    assert.ok(salvo.series[52].length >= 2, 'mudança de agressividade sempre entra na série');
    let dia = new Date(2026, 10, 7, 8, 0);
    const amanha = new Diario(path.join(pasta, 'dia.json'), () => dia);
    assert.deepEqual(amanha.serie(52), []);
    assert.ok(diario.serie(52).length >= 2);
});

test('motor: o teto geral soma só as URAs em execução, com ou sem regra, e corta a subida', async () => {
    const { fastway, ler, servico, motor } = montar({ regras: { 18: REGRA }, teto: 25 }); // URA: só o MP1 (20); MP3 é call center e não conta
    fastway.logados[18] = estados(1, 9);
    await ler();
    assert.equal(motor.soma, 20);
    assert.deepEqual(servico(18).decisao, { tipo: 'pendente', dir: 1, alvo: 25 }, 'sobe só 5, o que cabe até 25');
    await ler();
    assert.deepEqual(fastway.ajustes, [[18, 25]]);
    assert.equal(motor.soma, 25);
    await ler();
    assert.deepEqual(servico(18).decisao, { tipo: 'teto', dir: 1, teto: 'ura' });
    await ler();
    assert.deepEqual(fastway.ajustes, [[18, 25]], 'no teto não sobe mais');
});

test('motor: call center ativo (POWER) vai no máximo até 99 por serviço, independente das URAs', async () => {
    const { fastway, ler, servico, motor } = montar({ regras: { 52: { ...REGRA, maximo: 99 } }, teto: 1 }); // teto das URAs lotado não trava o call center
    fastway.servicos[0].agressividade = 95;
    fastway.logados[52] = estados(1, 9);
    await ler(); await ler();
    assert.deepEqual(fastway.ajustes, [[52, 99]], 'o passo de 10 vira 4: para no 99');
    assert.equal(motor.painel().eventos.find(e => e.tipo === 'ajuste').corte, 'maximo', 'o histórico diz que o passo foi cortado');
    assert.equal(motor.soma, 20, 'o call center não entra na soma das URAs');
    fastway.servicos[0].agressividade = 99;
    await ler();
    assert.deepEqual(servico(52).decisao, { tipo: 'limite', dir: 1 });
    assert.equal(validarRegra({ ...REGRA, maximo: 100 }, 100, 'POWER').erro, 'Serviço de call center ativo vai no máximo até 99.');
    assert.equal(validarRegra({ ...REGRA, maximo: 100 }, 1600, 'URA').erro, undefined);
});
