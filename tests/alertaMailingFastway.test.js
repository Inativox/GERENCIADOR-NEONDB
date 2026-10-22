const test = require('node:test');
const assert = require('node:assert/strict');

const { createFastwayFmi, FastwayErro, FastwayAuthErro, mascarar } = require('../src/main/alertaMailing/fastwayFmi');

// Respostas sintéticas no formato da API FMI (sem dados reais da Fastway).
const ATIVOS = { Id: 10, Status: 'Sucesso', SERVICOS: [{ id: '18', nome: 'MP PRINCIPAL', mailing_ativo_id: '0' }, { id: '52', nome: 'MP CADENCIA', mailing_ativo_id: '32775' }, { id: '87', nome: 'PAG PRINCIPAL ', mailing_ativo_id: 32563 }] };
const MAILING = { Id: 10, Status: 'Sucesso', Mailing: [{ id: '32586', arquivo: 'MP-teste PT3.csv', status: 'ATIVO', registros: '43474', total_registros_disponiveis: '1',
    total_registros_aguardando_tentativa: '10', quantidade_reciclagem: '1', campanha_id: '18', data_importacao: '2026-09-29 09:11:40.99' }] };
const LISTAR = { Id: 10, Status: 'Sucesso', Mailings: [{ id: '31712', campanha_id: '18', status: 'ATIVO', mailing_parent_id: '31655', life_discados_data_ultimo: '2026-10-06 10:00' },
    { id: '32586', campanha_id: '18', status: 'ATIVO', mailing_parent_id: null, life_discados_data_ultimo: '' }] };

function fixture({ respostas = [], intervaloMinMs = 0, relogio } = {}) {
    const requisicoes = [], esperas = [];
    const http = {
        async get(url, opcoes) {
            requisicoes.push({ url, params: opcoes.params });
            const proxima = respostas.shift();
            if (proxima === 'derrubar') throw Object.assign(new Error(`connect ECONNREFUSED ${url}?api_senha=${opcoes.params.api_senha}`), { code: 'ECONNREFUSED' });
            const [status, corpo] = proxima;
            return { status, data: typeof corpo === 'string' ? corpo : JSON.stringify(corpo) };
        },
    };
    const cliente = createFastwayFmi({ baseUrl: 'https://x/api/fmi/', logon: 'enzo', senha: 'segredo@1', http, intervaloMinMs, sleep: async ms => { esperas.push(ms); }, ...(relogio ? { relogio } : {}) });
    return { cliente, requisicoes, esperas };
}

test('lista os serviços ativos enviando o login e o comando', async () => {
    const f = fixture({ respostas: [[200, ATIVOS]] });
    const servicos = await f.cliente.listarServicosAtivos();
    assert.deepEqual(servicos.map(s => [s.id, s.mailingAtivoId]), [[18, 0], [52, 32775], [87, 32563]]);
    assert.equal(servicos[2].nome, 'PAG PRINCIPAL');
    assert.equal(f.requisicoes[0].url, 'https://x/api/fmi/servico/index.php');
    assert.deepEqual(f.requisicoes[0].params, { api_logon: 'enzo', api_senha: 'segredo@1', api_comando: 'ListarAtivos' });
});

test('lê o mailing com disponíveis, aguardando, reciclagens e data de importação', async () => {
    const f = fixture({ respostas: [[200, MAILING]] });
    const m = await f.cliente.obterMailing(32586);
    assert.deepEqual([m.id, m.registros, m.disponiveis, m.aguardando, m.quantidadeReciclagem, m.campanhaId], [32586, 43474, 1, 10, 1, 18]);
    assert.equal(m.arquivo, 'MP-teste PT3.csv');
    assert.ok(m.dataImportacao.startsWith('2026-09-29'));
});

test('lista os mailings com a mãe de cada filho', async () => {
    const f = fixture({ respostas: [[200, LISTAR]] });
    const filhos = Object.fromEntries((await f.cliente.listarMailings()).map(m => [m.id, m.mailingParentId]));
    assert.deepEqual(filhos, { 31712: 31655, 32586: null });
});

test('repete quando a conexão cai ou dá 5xx e desiste depois de três novas tentativas', async () => {
    const volta = fixture({ respostas: ['derrubar', [500, {}], [200, ATIVOS]] });
    assert.equal((await volta.cliente.listarServicosAtivos()).length, 3);
    assert.deepEqual(volta.esperas, [15000, 30000]);
    const desiste = fixture({ respostas: ['derrubar', 'derrubar', 'derrubar', 'derrubar'] });
    await assert.rejects(desiste.cliente.listarServicosAtivos(), error => error instanceof FastwayErro && /sem resposta/.test(error.message));
    assert.deepEqual(desiste.esperas, [15000, 30000, 45000]);
});

test('401 não repete e pede para conferir o login salvo', async () => {
    const f = fixture({ respostas: [[401, { status: '00', mensagem: 'Usuario nao encontrado!' }]] });
    await assert.rejects(f.cliente.listarServicosAtivos(), error => error instanceof FastwayAuthErro && /login Fastway/.test(error.message));
    assert.equal(f.requisicoes.length, 1);
    assert.deepEqual(f.esperas, []);
});

test('mensagem de erro da Fastway chega como erro, sem repetir', async () => {
    const f = fixture({ respostas: [[200, { Status: '14', Mensagem: 'Mailing informado nao existe' }]] });
    await assert.rejects(f.cliente.obterMailing(1), /nao existe/);
    assert.equal(f.requisicoes.length, 1);
});

test('espaça as chamadas pelo intervalo mínimo', async () => {
    const tempo = [100000];
    const f = fixture({ respostas: [[200, ATIVOS], [200, ATIVOS]], intervaloMinMs: 5000, relogio: () => tempo[0] });
    await f.cliente.listarServicosAtivos();
    tempo[0] += 2000;
    await f.cliente.listarServicosAtivos();
    assert.deepEqual(f.esperas, [3000]);
});

test('erro de conexão não vaza a senha', async () => {
    const f = fixture({ respostas: ['derrubar', 'derrubar', 'derrubar', 'derrubar'] });
    await assert.rejects(f.cliente.listarServicosAtivos(), error => !error.message.includes('segredo'));
    assert.equal(mascarar('x?api_logon=a&api_senha=Enzo%402026&y=1'), 'x?api_logon=a&api_senha=***&y=1');
});

test('reciclar envia só NÃO TABULADOS e o período, e nunca repete', async () => {
    const f = fixture({ respostas: ['derrubar'] });
    await assert.rejects(f.cliente.reciclar(32737, '2026-10-06', '2026-10-07'), FastwayErro);
    assert.equal(f.requisicoes.length, 1);
    assert.deepEqual(f.esperas, []);
    const { params } = f.requisicoes[0];
    assert.deepEqual([params.api_comando, params.id, params.periodo, params.reciclarCheckId], ['reciclarMailing', 32737, '06/10/2026 - 07/10/2026', '[{"tabulacao":[-99]}]']);
});

test('tabulações disponíveis aceitam id em texto ou número', async () => {
    const f = fixture({ respostas: [[200, { Id: 10, Status: 'Sucesso', Resultado: [{ id: '-99', nome: 'NÃO TABULADOS', quantidade: 216244 }, { id: 42, nome: 'SEM INTERESSE [42]', quantidade: 5 }, { id: 'x', quantidade: 1 }] }]] });
    assert.deepEqual(await f.cliente.tabulacoesDisponiveis(32563, '2026-09-28', '2026-10-07'), { '-99': 216244, 42: 5 });
    assert.deepEqual([f.requisicoes[0].params.api_comando, f.requisicoes[0].params.periodo], ['tabulacoesDisponiveis', '28/09/2026 - 07/10/2026']);
});

test('parar e iniciar serviço nunca repetem', async () => {
    for (const [metodo, comando] of [['pararServico', 'PararServico'], ['iniciarServico', 'IniciarServico']]) {
        const f = fixture({ respostas: ['derrubar'] });
        await assert.rejects(f.cliente[metodo](92), FastwayErro);
        assert.equal(f.requisicoes.length, 1);
        assert.deepEqual(f.esperas, []);
        assert.ok(f.requisicoes[0].url.endsWith('/servico/index.php'));
        assert.deepEqual([f.requisicoes[0].params.api_comando, f.requisicoes[0].params.servico_id], [comando, 92]);
    }
});
