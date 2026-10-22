const test = require('node:test');
const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const loadModule = require('./helpers/loadModule');

test('modal de fechamento recebe o pedido do main e devolve só uma resposta booleana', () => {
    const ipcRenderer = new EventEmitter(), sent = [];
    ipcRenderer.send = (...args) => sent.push(args);
    let api;
    loadModule('preload.js', { electron: { ipcRenderer, contextBridge: { exposeInMainWorld: (_, value) => { api = value; } } } });
    const requests = [];
    const unsubscribe = api.onConfirmCloseDuringFlow(request => requests.push(request));
    ipcRenderer.emit('confirm-close-during-flow', {}, { uploading: true });
    assert.deepEqual(requests, [{ uploading: true }]);
    api.answerCloseDuringFlow(true);
    api.answerCloseDuringFlow('sim');
    assert.deepEqual(sent, [['close-during-flow-answer', true], ['close-during-flow-answer', false]]);
    unsubscribe();
    assert.equal(ipcRenderer.listenerCount('confirm-close-during-flow'), 0);
});

test('assinatura de eventos de colunas pode ser removida sem afetar outro consumidor', () => {
    const ipcRenderer = new EventEmitter();
    let api;
    loadModule('preload.js', { electron: { ipcRenderer, contextBridge: { exposeInMainWorld: (_, value) => { api = value; } } } });
    let first = 0;
    let second = 0;
    const unsubscribe = api.onLimpezaColunasLog(() => { first++; });
    api.onLimpezaColunasLog(() => { second++; });
    ipcRenderer.emit('limpeza-colunas-log', {}, 'mensagem');
    assert.equal(first, 1);
    unsubscribe();
    ipcRenderer.emit('limpeza-colunas-log', {}, 'outra');
    assert.equal(first, 1);
    assert.equal(second, 2);
    assert.equal(ipcRenderer.listenerCount('limpeza-colunas-log'), 1);
});

test('importação de mailing chama os canais do main e envia o arquivo, o serviço e o login digitado', async () => {
    const ipcRenderer = new EventEmitter(), invoked = [];
    ipcRenderer.invoke = async (...args) => { invoked.push(args); return { success: true }; };
    let api;
    loadModule('preload.js', { electron: { ipcRenderer, contextBridge: { exposeInMainWorld: (_, value) => { api = value; } } } });
    const fastway = { login: 'ana.fastway', password: 'synthetic', save: true };
    await api.mailingImportState();
    await api.mailingImportFile();
    await api.mailingImportSend('arquivo-1', 63);
    await api.mailingImportSend('arquivo-2', 87, fastway);
    await api.mailingImportForget();
    assert.deepEqual(JSON.parse(JSON.stringify(invoked)), [['mailing-import-state'], ['mailing-import-file'], ['mailing-import-send', { fileId: 'arquivo-1', serviceId: 63 }],
        ['mailing-import-send', { fileId: 'arquivo-2', serviceId: 87, fastway }], ['mailing-import-forget']]);
});

test('consulta de lista da Receita envia a lista, as situações e as chaves de MEI e CSV', async () => {
    const ipcRenderer = new EventEmitter(), invoked = [];
    ipcRenderer.invoke = async (...args) => { invoked.push(args); return { success: true }; };
    let api;
    loadModule('preload.js', { electron: { ipcRenderer, contextBridge: { exposeInMainWorld: (_, value) => { api = value; } } } });
    await api.receitaSituacaoStart('lista-1', null);
    await api.receitaSituacaoStart('lista-2', ['ativa'], { removeMei: true, csv: true });
    assert.deepEqual(JSON.parse(JSON.stringify(invoked)), [['receita-situacao-start', { fileId: 'lista-1', keep: null }],
        ['receita-situacao-start', { fileId: 'lista-2', keep: ['ativa'], removeMei: true, csv: true }]]);
});

test('alerta de mailing chama os canais do main e repassa atualizações e foco', async () => {
    const ipcRenderer = new EventEmitter(), invoked = [];
    ipcRenderer.invoke = async (...args) => { invoked.push(args); return { success: true }; };
    let api;
    loadModule('preload.js', { electron: { ipcRenderer, contextBridge: { exposeInMainWorld: (_, value) => { api = value; } } } });
    await api.alertaMailingState();
    await api.alertaMailingVerificar();
    await api.alertaMailingDispensar('ev1');
    await api.alertaMailingLimparHistorico();
    await api.alertaMailingReciclador(true);
    await api.alertaMailingServico(92, true);
    await api.alertaMailingLogin('enzo', 'segredo');
    assert.deepEqual(JSON.parse(JSON.stringify(invoked)), [['alerta-mailing-state'], ['alerta-mailing-verificar'], ['alerta-mailing-dispensar', { eventoId: 'ev1' }],
        ['alerta-mailing-limpar-historico'], ['alerta-mailing-reciclador', { ligar: true }], ['alerta-mailing-servico', { servicoId: 92, parar: true }],
        ['alerta-mailing-login', { login: 'enzo', password: 'segredo' }]]);
    const recebidos = [];
    const sairUpdate = api.onAlertaMailingUpdate(painel => recebidos.push(['update', painel.linhas.length]));
    const sairFoco = api.onAlertaMailingFoco(foco => recebidos.push(['foco', foco.servicoId]));
    ipcRenderer.emit('alerta-mailing-update', {}, { linhas: [1, 2] });
    ipcRenderer.emit('alerta-mailing-foco', {}, { servicoId: 92 });
    sairUpdate(); sairFoco();
    ipcRenderer.emit('alerta-mailing-update', {}, { linhas: [] });
    assert.deepEqual(recebidos, [['update', 2], ['foco', 92]]);
    assert.equal(ipcRenderer.listenerCount('alerta-mailing-update'), 0);
});
