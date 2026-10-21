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

test('importação de mailing chama os canais do main e envia o arquivo e o serviço escolhidos', async () => {
    const ipcRenderer = new EventEmitter(), invoked = [];
    ipcRenderer.invoke = async (...args) => { invoked.push(args); return { success: true }; };
    let api;
    loadModule('preload.js', { electron: { ipcRenderer, contextBridge: { exposeInMainWorld: (_, value) => { api = value; } } } });
    await api.mailingImportState();
    await api.mailingImportFile();
    await api.mailingImportSend('arquivo-1', 63);
    assert.deepEqual(JSON.parse(JSON.stringify(invoked)), [['mailing-import-state'], ['mailing-import-file'], ['mailing-import-send', { fileId: 'arquivo-1', serviceId: 63 }]]);
});
